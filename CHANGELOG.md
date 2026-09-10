# Changelog

## 2026-09-10 — Pipeline reply coalescing; a command behind a blocking reserve is answered

### The command the server ate and never answered (upstream #647)

A command pipelined behind a blocking `reserve` was never replied to.
The dispatch loop stops at the reserve, so the rest of the burst stays
in the connection's command buffer — and nothing will ever announce it
again: the bytes are already off the socket, and the client is blocked
reading, so it sends nothing more. When the reserve was finally
answered (by a timeout, or by another connection's `put`), the buffered
command simply stayed there. The client waited forever for a reply the
server owed it.

Connections unblocked this way now go on a run queue that the main loop
drains after the event drain, in the same tick. Invariant #10 covers
both halves of the property now: a conn that can accept a command, and
has one, makes progress without needing the client to speak first.

Reproduced from the wire before the fix and pinned by
`cttest_command_pipelined_behind_blocking_reserve_is_answered` (a put
from another conn unblocks the reserve) and
`..._behind_a_timing_out_reserve_...` (nothing arrives at all —
prottick answers it).

### Pipelined replies leave in one write

A pipelined burst used to cost one `write` per command, wrapped in a
`TCP_CORK` pair: the cork only stopped the kernel from putting each
reply on the wire as its own segment, it did not save a single syscall.

The burst's replies are now staged in the per-conn buffer that durable
group commit already used and pushed with ONE `write` when the loop
ends; the cork is gone. A job reply small enough to fit joins the same
buffer — header and body are just more bytes — so a worker's pipelined
`reserve` run needs no `writev` of its own either. Bodies too large for
the buffer still take the `writev` road, with the staged acks merged in
front of the header so nothing overtakes anything.

The buffer now has two independent holds: durability ("not before
`fdatasync`", invariant #16) and pipelining ("not before the burst
ends"). Whichever is released last does the writing, so a `-D` burst
still obeys #16 — the syscall saving never outranks the ack contract.

`setsockopt` fell from 1318 calls to 2 (both at accept) and `writev`
left the hot path entirely. Depth 1 is unchanged by design: a lone
command still replies straight to the socket, because staging it would
cost a memcpy and save nothing. The numbers are in the table further
down, together with the input-buffer change they belong with.

### The input buffer was the next wall

With the replies coalesced, the remaining syscalls were one `read` per
~10 commands. The reason was the command buffer: it was sized at
exactly one command line (224 bytes), so a pipelined burst had to be
collected in several reads no matter how few segments it arrived in.

The input buffer is now `CMD_BUF_SIZE` (1024) — measured, not chosen:
512 leaves a third of the gain on the table, 2048 and 4096 add nothing,
the curve is flat past 1024. `struct Conn` grows from 5232 to 6032
bytes for it.

Sizing the buffer at one line was also, silently, how the protocol's
line-length limit was enforced: a longer line simply could not produce
an EOL inside the buffer and fell into the discard path. A buffer that
holds a whole burst can receive such a line COMPLETE, and the old code
would then have parsed it as a command. The rule is now stated where it
belongs — `dispatch_cmd` rejects `cmd_len > LINE_BUF_SIZE` with
`BAD_FORMAT`, the same answer the discard path gives — and the two
tests around it (an over-long line pipelined between two valid
commands; the longest LEGAL line, exactly LINE_BUF_SIZE bytes, still
accepted) both kill the mutant that removes the check.

### What the two changes are actually worth

Both columns below are for the `put`/`reserve`/`delete` loop, no WAL,
against the code before either change — a build with `TCP_CORK` and the
one-line input buffer put back, so the comparison is like for like.

Counted with `bench/syscalls/count.sh`:

| shape | syscalls/cmd |
|---|---|
| 1 conn, depth 1  | 3.00 -> 3.00 |
| 1 conn, depth 8  | 1.50 -> 0.42 |
| 1 conn, depth 32 | 1.34 -> 0.11 |
| 1 conn, depth 32, async WAL | 1.86 -> 0.78 |

Throughput, measured SEPARATELY and with no strace attached, three runs
of 60000 commands each (spread under 1%):

| shape | before | after |
|---|---|---|
| 1 conn, depth 1  | 34986 cmd/s | 35108 cmd/s |
| 1 conn, depth 8  | 261736 cmd/s | 274632 cmd/s (+4.9%) |
| 1 conn, depth 32 | 795762 cmd/s | 1052561 cmd/s (+32%) |
| 4 conns, depth 16 | 1142805 cmd/s | 2607079 cmd/s (+128%) |

The two measurements have to be taken separately, and that is the point
worth writing down: `count.sh` runs the server under strace, which is
right for COUNTING syscalls and useless for timing them. Under strace a
syscall costs roughly two orders of magnitude more than it does in
production, so removing syscalls looks far more valuable there than it
is — the same A/B read +642% at depth 32 under strace and +32% without
it. Take throughput claims from a run with no tracer attached.

Depth 8 barely moves with the bigger buffer: eight short commands
already fit the old 224-byte one. Depth 1 does not move at all, by
design. The multi-connection row gains most because there the client's
own work is spread over four processes, so more of what is left is the
server.

### Contract that was never written down

`delete` inside the DEADLINE_SOON window turned out to be untested
(upstream #609). The margin exists to let a client `delete` or
`release` its reserved job, and both now have a test saying so. Writing
it exposed protocol.txt describing reserve's answer in that window as
unconditionally DEADLINE_SOON, when the server hands over a ready job
if it has one — the whole point of the margin is that the client is
never left waiting. The text now says what the server does.

protocol.txt also states the pipelining contract outright (upstream
#617 asks for it): what "serially, in order" costs — a command behind a
blocking reserve waits, so work that must not wait belongs on a second
connection — and what it guarantees, which is that every command whose
bytes the server accepted is answered, exactly once and in place.

### Mutation, not just a green run

Every new path was mutated. Removing the `runq_run()` call reddened
exactly the two deferred-dispatch tests and nothing else. Four mutants
survived at first, and each one paid for the trip:

- dropping the staged acks on the oversized-job path survived because
  the first version of that test opened its burst with a `put`, whose
  body ends the dispatch loop before anything is staged — it never
  reached the code it was written for. Rewritten to open with a
  `delete`, it kills the mutant on the exact 11 bytes lost.
- releasing the burst regardless of the durability hold survived the
  SIGKILL bench, which cannot see it: killing a process does not lose
  the page cache, so records acked one tick too early still come back.
  `cttest_dur_pipelined_acks_never_outrun_the_commit` fails the commit
  instead and requires INTERNAL_ERROR; that kills it. The bench now
  also runs at depth 32 — it proves less than the unit test, but it
  proves it against the real binary.
- inverting the EAGAIN test in the flush path left the suite green:
  nothing covered a write error the socket will never recover from.
  Without that branch a dead conn is carried in the batch forever,
  retried every tick, and the main loop shortens its epoll park for as
  long as it is there. Now pinned with an injected EPIPE.
- the guard against registering a conn in the durable batch twice had
  grown a redundant twin. Neither copy failed alone — each covered for
  the other, which is how a mutation run reports a real guard as
  untested. One is gone; the survivor is pinned on the carried-conn
  path as well as the plain one.

Two of the seven survivors in that run were not survivors at all: the
mutator was rewriting `>` inside `printf(">%d ...")`, changing a
message and nothing else. It skips string and character literals now,
the same way it already skipped comments. It also gained
`MUTATE_ASAN=1`: a mutated loop bound (`k < len` -> `k <= len`) reads
one element past an array, which `-O1` usually gets away with and ASan
never does — without the detector such a mutant reads as a missing
test when what is missing is the detector.

A second run with both improvements scored 62% against the first run's
42%, no false survivors, and named two more real gaps:

- `which_cmd`'s length gates were untested at the boundary. They are
  what separates "known verb, missing argument" from "not a command at
  all", and both answers are contract: `stats-tube ` with no name is
  BAD_FORMAT, `stats-tube` without the separator is UNKNOWN_COMMAND
  (invariant #13). Off by one and the first starts answering the
  second, telling a client the command does not exist when it merely
  forgot the tube.
- the same inverted-EAGAIN mutant as before, on the ORDINARY reply path
  this time rather than the durable flush. Also green, also untested;
  now pinned with an injected EPIPE, which took some care because the
  fault table is inherited across the fork and the child's first write
  is the ready byte that tells the parent it came up.

### A drain that could park a conn mid-command

Found by re-reading the diff, not by a test, and it predates this work.
When a 257th WAL-dirty conn arrives in one tick, `dur_enqueue` drains
the batch inline — from inside `dispatch_cmd`, with a command still
being answered. If that drain's socket write came up short on the conn
being dispatched, the old code handed its reply buffer to the SEND_WORD
retry FSM. `STATE_WANT_COMMAND` looks like "between commands" there,
but it only means "between replies": the answer still being produced
would then be memcpy'd straight over the bytes the FSM had not sent
yet. The conn being dispatched now carries its remainder instead, so
that answer lands behind it.

The same reading turned up a `dur_reply_len = 0` in `dur_enqueue`
labelled "fresh batch". Every path that gives the buffer up already
zeroes it, so the only thing that reset could ever do was drop replies
the client was owed — a carried remainder, or acks staged earlier in
the same burst. It is gone, and the reason is written where it stood.

Both are pinned in `testinject2.c` against a full batch; the second
test kills its mutant with a SIGSEGV, which is roughly what the real
failure would look like. Mutation also showed the guard against
double-registration had grown a redundant twin — neither copy failed
alone, each covering for the other — so one of them is gone too, and
the survivor is now pinned on the carried-conn path as well.

### The WAL stays bounded with jobs pinned in the oldest file

Upstream reports binlogs growing without limit while the queue holds a
couple of hundred jobs (#599, #622). The shape behind it is a few
long-lived buried or delayed jobs sitting in the oldest binlog:
compaction cannot drop that file, it has to migrate them forward.
`bench/binlog/run.sh` runs 3000 put/reserve/delete cycles past four
such pinned jobs with `-s 65536`, so rotation happens every few hundred
records. The directory holds one or two binlogs throughout — the fork's
migration path does its job, and there is now a bench that says so if
it ever stops.

### A reservation handed over mid-burst could never time out

Introduced by the coalescing work and caught by re-reading the diff
against the path it replaced. The fast path that puts a job reply
straight into the burst buffer bypasses `conn_want_command`, and
`conn_want_command` is where `epollq_add` — and through it `connsched`
— gets called. The new path re-armed epoll only when the interest was
not already read, which mid-burst it always is. So `connsched` never
ran, and the TTR deadline the reserve had just created never reached
the connection's tick heap. An idle connection is not in that heap at
all, so this did not make the timeout late: it removed it. The job
stayed reserved for as long as the client kept the connection open, and
no other worker could have it.

`epollq_add` is unconditional there now, exactly as in the path it
replaced. `cttest_job_reserved_inside_a_burst_still_times_out` reserves
inside a burst and then has a SECOND worker ask for the same job with
`reserve-with-timeout`: nothing but the TTR lapsing can hand it over,
and the worker's own timeout turns "never" into a failed assertion
(`expected RESERVED, got TIMED_OUT`) rather than a hung read. The first
connection is never touched again — an event on it would reschedule the
conn and hide the bug.

Its first version polled `stats-tube` from a probe connection instead,
and that version was the fragile one: two five-second read timeouts
inside a 120-iteration loop, with no wall-clock bound, plus a reused
connection that desynced because `readstats` stops at the announced
body length and leaves the trailing CRLF unread. It failed a TSan run
for reasons entirely its own. A test that can fail for its own reasons
is worse than no test — it teaches you to ignore it.

### Three tests that were green and doing nothing

Coverage, run over the suite for the first time, put `prot.c` at 82.2%
and named the biggest untouched block: the overflow path in the reply
buffer, where more than 4KB of staged replies are pushed out at once.
A test written for it turned out to be unreachable as written — one
dispatch loop can only consume what one `read()` brought in, so
CMD_BUF_SIZE bounds a burst at about fifty short commands, roughly 700
bytes of "INSERTED n", nowhere near the cap. The overflow needs replies
much larger than the commands that produce them: `list-tube-used` is 16
bytes in and, on a tube with the longest legal name, 208 bytes out.

Chasing the fault injection into that path turned up something worse.
The injector wraps `write()`, and the forked test server's readiness
handshake — a byte up a pipe, added earlier to replace a sleep — is the
server's FIRST wrapped write. It had been silently
eating every fault armed at skip 0 ever since. Three tests that arm
"the first reply lands short" had stopped exercising a short reply at
all: `reply()`'s partial-write branch shows zero executions across the
whole suite. They passed, every run, proving nothing.

The handshake now uses a socketpair and `send()`, which the injector
does not wrap, so skip counts mean what their comments say and cannot
be shifted again by an unrelated change to the harness. The three tests
came back to life with no edit of their own — that branch now runs
twice per suite — and `prot.c` went to 84.2% from tests that already
existed.

Worth stating as a rule: a test that stops biting does not go red, it
goes quiet. Mutation catches that when it is aimed at the right line;
coverage catches it wholesale. Neither had been run against this
harness change.

### A leak bench, and the reason it refuses to answer here

`make check` under ASan runs with `detect_leaks=0`: ct forks one process
per test and never unwinds them, so every allocation the product forgets
goes unnoticed. Upstream carries two open issues about exactly that
(#642, #382) and this fork had no answer to them either.

`bench/leak/run.sh` drives the REAL binary through every command the
protocol has — the whole job life cycle, the stats bodies, the refusal
paths including the oversized put that walks the bit-bucket, a pipelined
burst, 150 short-lived connections, a worker still parked on reserve —
then sends SIGTERM and lets LeakSanitizer report on the way out.

Its first version reported PASS. It also reported PASS with a
deliberate `malloc` compiled into the accept path, which is how it came
to have a self-check: LeakSanitizer is not available everywhere ASan is,
and where it is missing it is missing silently — a probe that leaks 1234
bytes exits 0 and says nothing. That is the case on aarch64, and on
x86-64 under emulation, which is every configuration available on the
machine this was written on. So the bench now compiles and runs that
probe first and exits 2 with an explanation rather than a green tick it
cannot back. The workload is right and the plumbing is exercised; the
verdict has to come from a native x86-64 host, or from valgrind.

A bench that cannot detect the thing it is named after is worse than no
bench: it manufactures confidence. Better to say so out loud.

### Two ways the test harness could hang or corrupt itself

Repeating the TSan run rather than accepting one green pass turned up
both. Neither is in the shipped server; both were in the scaffolding
that every other result rests on.

TSan flagged `sync_thread_fn` unlocking an invalid mutex — once in two
runs, which is what a scheduling-dependent bug looks like. Four tests
started the fsync worker and returned without `walsyncstop`, leaving it
parked in `pthread_cond_wait` on a mutex that lived in the returning
frame. The worker then waits on memory its caller is free to reuse. A
genuine use-after-scope, in the one place the project has a second
thread; all four now join.

Then a run wedged: 62 tests in, no output, no progress for 45 minutes.
The forked test server called `exit()` straight from its SIGTERM
handler. That is not async-signal-safe — `exit()` runs atexit handlers
and flushes stdio, and under a sanitizer it enters the runtime's own
teardown, while the interrupted thread may hold exactly those locks.
It deadlocked, and the parent's unbounded `waitpid` then waited
forever. Both test servers now do what the real one does: set the flag,
wake the loop through the async-signal-safe eventfd, return from
`srvserve` and exit from ordinary context. (The old comment defended
`exit()` as needed for the gcov flush — gcov needs `exit()`, not
`exit()` from a handler.)

The unbounded `waitpid` was the second half of the damage: it turned a
stuck server into a suite that hangs with no output and no failing test
name, which is the least useful way to learn about anything. It now
waits ten seconds, kills, and says which pid it had to kill — and that
is how the next problem announced itself as a failing test instead of a
wedged run.

Because there was a next problem: with the handler fixed,
`cttest_binlog_empty_exit` still failed about one TSan run in ten — the
forked server not gone ten seconds after SIGTERM, and still with no TSan
report of any kind.

The second guess was the plain `exit(1)` after `srvserve` returns: this
process is forked out of a ThreadSanitizer-instrumented parent, TSan's
runtime has threads of its own, and the child inherits their locks
without the threads that would ever release them, so exit()'s teardown
can block on one forever. The forked servers were switched to `_exit`,
with coverage — the one thing that genuinely wanted `exit()`, issue
#443 — flushed by hand first through a weakly-declared `__gcov_dump`.

It was not the cause: the failure rate did not move, 1 in 12 against 1
in 10. And it cost something real, which only a coverage run showed.
`prot.c` fell from 82.2% to 39.9%, because a weak REFERENCE does not
pull the defining member out of libgcov — the pointer stayed null and
the dump was skipped in silence, taking every forked server's profile
with it, which is most of what exercises prot.c. So `exit()` is back.
The hazard it guards against is real in principle; a speculative fix
that misses its target and halves a measurement is not a fix, and the
comment there now says so.

Two wrong guesses were enough, so the harness was made to answer the
question instead: `kill_srvpid` now reads the process state and `wchan`
out of /proc before it kills, and puts both in the failure message.
Fourteen runs reproduced it three times, and all three said the same
thing — state `S`, wchan `do_epoll_wait`. The server was not ignoring
SIGTERM. It had never received it.

The third guess followed from that: ThreadSanitizer defers a signal
arriving while the thread is inside an intercepted blocking call and
runs the handler at the next safe point, and for an idle server that
point is whenever its epoll park ends — up to sixty seconds, since the
malloc_trim cadence is what caps the park. So the harness now connects
to the server a couple of times while it waits, to end the park.

That one turned out to be mostly right, and calling it wrong was itself
a mistake made on too little data. Fourteen runs with the connections in
place showed 1 failure against 3 in 14 without, which looked like no
change; fifty runs show 2. Ending the epoll park takes the failure rate
from about one run in five to about one in twenty-five (Fisher's exact
on 3/14 against 2/50 gives p ≈ 0.03) — so the deferred-signal account is
supported, and ending the park is most of the window but not all of it.
The last twenty runs had no SIGTERM failure at all; the one failure in
them was the drain test, where SIGUSR1 arrives between a put's header
and its body and the same delay lets the body land before drain mode is
on. Two signals, one mechanism. Three guesses,
three refutations, and each one only cost what it did because the
symptom is cheap to reproduce and the diagnosis was made to come from
the process rather than from a guess. The harness now also reports SigBlk /
SigCgt / SigPnd / ShdPnd on the timeout path, which separate the three
remaining possibilities — SIGTERM blocked by a mask, no handler
installed yet, or delivery pending — that a wchan alone cannot tell
apart. This entry will say which when the next failure names it.

The connection nudge stays: it costs nothing on the normal path (the
server is usually gone before the first poll) and it cannot weaken a
check, since a server that truly ignored SIGTERM would still never
exit.

The mask numbers, when a failure finally carried them, ruled out three
explanations at once: `SigBlk` 0 (not masked), `SigCgt` bit 14 set (a
handler IS installed), `SigPnd` and `ShdPnd` both 0 (nothing waiting to
be delivered). The kernel had delivered the signal, and the loop was
still asleep with its flag unset — which leaves only one thing:
delivered to the process is not the same as "the handler ran", and in an
instrumented build the instrumentation gets the signal first. That is
the same account the rate data supports, arrived at from the other
side.

So the handler now leaves proof. It writes one byte to a pipe the parent
holds, before touching anything else, and the timeout message says
`handler RAN` or `handler NEVER ran`. Those two strings separate the two
remaining stories completely: a handler that ran means the wake-up
mechanism failed and the fault is this project's, a handler that never
ran means the sanitizer swallowed the signal and the product is not
involved. It is the difference between a bug and an artifact, and it
should not have to be inferred.

Worth stating plainly, since three rounds of comments claimed otherwise
before the data arrived: none of this was ever the shipped server. A
TSan-built beanstalkd, driven through the same sequence by hand, answers
its TTR in 1.06s at 0% CPU and is gone before the probe can time the
SIGTERM; the ordinary binary, measured properly by
`bench/shutdown/run.sh` while parked in epoll, takes 4ms. And every run
of the suite, failing ones included, reports zero races.

### The A/B against upstream, re-measured

The README's comparison table dated from April and predated group commit,
the job pool, the tube hash and everything above. Re-run whole
(`Dockerfile.benchmark`, both binaries built in the same image, aarch64,
18 cores), twice, agreeing within a point except the single-connection
round-trip: pipelined and many-connection shapes are now 2x-4x upstream
(S1 +223%, S4 +306%, S5 +123%), and a client watching 500 tubes is 7.6x
(S7).

Two rows are losses, and they stay in the table because they are the
honest half of the result: with 500 tubes (S6) and with 10,000 (S11)
this fork runs 5-7% SLOWER than upstream, consistently across runs. Where
the work per command is dispatch and syscalls, the fork wins by multiples;
where it is tube lookup at scale, upstream's simpler structure is still
ahead. S7 places the deficit: watching many tubes is fast here, so what
costs is the tube table itself, not the watch set.

### Gates

`make check` on default flags, clang, UBSan, ASan and cppcheck
(`Dockerfile.build`), green. Both fuzzers, with the protocol one widened
to cover what changed — command lines around both length boundaries, and
whole pipelined bursts in one send with a blocking reserve inside them —
clean over 107M fragments and 75 corrupt-WAL restart rounds. The crash
bench at pipeline depth 32 as well as depth 1, under `-D` and without;
the shutdown, binlog and syscall benches. 1032 unit tests, 20 of them
new and every new path mutation-checked.

TSan is the one gate that is not simply green: it was run in loops of
ten to twenty rather than once, which is how the harness problems above
were found at all, and it still fails about one run in ten on a
signal-timing artifact that the diagnostics now describe in the failure
message. Every run — the failing ones too — reports zero races.

## 2026-09-09 — Angry-tests adjudication: 20 defects closed, global stats cache removed

The angry-tests batch (573 new unit tests over `conn.c`, `file.c`,
`walg.c` and `util.c`) landed 61 deterministic red tests. Each was
judged against the contract — protocol.txt, README, and the promises
the code documents about itself — and either fixed in the product or
withdrawn as a test demanding something never promised. `make check` is
green on default flags and under UBSan, and cppcheck is clean.

### Data-loss and durability

- WAL replay: an unreplayable record (unknown job state, malformed v7
  truncate marker) destroyed the job an earlier valid record had
  recovered. The record is now rejected without touching it.
- WAL replay: a legacy truncate marker with a zero-length body read as
  end-of-file, silently discarding every record behind it.
- `fileread` released its own reference through `filedecref`, which
  could hand the `File` to `walgc` and then write through the freed
  struct — a binlog whose jobs were all deleted hits this on the
  ordinary "compacted away" path.
- `moveone`: a failed migration unlinked the source binlog that still
  held the only copy of the job, and kept its reservation booked.
- `filewopen`: the version header was written at whatever offset the
  preallocation left (the `rawfalloc` fallback used on filesystems
  without `fallocate` leaves it at the end), the binlog was opened
  without `O_TRUNC` so records from an earlier file of the same name
  replayed as live, and a binlog smaller than its own header published
  a negative free count.
- `filewclose` / durable rollback: `ftruncate` on a negative count
  EXTENDED the binlog with a hole that replays as a fallocate tail.
- Detected corruption (`warn_if_not_tail`) now reaches the replay exit
  code instead of being reported as a clean read.
- `walgc` dropped a binlog from `nfile` even when the `unlink` failed,
  so the wal counted fewer files than the next start would find.
- `walsyncstop` discarded an fsync error the thread had recorded but
  never reported; the acks that rode on that fsync were already sent.

### Correctness and accounting

- `which_cmd` matched `stats-job`/`stats-tube` on two bytes and
  `pause-tube` without its separator: `stats-tubeZdefault` was served
  out of stats-tube's namespace and `pause-tubeNAME 30` paused NAME.
  README invariant #13 (strict literal prefix dispatch) now holds for
  these three verbs too.
- `put` with a sixth argument answers `BAD_FORMAT` before the size
  check instead of arming a multi-gigabyte bit-bucket on a number from
  a line already known to be malformed.
- `DEADLINE_SOON` left `pending_timeout` set, which kept the connection
  out of the `-I` idle gate for the rest of its life.
- `conntickat` could return the "nothing to wake for" sentinel for a
  live reservation (a wake-up landing exactly on 0), evicting the conn
  from the tick heap.
- `conn_reserve_job`: `now + ttr` overflowed for a ttr replayed from a
  binlog; a wrapped deadline times the job out the instant it is
  handed to a worker.
- `connclose` released the same job twice when `in_job` and `out_job`
  aliased; `conn_defer_free_end` had no floor, so a stray end left the
  next defer window protecting nothing.
- `filedecref` underflowed `uint` refs, pinning a binlog out of walgc's
  reach for the life of the process; `filermjob`'s repair path zeroed
  the global alive total, erasing the live bytes of every other file.
- `needfree` made a new file for a record no binlog could hold, driving
  its free count negative; `balancerest` moved a remainder onto the
  file it was already on and reported success.
- Binlog sequence: `w->next++` was undefined at INT_MAX, and the
  negative seq it produced named a `binlog.-1` the scan can never find.
  `walscandir` also accepted `binlog.+7` and `binlog. 7`, which strtol
  takes but this writer never produces.
- `conn_waitpos_reserve` overflowed its size computation and reported
  capacity the block does not have.
- Text-mode warnings are rendered once and emitted with a single
  `fputs`, as the JSON path already did: four stdio calls gave the
  fsync thread three places to land a warning inside another one.

### Removed: the 500ms global stats cache

`stats` served a body up to half a second old — a client that put a job
and then asked for stats was told the old ready count. The cache could
not be made correct: protocol.txt describes every field as its value
when the command is processed, and the body carries its own `cmd-stats`
counter, so even two `stats` in a row have different correct answers.
Formatting is ~1.5KB of snprintf on a command monitors issue about once
a second.

### Syscalls on the hot path

Measured with `strace -c` against a loopback load generator, 9000
commands as 3000 put/reserve/delete cycles:

- Non-pipelined, no WAL: **33012 -> 27010 syscalls** (3.7 -> 3.0 per
  command). `TCP_CORK` was armed whenever bytes sat behind the command
  line — which is every `put`, because its body arrives right there.
  Two setsockopt calls per put bought nothing: there was one reply to
  coalesce. Corking now keys off a second command actually being
  dispatched, with `put` judged on the next trip round the loop and
  every other verb still judged immediately, so a reserve/delete burst
  coalesces from its first reply as before. Pipelined runs (depth 32)
  are byte-for-byte identical in syscall count and throughput.
- With `-b` (WAL): **26016 -> 22014 syscalls**. `writev_all` called
  `lseek(SEEK_CUR)` before every staged record just to learn the offset
  a rollback would truncate back to. `File` now tracks that offset
  (`woff`, established lazily so a File handed a foreign fd still
  works), and the durable-commit rollback reuses it.

### Sanitizers

The suite now runs clean under AddressSanitizer and ThreadSanitizer as
well as UBSan, and the first two found real problems:

- Seven heap-use-after-frees, all the same shape: a test called
  `job_free(j)` and then `tube_dref(t)`, but `make_tube` hands back a
  tube with no reference of its own, so freeing the job freed the tube
  the next line then read. The tests hold a reference now.
- `shutdown_requested` and `drain_mode` were `volatile sig_atomic_t`,
  which orders nothing between threads — and a process-directed signal
  can be delivered to any thread, the fsync thread included. Both are
  lock-free atomics now, read and written with relaxed order.
- The fsync thread starts with every signal blocked, so a SIGTERM or
  SIGUSR1 is handled where the main loop can act on it.
- The fault-injection counters (`testinject.c`) are atomic, and the
  countdown is claimed with a compare-exchange: the wrapped syscalls
  fire on whichever thread makes the call.

`Dockerfile.build` now gates on default flags, UBSan, ASan and cppcheck.
TSan cannot run inside `docker build` (it needs ADDR_NO_RANDOMIZE); the
command to run it by hand is in that file.

### Fuzzing

`bench/fuzz/` holds two fuzzers, both run against a server built with
ASan+UBSan:

- `protocol.py` — hostile and half-valid command lines: every verb
  against every separator, numbers that overflow every width, tube names
  at and past the limit, raw random bytes, put headers whose body count
  does not match the body. Any reply is acceptable; a crash, a hang or a
  sanitizer report is not.
- `replay.py` — builds a real WAL through the wire protocol, corrupts it
  (bit flips, zero runs, truncation, and relabelling the file as the
  legacy v7 format so the v7 reader is driven by v8 bytes), then
  restarts the server. It must come up or refuse cleanly.

Current state: ~50M protocol fragments and 140 corrupt-WAL restarts with
no crash and no sanitizer report.

### Tube names off disk

A tube name is not just a key: every `stats-*` and `list-*` reply prints
it into a YAML document. The wire parser only ever accepted
`[A-Za-z0-9-+/;.$_()]`, so a name from a client can never carry a
separator — but a name replayed from a binlog was taken as-is, and a
corrupt or hand-made file could introduce a tube whose name contains
CRLF or a colon. `list-tubes` would then emit a body the client reads as
extra entries. Both WAL readers now hold a name off disk to exactly the
rule the wire parser applies (upstream #669 reached through the WAL
rather than the wire; the wire side was already closed here).

Reviewed the rest of upstream's open issues against this fork while
looking at that one: #670 (heapremove use-after-free on tied keys), #597
(release with delay=0 losing the new priority), #646 (DEADLINE_SOON
returned while a ready job exists) and #622 are already fixed here, and
#539 (F_FULLFSYNC) does not apply to a Linux-only build. Upstream itself
has had no code changes since March 2025.

### Job id space

A record replayed with id `UINT64_MAX` advanced the id counter past the
end of its own type to 0 — and 0 is not an id: `readrec` reads a zero id
as the start of the fallocate tail, so the next `put` would have written
a record that ends every later replay at its own offset, losing
everything behind it. Both readers now refuse `UINT64_MAX` the way they
already refused 0, which keeps `id + 1` in range for every id that can
reach `make_job_with_id`; the generator additionally refuses to hand out
0 rather than trusting that.

### Test coverage

Line coverage of `prot.c` was 79.7%; the biggest untested region was the
partial-write retry — the path the kernel takes every time a peer is
slow enough that the socket buffer only accepts part of a reply. A retry
that resent the whole line, or dropped the tail, would have gone
unnoticed. `testinject.c` grew `fault_set_short()` (write/writev pass
only N bytes through and report that count — a short write is not an
error, so the existing failure injection could not produce one), and new
tests cover:

- a truncated `SEND_WORD` reply, down to one byte at a time;
- a truncated job reply, where header and body go out through one
  `writev` and the retry has to resume at the right offset in the right
  buffer;
- a truncated durable-batch flush, whose tail is handed to the FSM from
  a buffer the conn owns;
- a worker that half-closes while parked on `reserve` — protocol.txt
  gives that command two outcomes, and the half-close has to resolve
  into one of them rather than leaving the conn in the waiting set (and
  its tube's current-waiting count) forever.

`prot.c` is at 81.5%.

### Mutation testing

`bench/mutate/` flips one comparison or boolean operator at a time,
rebuilds and runs the suite: coverage says a line ran, mutation says a
test would have noticed the line being wrong. First run over `conn.c`
and `job.c` — 20 mutants, 14 killed, 6 survived. Five survivors are
equivalent mutants (both sides of the boundary produce the same result,
or the bound is unreachable), which is a fact about the code rather
than a defect. The sixth was real: `on_watch_remove` mirrors the swap
`ms_delete` performs into the parallel hint array, and loosening its
bound to `<=` reads one element past the block — reachable whenever a
conn waits on N tubes and then watches one more. It is now covered by a
test that fails under the ASan gate.

### A configuration trap, caught at startup

`-z` (max job size) and `-s` (binlog size) are set independently, and a
job larger than a whole binlog can never be stored: the reservation is
refused and the client gets `OUT_OF_MEMORY`. That is the right answer to
give a client, but the wrong place to learn it — the first large put, in
production. Startup now checks the largest record a put could produce
(namelen + tube name + Jobrec + body + CRC trailer) against what a
binlog has after its header, and says so with both numbers if they do
not fit. Silent on defaults, where a 10MB binlog holds a 64KB job many
times over.

### A lost wake-up on EINTR

`srv_wake` is what a signal handler uses to get the main loop out of
`epoll_pwait`, and its `write` to the eventfd was not retried. An
interrupted write returns EINTR and the wake-up is simply gone: the loop
stays parked — up to an hour when the server is idle — with
`shutdown_requested` already set and nobody awake to act on it. It
retries now. EAGAIN still needs no retry, because it means the eventfd
counter is already saturated and a wake-up is pending either way.

Pinning this needed a way to see the counter, since the only observable
effect of `srv_wake` is that number; `srv_wake_fd()` exposes it, next to
the `srv_wake_init()` the tests already used.

### A clean exit now closes the binlog

`main()` returned from `srvserve` and went straight to `exit(0)`. Two
things were left undone. The records written since the last periodic
fsync lived only in the page cache, so a shutdown followed by a power
loss dropped jobs the client had been acked for — the failure mode the
WAL exists to prevent, reached through the ordinary way a server stops.
And the binlog kept its full preallocated size: on a default `-s`, a
file holding a few hundred bytes stayed at 64KB, and the next start read
a tail of zeroes nobody needed to write. A clean SIGTERM now calls
`filewclose` on the current binlog, which trims it and fsyncs it (under
`-D` the sync is skipped — every commit already did it). Measured: 65536
bytes down to 949, with every job still replaying.

The same path now also reports an async-fsync error the thread recorded
on its last round: `walsyncstop` joins the thread, and nothing had been
looking at what it left behind, so the process could exit 0 on a WAL
that had failed to sync.

Pinned by `bench/shutdown/run.sh` — the unit tests cannot reach this
path, because their server is `srvserve()` called directly with a
SIGTERM handler that exits immediately.

### Two heap guards, now pinned

Mutation testing over `heap.c` found two bounds nothing held at the
edge. Both guards were already correct; neither had a test that failed
when they moved:

- `heapremove`'s `k < h->len`. Loosening it to `<=` is exactly upstream
  #670, and the existing test could not see it: it looks for the removed
  element *inside* the heap, and the element lands one slot past what
  that loop walks. What the loosened version really does is write a
  position through an element it has just handed back to a caller free
  to release it — so the new test poisons that field and checks nothing
  wrote to it.
- `heapresift`'s `k >= h->len`. Callers pass a cached index
  (`Conn::tickpos`, `Tube::delay_heap_index`) that can go stale, and
  sifting from out of range reads neighbours that are not in the heap.

### Test-server startup is synchronised, not guessed

Both test harnesses forked a server and hoped. `testserv2.c` slept
100ms — over a hundred tests start a server, so that alone was ~13s of
the suite — and `testserv.c` did not wait at all, which is a race: the
listening socket exists before the fork, so a connect() succeeds whether
or not the child is serving yet, and a SIGTERM meant for a running
server could land before it had finished starting. (That race is what
made `cttest_binlog_empty_exit` look like a product bug earlier in this
batch.) Both now hand the child a pipe and block until it says it is
serving. The suite went from 58s to 44s, and the remaining 13s is tests
that wait out real timeouts, which is what they are for.

### Boundaries pinned from both sides

Several documented limits had tests for "well past the line" but nothing
at the line itself, which is where an off-by-one lives:

- a put body of exactly `max-job-size` is accepted and one byte more is
  refused (and the refusal consumes exactly the body it announced, so
  the next command is read as a command);
- a WAL record naming a tube of exactly `MAX_TUBE_NAME_LEN` is refused
  while `MAX_TUBE_NAME_LEN-1` replays — in both readers. Mutation
  testing found this one: loosening the v7 reader's bound writes the NUL
  terminator one past the name buffer, and nothing noticed.
- `-c` above `UINT_MAX` clamps rather than truncating (a truncated cap
  would silently become 1 and refuse every connection but the first),
  `-m` above its 1e9-second cap clamps rather than wrapping negative,
  and `-I 0` stays distinguishable from a small timeout.

### A syscall-profile check

`bench/syscalls/count.sh` counts what the server asks of the kernel per
command under a fixed workload. The server spends most of its time in
the kernel, so this is the number that moves throughput — and some of
the properties behind it cannot be seen from the wire at all. Corking a
lone command, for instance, produces identical bytes on the socket and
costs two setsockopt calls; only a count shows it. Expected today, one
connection without pipelining: `epoll_pwait` and `read` 1.0/cmd,
`write`+`writev` 1.0/cmd, and setsockopt **2 in total** — both at accept.

### Two operational checks in bench/

- `bench/crash/run.sh` — SIGKILL mid-traffic, restart, count. Under `-D`
  an ack means the record reached the disk, so every acked job must come
  back; without `-D` the promise is weaker by design and the run is a
  smoke test. It is explicit about what it does *not* simulate: losing
  the page cache, which needs a hard reset. The reader's side of that is
  what `bench/fuzz/replay.py` covers with corrupted and truncated
  binlogs.
- `bench/shutdown/run.sh` — the clean-exit path described above.

Measured while adding them, on this machine: 500k jobs held at 273 bytes
each with `stats` still answering in under a millisecond; 100k jobs
replayed from a 29MB WAL in 22ms; 5000 tubes with `list-tubes` returning
59KB in 0.1ms; 800 simultaneous waiters with the per-handoff cost flat
as the count grows.

### Documented promises, now tested as promises

Three statements in protocol.txt had no test that checked the statement
itself:

- "The server never inspects or modifies a job body and always sends it
  back in its original form." There was a test for a body of all 256
  byte values across a reserve — but never across a **restart**, where
  the body is framed with a length, checksummed, written with writev and
  read back by readrec. Each of those is a place a zero could be taken
  for a terminator or a CRLF for a record boundary.
- "If a tube is empty and no client refers to it, it will be deleted."
  Nothing watched `current-tubes` through a tube's life. A server that
  kept every name a client ever used would leak a Tube per name, and
  tube names are often per-customer or per-day.
- The peek family: all but `peek <id>` "operate only on the currently
  used tube", and each names which job it returns. The tube scope was
  untested — a peek answering from the wrong tube is worse than one
  answering NOT_FOUND, because it is believed. (Verified biting: letting
  peek-buried scan all tubes turns this test red.)
- `release <id> <pri> <delay>`: "the job will be in the 'delayed' state
  during this time". This is the retry-with-backoff every worker library
  is built on. Existing tests covered release with delay 0 and the
  NOT_FOUND path; the delay itself was untested, and a release that
  ignored it would hand the job straight back to the worker that just
  failed it — a backoff turned into a spin. (Verified biting.)
- kick "applies only to the currently used tube". The
  buried-before-delayed half of that sentence had two tests; the tube
  scope had none. Reaching into another tube puts work back in flight
  that nobody asked for, in a tube whose owner may not even be
  connected. (Verified biting: making kick scan all tubes for buried
  jobs turns exactly this test red and leaves the other two green.)
- A lapsed TTR requeues the job and counts a timeout. `timeouts`
  appeared nowhere in the suite, and neither did the global
  `job-timeouts` — the counter an operator watches to find jobs that
  keep being abandoned, and the requeue a worker depends on to get a
  second attempt.
- `touch` "resets the time left for the job to run to its original
  TTR". Every existing test covered a way touch can be *refused*
  (BAD_FORMAT, NOT_FOUND); nothing checked the thing it exists to do.
  A touch that answered TOUCHED without moving the deadline loses the
  job to a TTR timeout while a worker is still running it — precisely
  the failure the command prevents. (Verified biting: neutering the
  deadline update turns the new test red.)
- pause-tube: a paused tube is not an empty one. Its jobs stay ready and
  stay counted; they are only withheld. A client scaling workers off
  `current-jobs-ready` must not be told the queue drained when it was
  merely paused.

### The IPv6 listener is tested

README lists IPv6 as supported and nothing exercised it: mutation
testing over `net.c` survived a flipped `ai_family == AF_INET6`, which
is how the gap surfaced. The new test binds `::1`, checks the socket is
really AF_INET6 and bound to an assigned port, and connects to it. (The
`IPV6_V6ONLY` assertion alongside is honest about its limits — most
systems default to 1 already, so it catches the regression only where
the default is 0.)

### Every command counts itself

protocol.txt documents a `cmd-<name>` counter per command, each "the
cumulative number of <name> commands". Nothing checked the set as a
whole, and a command that forgets to bump its own counter leaves a field
permanently at zero — a dashboard reading it sees a command nobody ever
issues. The new test drives every verb once and then insists no `cmd-*`
field is still zero, reading the names out of the reply itself, so a
command added later is covered the moment it reports a counter.

### Invariants, tested as invariants

Two of them had no test that checked the property itself, only tests
that happened to exercise code near it:

- #2 (the global and per-tube reserved counters move in pairs) — a job
  is walked through reserve, release, reserve, bury, kick, reserve,
  delete, and after every step the global count must equal the sum of
  the tubes'. Nothing recomputes these counters, so a single missed pair
  is permanent.
- #1 (a job is on exactly one structure) — the four `current-jobs-*`
  counts must add up to the jobs that exist, through the same
  transitions. A job on two lists is counted twice and delivered twice;
  a job on none leaks and can never be handed out.

### Documentation of invariants

The tree cites numbered invariants in dozens of comments ("invariant
#11", "invariants #14 and #16") and nowhere listed them, so a reader
meeting one had no way to learn what it was. `doc/invariants.md` now
collects all eight: the property, what goes wrong when it does not hold,
and where it is pinned by tests. Numbers are left as they are —
renumbering would invalidate every comment that cites one.

### Warnings

`-Wshadow -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls
-Wundef -Wdouble-promotion` are on for the product objects. All six were
clean on this tree once three things were fixed: a local shadowing
another in `prottick`'s degraded-heap rescan, three exported functions
(`kick_buried_job`, `kick_delayed_job`, `walread`) defined with no
prototype to check them against — each had its own `extern` copy in a
test file, now a single declaration in `dat.h` — and `read_uint` taking
a `const char *` it then cast the const away from, which is simply a
mutable buffer. Test objects and the vendored `ct/` harness keep the two
prototype warnings off: `ct/gen` scans for the historic `cttest_foo()`
spelling and emits those declarations itself.

### Portability

- The aarch64 CRC32C path now compiles under clang as well as GCC. The
  two spell the target attribute differently, and clang will not let
  `<arm_acle.h>` declare the intrinsics under a function-level attribute
  — building the whole file for `+crc` instead would let CRC
  instructions escape into the dispatcher that runs on chips without
  them, so clang uses its own builtins. `Dockerfile.build` now runs the
  suite through clang too.

### Build

- `ctfailnow` is declared `noreturn`, so `assert()` narrows pointers for
  the optimiser; without it GCC read asserted-non-NULL pointers as
  zero-sized destinations and `make check` failed on default flags.
- `zalloc` carries `malloc` / `alloc_size(1)`.

### Documentation

- protocol.txt: the `delete` NOT_FOUND paragraph said a job "must be
  buried or reserved by the client", contradicting the paragraph above
  it and the code — ready and delayed jobs are deletable too. (Upstream
  made the same correction in 2025.)
- README: `TCP_DEFER_ACCEPT` is now listed under wire-observable
  differences — a client that connects without sending anything is not
  handed to the server for about a second, so the `-c` refusal reaches
  it with that delay.

## 2026-08-29 — Audit fixes, connsched OOM recovery, crc32c runtime dispatch, -Wextra

Final hardening batch on top of the `truncate` removal (see the entry
below — the wire command set is now identical to upstream; the
strict-parsing edges documented in README §Wire-observable differences
remain).

### Audit fixes

- WAL replay: v7/v8 `readrec` heap overflow via a forged `body_size`;
  `walresv` reservation leak on the kick-buried failure path (full
  rollback of wal/file/job counters).
- Stale-event UAF: conn pool free is deferred across the epoll batch
  drain (`conn_defer_free_begin/end`), so an event still queued for a
  conn being closed cannot dispatch into recycled memory.
- WAL rotation: `fdatasync` before `close` on the old file; `lastsync`
  accounting fix.
- Overflow guards in the 4-ary heap growth and `ms` size math;
  `allocate_job` failure gate on the put path.
- `connsched` OOM recovery: a failed `heapinsert(&srv->conns)` used to
  drop the conn's TTR / reserve-deadline / idle timers forever. Conns
  are now linked on an intrusive live list (`Conn::live_next/live_prev`);
  `connsched` flags the heap degraded and `conn_sched_recover` (called
  from `prottick`) re-derives each missing conn's tickat and retries the
  insert, waking within a second while OOM persists.

### Build

- `ct/gen` zero-test guard: an empty generated test list is a hard
  error, not a falsely green `make check`.
- Test objects build with `-fno-lto` so `nm` stays inspectable.
- CFLAGS stamp file: objects rebuild when the effective flags change.
- `-Wextra` added next to `-Wall` (still no `-Wpedantic`).

### Portability

- crc32c: SSE4.2 on x86-64 (compile-time, `-msse4.2` for this file
  only), ARM CRC32 on aarch64 with runtime `getauxval(AT_HWCAP)`
  dispatch, portable table-driven software fallback elsewhere. Verified
  against the known vector (`"123456789"` → 0xE3069283).

### Performance (Docker A/B vs upstream, `-O2 -DNDEBUG`)

- S3 +122% after the runtime CRC dispatch.
- Overall: +61% on S1/S4, +443% on S7, tail latency −34%.
- mimalloc was evaluated and rejected by measurement.

## 2026-08-29 — Remove the `truncate` command

The fork-specific `truncate <tube>` command (added 2026-04-16, hardened
2026-04-23) is removed entirely; the wire command set returns to
upstream: `truncate` dispatches to `UNKNOWN_COMMAND` again. (The
strict-parsing edges — trailing-space commands and strict literal
prefixes — remain, as documented in README §Wire-observable
differences.)

### Removed

- `truncate <tube>\r\n` → `TRUNCATED <count>\r\n` command: dispatch
  (`which_cmd` 't' branch now resolves `touch` directly), `OP_TRUNCATE`
  handler, `reply_truncated`, `cmd-truncate` stats key.
- Cutoff machinery that existed only for truncate: `Tube::purge_before_id`
  / `purge_drained`, the `truncated_tubes` registry, `job_is_purged`,
  `reap_purged_job`, the prottick lazy reap, the `enqueue_job` /
  `process_tube` purge intercepts, and every zombie guard in
  peek/reserve/delete/release/bury/kick/touch/stats-job. `touch` is back
  to upstream behavior (TOUCHED for any live reservation).
- WAL marker write path: `wal_write_truncate`, `filewrtruncate`,
  `Wal::compact_post` (and its `prot.c` callback), `File::marker_bytes` /
  `uncommitted_marker_bytes` accounting, marker re-emission in
  `prot_replay`. `job_next_id()` accessor (only the truncate handler
  used it).

### WAL replay compatibility

Old v7/v8 binlogs may still contain truncate-marker records (Invalid job
record with `namelen > 0`, id = cutoff). Both `readrec` and `readrec7`
now **skip them gracefully**: the record is consumed (body read, CRC
verified), a warning is logged, and replay continues. The cutoff is NOT
honored — jobs below it resurrect as live. This is the accepted
downgrade semantic: the marker's effects were already applied to the
live state before shutdown; silently dropping data at replay would be
worse than resurrecting jobs the operator thought deleted. Callers that
need the old semantics must drain such tubes before upgrading.

`testprot2.c` / `testserv2.c` / `testinject2.c`: all truncate tests
removed (45 integration + unit tests); `cttest_prot_dispatch_strict_prefix`
now locks in `truncate` → `UNKNOWN_COMMAND`. The
`cttest_epollq_double_insert_does_not_orphan_worker` scenario was
rewritten to force the reserve slow path without truncate (empty-tube
reserve + same-burst puts).

## 2026-07-02 — Job pool, wyhash tubes, incremental rehash, -c connection cap

Large performance and robustness batch (`505d91a`):

- 11-class job pool (64B–64KB slabs, O(1) reuse) replacing per-job
  malloc/free; cache-line-aligned `Conn`/`Tube` struct layout; Conn slab
  pool (256).
- wyhash v4 tube hashing (avalanche + length-aware) replacing DJB2;
  hash-first filter on lookup.
- Incremental job-hash rehash (16 buckets/op, dual-table) replacing
  stop-the-world rehash.
- `-c N` connection cap (0 = unlimited, upstream behavior preserved).
- Group-commit hardening: failure semantics and rollback paths covered
  by new fault-injection tests (~1750 lines of new tests).

## 2026-06-01 — Job-pool drain before malloc_trim

`-m` (periodic `malloc_trim`) could not reclaim pages held by pooled
jobs sitting on the free list. The pool is now drained on the `-m` tick
before trimming, so RSS actually drops on idle (`ac50b73`).
Counter balance preserved; covered by
`cttest_job_pool_drain_{balances_counters,spares_live_jobs}`.

Also: Lima A/B bench harness and the Mode-D pool-reclaim probe scripts
(`7b3d93b`, `0bf4be9`).

## 2026-04-25 — Pool size-class fix for power-of-2 bodies

`pool_class` rounded every stored `body_size` up to the next power of 2,
but callers store `body+2` (the `\r\n` trailer), so a 1024-byte user
body landed in the 2048-byte slab — ~1.80× upstream RSS on round-size
workloads. `POOL_PAD=2` shifts slab sizes to {66..65538}, so power-of-2
user bodies land exactly on class boundaries with zero waste
(`d28f5a7`). Measured: 2234 → 1207 B/job (upstream: 1312).

## 2026-04-24 — Durable group commit (-D)

Group commit for `-D` mode. No wire-protocol change. Invariant #14
(`ack ⇒ durable`) preserved; new invariant #16 documents the batch
lifecycle.

### Performance (Docker A/B, `-O2 -DNDEBUG`)

Durable-pipelined workload (8 conns × pipeline=64 × 128B × -D):

| Metric | Before | After | Δ |
|---|---|---|---|
| Throughput | 13K ops/s | 366K ops/s | **+2720%** |
| P50 latency | 43 ms | 1.24 ms | **-97%** |
| S10 / S1 ratio | 6.1% | 175% | — |

Durable-serial (1 conn × pipeline=1 × 4B × -D) also improved (13K → 22K,
P50 87µs → 54µs). Async mode (no `-D`) unchanged on all scenarios (S1–S8
within variance). Deep-watch regression suspected at start of this
iteration (S6, 500 tubes) was disproven — the `O(watched_tubes)` scan
in the reserve fast-path is not a real bottleneck at current scale;
the flat S6 result is Python-client GIL overhead, not server-side.

### Changed

- **`walwrite` / `wal_write_truncate` now stage records.** They perform
  the writev and accounting but defer the fdatasync. The serv main
  loop runs `walcommit(&s->wal); dur_flush_all(commit_ok);` once per
  epoll drain, amortising one `fdatasync` over every WAL-dirty command
  in the tick.
- **`filewrjobshort`, `filewrjobfull`, `filewrtruncate`, `filewrcommit`,
  `File::uncommitted_bytes`** — `file.c` API staged: writev + accounting
  now, fdatasync deferred to `filewrcommit`. Commit failure ftruncates
  the tail AND rolls back global WAL counters (`w->resv`, `f->resv`,
  `w->alive`) so accounting matches the post-ftruncate disk.
- **`Conn::dur_reply_buf` (4 KiB)** — deferred-ack buffer. WAL-dirty
  callsites (OP_PUT success, OP_DELETE, OP_RELEASE, OP_BURY, OP_KICK,
  OP_KICKJOB, OP_TOUCH, OP_TRUNCATE) now call `dur_enqueue(c)` before
  `reply_*`; the hook at the top of `reply()` appends to the buffer
  instead of the socket. `dur_flush_all(ok)` sends batched acks on
  commit success or `INTERNAL_ERROR\r\n` on commit failure.
- **`connclose` → `dur_remove`** — O(1) swap-remove from the batch
  array so a conn that drops mid-tick cannot be followed to a dangling
  pointer by the flush pass.

### Tests

Nine new hostile tests in `testinject2.c` (one pre-existing
`zalloc_oom` failure, unrelated):

- `cttest_inject_group_commit_fires_fdatasync_once_for_batch` — 5 stages
  + 1 commit = 1 fdatasync; empty commit = no-op.
- `cttest_inject_group_commit_rollback_disables_wal` — commit fail
  ftruncates, disables WAL, `walwrite` refuses afterwards (#14).
- `cttest_inject_group_commit_fail_rolls_back_global_counters` — w.resv,
  f.resv, w.alive restored to pre-batch values on commit fail.
- `cttest_dur_batch_swap_remove_preserves_indices` — middle-removal
  updates survivors' `dur_batch_idx` correctly.
- `cttest_dur_enqueue_noop_in_async_mode` — enqueue is a no-op without
  `-D`.
- `cttest_dur_enqueue_idempotent` — duplicate enqueue does not corrupt
  indices.
- `cttest_dur_flush_all_success_emits_buffered_replies` — socketpair:
  buffered acks reach the peer byte-exact on commit success.
- `cttest_dur_flush_all_failure_emits_internal_error` — socketpair:
  `INTERNAL_ERROR\r\n` (not the buffered acks) reaches the peer on
  commit fail. Protects invariant #14 on the wire.
- `cttest_dur_flush_all_partial_write_saves_remainder` — socketpair
  with saturated send buffer: partial write parks remainder on
  `c->reply`/`c->reply_sent` with state `SEND_WORD` so the epoll 'w'
  handler can finish.

Two `testinject2.c` tests updated to match the new contract (walwrite
stages; walcommit issues fdatasync): `cttest_inject_durable_fdatasync_fail_disables_wal`
and `cttest_inject_durable_fdatasync_fires_once_on_success`.

One `testinject2.c` test removed as now fully duplicated by the new
group-commit coverage: `cttest_inject_durable_fdatasync_fail_rolls_back_tail`
(its ftruncate + counter assertions live in
`cttest_inject_group_commit_rollback_disables_wal` and
`cttest_inject_group_commit_fail_rolls_back_global_counters`).

### Known gap (documented, not blocking)

Per-job counters (`j->walresv`, `j->walused`) are not rolled back on
commit fail. Acceptable because `walcommit` fail disables the WAL and
all subsequent `walresv*` / `walwrite` refuse (#14), so the per-job
slop is reclaimed at `job_free`. Will need a pending-job list on `File`
if a future design retries commits without disabling the WAL.

### Benchmark harness additions

`test/bench.c` gained `-W N` (deep-watch mode) and now reports P99.99
and max. `test/benchmark.sh` gained S7 (deep-watch, tests
`O(watched_tubes)` in reserve fast-path), S8 (100K-sample tail probe,
replaces the too-noisy S2 P99.9 signal), S9 (`-D` serial), S10 (`-D`
pipelined), plus `S9/S2` and `S10/S1` cost-of-durability ratios.

## 2026-04-24 — Wire-observable differences documented; systemd unit hardened

No code changes — operator-facing documentation only, shipped ahead of the
first tagged release so migration from upstream is an informed decision.

### Documented

- **Wire-observable differences from upstream** (`README.md` §Compatibility)
  — exhaustive table of every point where a client can tell the two
  binaries apart on the wire: the `truncate` command, the `touch`-after-
  `truncate` → `NOT_FOUND` edge (behaviour change from upstream, accepted
  per invariant #8), the additive `cmd-truncate` line in `stats` (shifts
  later fields down one line for index-based parsers; key-value parsers
  unaffected), strict-prefix dispatch upgrading trailing-space variants
  (`"stats \r\n"`) from `BAD_FORMAT` to `UNKNOWN_COMMAND` (invariant #13),
  and `-D` without `-b` surfacing real errors rather than ghost-acking
  (invariant #14). The previous blanket claim "No new commands, no
  changed responses, no modified stats fields" was literally false since
  the `truncate` extension in 2026-04-17 and has been rewritten.

### Changed

- **`adm/systemd/beanstalkd.service`** — added `Restart=on-failure`,
  `LimitNOFILE=65536`, `MemoryMax=2G` (OOM backstop since tubes/jobs are
  unbounded inside the process), `StandardOutput=journal` +
  `SyslogIdentifier=beanstalkd`, a commented `ExecStart` example with
  `-b` and `--log-json`, and a graceful `ExecStop` that sends SIGUSR1
  (drain) and sleeps 25s before systemd escalates to SIGTERM (total
  `TimeoutStopSec=35s`). `User=nobody` and `ExecStart=/usr/bin/beanstalkd`
  unchanged, so no new package metadata (sysusers, state dir) is required
  to deploy.

## 2026-04-24 — Injection framework coverage + SIGUSR1 snapshot

Small hardening pass closing three gaps surfaced during the post-2026-04-23
retrospective. All changes are additive or behaviour-preserving.

### Fixed

- **SIGUSR1 race in `http_health_reply`** — `drain_mode` is `volatile
  sig_atomic_t`; the old code read it twice (line for `hdr`, line for
  `body`). A SIGUSR1 delivered between those two reads shipped a
  mismatched reply: `Content-Length: 2` + `"draining"` (probe reads the
  truncated `"dr"` as 200 OK and silently masks the drain signal), or
  the inverse (probe hangs until FIN). Fix: single-read snapshot into a
  local `int draining`. Tiny window in practice, but guaranteed wrong
  semantics when it hits. (`prot.c`)

- **`walsyncstart` resource hygiene** — on `pthread_create` failure the
  previous code left `sync_mu` and `sync_cond` initialised but never
  destroyed. Production impact near zero (called once at startup), but
  a retry or second-Wal scenario would re-init a still-live mutex/cond,
  which POSIX classifies as UB. Fix: destroy the pair on fallback. Also
  added matching destroy in `walsyncstop` after `pthread_join` so the
  start/stop contract is symmetric. (`walg.c`)

### Added

- **Injection framework: two new wraps** (`FAULT_STAT`, `FAULT_PTHREAD_CREATE`).
  The original 10-wrap set (malloc, calloc, realloc, write, writev, read,
  open, ftruncate, unlink, fdatasync) did not cover `stat()` — the only
  remaining gap in the `make_unix_socket` TOCTOU hardening shipped in
  e7a97d0 — and did not cover `pthread_create()`, which gates the
  `walsyncstart` graceful-fallback contract. These are the only
  production call sites that were failure-path-unreachable from tests;
  `fsync` and `rename` audit recommendations were dropped because the
  production code uses neither (only `fdatasync` via `durable_fsync`,
  and WAL rotation is open/write/unlink without a rename step).
  (`testinject.h`, `testinject.c`, `Makefile`)

- **5 new hostile tests** in `testinject2.c`:
  - `cttest_inject_make_server_socket_stat_eacces_rejects` — injects
    EACCES on stat, asserts make_unix_socket returns -1.
  - `cttest_inject_make_server_socket_stat_happy_path_untouched` — no
    fault armed; verifies exactly one wrapped `stat()` call per call
    site. Flags glibc symbol-alias regressions (`__xstat`, `__statx`)
    that would silently bypass the wrap.
  - `cttest_inject_walsyncstart_pthread_create_fail_falls_back` —
    injects EAGAIN, asserts `sync_on == 0`, verifies walsyncstop is a
    safe no-op on the fallback Wal.
  - `cttest_inject_walsyncstart_pthread_create_happy_path_untouched` —
    unfaulted start spawns thread, call counter increments.
  - `cttest_inject_walsyncstart_pthread_create_skip_then_fail` —
    countdown=1 semantic: first start succeeds, second falls back.
    Guards against regressions in fault_fire's skip-then-fire logic
    that would break every wrap simultaneously.

### Verification

Dockerfile.build CI: UBSan + cppcheck + 360 hostile tests — green.

---

## 2026-04-23 — Cloud Team audit hardening

Tenth post-truncate hardening wave. Closes the regressions surfaced by
three independent adversarial audit rounds plus their hardening
follow-ups.

### Fixed

- **#C1** `-D` mode no longer silently degrades to in-memory after a WAL
  I/O failure. `walwrite`, `wal_write_truncate`, and `reserve` now
  refuse (return 0) when `w->use == 0 && durable_sync`, propagating a
  real error (BURIED / INTERNAL_ERROR / OUT_OF_MEMORY) to the client
  instead of a ghost INSERTED. Legacy non-durable pass-through is
  preserved. (`walg.c`)

- **#C2** Durable `fdatasync` failure now rolls back the bytes it
  couldn't sync: `filewrite_commit_durable` `ftruncate`s the file back
  to the pre-write offset and re-fsyncs the rollback. Without this,
  a successful `writev` + failed `fdatasync` left the record on disk
  while the client was told INTERNAL_ERROR — a client/server state
  divergence visible only after restart. (`file.c`)

- **#DS-2** Every `w->use = 0` transition now emits an explicit
  `twarnx("wal: disabling WAL after ...")` so operators find the
  transition in logs instead of inferring it from sub-errors. (`walg.c`)

- **#P1** `which_cmd` for commands starting with `s` now requires the
  literal `"stats"` prefix before dispatching. Previously, 7-byte
  commands like `sleep\r\n`, `steal\r\n`, `stash\r\n` slipped into
  OP_STATS and leaked the global stats block. `"stats-job"` and
  `"stats-tube"` additionally require `cmd[5]=='-'`. (`prot.c`)

- **#P2** `which_cmd` for 12-byte `l...s\r\n` commands now requires
  the literal `"list-tubes"` prefix. Previously, any byte soup with
  `cmd[9]=='s'` (e.g. `l12345678s\r\n`) dispatched as OP_LIST_TUBES and
  leaked the tube namespace. (`prot.c`)

- **#S1** `reply()` fast path now re-arms the socket for EPOLLIN when
  the conn's epoll interest was `'h'` (STATE_WAIT) or `'w'` (partial).
  Without the re-arm, TIMED_OUT and DEADLINE_SOON replies left the
  socket registered for hangup-only and the next client command sat
  in the kernel buffer until disconnect. (`prot.c`)

- **#J1** `job_copy` now nulls `n->reserver` alongside every other
  pointer field. The previous `malloc` (not `calloc`) left the field
  uninitialised; today the copy's `state == Copy` gates every reader,
  but a future caller without that guard would dereference garbage —
  exactly the fingerprint of bug #22. (`job.c`)

- **#N5** `enqueue_reserved_jobs` now nulls `j->reserver` before the
  Conn returns to the pool. The back-pointer would otherwise target a
  recycled Conn slab; safe today through the `state == Reserved` guard
  in `is_job_reserved_by_conn`, fragile against future refactors.
  (`prot.c`)

- **#TR-A1** `truncate` count on an already-truncated tube now only
  counts jobs with `id > old_purge`, preventing inflated replies when
  a second truncate lands before prottick has lazy-reaped the zombies
  from the first. (`prot.c`)

- **#TR-A3** `prot_replay` now purge-guards Buried jobs before calling
  `bury_job`, avoiding transient `buried_ct` inflation between boot
  and the first prottick tick. The guard is hoisted above
  `walresvupdate` so it also covers the WAL-exhaustion replay path
  and incidentally closes the `bury_ct` double-increment on that
  path (analogous to #668). (`prot.c`)

### Changed

- Trailing-whitespace variants of `stats` and `list-tubes` now reply
  `UNKNOWN_COMMAND` instead of `BAD_FORMAT`. Canonical client requests
  are unchanged; tolerant clients that shipped `"stats \r\n"` or
  `"list-tubes \r\n"` will see a different error code. No wire-compat
  break.
- `-D` without `-b` continues to start, but every persistent command
  now surfaces a real error (BURIED / INTERNAL_ERROR / OUT_OF_MEMORY)
  instead of ghost-ack'ing. The startup warning is strengthened to
  describe this clearly. (`main.c`)

### Tests added (18 new cttest_* cases)

- `cttest_job_copy_reserver_is_null` — #J1 direct.
- `cttest_enqueue_reserved_jobs_clears_reserver` — #N5 single-job.
- `cttest_enqueue_reserved_jobs_clears_all_reservers` — #N5 multi-job
  (catches loop-body mutations that only null the first reserver).
- `cttest_which_cmd_stats_prefix_strict` — #P1.
- `cttest_which_cmd_list_tubes_prefix_strict` — #P2.
- `cttest_reserve_timeout_then_next_command_does_not_hang` — #S1
  (TIMED_OUT path).
- `cttest_deadline_soon_then_next_command_does_not_hang` — #S1 sister
  (DEADLINE_SOON path).
- `cttest_truncate_count_excludes_old_zombies` — #TR-A1 pipelined
  double truncate.
- `cttest_truncate_count_mixed_new_and_old_jobs` — #TR-A1 mix of
  pre- and post-cutoff ids.
- `cttest_truncate_count_covers_ready_delay_buried` — #TR-A1 all
  three structures in the slow path.
- `cttest_wal_truncate_replay_buried_does_not_inflate` — #TR-A3.
- `cttest_inject_walwrite_refuses_when_durable_and_wal_disabled` — #C1
  walwrite gate.
- `cttest_inject_wal_write_truncate_refuses_when_durable_and_wal_disabled`
  — #C1 truncate gate.
- `cttest_inject_walresv_refuses_when_durable_and_wal_disabled` — #C1
  `reserve()` / `walresvput` / `walresvupdate` gate.
- `cttest_inject_durable_fdatasync_fail_rolls_back_tail` — #C2
  rollback via `ftruncate`.
- `cttest_inject_kick_buried_job_refuses_when_durable_and_wal_disabled`
  — #C1 propagation through `kick_buried_job` and `kick_delayed_job`.
- `cttest_walsync_thread_roundtrip` — async fsync thread lifecycle.
- `cttest_walsync_thread_error_surface` — async fdatasync failure
  surfaces via atomic `sync_err`.
- `cttest_walsync_thread_busy_slot_holds_without_signal` — pre-occupied
  `sync_fd` persists until `cond_signal`, verifying the busy-fallback
  invariant that `walsync` and `dirsync` depend on.

### Verification

Dockerfile.build CI pipeline: UBSan + cppcheck + full unit-test
matrix — green after every change.
