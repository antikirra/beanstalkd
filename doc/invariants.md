# Invariants

The code refers to these by number ("invariant #11", "invariants #14 and
#16"). They are the properties the server is built to keep; a change
that breaks one is a bug even when every test passes, so the entry says
what goes wrong when it does not hold and where the property is pinned
down.

Numbers are historical and not contiguous — they come from the audit
that introduced them, and renumbering would invalidate every comment in
the tree that cites one.

## #1 — A job lives in exactly one place

At any moment a job is on exactly one of: a tube's ready heap, a tube's
delay heap, a tube's buried list, or a connection's reserved list. Never
two, never none.

**When it breaks:** a job on two structures is delivered twice and freed
twice; a job on none leaks and its counters describe a queue nobody can
drain. `process_tube`'s OOM path buries a job it could not re-enqueue
precisely to avoid the second case.

**Pinned by:** `testconn_conn_reserve_job.c`,
`testprot2.c`, and the `current-jobs-*` assertions in `testserv2.c`.

## #2 — The reserved counters move in pairs

`global_stat.reserved_ct` and `tube->stat.reserved_ct` change together,
in both directions, including on the rollback paths.

**When it breaks:** `stats` reports reservations that do not exist, and
the drift is permanent — nothing recomputes these counters.

**Pinned by:** `restore_reserved_job` (prot.c), exercised through the
release/bury WAL-failure tests in `testinject2.c`.

## #6 — Reserved WAL bytes are conserved

Every `walresv*` reservation is either spent by a write or returned. A
job freed while still holding one inflates `w->resv` for the life of the
process.

**When it breaks:** `ratio()` reads `w->resv` on every maintenance pass,
so compaction runs against a number that no longer describes the disk.

**Caveat:** `j->walresv` only holds bytes while `w->use` is set. With the
WAL off, `walresvput`/`walresvupdate` return the legacy success value 1
(`wal_disabled_result`), which lands in the same field. Do not assert on
it without checking `w->use`.

**Pinned by:** `kick_resv_return` (prot.c), `testwal_reserve.c`,
`testwal_walresvreturn.c`, `testwal_moveone.c`.

## #10 — A conn between commands is armed for read

A connection in `STATE_WANT_COMMAND` is registered with epoll for read.
The fast reply path bypasses `conn_want_command`, so it re-arms 'r'
itself when the previous interest was 'h' or 'w'.

Arming epoll only covers commands still in the KERNEL's receive buffer.
A command already read into `c->cmd` — one pipelined behind a `reserve`
that then blocked — has no event left to announce it: the bytes are
gone from the socket, and the client is blocked reading, so it sends
nothing more. Whatever unblocks the conn (prottick's timeout, another
conn's `put`) must therefore put it on the run queue, and the main loop
drains that queue after the event drain. Both halves say the same
thing: a conn that can accept a command, and has one, must make
progress without needing the client to speak first.

**When it breaks:** the kernel holds the client's next command in its
receive buffer and `epoll_wait` never reports it, or the buffered one
is never dispatched. Either way the client waits forever on a healthy
connection for a reply the server already owes it (#S1, upstream #647).

**Pinned by:** `testserv2.c` pipelining and reply-order tests, and
`cttest_command_pipelined_behind_blocking_reserve_is_answered` /
`..._behind_a_timing_out_reserve_...` for the buffered half.

## #11 — `j->reserver` points at the conn that holds the job

The back-pointer and the connection's reserved list agree; both are
cleared together when the job leaves.

**When it breaks:** `connclose` walks a list that disagrees with the
job's own idea of its owner, and a stale `reserver` is a
use-after-free once that conn returns to the pool.

**Pinned by:** `testconn_connclose.c`, `testconn_conn_reserve_job.c`.

## #13 — Command dispatch matches literal prefixes strictly

A verb is recognised only by its whole literal, separator included.
`stats-tubeZdefault` is not `stats-tube`, `sleep\r\n` is not `stats`,
and `pause-tubeNAME 30` is not `pause-tube`.

**When it breaks:** one command's namespace leaks into another's — the
server answers `stats-tube` for a line the client never sent, or pauses
a tube named by a typo.

**Pinned by:** the `cttest_which_cmd_*` family in `testserv2.c`.
Documented as a wire-observable difference in README.md.

## #14 — Ack implies durable (under `-D`)

In durable mode a client sees a success reply only after the record
behind it has reached the disk. A WAL that cannot write refuses rather
than acknowledging, and a failed commit turns every ack pending on it
into `INTERNAL_ERROR` plus a FIN.

**When it breaks:** the client believes a job is stored that a restart
will not produce — the failure mode durability exists to prevent.

**Pinned by:** `testinject2.c` (`*_refuses_when_durable_and_wal_disabled`,
the group-commit failure tests), `testserv2.c` durable tests.

## #16 — One fdatasync per tick, and no ack overtakes it

Every WAL-dirty command in an epoll drain is staged without syncing;
one `walcommit` covers the batch, and the replies buffered meanwhile
are released by `dur_flush_all` afterwards — in wire order, each
exactly once.

The same per-conn buffer now serves a second purpose: coalescing a
pipelined burst's replies into one `write`. That gives it two
independent holds — durability says "not before the fdatasync",
pipelining says "not before the burst ends" — and the rule between them
is that whichever is released LAST does the writing. Durability
therefore always outranks the syscall saving: `conn_dispatch` leaves
the bytes alone when the durable hold is still on, and `dur_flush_all`
sends them after the commit.

**When it breaks:** either the group commit collapses to one fsync per
command (the cost the batching exists to avoid), or a reply overtakes
the sync it was waiting for, which is #14 again.

**Pinned by:** `testinject2.c` `dur_*` tests, including the short-write
and deferred-job-reply paths, and
`cttest_dur_pipelined_acks_never_outrun_the_commit` in `testserv2.c`,
which fails the commit and requires the burst to come back as
INTERNAL_ERROR rather than acked. A SIGKILL bench cannot pin this:
killing the process does not lose the page cache, so records acked a
moment too early still come back.
