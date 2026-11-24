# Experiment log

One entry per iteration, accepted and rejected alike (AGENTS.md, rule 7).
Environment A: Docker on macOS, aarch64, Debian bookworm, gcc 12.

## E-0001 B00 wall-clock timestamps in the WAL — ACCEPTED (2026-11-24)
- Hypothesis: translating created_at/deadline_at to wall-clock time at
  the file boundary fixes delayed jobs after a reboot and after an
  upgrade from upstream, without touching the record layout or the
  scheduler's clock, at no measurable cost per write.
- Environment: A. Reboots emulated with `unshare --time --monotonic=N`.
- `put 0 30 60`, kill -9, replay, `stats-job` 3 s after the put
  (time-left / age), before -> after:
  - upstream binlog -> fork: 1794738220 / -1794738190 -> 26 / 3
  - reboot, uptime 30 d -> 60 s: 2591966 / -2591936 -> 26 / 3
  - reboot, uptime 60 s -> 30 d: (not measured before) -> 26 / 3
  - plain restart: correct -> 26 / 3
  - binlog of the previous build, reboot 30 d -> 60 s: -> 29 / 0
  - binlog of the previous build, same boot: -> 26 / 3
- Server CPU per put, 320000 pipelined puts at depth 32, async WAL on
  tmpfs, 3 runs: 0.813 / 0.813 / 0.844 -> 0.813 / 0.844 / 0.844 us
  (one clock tick of /proc stat is 0.031 us here: no difference).
- Mutations, each red in testfile_waltime.c: no clamp to delay; created_at
  not translated on write; deadline_at not translated on write; future
  created_at kept; wall-clock deadline not translated on read; wrapping
  instead of saturating add.
- Not covered: x86-64; a real reboot of a VM (time namespaces only).

## E-0002 B02 leak bench self-check — ACCEPTED (2026-11-24)
- Hypothesis: the probe is deleted by the optimiser; a volatile store
  makes LeakSanitizer report it, on aarch64 too.
- Environment: A.
- bench/leak/run.sh: SKIP (exit 2) -> PASS on the tree.
- Mutation: malloc(77) planted in make_conn -> FAIL, 11704 bytes in 152
  allocations.
- In the gate since this entry (Dockerfile.build, 3.7 s).

## E-0003 B06 hardening flags in the production image — ACCEPTED (2026-11-25)
- Hypothesis: canary, fortify, stack-clash and CET/PAC cost under 2% of
  server CPU per command.
- Environment: A (arm64, so PAC is measured, CET is not).
- Method: wall-clock CPU/op was bimodal on this host (0.407 / 0.447 us
  for the same binary, 7 runs), so user-space instructions per command
  were counted under callgrind instead: two runs agree to 0.1.
- Instructions/command, put -> reserve -> delete, 64-byte bodies, no WAL:
  - depth 32: 849.4 -> 901.8 (+6.2%); canary 870.2, fortify 860.5,
    PAC 872.6, stack-clash 844.2
  - serial:   1119.9 -> 1207.6 (+7.8%); canary 1168.8, fortify 1131.9,
    PAC 1155.9, stack-clash 1110.6
- Total CPU/op for scale: 0.41 us at depth 32, ~8 us serial (kernel
  dominated), so +53 / +88 instructions is under 2% / 0.5%.
- With the WAL the callgrind count is not repeatable (3.7k..18k per
  command across runs of one binary); not used.
- Mutation: flags removed from the Dockerfile -> its hardening check
  fails the image build.
- Not covered: x86-64 cost of CET.

## E-0004 B01 trim only when it pays — ACCEPTED, partly (2026-11-25)
- Hypothesis: gating malloc_trim on bytes released since the last trim
  removes the periodic stall without losing the return of memory.
- Environment: A. bench/stall/run.sh, 600000 x 200 B, -m 1, 3 runs.
- Replies slower than 5 ms in a 6 s window: 6 / 6 / 6 -> 1 / 1 / 1
  (max 35.6 -> 34.6 ms: the single remaining walk).
- RSS with 120 scattered jobs left: 6.7-8.4 MB -> 6.7-8.2 MB
  (never-trim mutant: 264.2 MB).
- Instructions/command (callgrind): p32 849.4 -> 849.2, serial 1119.9 ->
  1116.8.
- Mutations: always trim -> bench FAIL (6 stalls) and 2 unit tests red;
  never trim -> bench FAIL (264 MB) and 2 unit tests red; counter not
  reset, free not counted, pooled counted as released -> 1 unit test
  red each (testprot_trim.c).
- Not achieved: the backlog's "max pause < 1 ms". One O(heap) walk per
  mass release remains; glibc has no incremental trim. Revisit if a
  production pause is traced to it: options are -m 0 by default (user
  decision) or an allocator with background purging via LD_PRELOAD.
- Side finding: when every job is deleted glibc shrinks the heap by
  itself (6.8 MB without any trim); the trim matters only when
  survivors pin the top of the heap. The bench's second phase was
  changed to that case after its first version passed a never-trim
  build.

## E-0005 B10 batch WAL records into one write per burst — REJECTED (2026-11-25)
- Hypothesis: staging a burst's records in a buffer and writing them
  once cuts server CPU per command with the WAL on.
- Environment: A. Prototype outside the tree: 64 KB buffer in
  writev_all, flushed at walcommit and on close (no flush-before-ack
  ordering, so this is the upper bound of the gain).
- Server CPU per command, put -> reserve -> delete, async WAL on tmpfs,
  5 runs each, median (min..max):
  - depth 32: 0.678 (0.667..0.722) -> 0.544 (0.500..0.600) us, -20%
  - depth 8:  1.583 (1.400..1.783) -> 1.450 (1.233..1.500) us, inside the spread
  - serial:   no difference (8.5..10.7 both)
- Decision: not built. The gain exists only for deep pipelines with the
  WAL on; a client that sends one command and waits gets nothing, and
  that is what producers and workers do. The price is a staging buffer,
  its flush-before-ack rule and its failure handling in the one path
  where a mistake loses acknowledged jobs — the reason bench/syscalls
  gives for rejecting the same idea under -D, which this measurement
  agrees with at depth 8 (~8%).
- Revisit when: a production workload pipelines with -b at depth >= 16,
  or acks are to be held to the end of the tick in the default mode too
  (group write across connections) — that would help serial clients
  under concurrency, and is a behaviour change to be decided first.

## E-0006 B31 systemd notify, watchdog, sandbox — ACCEPTED (2026-11-25)
- Environment: debian:bookworm container running systemd 252 as PID 1
  (privileged), aarch64.
- systemd-analyze security: 9.5 UNSAFE -> 1.7 OK.
- Under the new unit: start returns after READY=1; put with -b writes
  to /var/lib/beanstalkd (0700, owned by User=); -t 0 pins (Cpus_allowed
  0); CapEff 0, NoNewPrivs 1, seccomp filter on; stop drains and exits
  in 25 s with Result=success.
- Watchdog (WatchdogSec=5s for the test): 12 s alive with NRestarts=0;
  kill -STOP -> "Killing process with signal SIGABRT", Result=watchdog,
  NRestarts=1, both jobs present after the restart.
- Control: the previous binary under the same unit ->
  "failed because a timeout was exceeded" (it never sends READY).
- Not covered: x86-64; a binlog large enough to make replay slow.
