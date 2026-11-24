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
