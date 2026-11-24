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
