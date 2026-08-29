# Changelog

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
