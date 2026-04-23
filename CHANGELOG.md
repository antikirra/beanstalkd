# Changelog

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
