# beanstalkd (hardened fork)

Hardened, optimized fork of [beanstalkd](https://github.com/beanstalkd/beanstalkd) — simple, fast general purpose work queue.

Drop-in replacement for upstream: **100% wire protocol and WAL format compatibility**. Every existing client works unmodified. Fixes 32 bugs, adds 92 tests, delivers lower latency through O(1) scheduling and reduced syscall overhead.

See [doc/protocol.txt](doc/protocol.txt) for the network protocol.

## Quick start

    $ make
    $ ./beanstalkd

    $ make check           # run 192 unit tests
    $ make bench           # run benchmarks
    $ ./beanstalkd -h      # show all options

Requires Linux (2.6.17+), Mac OS X, FreeBSD, or Illumos. Any C99 compiler; tested with GCC and clang.

## What's different from upstream

### Bug fixes (32)

- **P0 crashes** — NULL dereference in conn_timeout (SIGSEGV), infinite loop in rawfalloc, WAL balance rollback corruption, `exit()` in signal handler replaced with async-signal-safe `_exit()`, `EPOLLERR` handling prevents 100% CPU busy-loop, `prot_init` exits on default tube OOM instead of silent SIGSEGV
- **P1 data loss** — memory leak in h_accept, job leak in enqueue_incoming_job, job_copy dangling body pointer, WAL nrec increment on failure, corrupt WAL records silently skipped, walmaint errors silently ignored, prot_replay orphaned jobs and leaked WAL space, `enqueue_job` WAL failure now rolls back heap insertion (prevents job in two data structures), `release`/`bury` WAL failure restores job to reserved list (prevents orphan)
- **P2 hardening** — OP_KICK/OP_RESERVE_TIMEOUT input validation, darwin/sunos event loop fixes, option parsing boundary checks, integer overflow in `-f` flag, parallel make race condition, tube memory leak on `ms_append` failure
- **Clock** — replaced gettimeofday with `clock_gettime(CLOCK_MONOTONIC)`, NTP jumps no longer cause mass job timeouts
- **WAL integrity** — directory fsync after binlog creation/deletion, durable fsync via `F_FULLFSYNC` on macOS, `readfull` handles EINTR and short reads, `rawfalloc` EINTR no longer corrupts loop counter, `readrec` body_size consistency check fixed (was dead code)
- **Build** — strncpy buffer safety for GCC 9+ (`-Werror=stringop-truncation`)

### Performance

**Scheduling — O(1) algorithms replacing O(n) scans:**

- **Matchable tube heap** — finds next job-to-connection match in O(1), was O(tubes)
- **Delay tube heap** — finds nearest delayed deadline in O(1), was O(tubes)
- **Pause tube heap** — finds soonest tube to unpause in O(1), was O(tubes)
- **Waiting conn index** — removes waiting connection from tube in O(1) via `ms_remove_at`, was O(conns)
- **Tube hash table** — global tube-by-name lookup in O(1), was O(tubes)

**Syscall and I/O reduction:**

- **Batch epoll** — `epoll_wait` returns up to 64 events per syscall, ~64x fewer kernel transitions under load
- **Per-tick time cache** — global `now` eliminates ~13 redundant `clock_gettime` calls per tick
- **Vectorized WAL writes** — single `writev` per record instead of 2-4 `write` calls
- **Async fsync** — WAL fsync runs in a background pthread, event loop never blocks on disk
- **TCP_NODELAY on accept** — eliminates Nagle's ~40 ms buffering delay on accepted connections
- **Consolidated process_queue** — single call per tick instead of N scattered calls

**Memory:**

- **Size-classed job pool** — 11 power-of-2 buckets (64 B .. 64 KB) for O(1) reuse across varied body sizes, 512 entries per class, 8 MB total cap
- **Periodic malloc_trim** — returns unused glibc heap pages to OS, addresses RSS staying high after mass job deletion (configurable via `-m`, default 60 s)
- **Cache-friendly Job struct** — hot fields grouped adjacent, sizeof(Job) reduced from 176 to 168 bytes
- **In-place realloc** — heap and ms growth via `realloc` allows expansion without copy

**Command parsing:**

- **First-byte dispatch** — switch on `cmd[0]` reduces average strncmp calls from ~12 to ~2
- **Cached tube name_len** — eliminates repeated strlen in WAL write and list-tubes paths
- **Single-pass stats formatting** — direct write into output buffer, no measure-then-format

**Build:**

- Default `-O2` optimization (upstream builds at `-O0`)

### Stability

- **Bounded loops** — break on impossible states (NASA rule 2)
- **Accurate counters** — `delayed_ct`, `paused_ct`, `ready_ct` tracked with matching inc/dec on all paths including OOM and tube destruction
- **Graceful OOM** — heap insertion failures are non-fatal, retried on next operation
- **Incremental rehash** — job hash table migrates 16 buckets per operation instead of stop-the-world O(n), dual-table lookup during migration
- **Downscale hysteresis** — hash table never shrinks below initial capacity, prevents thrashing
- **Explicit process_queue** — no longer called implicitly from `enqueue_job`, all call sites visible
- **EMFILE logging** — `accept()` file descriptor exhaustion logged for operator awareness

### Testing

- **192 unit tests** — 100 legacy + 92 hostile (edge cases, OOM paths, copy independence, boundary values, heap ordering, counter invariants, stress up to 1000 elements)
- **Docker integration** — AddressSanitizer (memory safety), Valgrind memcheck (leak detection), WAL crash recovery (`kill -9` + restart + verify)

## Configuration

All upstream flags are preserved. New flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-m SEC` | 60 | Return unused memory to OS every SEC seconds. `0` to disable. Only effective on glibc. |

Full flag list: `./beanstalkd -h`

## Testing

### Unit tests

    $ make check

### Benchmarks

    $ make bench

### Docker integration tests

    $ docker build -f test/Dockerfile.loadtest -t beanstalkd-test .
    $ docker run --rm beanstalkd-test

Runs three phases: AddressSanitizer (memory safety), Valgrind memcheck (leak detection), WAL crash recovery (`kill -9` + restart + verify).

## Project structure

| Directory | Contents |
|-----------|----------|
| `adm` | Files for system administrators |
| `ct` | Test framework (vendored from [kr/ct](https://github.com/kr/ct)) |
| `doc` | Protocol documentation, man page |
| `pkg` | Release scripts |
| `test` | Docker-based integration tests |

## Links

- Upstream: https://beanstalkd.github.io/
- Protocol: [doc/protocol.txt](doc/protocol.txt)
