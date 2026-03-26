# beanstalkd (hardened fork)

Blazing fast, hardened fork of [beanstalkd](https://github.com/beanstalkd/beanstalkd) — simple and ultra-performant general purpose work queue.

This fork delivers significantly higher throughput and lower latency than the original through O(1) scheduling algorithms, minimal syscall overhead, and vectorized WAL writes. It fixes **22 confirmed bugs**, adds 92 hostile unit tests, and maintains **100% wire protocol and WAL format compatibility**. Every existing client works unmodified — drop-in replacement for upstream with immediate performance gains.

See [doc/protocol.txt](doc/protocol.txt) for details of the network protocol.

## What's different from upstream

### Bug fixes (22 total)

- **P0 crashes fixed** — NULL dereference in conn_timeout (SIGSEGV), infinite loop in rawfalloc, WAL balance rollback corruption
- **P1 leaks and data loss** — memory leak in h_accept, job leak in enqueue_incoming_job, job_copy dangling body pointer, WAL nrec increment on failure, corrupt WAL records silently skipped, walmaint errors silently ignored, prot_replay orphaned jobs and leaked WAL space
- **P2 hardening** — OP_KICK/OP_RESERVE_TIMEOUT input validation, darwin/sunos event loop fixes, option parsing boundary checks, integer overflow in -f flag, parallel make race condition
- **CLOCK_MONOTONIC** — replaced gettimeofday with clock_gettime(CLOCK_MONOTONIC); NTP jumps no longer cause mass job timeouts
- **Directory fsync** — WAL file creation and deletion followed by fsync(dirfd) to prevent crash-resurrection of unlinked binlogs

### Performance

- **O(1) job scheduling** — matchable tube heap replaces O(tubes) linear scan in process_queue
- **O(1) delayed job lookup** — global delay tube heap replaces O(tubes) scan per tick
- **O(1) waiting conn removal** — index-hinted ms_remove_at with swap-tracking callback
- **O(1) tube lookup** — hash table replaces linear scan for global tube lookup
- **Minimal syscalls** — 2 clock_gettime per tick (was ~15), single writev per WAL record (was 2-4 write calls)
- **Per-tick time cache** — global `now` variable eliminates redundant clock_gettime across all hot paths
- **Async fsync** — WAL fsync runs in a background pthread, so the event loop never blocks on disk I/O
- **TCP_NODELAY on client connections** — eliminates Nagle's ~40ms buffering delay on small replies
- **First-byte command dispatch** — switch on first character reduces average strncmp calls from ~12 to ~2
- **Cache-aligned Job struct** — hot fields (heap_index, tube, reserver) grouped adjacent to Jobrec; sizeof(Job) reduced 176 → 168 bytes, better L1 cache locality
- **Cached tube name length** — eliminates repeated strlen calls in WAL write and list-tubes paths
- **Consolidated process_queue** — single call at end of prottick replaces N scattered calls; eliminates redundant O(tubes) scans per tick
- **Single-pass formatting** — stats and list-tubes responses formatted directly into output buffer; no two-pass measure+format overhead
- **realloc in heap/ms growth** — allows in-place expansion without copy

### Stability

- **NASA-inspired loop safety** — bounded loops with break on impossible states, no infinite loop risk
- **Counter consistency** — delayed_ct, paused_ct, ready_ct tracked with matching increment/decrement on all paths including OOM and tube destruction
- **Graceful OOM degradation** — heap insertion failures are non-fatal and retried on next operation
- **Hash table resilience** — removed permanent OOM latch that blocked rehash forever after one failure; downscale hysteresis prevents thrashing at initial capacity
- **Orphan-safe process_queue** — re-enqueues job on impossible state instead of leaking it
- **Hidden side-effect removal** — process_queue no longer called implicitly from enqueue_job; all callers explicit
- **EMFILE visibility** — accept() EMFILE/ENFILE condition logged for operator awareness

### Testing

- **192 tests** (100 legacy + 92 hostile) — adversarial tests that cover edge cases, OOM paths, copy independence, boundary values, heap ordering, counter invariants, and stress scenarios
- **Docker-based integration testing** — ASan, Valgrind, and WAL crash recovery validation

## Quick Start

    $ make
    $ ./beanstalkd

also try,

    $ ./beanstalkd -h
    $ ./beanstalkd -VVV
    $ make -j4 CFLAGS=-O2
    $ make CC=clang
    $ make check
    $ make install
    $ make install PREFIX=/usr

Requires Linux (2.6.17+), Mac OS X, FreeBSD, or Illumos.
Any C99 compiler should work; tested with GCC and clang.

On Linux, the build links `-lpthread` (for the async fsync thread) and `-lrt` automatically.

## Tests

### Unit tests

Unit tests are in `test*.c`. Run them with:

    $ make check

The suite includes 100 legacy regression tests and 92 hostile tests covering edge cases, OOM paths, copy independence, boundary values, heap ordering, counter invariants, O(1) removal hints, and stress scenarios up to 1000 elements.

See https://github.com/kr/ct for information on the test framework.

### Docker integration tests

The Docker-based test suite runs a full load test with three phases:

1. **AddressSanitizer** — memory safety under load (500 put/reserve/delete cycles)
2. **Valgrind memcheck** — leak detection with definite/indirect leak checks
3. **WAL crash recovery** — insert 100 jobs, `kill -9`, restart, verify recovery

Run it with:

    $ docker build -f test/Dockerfile.loadtest -t beanstalkd-test .
    $ docker run --rm beanstalkd-test

## Subdirectories

- `adm` — files useful for system administrators
- `ct` — testing tool; vendored from https://github.com/kr/ct
- `doc` — documentation
- `pkg` — scripts to make releases
- `test` — Docker-based integration test suite

## Links

- Upstream: https://beanstalkd.github.io/
- Protocol: [doc/protocol.txt](doc/protocol.txt)
