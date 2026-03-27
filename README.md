# beanstalkd (hardened fork)

Hardened, optimized fork of [beanstalkd](https://github.com/beanstalkd/beanstalkd) — simple, fast general purpose work queue.

Drop-in replacement for upstream: **100% wire protocol and WAL format compatibility**. Every existing client works unmodified.

See [doc/protocol.txt](doc/protocol.txt) for the network protocol.

## Quick start

    $ make
    $ ./beanstalkd

    $ make check           # run 197 unit tests
    $ make bench           # run benchmarks
    $ ./beanstalkd -h      # show all options

Requires Linux (2.6.17+), Mac OS X, FreeBSD, or Illumos. Any C99 compiler; tested with GCC and clang.

## Why this fork

| Area | Upstream | This fork |
|------|----------|-----------|
| Known crash/data-loss bugs | 22 open | 32 fixed |
| Scheduling complexity | O(tubes) per tick | O(1) via heaps |
| Syscalls per command | ~4 (read + epoll + write + epoll) | ~2 (read + write, inline flush) |
| Job hash table rehash | Stop-the-world O(n) | Incremental, 16 buckets/op |
| Job pool reuse | Exact size match only | 11 size classes, O(1) hit rate |
| WAL disk I/O | Single file chain, 1 fsync thread | Per-CPU sharded WAL, N fsync threads |
| Unit tests | 100 | 197 |

## What changed

### Bug fixes (32)

**Crashes (P0):**
NULL deref in conn_timeout, infinite loop in rawfalloc, WAL balance rollback corruption, unsafe `exit()` in signal handler, `EPOLLERR` causing 100% CPU, OOM in `prot_init` leading to SIGSEGV.

**Data loss (P1):**
Memory/job leaks in h_accept and enqueue_incoming_job, job_copy dangling pointer, WAL nrec increment on failure, corrupt WAL records silently skipped, walmaint errors silently ignored, prot_replay orphaning jobs and leaking WAL space, `enqueue_job` WAL failure leaving job in two structures, `release`/`bury` WAL failure orphaning jobs.

**Hardening (P2):**
Input validation for OP_KICK/OP_RESERVE_TIMEOUT, darwin/sunos event loop fixes, option parsing boundary checks, `-f` flag overflow, parallel make race, tube leak on `ms_append` failure.

**Clock:**
`clock_gettime(CLOCK_MONOTONIC)` replaces gettimeofday; NTP jumps no longer cause mass timeouts.

**WAL integrity:**
Directory fsync after binlog ops, `F_FULLFSYNC` on macOS, EINTR/short-read handling in readfull and rawfalloc.

### Performance

**O(1) scheduling** — five heaps replace linear scans:

| Structure | Lookup | Was |
|-----------|--------|-----|
| Matchable tube heap | Next job-to-connection match | O(tubes) |
| Delay tube heap | Nearest delayed deadline | O(tubes) |
| Pause tube heap | Soonest tube to unpause | O(tubes) |
| Waiting conn index | Remove conn from tube | O(conns) |
| Tube hash table | Find tube by name | O(tubes) |

**Syscall reduction** — measured via strace on 10K put+delete cycles:

| Optimization | Effect |
|-------------|--------|
| Inline reply flush | -50% epoll_wait, -99% epoll_ctl |
| Batch epoll (64 events) | ~64x fewer kernel transitions under concurrent load |
| `accept4(SOCK_NONBLOCK)` | Eliminates `fcntl` per connection |
| Per-tick time cache | -13 `clock_gettime` calls per tick |
| Vectorized WAL writes | 1 `writev` instead of 2-4 `write` per record |
| Async fsync pthread | Event loop never blocks on disk |
| TCP_NODELAY on accept | Eliminates Nagle's ~40 ms delay |

**Memory:**

| Optimization | Effect |
|-------------|--------|
| Size-classed job pool | O(1) reuse across varied sizes (11 buckets, 64 B .. 64 KB) |
| Incremental rehash | 16 buckets/op instead of stop-the-world O(n) |
| Periodic `malloc_trim` | Returns unused glibc pages to OS (configurable via `-m`) |
| Cache-friendly Job struct | Hot fields grouped; sizeof(Job) 176 → 168 bytes |

**WAL sharding** — parallel disk I/O across CPU cores:

When WAL is enabled (`-b`), jobs are distributed across N independent WAL instances (one per CPU core) by tube name hash. Each shard has its own directory, file chain, compaction, and async fsync thread. The event loop (single-threaded) routes `walwrite` to the correct shard in O(1). Shard count is detected from CPU cores on first run, then persisted to `<wal_dir>/shards` so routing stays stable across hardware changes.

| Metric | Value |
|--------|-------|
| Shard count | auto-detected from CPU cores, persisted on first run |
| Routing | `DJB2(tube_name) % nshards` — deterministic, O(1) |
| Aggregate throughput | ~33K ops/s with 8 concurrent clients + WAL + fsync=50ms |
| Legacy compatibility | Old `binlog.*` in root dir replayed at startup, new writes go to shard subdirs |

**Parsing:** first-byte command dispatch (~6x fewer strncmp), cached tube name_len, single-pass stats formatting. Default `-O2` (upstream: `-O0`).

### Stability

- **Bounded loops** — break on impossible states (NASA rule 2)
- **Accurate counters** — `delayed_ct`, `paused_ct`, `ready_ct` tracked on all paths including OOM and tube destruction
- **Graceful OOM** — heap insertion failures non-fatal, retried on next operation
- **Incremental rehash** — dual-table lookup during migration, no job invisible mid-rehash
- **Downscale hysteresis** — hash table never shrinks below initial capacity
- **Explicit process_queue** — no longer called implicitly from `enqueue_job`
- **EMFILE logging** — fd exhaustion logged for operator awareness

## Testing

### Unit tests

    $ make check

197 tests: 100 legacy + 97 hostile (OOM paths, boundary values, heap ordering, counter invariants, copy independence, WAL shard routing/recovery, stress up to 1000 elements).

### Benchmarks

    $ make bench

### Docker integration

    $ docker build -f test/Dockerfile.loadtest -t beanstalkd-test .
    $ docker run --rm beanstalkd-test

Three phases: AddressSanitizer (memory safety), Valgrind memcheck (leak detection), WAL crash recovery (`kill -9` + restart + verify).

## Configuration

All upstream flags preserved. New:

| Flag | Default | Description |
|------|---------|-------------|
| `-m SEC` | 60 | Return unused memory to OS every SEC seconds. `0` to disable. glibc only. |

Full list: `./beanstalkd -h`

### WAL sharding

When persistence is enabled (`-b DIR`), the WAL is automatically sharded across CPU cores:

```
DIR/
  shards          ← shard count (written once on first run)
  lock            ← root directory lock
  binlog.*        ← legacy WAL (read on startup for migration)
  s0/binlog.*     ← shard 0
  s1/binlog.*     ← shard 1
  ...
  sN/binlog.*     ← shard N-1
```

**Shard count stability.** The number of shards is auto-detected from CPU cores on first run and persisted to `DIR/shards`. On subsequent runs this file is read, keeping routing stable even if CPU count changes. If the file is missing, a new shard layout is created.

**Changing shard count.** To reshard (e.g., after hardware upgrade):

1. Stop beanstalkd
2. Delete `DIR/shards`
3. Start beanstalkd — it detects the new CPU count and creates a fresh shard layout
4. Existing shard WALs are replayed into the new layout automatically

**Disabling sharding.** Delete `DIR/shards` and all `DIR/s*/` directories. beanstalkd falls back to the single legacy WAL in `DIR/`.

## Project structure

| Directory | Contents |
|-----------|----------|
| `adm` | systemd, sysv, upstart, launchd configs |
| `ct` | Test framework (vendored from [kr/ct](https://github.com/kr/ct)) |
| `doc` | Protocol spec, man page |
| `pkg` | Release and packaging scripts |
| `test` | Docker-based integration tests |

## Links

- Upstream: https://beanstalkd.github.io/
- Protocol: [doc/protocol.txt](doc/protocol.txt)
