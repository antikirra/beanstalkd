# beanstalkd

High-performance work queue. Pure C, Linux-native, single binary.

Drop-in replacement for [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd): **100% wire protocol and WAL format compatible**. Every existing client library works unmodified.

## Platform

This fork targets **Linux 6.1+** exclusively. No cross-platform abstractions, no trade-offs. The codebase uses Linux-specific APIs directly: `epoll_create1`, `accept4`, `fallocate`, `fdatasync`, `CLOCK_MONOTONIC_COARSE`, `SOCK_NONBLOCK`, `SOCK_CLOEXEC`, `O_CLOEXEC`, `TCP_FASTOPEN`, `posix_fadvise`, `malloc_trim`.

Three ways to run:

| Method | Command |
|--------|---------|
| **Docker** (recommended) | `docker build -t beanstalkd .` then `docker run -p 11300:11300 beanstalkd` |
| **Native Linux host** | `make && ./beanstalkd` (requires Linux 6.1+, glibc, gcc) |
| **Linux VM** | Same as native, inside any VM with a 6.1+ kernel |

## Build and test

```sh
# Full pipeline: build + 205 unit tests (UBSan) + cppcheck (works on any host OS):
docker build -f Dockerfile.build .

# Native (Linux 6.1+ only):
make            # build
make check      # 205 unit tests
make bench      # benchmarks

# Integration tests (ASan + Valgrind + WAL crash recovery):
docker build -f test/Dockerfile.loadtest -t beanstalkd-test .
docker run --rm beanstalkd-test
```

**CI pipeline** (`Dockerfile.build`):
1. **Build + 205 unit tests** under UndefinedBehaviorSanitizer (traps on first UB)
2. **cppcheck** static analysis (zero false-positive policy)

**Production image** (`Dockerfile`): LTO-optimized, stripped binary (~68KB), multi-stage build on bookworm-slim.

## Benchmark

Reproducible A/B comparison — upstream vs fork, identical compiler flags (`gcc -O2 -DNDEBUG`), sequential isolated runs inside a single container:

```sh
docker build -f Dockerfile.benchmark -t beanstalkd-bench .
docker run --rm beanstalkd-bench
```

Five scenarios with WAL persistence enabled (`-b`, fsync every 50ms):

| # | Scenario | Workload | What it measures |
|---|----------|----------|------------------|
| 1 | Throughput | 8 clients × 10K jobs, 128B body | Peak aggregate ops/s |
| 2 | Multi-tube | 20 tubes, mixed 8B-8KB bodies | Realistic heterogeneous load |
| 3 | Massive tubes | 500 tubes × 100 jobs, 4 clients | O(1) vs O(n) scheduling |
| 4 | Latency | 1 client, 5K cycles, 4B body | P50/P99/P99.9 round-trip |
| 5 | Connection storm | 200 connect+stats+close, 8 threads | Accept path efficiency |

### Bare metal results

Server: Debian 12 (bookworm), kernel 6.1.0, 8 vCPU AMD QEMU @ 3.1 GHz, 12GB RAM, SSD.
Both binaries: `gcc -O2 -DNDEBUG`, identical flags.

| Scenario | Metric | Upstream | Fork | Delta |
|----------|--------|----------|------|-------|
| Throughput (8 clients) | ops/s | 7,599 | 7,932 | **+4.4%** |
| Throughput (8 clients) | PUT ops/s | 2,922 | 3,150 | **+7.8%** |
| Multi-tube (20 tubes) | ops/s | 8,888 | 9,157 | **+3.0%** |
| 500 tubes | ops/s | 30,983 | 33,841 | **+9.2%** |
| 500 tubes | CPU time | 1.77s | 1.61s | **-9% CPU** |
| Latency (single client) | P50 | 135µs | 136µs | ~same |
| Latency (single client) | P99.9 | **1,595µs** | **228µs** | **7x lower tail** |

**Key finding: tail latency.** P99.9 drops from 1.6ms to 228µs — a **7x improvement** on bare metal. This means the fork eliminates latency spikes that upstream suffers under load. The inline reply flush and batched epoll prevent the event loop from stalling on occasional slow syscalls.

Throughput scales with tube count: at 500 tubes, O(1) heaps deliver +9% over upstream's O(n) scans. On workloads with thousands of tubes the gap widens further.

Trade-off: WAL sharding allocates per-CPU file chains, increasing RSS when persistence is enabled (~16MB vs ~5MB with 8 shards × 10MB WAL files). At 500 tubes without large WAL files, RSS is comparable (+22%).

### Docker results

Same benchmark inside `docker run` (bookworm-slim, ARM64 host):

| Scenario | Metric | Upstream | Fork | Delta |
|----------|--------|----------|------|-------|
| Throughput | ops/s | 11,270 | 12,785 | +13% |
| 500 tubes | ops/s | 38,652 | 50,268 | **+30%** |
| Latency | P99.9 | 714µs | 305µs | **2.3x lower** |

## Why this fork

| Area | Upstream | This fork |
|------|----------|-----------|
| Known crash/data-loss bugs | 22 open | 35 fixed |
| Scheduling complexity | O(tubes) per tick | O(1) via heaps |
| Syscalls per command | ~4 (read + epoll + write + epoll) | ~2 (read + write, inline flush) |
| Job hash table rehash | Stop-the-world O(n) | Incremental, 16 buckets/op |
| Job pool reuse | Exact size match only | 11 size classes, O(1) hit rate |
| WAL disk I/O | Single file, 1 fsync thread | Per-CPU sharded, N fsync threads |
| Unit tests | 100 | 205 |

## What changed

### Bug fixes (35)

**Crashes (P0):**
NULL deref in conn_timeout, infinite loop in rawfalloc, WAL balance rollback corruption, unsafe `exit()` in signal handler, `EPOLLERR` causing 100% CPU, OOM in `prot_init` leading to SIGSEGV.

**Data loss (P1):**
Memory/job leaks in h_accept and enqueue_incoming_job, job_copy dangling pointer, WAL nrec increment on failure, corrupt WAL records silently skipped, walmaint errors silently ignored, prot_replay orphaning jobs and leaking WAL space, `enqueue_job` WAL failure leaving job in two structures, `release`/`bury` WAL failure orphaning jobs, `delayed_ct` stats truncated to 32 bits, `pause-time-left` uint64 wraparound, WAL stats not aggregated across shards, `total_jobs_ct` incremented before enqueue check, OOM in heap updates silently ignored.

**Hardening (P2):**
Input validation for OP_KICK/OP_RESERVE_TIMEOUT, option parsing boundary checks, `-f` flag overflow, tube leak on `ms_append` failure, EMFILE busy loop (listen socket deregistration + auto re-add), `scan_line_end` bare `\r` handling.

**WAL integrity:**
Directory fsync after binlog ops, `fdatasync()` for data-only durability, EINTR/short-read handling in readfull, `fallocate()` for O(1) WAL preallocation, partial recovery in `prot_replay` (no longer aborts on first failure).

### Performance

**O(1) scheduling** — five heaps replace linear scans:

| Structure | Lookup | Was |
|-----------|--------|-----|
| Matchable tube heap | Next job-to-connection match | O(tubes) |
| Delay tube heap | Nearest delayed deadline | O(tubes) |
| Pause tube heap | Soonest tube to unpause | O(tubes) |
| Waiting conn index | Remove conn from tube | O(conns) |
| Tube hash table | Find tube by name | O(tubes) |

**Syscall reduction:**

| Optimization | Effect |
|-------------|--------|
| Inline reply flush | -50% epoll_wait, -99% epoll_ctl |
| Batch epoll (64 events) | ~64x fewer kernel transitions under load |
| `accept4(SOCK_NONBLOCK\|SOCK_CLOEXEC)` | Atomic, no fcntl |
| `CLOCK_MONOTONIC_COARSE` | ~5ns vs ~25ns per call |
| Vectorized WAL writes (`writev`) | 1 syscall instead of 2-4 per record |
| `fallocate()` for WAL prealloc | O(1) instead of N x write(4KB) |
| Async fsync pthread | Event loop never blocks on disk |
| `fdatasync()` | Skips metadata flush |
| `TCP_NODELAY` + `TCP_FASTOPEN` | No Nagle delay, -1 RTT for new connections |

**Memory:**

| Optimization | Effect |
|-------------|--------|
| Size-classed job pool | O(1) reuse across 11 buckets (64 B .. 64 KB) |
| Incremental rehash | 16 buckets/op instead of stop-the-world O(n) |
| Periodic `malloc_trim` | Returns unused glibc pages to OS (`-m` flag) |
| Cache-friendly Job struct | Hot fields grouped; sizeof(Job) 176 -> 168 bytes |

**WAL sharding** — parallel disk I/O across CPU cores:

When WAL is enabled (`-b`), jobs are distributed across N independent WAL instances (one per CPU core) by tube name hash. Each shard has its own directory, file chain, compaction, and async fsync thread. Shard count is detected on first run and persisted to `<wal_dir>/shards`.

### Stability

- **Bounded loops** with break on impossible states
- **Accurate counters** on all paths including OOM and tube destruction
- **Graceful OOM** — heap insertion failures are non-fatal, logged, retried on next operation
- **Incremental rehash** — dual-table lookup during migration, no job invisible mid-rehash
- **EMFILE backpressure** — fd exhaustion deregisters the listen socket, re-registers on close
- **Robust line scanning** — `scan_line_end` handles bare `\r` bytes

## Configuration

```
./beanstalkd -h      # show all options
```

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | disabled | Enable WAL persistence (auto-sharded across CPU cores) |
| `-f MS` | 50 | fsync interval in milliseconds (0 = every write) |
| `-F` | | Never fsync |
| `-l ADDR` | 0.0.0.0 | Listen address (prefix `unix:` for Unix socket) |
| `-p PORT` | 11300 | Listen port |
| `-z BYTES` | 65535 | Maximum job body size |
| `-s BYTES` | 10MB | WAL file size |
| `-u USER` | | Drop privileges to user |
| `-m SEC` | 60 | Return unused memory to OS interval (0 = disable) |
| `-V` | | Increase verbosity (repeatable) |

## WAL sharding layout

```
DIR/
  shards          # shard count (persisted on first run)
  lock            # directory lock
  binlog.*        # legacy WAL (read on startup for migration)
  s0/binlog.*     # shard 0
  s1/binlog.*     # shard 1
  ...
  sN/binlog.*     # shard N-1
```

To reshard after hardware change: stop beanstalkd, delete `DIR/shards`, restart. Existing WALs are replayed into the new layout automatically.

## Project structure

```
.
├── dat.h                   # All types, macros, declarations
├── main.c                  # Entry point, signals, privilege drop
├── prot.c                  # Protocol: commands, scheduling, stats
├── serv.c                  # Event loop
├── conn.c                  # Connection state machine
├── job.c                   # Job alloc/free, hash table, memory pool
├── tube.c                  # Tube CRUD, hash map, refcounting
├── heap.c                  # Generic min-heap
├── ms.c                    # Dynamic array
├── walg.c                  # WAL lifecycle, async fsync, sharding
├── file.c                  # WAL file I/O, writev, fallocate
├── linux.c                 # epoll backend
├── net.c                   # Socket creation, TCP_FASTOPEN
├── time.c                  # CLOCK_MONOTONIC_COARSE
├── util.c                  # Logging, options, zalloc
├── primes.c                # Hash table prime sizes
├── test*.c                 # 205 unit tests (14 files)
├── ct/                     # Vendored test framework
├── doc/protocol.txt        # Wire protocol specification (frozen)
├── adm/systemd/            # systemd service + socket units
├── Dockerfile              # Production image (multi-stage, bookworm-slim)
├── Dockerfile.build        # CI pipeline: UBSan + cppcheck
└── test/                   # Integration tests (ASan, Valgrind, WAL recovery)
```

## License

MIT. See [LICENSE](LICENSE).

Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
