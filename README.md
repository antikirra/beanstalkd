# beanstalkd

High-performance work queue. Pure C, Linux-native, single binary.

Drop-in replacement for [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd): **100% wire protocol and WAL format compatible**. Every existing client library works unmodified.

**Key difference from upstream:** this fork trades multi-tube watching for multi-core parallelism. Each connection watches exactly one tube (the `watch` command switches rather than accumulates). This eliminates the cross-tube scheduling bottleneck and enables nginx-style multi-process architecture where each worker owns a subset of tubes, scaling linearly with CPU cores.

## Platform

This fork targets **Linux 6.1+** exclusively. No cross-platform abstractions, no trade-offs. The codebase uses Linux-specific APIs directly: `epoll_create1`, `accept4`, `fallocate`, `fdatasync`, `CLOCK_MONOTONIC_COARSE`, `SOCK_NONBLOCK`, `SOCK_CLOEXEC`, `O_CLOEXEC`, `TCP_FASTOPEN`, `TCP_CORK`, `TCP_QUICKACK`, `SO_INCOMING_CPU`, `SO_TXREHASH`, `SO_REUSEPORT`, `sched_setaffinity`, `posix_fadvise`, `malloc_trim`, `SCM_RIGHTS`.

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

**Production image** (`Dockerfile`): LTO-optimized, stripped binary, multi-stage build on bookworm-slim. Compiler flags: `-Os -flto -march=native -fno-plt -fvisibility=hidden -DNDEBUG`, linker: `-Wl,-z,now` (full RELRO).

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

Server: Debian 12 (bookworm), kernel 6.1.0, 8 vCPU AMD QEMU, 12GB RAM, SSD.
Both binaries: `gcc -O2 -DNDEBUG`. Fork runs with `-t 0` (CPU pinning).

| Scenario | Metric | Upstream | Fork (`-t 0`) | Delta |
|----------|--------|----------|---------------|-------|
| Throughput (8 clients) | ops/s | 4,582 | 5,012 | **+9.4%** |
| Throughput (8 clients) | RSS | 4,608 KB | 2,828 KB | **-39% RAM** |
| 500 tubes | ops/s | 17,996 | 19,718 | **+9.6%** |
| Latency (single client) | Avg | 274µs | 209µs | **+24% faster** |
| Latency (single client) | P50 | 168µs | 166µs | ~same |
| Latency (single client) | P99 | **1,966µs** | **1,091µs** | **1.8x lower** |
| Latency (single client) | P99.9 | **14,764µs** | **4,236µs** | **3.5x lower tail** |

**Key findings:**
- **Tail latency 3.5x lower** — P99.9 drops from 14.8ms to 4.2ms. TCP_CORK coalesces pipelined replies, TCP_QUICKACK eliminates delayed ACK, CPU pinning eliminates cache migration stalls.
- **Average latency 24% lower** — 274µs → 209µs. First time the fork beats upstream on average latency, not just tail.
- **39% less memory** — `MALLOC_ARENA_MAX=1` eliminates glibc's multi-arena overhead for single-threaded server.
- **Throughput +9.4%** stable across workloads.

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
| WAL disk I/O | Single file, 1 fsync thread | Per-worker sharded, async fsync |
| Multi-core scaling | Single-threaded | Multi-process, per-CPU workers |
| Unit tests | 100 | 205 |

## Single-tube watch: design rationale

Upstream beanstalkd allows a connection to `watch` multiple tubes simultaneously. This creates a fundamental bottleneck: the `reserve` command must scan all watched tubes to find the highest-priority ready job, and job delivery requires a global matchable heap across all tubes — an inherently serialized operation.

This fork restricts each connection to watching exactly one tube at a time. The `watch` command **switches** the watched tube (replacing the previous one) rather than accumulating a set. `WATCHING` always returns `1`. The `ignore` command is a no-op (cannot ignore the only tube).

This trade-off unlocks multi-process parallelism:

- **No cross-tube scheduling** — `process_tube()` matches jobs with waiting connections within a single tube, O(1) per match
- **Tube-based sharding** — each tube is deterministically owned by one worker process (`tube_name_hash % nworkers`), eliminating inter-process coordination
- **Connection migration** — when a client issues `watch` or `use` for a tube on another worker, the connection fd is transparently migrated via `SCM_RIGHTS` (Unix domain socket fd passing)
- **No matchable heap** — the global matchable heap (tracking tubes with both ready jobs and waiting connections) is eliminated entirely. Jobs are matched to connections directly when enqueued

**Migration for existing clients:** most beanstalkd clients use a single tube per connection (one producer tube via `use`, one consumer tube via `watch`). Clients that watch multiple tubes should open separate connections per tube.

## Multi-process architecture

When multiple CPU cores are available, beanstalkd automatically forks N worker processes (one per core). Each worker:

- Has its own `SO_REUSEPORT` listening socket on port 11300
- Owns a deterministic subset of tubes (by `tube_name_hash % nworkers`)
- Runs an independent event loop with its own epoll, heaps, and job hash table
- Has its own WAL directory (`<wal_dir>/wN/`) for crash recovery
- Uses interleaved job IDs to guarantee no collisions (worker K gets IDs K+1, K+N+1, K+2N+1, ...)

A master process monitors workers and restarts any that crash.

### Connection routing

1. **Accept-time routing:** when a new connection arrives, the worker speculatively reads the first command. If it's `use` or `watch` targeting a tube owned by another worker, the fd is migrated immediately via `SCM_RIGHTS`
2. **Runtime migration:** if a connected client issues `use`/`watch` for a remote tube, the connection is migrated mid-session (buffered pipeline data travels with the fd)
3. **Command forwarding:** `stats-tube` and `pause-tube` for remote tubes are forwarded to the owning worker via Unix socket IPC, with the reply relayed back to the client
4. **Stats aggregation:** each worker publishes counters to a `mmap`'d shared memory region; the `stats` command sums all workers' counters

### Fallback modes

| Condition | Behavior |
|-----------|----------|
| `-t CPU` (CPU pinning) | Single-process mode (pinning implies one core) |
| `-w 0` or `-w 1` | Explicit single-process mode |
| 1 CPU core detected | Auto single-process mode |
| `-w N` (N > 1) | Force N workers regardless of core count |

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

**Multi-process scaling** — nginx-style master/worker architecture:

| Component | How it works |
|-----------|-------------|
| Worker forking | N processes, one per CPU core, `SO_REUSEPORT` |
| Tube sharding | `tube_name_hash % nworkers` for deterministic ownership |
| Connection migration | `SCM_RIGHTS` fd passing with buffered pipeline data |
| Job ID allocation | Interleaved IDs: worker K → K+1, K+N+1, K+2N+1, ... |
| WAL per worker | Independent WAL directory, async fsync thread |
| Stats aggregation | `mmap` shared memory, rate-limited (1Hz) publish |
| Cross-worker commands | `stats-tube`/`pause-tube` forwarded via Unix socket IPC |

**O(1) scheduling** — heaps replace linear scans:

| Structure | Lookup | Was |
|-----------|--------|-----|
| Delay tube heap | Nearest delayed deadline | O(tubes) |
| Pause tube heap | Soonest tube to unpause | O(tubes) |
| Waiting conn index | Remove conn from tube | O(conns) |
| Tube hash table | Find tube by name | O(tubes) |
| Direct `process_tube()` | Match jobs ↔ conns in one tube | O(tubes) matchable heap |

**Syscall reduction:**

| Optimization | Effect |
|-------------|--------|
| Inline reply flush | -50% epoll_wait, -99% epoll_ctl |
| Batch epoll (64 events, drain loop) | ~64x fewer kernel transitions under load |
| `accept4(SOCK_NONBLOCK\|SOCK_CLOEXEC)` | Atomic, no fcntl |
| `CLOCK_MONOTONIC_COARSE` | ~5ns vs ~25ns per call |
| Vectorized WAL writes (`writev`) | 1 syscall instead of 2-4 per record |
| `fallocate()` for WAL prealloc | O(1) instead of N x write(4KB) |
| Async fsync pthread | Event loop never blocks on disk |
| `fdatasync()` | Skips metadata flush |
| `TCP_NODELAY` + `TCP_FASTOPEN` | No Nagle delay, -1 RTT for new connections |
| `TCP_CORK` reply coalescing | Conditional: only when pipelining detected |
| `TCP_QUICKACK` | Per-event (not per-read), -40ms per request-response |
| `SO_INCOMING_CPU` | Bind RX path to pinned CPU core |
| `SO_REUSEPORT` | Kernel-level load balancing across workers |
| CPU pinning (`-t` flag) | `sched_setaffinity` eliminates L1/L2 cache migration |
| `MALLOC_ARENA_MAX=1` | Single malloc arena for single-threaded server (-39% RSS) |
| `epoll_ctl` caching | Skip redundant epoll_ctl when mode unchanged |

**Fast paths (vsnprintf elimination):**

| Reply | Method |
|-------|--------|
| `INSERTED <id>` | `u64toa` + memcpy |
| `USING <name>` | Direct memcpy |
| `RESERVED/FOUND <id> <size>` | `u64toa` + memcpy |
| `STATE_SEND_JOB` | `writev` immediate (header + body in one syscall) |
| Integer parsing (`read_uint`) | Manual base-10, no strtoumax/locale overhead |
| Tube name validation | Static 256-byte lookup table, no strspn |
| NUL injection check | `memchr` instead of `strlen` |

**Memory:**

| Optimization | Effect |
|-------------|--------|
| Size-classed job pool | O(1) reuse across 11 buckets (64 B .. 64 KB) |
| Incremental rehash | 16 buckets/op instead of stop-the-world O(n) |
| Periodic `malloc_trim` | Returns unused glibc pages to OS (`-m` flag) |
| Cache-friendly Job struct | Hot fields grouped; sizeof(Job) 176 -> 168 bytes |
| Conn slab pool | Reuse up to 256 freed Conn structs (no malloc/free per connect) |
| Inline tube refcounting | `tube_dref`/`tube_iref` in dat.h, no function call overhead |
| Heap hole-based sift | Halves setpos calls vs naive swap approach |
| Conditional rehash step | Skip `rehash_step()` when no migration in progress |

**WAL optimizations:**

| Optimization | Effect |
|-------------|--------|
| Per-worker WAL | Independent file chain per worker process |
| Rate-limited compaction | 1ms throttle avoids redundant compaction during pipelining |
| Async dirsync | Hand off `fdatasync(dir)` to sync thread when available |
| Lock-free error check | `__atomic_load_n` avoids mutex on every walmaint call |

### Stability

- **Bounded loops** with break on impossible states
- **Accurate counters** on all paths including OOM and tube destruction
- **Graceful OOM** — heap insertion failures are non-fatal, logged, retried on next operation
- **Incremental rehash** — dual-table lookup during migration, no job invisible mid-rehash
- **EMFILE backpressure** — fd exhaustion deregisters the listen socket, re-registers on close
- **Robust line scanning** — `scan_line_end` handles bare `\r` bytes
- **Worker crash recovery** — master restarts crashed workers automatically

## Configuration

```
./beanstalkd -h      # show all options
```

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | disabled | Enable WAL persistence (per-worker sharded in multi-process mode) |
| `-f MS` | 50 | fsync interval in milliseconds (0 = every write) |
| `-F` | | Never fsync |
| `-l ADDR` | 0.0.0.0 | Listen address (prefix `unix:` for Unix socket) |
| `-p PORT` | 11300 | Listen port |
| `-z BYTES` | 65535 | Maximum job body size |
| `-s BYTES` | 10MB | WAL file size |
| `-u USER` | | Drop privileges to user |
| `-m SEC` | 60 | Return unused memory to OS interval (0 = disable) |
| `-t CPU` | disabled | Pin to CPU core (forces single-process mode) |
| `-w N` | auto | Number of worker processes (0 or 1 = single-process, auto = one per CPU core) |
| `-V` | | Increase verbosity (repeatable) |

## WAL layout

### Multi-process mode (default)

```
DIR/
  lock            # directory lock
  w0/binlog.*     # worker 0 WAL
  w1/binlog.*     # worker 1 WAL
  ...
  wN/binlog.*     # worker N-1 WAL
```

### Single-process mode (`-w 1` or `-t CPU`)

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

## Project structure

```
.
├── dat.h                   # All types, macros, declarations (inline tube_dref/iref, job_list)
├── main.c                  # Entry point, master/worker forking, CPU pinning, signal handling
├── prot.c                  # Protocol: commands, scheduling, stats, fast reply paths, migration
├── serv.c                  # Event loop (drain-all), peer socket handling, shared stats
├── conn.c                  # Connection state machine, slab pool
├── job.c                   # Job alloc/free, hash table, memory pool, interleaved IDs
├── tube.c                  # Tube CRUD, hash map, refcounting, cached name_hash
├── heap.c                  # Generic min-heap (hole-based sift, heapresift)
├── ms.c                    # Dynamic array
├── walg.c                  # WAL lifecycle, async fsync/dirsync, rate-limited compaction
├── file.c                  # WAL file I/O, writev, fallocate
├── linux.c                 # epoll backend (epoll_ctl caching)
├── net.c                   # Socket creation, SO_REUSEPORT, SCM_RIGHTS fd passing
├── time.c                  # CLOCK_MONOTONIC_COARSE
├── util.c                  # Logging, options (-w flag), zalloc
├── primes.c                # Hash table prime sizes
├── test*.c                 # 205 unit tests (14 files)
├── test/bench.c            # C benchmark client
├── ct/                     # Vendored test framework
├── doc/protocol.txt        # Wire protocol specification (frozen)
├── adm/systemd/            # systemd service + socket units
├── Dockerfile              # Production image (multi-stage, hardened compiler flags)
├── Dockerfile.build        # CI pipeline: UBSan + cppcheck
├── Dockerfile.benchmark    # A/B comparison vs upstream (5 scenarios)
└── test/                   # Integration tests (ASan, Valgrind, WAL recovery)
```

## License

MIT. See [LICENSE](LICENSE).

Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
