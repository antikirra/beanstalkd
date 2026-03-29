# beanstalkd

Linux-native, multi-core work queue in C99. Single binary, zero dependencies.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd) with 35 bug fixes, multi-process architecture, and per-syscall optimizations. Wire protocol and WAL format are unchanged — existing client libraries work unmodified.

> **Breaking change:** each connection watches exactly one tube. The `watch` command switches the tube (not accumulates). `use` and `watch` are independent — `use` sets the producing tube locally, `watch` migrates the connection to the consuming tube's worker. See [Single-tube watch](#single-tube-watch) below.

## Requirements

**Linux kernel 6.1+**, glibc, gcc. No cross-platform support — the codebase uses Linux APIs directly without abstractions or `#ifdef` guards.

Linux APIs used: `epoll_create1`, `accept4`, `fallocate`, `fdatasync`, `CLOCK_MONOTONIC_COARSE`, `SOCK_NONBLOCK|SOCK_CLOEXEC`, `O_CLOEXEC`, `TCP_FASTOPEN`, `TCP_CORK`, `TCP_QUICKACK`, `SO_INCOMING_CPU`, `SO_TXREHASH`, `SO_REUSEPORT`, `sched_setaffinity`, `posix_fadvise`, `malloc_trim`, `SCM_RIGHTS`.

## Quick start

```sh
# Docker (any host OS):
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd

# Native (Linux 6.1+):
make && ./beanstalkd

# With WAL persistence and 4 workers:
./beanstalkd -b /var/lib/beanstalkd -w 4
```

## Build and test

```sh
make                                    # build
make check                              # 205 unit tests
make check-mw                           # 9 multi-worker integration tests
make bench                              # benchmarks
docker build -f Dockerfile.build .      # CI: UBSan + cppcheck
docker build -f Dockerfile.benchmark .  # A/B comparison vs upstream
```

CI pipeline (`Dockerfile.build`): 205 unit tests (UBSan) + 9 multi-worker integration tests + cppcheck.

Production image (`Dockerfile`): multi-stage build on bookworm-slim, LTO, `-Os -march=native -fvisibility=hidden`, full RELRO.

## Fork vs upstream

| | Upstream | This fork |
|---|----------|-----------|
| Platform | Linux, macOS, FreeBSD, SunOS | Linux 6.1+ only |
| Multi-tube watch | Yes (N tubes per connection) | No (1 tube per connection) |
| Multi-core | Single-threaded | N workers, one per CPU core |
| Scheduling | O(tubes) scan per tick | O(1) heaps + direct match |
| Syscalls per command | ~4 (read→epoll→write→epoll) | ~2 (read→write, inline flush) |
| Job hash rehash | Stop-the-world O(n) | Incremental, 16 buckets/op |
| Job pool | Exact size match | 11 size classes, O(1) reuse |
| WAL | Single file, 1 fsync thread | Per-worker, async fsync |
| Known crash/data bugs | 22 open | 35 fixed |
| Tests | 100 unit | 205 unit + 13 multi-worker integration |

## Single-tube watch

Upstream beanstalkd allows `watch`ing multiple tubes per connection. The `reserve` command scans all watched tubes to find the highest-priority ready job, requiring a global matchable heap — an inherently serial operation that cannot be partitioned across CPU cores.

This fork restricts each connection to one watched tube:

- `watch <tube>` — **switches** to `<tube>`, always replies `WATCHING 1`
- `ignore <tube>` — no-op if it's the current tube (cannot have zero), always replies `WATCHING 1`
- `reserve` — blocks on the single watched tube only
- `use <tube>` — changes producer tube **independently** of `watch` (no migration)

`use` and `watch` are fully independent, as in upstream. `use` determines where `put` sends jobs. `watch` determines where `reserve` receives jobs. A client can `use` one tube for producing and `watch` another for consuming. When the `use` tube is on a different worker, puts are transparently forwarded via Unix socket IPC.

**Client library compatibility:** all major libraries (Pheanstalk, Beaneater, go-beanstalk, beanstalkc) support multi-tube watch as a core feature. In this fork, calling `watch` multiple times switches the tube — only the last one is active. Clients that use multi-tube watch need one connection per tube. Run with `-V` to log tube switches for debugging.

```
# Before (upstream, one connection):
conn.watch('emails').watch('notifications').reserve()

# After (this fork, two connections):
conn1.watch('emails').reserve()        # connection 1
conn2.watch('notifications').reserve() # connection 2
```

## Multi-process architecture

By default, beanstalkd forks N worker processes (one per CPU core). Override with `-w N`.

**Master process:** monitors workers via `waitpid`, restarts on crash, forwards `SIGTERM`/`SIGUSR1`.

**Each worker:**
- Binds its own `SO_REUSEPORT` socket on port 11300
- Owns tubes deterministically: `tube_name_hash(name) % nworkers`
- Runs independent event loop (epoll, heaps, job hash table, counters)
- Has its own WAL directory (`<wal_dir>/wN/`)
- Allocates interleaved job IDs (worker K → 1+K, 1+K+N, 1+K+2N, ...)

**Connection routing:**
- `watch` migrates the connection to the tube's owner worker via `SCM_RIGHTS` fd passing (buffered pipeline data travels with the fd)
- `use` is local — does not migrate. `put` commands are forwarded to the tube's owner worker via `PutFwdMsg` (up to 32KB body, larger jobs enqueue locally)
- `peek <id>`, `stats-job <id>`, `kick-job <id>` are forwarded to the job's owner worker by ID routing (`(id-1) % nworkers`)
- `stats-tube`, `pause-tube`, `peek-ready/delayed/buried`, `kick` are forwarded to the tube's owner worker
- `stats` aggregates all workers' counters via `mmap`'d shared memory (1Hz publish)
- On accept, worker speculatively reads the first command; if `watch` targets a remote tube, the fd is migrated before creating a `Conn`
- Master monitors workers and restarts crashed ones with full mesh recovery (new socketpairs distributed via control pipe + `SCM_RIGHTS`)

**Single-process fallback:** `-t CPU` (pin to core), `-w 0`, `-w 1`, or 1 CPU detected.

## Bug fixes (35)

**Crashes:** NULL deref in conn_timeout, infinite loop in rawfalloc, WAL rollback corruption, unsafe `exit()` in signal handler, `EPOLLERR` causing 100% CPU, OOM in `prot_init` → SIGSEGV.

**Data loss:** job/memory leaks in h_accept and enqueue_incoming_job, job_copy dangling pointer, WAL nrec increment on failure, corrupt WAL records silently skipped, walmaint errors silently ignored, prot_replay orphaning jobs, `enqueue_job` WAL failure leaving job in two structures, `release`/`bury` WAL failure orphaning jobs, `delayed_ct` truncated to 32 bits, `pause-time-left` wraparound, WAL stats not aggregated across shards, `total_jobs_ct` incremented before enqueue check, heap OOM silently ignored, WAL reservation leak in kick/release paths.

**Hardening:** input validation for kick/reserve-timeout, option parsing overflow, EMFILE backpressure (deregister listen socket), `scan_line_end` bare `\r`, connsched heap corruption on OOM, snprintf negative return, stale fd after WAL file close, IPv6 stack overflow in verbose stats.

**WAL integrity:** directory fsync, `fdatasync()`, EINTR/short-read handling, `fallocate()` preallocation, partial replay recovery.

## Performance changes

**Scheduling:**
- O(1) heaps for delayed tubes, paused tubes, connection timeouts (was O(tubes) scan)
- Direct `process_tube()` matches jobs↔conns within a tube on enqueue/unpause (eliminates global matchable heap)
- `heapresift()` for in-place re-ordering (was remove+insert)

**Syscall reduction:**
- Inline reply flush: write directly in `reply()`, skip epoll round-trip
- `writev` fast path for job body (header+body in one syscall)
- Batch epoll: 64 events per `epoll_wait`, drain loop before next `prottick`
- `epoll_ctl` caching: skip when registered mode unchanged
- `TCP_CORK` only when pipelining detected (was unconditional)
- `TCP_QUICKACK` once per epoll event (was per-read)
- `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` — atomic, no fcntl
- `CLOCK_MONOTONIC_COARSE` — ~5ns vs ~25ns, cached per tick

**Parsing and formatting:**
- `reply_inserted`/`reply_using`/`reply_job` via `u64toa`+memcpy (no vsnprintf)
- `read_uint` manual base-10 (no strtoumax/locale)
- Tube name validation via 256-byte lookup table (no strspn)
- NUL injection check via `memchr` (no strlen)
- Command dispatch: first-byte switch, then branch by character (was linear TEST_CMD chain)

**Memory:**
- Size-classed job pool: 11 buckets (64B–64KB), O(1) reuse
- Conn slab pool: up to 256 recycled structs
- Incremental rehash: 16 buckets/op, dual-table lookup during migration
- Job struct: hot fields grouped, 176→168 bytes
- `MALLOC_ARENA_MAX=1` — single glibc arena (-39% RSS)
- Periodic `malloc_trim` returns pages to OS
- Inline `tube_dref`/`tube_iref`/`job_list_reset`/`job_list_is_empty`

**WAL:**
- Per-worker WAL directories with independent async fsync threads
- `writev` for WAL records (was 2-4 separate writes)
- `fallocate()` for O(1) preallocation
- Rate-limited compaction (1ms throttle)
- Async dirsync handoff to fsync thread
- Lock-free error check via `__atomic_load_n`

**Network:**
- `SO_REUSEPORT` for kernel-level load balancing across workers
- `TCP_FASTOPEN` — -1 RTT for new connections
- `SO_INCOMING_CPU` — bind RX path to pinned CPU
- CPU pinning via `sched_setaffinity` (`-t` flag)

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | disabled | WAL persistence (per-worker sharded) |
| `-f MS` | 50 | fsync interval (0 = every write) |
| `-F` | | Never fsync |
| `-l ADDR` | 0.0.0.0 | Listen address (`unix:` prefix for Unix socket) |
| `-p PORT` | 11300 | Listen port |
| `-z BYTES` | 65535 | Max job body size |
| `-s BYTES` | 10MB | WAL file size |
| `-u USER` | | Drop privileges |
| `-m SEC` | 60 | Memory trim interval (0 = disable) |
| `-t CPU` | disabled | Pin to CPU core (forces single-process) |
| `-w N` | auto | Workers (0/1 = single-process, auto = per CPU) |
| `-V` | | Increase verbosity |

## Benchmark

A/B comparison, identical compiler flags (`gcc -O2 -DNDEBUG`), WAL enabled:

```sh
docker build -f Dockerfile.benchmark -t bench . && docker run --rm bench
```

### Bare metal results

Server: Debian 12 (bookworm), kernel 6.1.0, 8 vCPU QEMU, 12GB RAM, SSD.
Both binaries: `gcc -O2 -DNDEBUG`. Fork: `-w 1` (single-process mode).

| Scenario | Metric | Upstream | Fork (`-w 1`) | Delta |
|----------|--------|----------|---------------|-------|
| Throughput (8 clients) | ops/s | 8,282 | 9,477 | **+14.4%** |
| Throughput (8 clients) | PUT ops/s | 2,666 | 3,618 | **+35.7%** |
| Multi-tube (20 tubes) | ops/s | 6,624 | 11,500 | **+73.6%** |
| 500 tubes × 100 jobs | ops/s | 27,203 | 42,961 | **+57.9%** |
| 500 tubes × 100 jobs | CPU time | 2.36s | 1.68s | **+28.8%** |
| Latency (single client) | Avg | 245µs | 102µs | **+58% faster** |
| Latency (single client) | P50 | 164µs | 95µs | **+42% faster** |
| Latency (single client) | P99 | 1,969µs | 187µs | **10.5x lower** |
| Latency (single client) | P99.9 | 11,721µs | 252µs | **46.5x lower** |
| Connection storm | conn/s | 7,381 | 8,873 | **+20.2%** |

## License

MIT. See [LICENSE](LICENSE).

Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
