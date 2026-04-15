# beanstalkd

Linux-native work queue. Single C11 binary, requires only glibc.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd) with O(1) scheduling, per-syscall optimizations, and 48 bug fixes. Wire protocol unchanged — every existing client works unmodified. WAL format upgraded to v8 with per-record CRC32C; upstream v7 binlogs replay transparently on startup.

## Quick start

```sh
make && ./beanstalkd                          # in-memory
./beanstalkd -b /var/lib/beanstalkd           # with WAL persistence
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd
```

Requires **Linux 6.1+**, glibc, gcc 12+. Compatible with GCC 15.

## Compatibility

100% wire protocol compatible with upstream beanstalkd v1.13. No new commands, no changed responses, no modified stats fields. All client libraries (Go, Python, Ruby, PHP, Java, etc.) work without changes. WAL writer emits v8 (4-byte CRC32C trailer per record for silent-corruption detection); upstream v7 binlogs are read transparently on startup, so upgrade requires no migration. Downgrade to a pre-v8 binary is not supported. Legacy v5 (beanstalkd 1.4.6) reader removed — drain pre-v7 binlogs on the old binary before upgrading.

## Fork vs upstream

| | Upstream (v1.13) | This fork |
|---|---|---|
| Language standard | C99 | C11 (`_Static_assert`, `_Atomic`, `_Alignas`) |
| Platform | Linux, macOS, FreeBSD | Linux 6.1+ only (uses Linux APIs directly) |
| Architecture | Single-threaded | Single-threaded, Linux-optimized |
| Scheduling | O(tubes) scan per tick | O(1) heaps + direct match |
| Syscalls per command | ~4 | ~2 (inline reply flush) |
| Job hash rehash | Stop-the-world | Incremental, 16 buckets/op |
| Job memory | malloc/free per job | 11 size classes, O(1) pool reuse |
| WAL fsync | Synchronous | Async fsync thread |
| WAL compaction | Broken on release cycles | Fixed alive tracking |
| WAL integrity | None | CRC32C per record (v8 format, SSE4.2) |
| Crash/data bugs | 22+ open | 48 fixed |
| Tests | ~100 unit | 213 unit + hostile WAL + ASan + Valgrind |
| Status | Maintenance mode (last code 2020) | Active development |

## Build and test

```sh
make check                                # 213 unit tests (UBSan in CI)
docker build -f Dockerfile.build .        # CI: UBSan + cppcheck (C11)
docker build -f Dockerfile.benchmark .    # A/B benchmark vs upstream
docker build -f test/Dockerfile.loadtest -t loadtest . && docker run --rm loadtest
                                          # ASan + Valgrind + WAL crash recovery
```

Tested with GCC 12 (Debian bookworm) and GCC 15 (Debian sid). Production Dockerfile uses `-O2 -flto -fipa-pta -fno-plt -fvisibility=hidden -Wl,--gc-sections` with branch prediction hints on hot paths.

## Bug fixes (48)

**Crashes:**
NULL deref in conn_timeout, infinite loop in rawfalloc, WAL rollback corruption, unsafe signal handler exit, EPOLLERR 100% CPU, prot_init OOM SIGSEGV.

**Data loss:**
job leaks in h_accept/enqueue_incoming_job, job_copy dangling pointer, WAL nrec on failure, corrupt WAL records skipped, walmaint errors ignored, prot_replay orphans, enqueue_job WAL failure leaving job in two structures, release/bury WAL failure orphans, delayed_ct 32-bit truncation, pause-time-left wraparound, total_jobs_ct premature increment, heap OOM ignored, WAL reservation leak in kick/release.

**WAL durability:**
release with delay=0 priority lost on restart, delete creates ghost jobs on WAL failure, compaction blocked by phantom alive bytes from short records, buried job order scrambled on replay, bury_ct double-incremented on each restart, partial WAL file not cleaned up on write error.

**Error handling:**
kick-job returns NOT_FOUND on IO error instead of INTERNAL_ERROR, kick bulk spins forever on failure, kick_ct falsely incremented on failed kick, dirsync fdatasync errors silently ignored, async fsync errno lost before close().

**Hardening:**
input validation for kick/reserve-timeout, option parsing overflow, EMFILE backpressure, scan_line_end bare `\r`, connsched heap OOM, snprintf negative return, stale WAL fd, IPv6 stats overflow.

## Performance

**Scheduling:**
O(1) delay/pause/timeout heaps. Direct process_tube match on enqueue. Heapresift in-place. Heap grandchild prefetch.

**Syscall reduction:**
Inline reply flush (skip epoll round-trip). writev for job body. 64-event epoll batch with drain loop. epoll_ctl caching. TCP_CORK only on pipeline. TCP_QUICKACK at accept. accept4 atomic flags. CLOCK_MONOTONIC_COARSE per batch. Incremental scan_line_end. Reserve fast path (skip ms round-trip when job ready).

**Parsing:**
u64toa two-digit pair table. reply_inserted/reply_job backward build into reply_buf. Manual base-10 read_uint. 256-byte tube name lookup table (_Alignas(64) cache-line aligned). Two-level byte command dispatch.

**Memory:**
11-class job pool (64B-64KB, `__attribute__((malloc))`). Cache-line Conn/Tube struct layout. Conn slab pool (256). Incremental rehash (16 buckets/op, dual-table). DJB2+finalizer tube hash with hash-first filter. MALLOC_ARENA_MAX=1. Periodic malloc_trim.

**WAL:**
Async fsync thread (_Atomic error signaling). writev records with per-record CRC32C trailer (v8 format, Intel SSE4.2 `_mm_crc32_u64` ~0.33 cyc/byte, negligible overhead). fallocate prealloc. Rate-limited compaction with correct alive tracking. Lock-free error check. Readahead on recovery. _Static_assert guards on WAL record sizes. Transparent v7 binlog replay for upgrade from upstream.

**Network:**
TCP_FASTOPEN(1024). TCP_DEFER_ACCEPT. TCP_NOTSENT_LOWAT(16KB). TCP_USER_TIMEOUT(30s). SO_INCOMING_CPU. sched_setaffinity.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | — | WAL directory (enables persistence) |
| `-f MS` | 50 | fsync interval (0 = every write) |
| `-F` | | Never fsync |
| `-l ADDR` | 0.0.0.0 | Listen address (`unix:` for Unix socket) |
| `-p PORT` | 11300 | Listen port |
| `-z BYTES` | 65535 | Max job body size |
| `-s BYTES` | 10MB | WAL file size |
| `-u USER` | | Drop privileges |
| `-m SEC` | 60 | malloc_trim interval (0 = disable) |
| `-t CPU` | — | Pin to CPU core |
| `-V` | | Verbose logging (`-VV` for command trace) |

## Docker benchmark results

`docker build -f Dockerfile.benchmark .` — both binaries compiled with identical `gcc -O2 -DNDEBUG`, WAL enabled, fsync 50ms. Docker container, not bare metal.

| Scenario | Upstream | Fork | Delta |
|---|---|---|---|
| S1: Throughput (ops/s) | 124,795 | 207,857 | **+66.6%** |
| S1: P50 latency (us) | 2,243 | 1,298 | **-42.2%** |
| S1: P99.9 latency (us) | 13,364 | 5,913 | **-55.8%** |
| S2: Latency (ops/s) | 39,691 | 53,286 | **+34.3%** |
| S2: P50 latency (us) | 36.7 | 24.1 | **-34.3%** |
| S3: Large body 16KB (ops/s) | 83,138 | 116,528 | **+40.2%** |
| S4: 32 connections (ops/s) | 150,214 | 260,391 | **+73.3%** |
| S5: Deep pipeline (ops/s) | 104,255 | 146,907 | **+40.9%** |
| S6: 500 tubes (ops/s) | 42,822 | 42,444 | -0.9% |

**Scenarios:** S1: 8 conn x 10K put+reserve+delete, 128B body, pipeline=64. S2: 1 conn x 5K ops, 4B body, pipeline=1 (round-trip). S3: 8 conn x 2K ops, 16KB body, pipeline=16. S4: 32 conn x 5K ops, 128B body, pipeline=32. S5: 1 conn x 20K ops, 128B body, pipeline=256. S6: 500 tubes x 100 jobs each, 4 clients, 16-256B mixed bodies.

## License

MIT. See [LICENSE](LICENSE). Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
