# beanstalkd

Linux-native work queue. Single C99 binary, zero dependencies.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd): O(1) scheduling, per-syscall optimizations, 35 bug fixes. Wire protocol and WAL format unchanged — existing clients work unmodified.

## Quick start

```sh
make && ./beanstalkd                          # in-memory
./beanstalkd -b /var/lib/beanstalkd           # with WAL persistence
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd
```

Requires **Linux 6.1+**, glibc, gcc. Uses Linux APIs directly: `epoll`, `accept4`, `fallocate`, `fdatasync`, `TCP_FASTOPEN`, `TCP_DEFER_ACCEPT`, `TCP_CORK`, `sched_setaffinity`, `CLOCK_MONOTONIC_COARSE`.

## Fork vs upstream

| | Upstream | This fork |
|---|---|---|
| Architecture | Single-threaded | Single-threaded, Linux-optimized |
| Scheduling | O(tubes) scan per tick | O(1) heaps + direct match |
| Syscalls | ~4 per command | ~2 (inline reply flush) |
| Job hash rehash | Stop-the-world | Incremental, 16 buckets/op |
| Job pool | Exact size match | 11 size classes, O(1) reuse |
| WAL | Single file chain, sync fsync | Single file chain, async fsync |
| Crash/data bugs | 22 open | 35 fixed |
| Tests | 100 unit | 200+ unit |
| Platform | Linux, macOS, FreeBSD | Linux 6.1+ only |

## Build and test

```sh
make check                              # unit tests (UBSan in CI)
docker build -f Dockerfile.build .      # full CI: UBSan + cppcheck
docker build -f Dockerfile.benchmark .  # A/B benchmark vs upstream
```

## Bug fixes (35)

**Crashes:** NULL deref in conn_timeout, infinite loop in rawfalloc, WAL rollback corruption, unsafe signal handler exit, EPOLLERR 100% CPU, prot_init OOM SIGSEGV.

**Data loss:** job leaks in h_accept/enqueue_incoming_job, job_copy dangling pointer, WAL nrec on failure, corrupt WAL records skipped, walmaint errors ignored, prot_replay orphans, enqueue_job WAL failure leaving job in two structures, release/bury WAL failure orphans, delayed_ct 32-bit truncation, pause-time-left wraparound, total_jobs_ct premature increment, heap OOM ignored, WAL reservation leak in kick/release.

**Hardening:** input validation for kick/reserve-timeout, option parsing overflow, EMFILE backpressure, scan_line_end bare `\r`, connsched heap OOM, snprintf negative return, stale WAL fd, IPv6 stats overflow.

## Performance

**Scheduling:** O(1) delay/pause/timeout heaps; direct process_tube match on enqueue; heapresift in-place; heap grandchild prefetch.

**Syscall reduction:** inline reply flush (skip epoll round-trip); writev for job body; 64-event epoll batch with drain loop; epoll_ctl caching; TCP_CORK only on pipeline; TCP_QUICKACK at accept; accept4 atomic flags; CLOCK_MONOTONIC_COARSE per batch; incremental scan_line_end; reserve fast path (skip ms round-trip when job ready).

**Parsing:** u64toa two-digit pair table; reply_inserted/reply_job backward build into reply_buf; manual base-10 read_uint; 256-byte tube name lookup table; two-level byte command dispatch.

**Memory:** 11-class job pool (64B-64KB); cache-line Conn/Tube struct layout; Conn slab pool (256); incremental rehash (16 buckets/op, dual-table); DJB2+finalizer tube hash with hash-first filter; MALLOC_ARENA_MAX=1; periodic malloc_trim.

**WAL:** async fsync thread; writev records; fallocate prealloc; rate-limited compaction; lock-free error check; readahead on recovery.

**Network:** TCP_FASTOPEN(1024); TCP_DEFER_ACCEPT; TCP_NOTSENT_LOWAT(16KB); TCP_USER_TIMEOUT(30s); SO_INCOMING_CPU; sched_setaffinity.

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
| `-V` | | Verbose logging |

## License

MIT. See [LICENSE](LICENSE). Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
