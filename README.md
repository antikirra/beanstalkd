# beanstalkd

Linux-native, multi-core work queue. Single C99 binary, zero dependencies.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd): multi-process architecture, O(1) scheduling, per-syscall optimizations, 35 bug fixes. Wire protocol and WAL format unchanged — existing clients work unmodified.

> **Breaking change:** each connection watches one tube. `watch` switches (not accumulates). See [Single-tube watch](#single-tube-watch).

## Quick start

```sh
make && ./beanstalkd                          # single process
./beanstalkd -b /var/lib/beanstalkd -w 4      # 4 workers + WAL
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd
```

Requires **Linux 6.1+**, glibc, gcc. Uses Linux APIs directly: `epoll`, `accept4`, `fallocate`, `fdatasync`, `SCM_RIGHTS`, `SO_REUSEPORT`, `TCP_FASTOPEN`, `TCP_DEFER_ACCEPT`, `TCP_CORK`, `sched_setaffinity`, `CLOCK_MONOTONIC_COARSE`.

## Benchmark

Server: Debian 12, kernel 6.1.0-44, 8 vCPU, 12GB RAM, SSD. Both binaries: `gcc -O2 -DNDEBUG`, WAL enabled, fsync 50ms. C bench client (epoll, pipelined). All values are ops/s (put + reserve + delete cycles).

| Scenario | Upstream | Fork (`-w 1`) | Fork (8 workers) |
|----------|----------|---------------|-------------------|
| **S1** Throughput 8 conn, pipe=64 | 49,486 | **124,126** (+151%) | **203,054** (+310%) |
| **S2** Latency 1 conn, pipe=1 | 24,858 | **62,264** (+150%) | 50,435 (+103%) |
| **S3** Large body 16KB, 8 conn | 27,026 | **38,224** (+41%) | **81,772** (+203%) |
| **S4** High concurrency 32 conn | 34,762 | **97,191** (+180%) | **194,889** (+461%) |
| **S5** Deep pipeline 1 conn, pipe=256 | 67,859 | **123,429** (+82%) | **139,191** (+105%) |
| **S6** 500 tubes, 4 clients | 20,742 | 20,198 | **37,229** (+80%) |

<details>
<summary>Latency breakdown</summary>

| | Upstream | Fork (`-w 1`) | Fork (8W) |
|---|---|---|---|
| S1 P50 | 5,173 µs | 2,742 µs | **1,796 µs** |
| S1 P99.9 | 51,598 µs | **10,118 µs** | 38,728 µs |
| S2 P50 | 34.0 µs | **26.2 µs** | 28.7 µs |
| S2 P99.9 | 16,708 µs | **7,571 µs** | 11,128 µs |

</details>

```sh
docker build -f Dockerfile.benchmark . && docker run --rm <image>  # reproducible A/B
```

## Fork vs upstream

| | Upstream | This fork |
|---|---|---|
| Architecture | Single-threaded | N workers, one per CPU core |
| Scheduling | O(tubes) scan per tick | O(1) heaps + direct match |
| Syscalls | ~4 per command | ~2 (inline reply flush) |
| Job hash rehash | Stop-the-world | Incremental, 16 buckets/op |
| Job pool | Exact size match | 11 size classes, O(1) reuse |
| WAL | Single file chain | Per-worker, async fsync |
| Crash/data bugs | 22 open | 35 fixed |
| Tests | 100 unit | 203 unit + 12 MW integration |
| Platform | Linux, macOS, FreeBSD | Linux 6.1+ only |

## Build and test

```sh
make check                              # 203 unit tests (UBSan in CI)
make check-mw                           # 12 multi-worker integration tests
docker build -f Dockerfile.build .      # full CI: UBSan + MW tests + cppcheck
```

## Single-tube watch

Upstream allows watching N tubes per connection. `reserve` scans all watched tubes — an inherently serial operation that cannot be partitioned across cores.

This fork: one watched tube per connection.

- `watch <tube>` — switches to `<tube>`, replies `WATCHING 1`
- `ignore <tube>` — no-op, replies `WATCHING 1`
- `use` and `watch` are independent: `use` sets producer tube, `watch` sets consumer tube

```
# Before (upstream, one connection):
conn.watch('emails').watch('notifications').reserve()

# After (this fork, one connection per tube):
conn1.watch('emails').reserve()
conn2.watch('notifications').reserve()
```

## Multi-process architecture

Forks N workers (one per CPU core). Override with `-w N`. Single-process: `-w 0`, `-w 1`, or `-t CPU`.

**Master:** monitors workers via `waitpid`, restarts on crash, forwards signals.

**Workers:** each binds `SO_REUSEPORT` on port 11300, owns tubes by `hash(name) % N`, runs independent epoll loop with own WAL directory and interleaved job IDs.

**Routing:**
- `watch` → migrates connection via `SCM_RIGHTS` to tube's owner
- `put` → forwarded to tube owner via `PutFwdMsg` (zero-copy sendmsg scatter-gather)
- `peek/stats-job/kick-job/delete <id>` → forwarded by `(id-1) % N`
- `stats-tube/pause-tube/peek-ready/kick` → forwarded by tube hash
- `stats` → aggregates via `mmap`'d shared memory (seqlock, 1Hz publish)
- Mesh recovery: master distributes new socketpairs via control pipe on worker restart

## Bug fixes (35)

**Crashes:** NULL deref in conn_timeout, infinite loop in rawfalloc, WAL rollback corruption, unsafe signal handler exit, EPOLLERR 100% CPU, prot_init OOM SIGSEGV.

**Data loss:** job leaks in h_accept/enqueue_incoming_job, job_copy dangling pointer, WAL nrec on failure, corrupt WAL records skipped, walmaint errors ignored, prot_replay orphans, enqueue_job WAL failure leaving job in two structures, release/bury WAL failure orphans, delayed_ct 32-bit truncation, pause-time-left wraparound, WAL stats not aggregated, total_jobs_ct premature increment, heap OOM ignored, WAL reservation leak in kick/release.

**Hardening:** input validation for kick/reserve-timeout, option parsing overflow, EMFILE backpressure, scan_line_end bare `\r`, connsched heap OOM, snprintf negative return, stale WAL fd, IPv6 stats overflow.

## Performance

**Scheduling:** O(1) delay/pause/timeout heaps; direct process_tube match on enqueue; heapresift in-place; heap grandchild prefetch.

**Syscall reduction:** inline reply flush (skip epoll round-trip); writev for job body; 64-event epoll batch with drain loop; epoll_ctl caching; TCP_CORK only on pipeline; TCP_QUICKACK at accept; accept4 atomic flags; CLOCK_MONOTONIC_COARSE per batch; incremental scan_line_end; reserve fast path (skip ms round-trip when job ready).

**Parsing:** u64toa two-digit pair table; reply_inserted/reply_job backward build into reply_buf; manual base-10 read_uint; 256-byte tube name lookup table; two-level byte command dispatch.

**Memory:** 11-class job pool (64B–64KB); cache-line Conn/Tube struct layout; Conn slab pool (256); incremental rehash (16 buckets/op, dual-table); DJB2+finalizer tube hash with hash-first filter; cached tube owner/shard; MALLOC_ARENA_MAX=1; periodic malloc_trim; lazy heap alloc for MW-only buffers.

**WAL:** per-worker directories; async fsync threads; writev records; fallocate prealloc; rate-limited compaction; lock-free error check; readahead on recovery.

**Network:** SO_REUSEPORT; TCP_FASTOPEN(1024); TCP_DEFER_ACCEPT; TCP_NOTSENT_LOWAT(16KB); TCP_USER_TIMEOUT(30s); SO_INCOMING_CPU; sched_setaffinity.

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
| `-t CPU` | — | Pin to CPU core (forces single-process) |
| `-w N` | auto | Workers (0/1 = single, auto = per core) |
| `-V` | | Verbose logging |

## License

MIT. See [LICENSE](LICENSE). Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
