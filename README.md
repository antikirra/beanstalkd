# beanstalkd

Linux-native work queue. Single C11 binary, requires only glibc.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd) with O(1) scheduling, per-syscall optimizations, and dozens of crash/data-loss fixes. Drop-in replacement for the upstream binary: every upstream command, response, error string, and stats key works unchanged (see "Wire-observable differences" for the short list of additive extensions). WAL format upgraded to v8 with per-record CRC32C; upstream v7 binlogs replay transparently on startup.

## Quick start

```sh
make && ./beanstalkd                          # in-memory
./beanstalkd -b /var/lib/beanstalkd           # with WAL persistence
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd
```

Requires **Linux 6.1+**, glibc, gcc 12+. Compatible with GCC 15.

## Compatibility

Drop-in replacement for upstream beanstalkd v1.13 for every non-administrative workload. All client libraries (Go, Python, Ruby, PHP, Java, etc.) connect and drive jobs without changes; every upstream command, response, error string, and stats key is preserved. The differences are limited to one new command (`truncate`), one additive stats field (`cmd-truncate`), and a few strict-parsing edges — see "Wire-observable differences" below before migrating.

WAL writer emits v8 (4-byte CRC32C trailer per record for silent-corruption detection); upstream v7 binlogs are read transparently on startup, so upgrade requires no migration. Downgrade to a pre-v8 binary is not supported. Legacy v5 (beanstalkd 1.4.6) reader removed — drain pre-v7 binlogs on the old binary before upgrading.

### Wire-observable differences from upstream

| Case | Upstream | This fork | Impact on legacy clients |
|---|---|---|---|
| `truncate <tube>\r\n` | `UNKNOWN_COMMAND` | `TRUNCATED <count>\r\n` | None — legacy clients never send `truncate`. |
| `touch <id>` on a job whose tube was `truncate`d since it was reserved | `TOUCHED` | `NOT_FOUND` | Only observable in deployments that use `truncate`. |
| `stats` YAML body | — | One additive line `cmd-truncate: N` inserted between `cmd-pause-tube` and `job-timeouts`. | Key-value (YAML) parsers: none. Line-index parsers: `job-timeouts` and later fields shift down one line. |
| Command with trailing space (e.g. `"stats \r\n"`, `"list-tubes \r\n"`) | `BAD_FORMAT` | `UNKNOWN_COMMAND` | Only if the client specifically switches on `BAD_FORMAT`. Canonical clients send the bare verb and are unaffected. (Invariant #13: strict literal prefix dispatch closes a namespace-leak through `sleep\r\n` / `steal\r\n` / `l12345678s\r\n`.) |
| `-D` without `-b` | flag does not exist | Server starts, but every persistent command (put, release, bury, kick) surfaces a real error (`BURIED` / `INTERNAL_ERROR` / `OUT_OF_MEMORY`) rather than ghost-acknowledging. Canonical durable use is `-D -b`. | Only affects deployments that misconfigure `-D`; no client sees this under `-D -b`. |

Opt-in features that are silent unless enabled: HTTP health (`-H`) replies only to `GET `/`HEAD ` prefixes (no beanstalk verb starts with either); idle timeout (`-I SEC`) just closes connections that would otherwise idle forever; connection cap (`-c N`) is off by default.

No upstream command, response string, error string, stats key, or CLI flag was removed or renamed.

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
| WAL compaction | Broken on release cycles | Fixed alive tracking |
| WAL integrity | None | CRC32C per record (v8 format, SSE4.2) |
| WAL durability | Async only, errors can be silently dropped | Async with `_Atomic` error signalling + EINTR retry; opt-in synchronous `-D` (`ack ⇒ durable`) |
| Tube hash | Stock DJB2 | wyhash v4 (avalanche + length-aware) |
| Heap layout | Binary (2-ary) | 4-ary (shallower, cache-line-fit children) |
| Crash/data bugs | 22+ open in upstream tracker | see §Bug fixes and `CHANGELOG.md` |
| Tests | ~100 unit | 360 unit + hostile WAL + ASan + Valgrind |
| Status | Maintenance mode (last code 2020) | Active development |

## Build and test

```sh
make check                                # 360 unit tests (UBSan in CI)
docker build -f Dockerfile.build .        # CI: UBSan + cppcheck (C11)
docker build -f Dockerfile.benchmark .    # A/B benchmark vs upstream
docker build -f test/Dockerfile.loadtest -t loadtest . && docker run --rm loadtest
                                          # ASan + Valgrind + WAL crash recovery
```

Tested with GCC 12 (Debian bookworm) and GCC 15 (Debian sid). Production Dockerfile uses `-O2 -flto -fipa-pta -fno-plt -fvisibility=hidden -Wl,--gc-sections` with branch prediction hints on hot paths.

## Bug fixes

The lists below are not exhaustive — they cover named regressions against
upstream beanstalkd v1.13. The 2026-04-23/24 pre-production audit added a
further ~75 fixes and hardening patches across WAL replay, truncate
crosscutting, and fault-injection paths; see `CHANGELOG.md` for the canonical log.

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
O(1) delay/pause/timeout heaps (4-ary, cache-friendlier than binary). Direct process_tube match on enqueue. Heapresift in-place. Grandchild prefetch for large heaps.

**Syscall reduction:**
Inline reply flush (skip epoll round-trip). writev for job body. 64-event epoll batch with drain loop. epoll_ctl caching. TCP_CORK only on pipeline. TCP_QUICKACK at accept. accept4 atomic flags. CLOCK_MONOTONIC_COARSE per batch. Incremental scan_line_end. Reserve fast path (skip ms round-trip when job ready).

**Parsing:**
u64toa two-digit pair table. reply_inserted/reply_job backward build into reply_buf. Manual base-10 read_uint. 256-byte tube name lookup table (_Alignas(64) cache-line aligned). Two-level byte command dispatch.

**Memory:**
11-class job pool (64B-64KB, `__attribute__((malloc))`). Cache-line Conn/Tube struct layout. Conn slab pool (256). Incremental rehash (16 buckets/op, dual-table). wyhash (final v4) tube hash with hash-first filter and length-aware API. MALLOC_ARENA_MAX=1. Periodic malloc_trim.

**WAL:**
Async fsync thread (_Atomic error signaling). Optional synchronous mode via `-D` (`ack ⇒ durable`, blocking fdatasync per write, EINTR-aware retry). writev records with per-record CRC32C trailer (v8 format, Intel SSE4.2 `_mm_crc32_u64` ~0.33 cyc/byte, negligible overhead). fallocate prealloc. Rate-limited compaction with correct alive tracking. Lock-free error check. Readahead on recovery. _Static_assert guards on WAL record sizes. Transparent v7 binlog replay for upgrade from upstream.

**Network:**
TCP_FASTOPEN(1024). TCP_DEFER_ACCEPT. TCP_NOTSENT_LOWAT(16KB). TCP_USER_TIMEOUT(30s). SO_INCOMING_CPU. sched_setaffinity.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | — | WAL directory (enables persistence) |
| `-f MS` | 50 | fsync interval (0 = every write) |
| `-F` | | Never fsync |
| `-D` | | Durable: block on fdatasync per WAL write; `ack ⇒ durable` (implies `-F`, needs `-b`) |
| `-l ADDR` | 0.0.0.0 | Listen address (`unix:` for Unix socket) |
| `-p PORT` | 11300 | Listen port |
| `-z BYTES` | 65535 | Max job body size |
| `-s BYTES` | 10MB | WAL file size |
| `-u USER` | | Drop privileges |
| `-m SEC` | 60 | malloc_trim interval (0 = disable) |
| `-t CPU` | — | Pin to CPU core |
| `-c N` | 0 (off) | Reject new connections when count reaches N; 0 = unlimited (preserves upstream behaviour). EMFILE remains the kernel hard cap |
| `-I SEC` | 0 (off) | Close conns idle for SEC seconds. A worker blocked on `reserve` is NOT idle (waiting clients are excluded), so it is safe to enable with worker pools |
| `-H` | off | Reply to HTTP `GET`/`HEAD` on the beanstalk port (`200 ok`, `503 draining`); intended for Kubernetes `httpGet` probes. Off by default — beanstalk clients never send these verbs, so enabling is harmless |
| `-V` | | Verbose logging (`-VV` for command trace) |
| `--log-json` | | Emit warnings as JSON objects on stderr (`{"ts":...,"level":"warn\|error","msg":"...","errno":"..."}`) |

## Docker benchmark results

`docker build -f Dockerfile.benchmark .` — both binaries compiled with identical `gcc -O2 -DNDEBUG`, WAL enabled, fsync 50ms. Docker container, not bare metal.

| Scenario | Upstream | Fork | Delta |
|---|---|---|---|
| S1: Throughput (ops/s) | 121,598 | 204,250 | **+68.0%** |
| S1: P50 latency (us) | 2,735.7 | 1,420.1 | **-48.1%** |
| S1: P99.9 latency (us) | 8,188.0 | 5,538.8 | **-32.4%** |
| S2: Latency (ops/s) | 129,905 | 186,618 | **+43.7%** |
| S2: P50 latency (us) | 6.9 | 5.8 | **-15.9%** |
| S2: P99.9 latency (us) | 3,051.5 | 2,194.8 | **-28.1%** |
| S3: Large body 16KB (ops/s) | 69,886 | 87,457 | **+25.1%** |
| S4: 32 connections (ops/s) | 137,468 | 252,682 | **+83.8%** |
| S5: Deep pipeline (ops/s) | 103,914 | 138,893 | **+33.7%** |
| S6: 500 tubes (ops/s) | 36,367 | 36,889 | +1.4% |

**Scenarios:** S1: 8 conn x 10K put+reserve+delete, 128B body, pipeline=64. S2: 1 conn x 5K ops, 4B body, pipeline=1 (round-trip). S3: 8 conn x 2K ops, 16KB body, pipeline=16. S4: 32 conn x 5K ops, 128B body, pipeline=32. S5: 1 conn x 20K ops, 128B body, pipeline=256. S6: 500 tubes x 100 jobs each, 4 clients, 16-256B mixed bodies.

## License

MIT. See [LICENSE](LICENSE). Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
