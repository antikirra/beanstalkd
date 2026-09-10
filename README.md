# beanstalkd

Linux-native work queue. Single C11 binary, requires only glibc.

Fork of [upstream beanstalkd](https://github.com/beanstalkd/beanstalkd) with O(1) scheduling, per-syscall optimizations, and dozens of crash/data-loss fixes. Drop-in replacement for the upstream binary: every upstream command, response, error string, and stats key works unchanged (see "Wire-observable differences" for the short list of additive extensions). WAL format upgraded to v8 with per-record CRC32C; upstream v7 binlogs replay transparently on startup.

## Quick start

```sh
make && ./beanstalkd                          # in-memory
./beanstalkd -b /var/lib/beanstalkd           # with WAL persistence
docker build -t beanstalkd . && docker run -p 11300:11300 beanstalkd
```

Requires **Linux 6.1+** and glibc. Builds with gcc 12+ (compatible with GCC 15) or clang 14+; CI gates on both, because a second front end catches what the first one's warning set does not.

## Compatibility

Drop-in replacement for upstream beanstalkd v1.13 for every non-administrative workload. All client libraries (Go, Python, Ruby, PHP, Java, etc.) connect and drive jobs without changes; every upstream command, response, error string, and stats key is preserved. The differences are limited to a few strict-parsing edges and the opt-in durability mode — see "Wire-observable differences" below before migrating.

WAL writer emits v8 (4-byte CRC32C trailer per record for silent-corruption detection); upstream v7 binlogs are read transparently on startup, so upgrade requires no migration. Downgrade to a pre-v8 binary is not supported. Legacy v5 (beanstalkd 1.4.6) reader removed — drain pre-v7 binlogs on the old binary before upgrading. Binlogs written by the short-lived fork builds that had the `truncate` command (2026-04-16…2026-08-29) may contain truncate-marker records: replay consumes them with a logged warning and skips the cutoff, so jobs deleted by `truncate` resurrect as live — drain such tubes on the old binary first if that matters.

The properties the server is built to keep — the ones the code cites as
"invariant #11" and the like — are listed in [doc/invariants.md](doc/invariants.md),
with what breaks when each one does not hold and where it is pinned by tests.

### Wire-observable differences from upstream

| Case | Upstream | This fork | Impact on legacy clients |
|---|---|---|---|
| Command with trailing space (e.g. `"stats \r\n"`, `"list-tubes \r\n"`) | `BAD_FORMAT` | `UNKNOWN_COMMAND` | Only if the client specifically switches on `BAD_FORMAT`. Canonical clients send the bare verb and are unaffected. (Invariant #13: strict literal prefix dispatch closes a namespace-leak through `sleep\r\n` / `steal\r\n` / `l12345678s\r\n`.) |
| `-D` without `-b` | flag does not exist | Server starts, but every persistent command (put, release, bury, kick) surfaces a real error (`BURIED` / `INTERNAL_ERROR` / `OUT_OF_MEMORY`) rather than ghost-acknowledging. Canonical durable use is `-D -b`. | Only affects deployments that misconfigure `-D`; no client sees this under `-D -b`. |
| `-D` group-commit failure (fdatasync error) | flag does not exist | Every connection whose ack was pending on the failed commit receives `INTERNAL_ERROR\r\n` followed by FIN (connection close). The close is what keeps a pipelining client's reply stream aligned: outstanding commands resolve as "connection closed" instead of being silently unanswered. The WAL is disabled for the rest of the process lifetime (same as upstream's binlog-failure semantics). | Only reachable under `-D` on a failing disk; upstream clients never see this path. |
| Malformed `put` line (extra argument) with an oversized body count | `JOB_TOO_BIG` after reading and discarding the announced body | `BAD_FORMAT` immediately: the line is judged before its size field is trusted | Only clients that send a malformed put and rely on the body being consumed. Removes a bit-bucket armed by an attacker-chosen byte count. |
| Command pipelined behind a blocking `reserve` | never answered: the server had already taken the bytes off the socket, and nothing was left to announce them once the reserve completed, so the client waited forever (upstream #647) | answered as soon as the reserve is, in order, whether the reserve ended in a job, a timeout or `DEADLINE_SOON` | Strictly additive: a reply that never came now comes. Only a client that depended on the hang is affected, and there is no such client. |
| `reserve` inside its own job's `DEADLINE_SOON` margin, with a job ready to hand over | `DEADLINE_SOON` | the ready job (`RESERVED`), because the margin exists so the client is never left WAITING, and a reserve answerable at once is not waiting (upstream #646) | A client that treats `DEADLINE_SOON` as its cue to release gets a job instead; it still gets `DEADLINE_SOON` whenever nothing is ready. |
| Connection that sends nothing | accepted immediately | not handed to the server until it sends data (`TCP_DEFER_ACCEPT`), so a silent client is invisible for about a second — including to the `-c` cap, whose refusal it therefore sees with that delay | None: a client that sends a command is served immediately. Only affects probes that connect without writing. |

Opt-in features that are silent unless enabled: HTTP health (`-H`) replies only to `GET `/`HEAD ` prefixes (no beanstalk verb starts with either); idle timeout (`-I SEC`) just closes connections that would otherwise idle forever; connection cap (`-c N`) is off by default.

No upstream command, response string, error string, or stats key was removed or renamed. The fork-specific `truncate` command was removed (2026-08-29), so the wire command set is again identical to upstream.

CLI (not wire-observable): upstream's deprecated no-op stubs `-c`/`-n` (warn-and-continue since upstream 1.10, "binlog is always compacted") were dropped. In this fork `-n` is a fatal unknown flag, and `-c` is repurposed as the connection cap and requires an argument (`-c N`). Impact is limited to init scripts that still pass these deprecated flags — such scripts must drop them before migrating; no protocol client is affected.

## Fork vs upstream

| | Upstream (v1.13) | This fork |
|---|---|---|
| Language standard | C99 | C11 (`_Static_assert`, `_Atomic`, `_Alignas`) |
| Platform | Linux, macOS, FreeBSD | Linux 6.1+ only (uses Linux APIs directly) |
| Architecture | Single-threaded | Single-threaded, Linux-optimized |
| Scheduling | O(tubes) scan per tick | O(1) heaps + direct match |
| Syscalls per command | ~4 | 3.0 for a lone command (wait, read, reply — the floor); 0.11 at pipeline depth 32, where a burst's replies leave in ONE write and its commands arrive in ONE read |
| Job hash rehash | Stop-the-world | Incremental, 16 buckets/op |
| Job memory | malloc/free per job | 11 size classes, O(1) pool reuse |
| WAL compaction | Broken on release cycles | Fixed alive tracking |
| WAL integrity | None | CRC32C per record (v8 format; SSE4.2 on x86-64, ARM CRC32 with runtime dispatch on aarch64, portable fallback elsewhere) |
| WAL durability | Async only, errors can be silently dropped | Async with `_Atomic` error signalling + EINTR retry; opt-in synchronous `-D` (`ack ⇒ durable`) with **group commit** — one `fdatasync` per event-loop tick amortises across every staged record, durable pipelined throughput now matches async mode |
| Tube hash | Stock DJB2 | wyhash v4 (avalanche + length-aware) |
| Heap layout | Binary (2-ary) | 4-ary (shallower, cache-line-fit children) |
| Crash/data bugs | 22+ open in upstream tracker | see §Bug fixes and `CHANGELOG.md` |
| Tests | ~100 unit | 1032 unit, gated on gcc and clang under UBSan and ASan plus cppcheck; TSan, two fuzzers and the crash/shutdown/binlog/syscall benches by hand |
| Status | Maintenance mode (last code change March 2025, infrastructure only) | Active development |

## Build and test

```sh
make check                                # 1032 unit tests (~45s)
docker build -f Dockerfile.build .        # the gate: gcc + clang, default
                                          # flags + UBSan + ASan + cppcheck
docker build -f Dockerfile.benchmark -t bsbench . && docker run --rm bsbench
                                          # A/B benchmark vs upstream
docker build -f test/Dockerfile.loadtest -t loadtest . && docker run --rm loadtest
                                          # ASan + Valgrind + WAL crash recovery
```

Beyond the gate, and worth running before a release:

```sh
bench/fuzz/run.sh 60 1        # hostile commands, and restarts on a corrupted WAL
bench/syscalls/count.sh       # syscalls per command (needs --cap-add=SYS_PTRACE)
bench/shutdown/run.sh         # a clean SIGTERM leaves the binlog durable and trimmed
bench/crash/run.sh            # SIGKILL mid-traffic: under -D every acked job comes back
bench/binlog/run.sh           # the WAL stays bounded with jobs pinned in the oldest file
bench/leak/run.sh             # every command, then a clean exit, under LeakSanitizer
                              #   (exits 2 and says so where LSan cannot report: aarch64,
                              #    or x86-64 under emulation — needs a native x86-64 host)
python3 bench/mutate/mutate.py conn.c,job.c 12 1   # do the tests actually bite?
```

ThreadSanitizer is not in the gate because it needs `ADDR_NO_RANDOMIZE`,
which a `docker build` step cannot request; the command to run it by
hand is in `Dockerfile.build`.

Tested with GCC 12 (Debian bookworm), GCC 15 (Debian sid) and clang 14. Production Dockerfile uses `-O2 -flto -fipa-pta -fno-plt -fvisibility=hidden -Wl,--gc-sections` with branch prediction hints on hot paths.

## Bug fixes

The lists below are not exhaustive — they cover named regressions against
upstream beanstalkd v1.13. The 2026-04-23/24 pre-production audit added a
further ~75 fixes and hardening patches across WAL replay and
fault-injection paths; see `CHANGELOG.md` for the canonical log.

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

**Protocol liveness:**
a command pipelined behind a blocking `reserve` was never answered — the bytes were already off the socket and nothing was left to announce them, so the client waited forever (upstream #647); the command-line length limit is now enforced against the line rather than implied by the size of the input buffer; a job reserved inside a coalesced burst gets its TTR deadline into the tick heap, so the reservation still times out.

## Performance

**Scheduling:**
O(1) delay/pause/timeout heaps (4-ary, cache-friendlier than binary). Direct process_tube match on enqueue. Heapresift in-place. Grandchild prefetch for large heaps.

**Syscall reduction:**
Pipelined replies coalesce: a burst's acks are staged in one per-conn buffer and pushed with a single `write`, and a job reply small enough joins the same buffer, header and body, so `writev` leaves the hot path too. This replaced `TCP_CORK`, which saved no syscall at all — it only stopped the kernel from putting each reply on the wire separately, while the burst still paid one `write` per command and two `setsockopt` on top. Durability outranks the saving: a `-D` burst's acks stay buffered until the tick's `fdatasync`.
A 1024-byte command input buffer (`CMD_BUF_SIZE`) so a burst arrives in one `read` rather than one per ~10 commands; the size is the measured knee, past which `fill_extra_data`'s tail memmove costs more than the syscalls saved.
Inline reply flush (skip epoll round-trip). No lseek per WAL record (the File tracks its own write offset). 64-event epoll batch with drain loop. epoll_ctl caching. TCP_QUICKACK at accept. accept4 atomic flags. CLOCK_MONOTONIC_COARSE per batch. Incremental scan_line_end. Reserve fast path (skip ms round-trip when job ready).
At pipeline depth 32 this is 0.11 syscalls per command, against 1.34 before; a lone command still costs the irreducible three (wait, read, reply).

**Parsing:**
u64toa two-digit pair table. reply_inserted/reply_job backward build into reply_buf. Manual base-10 read_uint. 256-byte tube name lookup table (_Alignas(64) cache-line aligned). Two-level byte command dispatch.

**Memory:**
11-class job pool (64B-64KB, `__attribute__((malloc))`). Cache-line Conn/Tube struct layout. Conn slab pool (256). Incremental rehash (16 buckets/op, dual-table). wyhash (final v4) tube hash with hash-first filter and length-aware API. `mallopt(M_ARENA_MAX, 1)`. Periodic malloc_trim.

**WAL:**
Async fsync thread (_Atomic error signaling). Optional synchronous mode via `-D` (`ack ⇒ durable`, EINTR-aware retry) with **group commit**: `walwrite` stages the record (writev + accounting) without fsync, the event loop batches every WAL-dirty command in the current epoll drain, and one `walcommit` issues a single `fdatasync` covering the whole batch before acks fan out. Buffered replies land in `Conn::dur_reply_buf` and are drained by `dur_flush_all` after commit; commit failure ftruncates the tail, rolls back global counters, disables the WAL, and emits `INTERNAL_ERROR` to every conn in the batch — `ack ⇒ durable` holds. writev records with per-record CRC32C trailer (v8 format; SSE4.2 on x86-64, ARM CRC32 with runtime `getauxval(AT_HWCAP)` dispatch on aarch64, portable table fallback elsewhere, ~0.33 cyc/byte on SSE4.2, negligible overhead). fallocate prealloc. Rate-limited compaction with correct alive tracking. Lock-free error check. Readahead on recovery. _Static_assert guards on WAL record sizes. Transparent v7 binlog replay for upgrade from upstream.

**Network:**
TCP_FASTOPEN(1024). TCP_DEFER_ACCEPT. TCP_NOTSENT_LOWAT(16KB). TCP_USER_TIMEOUT(30s). SO_INCOMING_CPU. sched_setaffinity.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `-b DIR` | — | WAL directory (enables persistence) |
| `-f MS` | 50 | fsync interval (0 = every write) |
| `-F` | | Never fsync |
| `-D` | | Durable: one fdatasync per event-loop tick (group commit); `ack ⇒ durable` (implies `-F`, needs `-b`) |
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
| `-v` | | Print version and exit |
| `-h` | | Print usage and exit |
| `--log-json` | | Emit warnings as JSON objects on stderr (`{"ts":...,"level":"warn\|error","msg":"...","errno":"..."}`) |

## Docker benchmark results

`docker build -f Dockerfile.benchmark -t bsbench . && docker run --rm bsbench` — both
binaries compiled with identical `gcc -O2 -DNDEBUG`, WAL enabled, fsync 50ms. Docker
container, not bare metal. Measured 2026-09-10 on aarch64 (18 cores); two runs agreed
within a point except S2, whose single-connection round-trip is the noisiest row
(+26% and +40% across the two). On latency rows the delta is the change in the
number, so negative is better and S8's +7.6% is a tail that got worse.

| Scenario | Upstream | Fork | Delta |
|---|---|---|---|
| S1: Throughput (ops/s) | 125,903 | 406,115 | **+222.6%** |
| S1: P50 latency (us) | 2,352.1 | 813.7 | **-65.4%** |
| S1: P99.9 latency (us) | 11,917.8 | 5,007.7 | **-58.0%** |
| S2: Round-trip (ops/s) | 41,299 | 51,987 | **+25.9%** |
| S3: Large body 16KB (ops/s) | 67,356 | 74,708 | **+10.9%** |
| S4: 32 connections (ops/s) | 152,707 | 619,384 | **+305.6%** |
| S5: Deep pipeline (ops/s) | 117,900 | 263,132 | **+123.2%** |
| S6: 500 tubes (ops/s) | 31,724 | 29,669 | **-6.5%** |
| S7: Deep-watch (ops/s) | 29,094 | 250,762 | **+761.9%** |
| S7: P50 latency (us) | 2,514.8 | 360.9 | **-85.6%** |
| S8: Tail-probe (ops/s) | 35,713 | 39,971 | **+11.9%** |
| S8: P99.9 latency (us) | 7,181.2 | 7,725.4 | **+7.6%** |
| S11: 10K tubes (ops/s) | 29,064 | 27,715 | **-4.6%** |
| S13: Conn churn (conn/s) | 17,412 | 17,877 | **+2.7%** |

Durable mode is fork-only — upstream has no `-D`, so there is nothing to compare it
against. S9 (`-D`, serial) 11,886 ops/s; S10 (`-D`, pipelined) 138,580 ops/s. What
durability costs the fork against its own async numbers: 22.9% of S2 serially, 34.1%
of S1 under pipeline — the group commit is what keeps the second figure from being
the first one.

Two rows are losses and stay in the table for that reason: with 500 tubes (S6) and
with 10,000 (S11) this fork is 5-7% SLOWER than upstream, consistently across runs.
The scenarios that gain are the ones where the work per command is dispatch and
syscalls; where it is tube lookup at scale, upstream's simpler structure is still
ahead. S7 says the same thing from the other side: a client watching 500 tubes is
7-8x faster here, so the deficit is in the tube table itself rather than in watching.

**Scenarios:** S1: 8 conn x 10K put+reserve+delete, 128B body, pipeline=64. S2: 1 conn
x 5K ops, 4B body, pipeline=1 (round-trip). S3: 8 conn x 2K ops, 16KB body,
pipeline=16. S4: 32 conn x 5K ops, 128B body, pipeline=32. S5: 1 conn x 20K ops, 128B
body, pipeline=256. S6: 500 tubes x 100 jobs each, 4 clients, 16-256B mixed bodies.
S7: 8 conn x 500 ops each watching 500 tubes, PUTs targeting the last-watched one.
S8: 1 conn x 100K ops, pipeline=1, 4B body (long serial run for tail latency). S9:
`-D`, 1 conn x 5K ops, pipeline=1, 4B body. S10: `-D`, 8 conn x 5K ops, pipeline=64,
128B body. S11: 10K tubes x 10 jobs each, 4 clients. S13: open+put+quit x 2000 x 4
workers (connection churn).

## License

MIT. See [LICENSE](LICENSE). Based on [beanstalkd](https://github.com/beanstalkd/beanstalkd) by Keith Rarick and contributors.
