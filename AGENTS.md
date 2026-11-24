# AGENTS.md

Rules for coding agents (and humans) working in this repository.

## Project

Fork of beanstalkd: C11, Linux 6.1+, glibc, one event loop (epoll) plus
one fsync thread. Drop-in replacement for upstream 1.13 on the wire;
WAL v8 (CRC32C per record), reads v7.

## Frozen

- Wire protocol (`doc/protocol.txt` plus the "Wire-observable
  differences" table in README) — byte for byte.
- WAL v8 format — record layout and field semantics (timestamps in the
  file are wall-clock nanoseconds, as in upstream v7).
- The concurrency model, and no runtime dependencies.
- Default CLI behaviour. New flags are opt-in only.

## Commands

```sh
make                                # build
make check                          # unit tests (ct), ~45-70 s
make verify                         # THE GATE (docker build -f Dockerfile.build .):
                                    # gcc, clang, UBSan, ASan, leak bench, cppcheck
docker build -f Dockerfile.benchmark -t bsbench . && docker run --rm bsbench   # A/B vs upstream
bench/syscalls/count.sh PORT ITERS DEPTH CONNS [wal|durable]                   # needs SYS_PTRACE
bench/leak/run.sh                   # leak check (LSan)
```

The tree builds on Linux only; on another host run everything in Docker.

## Rules for changes

1. One hypothesis, one commit; Conventional Commits; "before -> after"
   numbers in the body of any performance commit.
2. A performance change is accepted only with a measurement: 3 runs,
   median and spread; server CPU/op, syscalls/op, RSS/job, RSS/conn,
   longest event-loop pause.
3. A defect takes two commits: the quarantined test with a reference
   first (assertions untouched), then the fix that lifts the quarantine.
4. Every benchmark checks itself: mutating the property it measures
   must turn it red.
5. No O(n) or O(heap) work in the event loop without a per-tick budget.
6. Claims in README/CHANGELOG are reproduced ones only, with a date and
   the command.
7. Experiment log: `doc/experiments.md`, accepted and rejected alike.
8. Invariants: `doc/invariants.md`; a change next to one comes with a
   test that catches its violation.

## Map

`prot.c` protocol and scheduling · `file.c`/`walg.c` WAL · `conn.c`
connections · `job.c` job pool and hash · `tube.c` tubes · `heap.c`
4-ary heap · `linux.c`/`serv.c` epoll and the loop · `net.c` sockets ·
tests are `test*.c` in the root (ct) · `bench/` benchmarks and fuzzers.
