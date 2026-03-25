# beanstalkd (hardened fork)

A fork of [beanstalkd](https://github.com/beanstalkd/beanstalkd) — simple and fast general purpose work queue.

This fork fixes **22 confirmed bugs** (including crashes, memory leaks, data loss, and WAL corruption scenarios), adds an async fsync thread, and includes 69 additional hostile unit tests — all while preserving **full wire protocol and backward compatibility**. Every existing client works unmodified.

See [doc/protocol.txt](doc/protocol.txt) for details of the network protocol.

## What's different from upstream

- **22 bug fixes** — NULL dereference crashes, WAL write failures, memory/job leaks, integer overflows, clock drift issues (CLOCK_MONOTONIC), and more
- **Async fsync** — WAL fsync runs in a background pthread, so the event loop never blocks on disk I/O
- **O(1) tube lookup** — hash table replaces linear scan for global tube lookup
- **169 tests** (100 legacy + 69 hostile) — adversarial tests that actively try to break the code
- **Docker-based integration testing** — ASan, Valgrind, and WAL crash recovery validation

## Quick Start

    $ make
    $ ./beanstalkd

also try,

    $ ./beanstalkd -h
    $ ./beanstalkd -VVV
    $ make -j4 CFLAGS=-O2
    $ make CC=clang
    $ make check
    $ make install
    $ make install PREFIX=/usr

Requires Linux (2.6.17+), Mac OS X, FreeBSD, or Illumos.
Any C99 compiler should work; tested with GCC and clang.

On Linux, the build links `-lpthread` (for the async fsync thread) and `-lrt` automatically.

## Tests

### Unit tests

Unit tests are in `test*.c`. Run them with:

    $ make check

The suite includes 100 legacy regression tests and 69 hostile tests that cover edge cases, OOM paths, copy independence, boundary values, and stress scenarios.

See https://github.com/kr/ct for information on the test framework.

### Docker integration tests

The Docker-based test suite runs a full load test with three phases:

1. **AddressSanitizer** — memory safety under load (500 put/reserve/delete cycles)
2. **Valgrind memcheck** — leak detection with definite/indirect leak checks
3. **WAL crash recovery** — insert 100 jobs, `kill -9`, restart, verify recovery

Run it with:

    $ docker build -f test/Dockerfile.loadtest -t beanstalkd-test .
    $ docker run --rm beanstalkd-test

## Subdirectories

- `adm` — files useful for system administrators
- `ct` — testing tool; vendored from https://github.com/kr/ct
- `doc` — documentation
- `pkg` — scripts to make releases
- `test` — Docker-based integration test suite

## Links

- Upstream: https://beanstalkd.github.io/
- Protocol: [doc/protocol.txt](doc/protocol.txt)
