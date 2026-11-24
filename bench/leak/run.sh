#!/bin/sh
# bench/leak/run.sh
#
# Does the server leak? The unit suite cannot answer that: ct forks one
# process per test and never unwinds them, so `make check` under ASan
# runs with detect_leaks=0 and every allocation the product forgets goes
# unnoticed. Upstream carries two open issues about exactly this (#642,
# #382).
#
# So drive the REAL binary through every command the protocol has —
# the whole job life cycle, the stats bodies, the refusal paths, a
# pipelined burst, a hundred and fifty short-lived connections, and a
# worker still parked on reserve — then send SIGTERM and let
# LeakSanitizer report on the way out.
#
# What counts as a leak here is what LSan calls one: memory unreachable
# at exit. Jobs still queued and tubes still in the hash are reachable
# from the server's own structures and are not reported, which is right
# — a queue that still holds its jobs at shutdown has not lost them.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"

# Prove the detector works BEFORE trusting it. LeakSanitizer is not
# available everywhere ASan is, and where it is missing it is missing
# SILENTLY: a program that leaks 1234 bytes and returns exits 0 and says
# nothing. A leak bench that cannot detect a leak is worse than no bench
# at all, so this one refuses to report anything until a known leak is
# caught.
#
# The probe has to survive the optimiser, too. Its first spelling,
# `void *p = malloc(1234); (void)p;`, is a dead allocation that gcc and
# clang both delete at -O1 — so nothing leaked, the probe "failed", and
# this bench answered SKIP on every platform while blaming aarch64. The
# volatile store keeps the malloc; clearing it makes the block
# unreachable.
PROBE=$(mktemp -d)
cat > "$PROBE/probe.c" <<'PROBE_EOF'
#include <stdlib.h>
void *volatile sink;
int main(void) { sink = malloc(1234); sink = 0; return 0; }
PROBE_EOF
${CC:-cc} -O1 -g -fsanitize=address -o "$PROBE/probe" "$PROBE/probe.c"
if ASAN_OPTIONS=detect_leaks=1 "$PROBE/probe" 2>&1 | grep -q "LeakSanitizer"; then
    rm -rf "$PROBE"
else
    rm -rf "$PROBE"
    echo "SKIP: LeakSanitizer does not report on this platform."
    echo "      A probe that leaks 1234 bytes exited clean, so anything"
    echo "      this bench said afterwards would be meaningless."
    echo "      Known case: x86-64 under emulation (LSan needs the real"
    echo "      registers and stack, which qemu/Rosetta do not give it),"
    echo "      and a sandbox that forbids ptrace. Run it natively, or"
    echo "      drive bench/leak/workload.py against ./beanstalkd under"
    echo "      valgrind --leak-check=full."
    exit 2
fi

make clean >/dev/null
make beanstalkd \
    CFLAGS="-O1 -g -fsanitize=address -fno-omit-frame-pointer -Wall -Wformat=2" \
    LDFLAGS="-fsanitize=address" LDLIBS="-lrt -lpthread" >/dev/null
python3 "$HERE/workload.py" "${1:-11810}"
