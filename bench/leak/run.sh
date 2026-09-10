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
# nothing. The first version of this bench reported PASS on such a
# platform, and kept reporting PASS with a deliberate leak compiled into
# the accept path — which is how the check below came to exist. A leak
# bench that cannot detect a leak is worse than no bench at all, so this
# one refuses to report anything until a known leak is caught.
PROBE=$(mktemp -d)
cat > "$PROBE/probe.c" <<'PROBE_EOF'
#include <stdlib.h>
int main(void) { void *p = malloc(1234); (void)p; return 0; }
PROBE_EOF
${CC:-cc} -O1 -g -fsanitize=address -o "$PROBE/probe" "$PROBE/probe.c"
if ASAN_OPTIONS=detect_leaks=1 "$PROBE/probe" 2>&1 | grep -q "LeakSanitizer"; then
    rm -rf "$PROBE"
else
    rm -rf "$PROBE"
    echo "SKIP: LeakSanitizer does not report on this platform."
    echo "      A probe that leaks 1234 bytes exited clean, so anything"
    echo "      this bench said afterwards would be meaningless."
    echo "      Known cases: aarch64, and x86-64 under emulation (LSan"
    echo "      needs the real registers and stack, which qemu/Rosetta"
    echo "      do not give it) — both were checked on the machine this"
    echo "      was written on, and both are silent. Run it on NATIVE"
    echo "      x86-64 Linux, or drive bench/leak/workload.py against"
    echo "      ./beanstalkd under valgrind --leak-check=full."
    exit 2
fi

make clean >/dev/null
make beanstalkd \
    CFLAGS="-O1 -g -fsanitize=address -fno-omit-frame-pointer -Wall -Wformat=2" \
    LDFLAGS="-fsanitize=address" LDLIBS="-lrt -lpthread" >/dev/null
python3 "$HERE/workload.py" "${1:-11810}"
