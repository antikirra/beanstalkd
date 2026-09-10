#!/bin/sh
# bench/fuzz/run.sh [seconds] [seed]
#
# Builds the server under ASan+UBSan and runs both fuzzers against it:
#
#   protocol.py — hostile and half-valid command lines over a socket.
#                 Every reply is allowed; a crash, a hang, or a
#                 sanitizer report is not.
#   replay.py   — builds a real WAL, corrupts it (including relabelling
#                 it as the legacy v7 format) and restarts. The server
#                 must come up or refuse cleanly, never crash.
#
# Linux only, like the rest of the project. Run it before a release and
# after any change to the command parser or the WAL reader.
set -e
SECS=${1:-60}
SEED=${2:-1}
PORT=${FUZZ_PORT:-11260}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)

cd "$ROOT"
make clean >/dev/null
make CFLAGS="-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Wall -Wformat=2" \
     LDFLAGS="-fsanitize=address,undefined" LDLIBS="-lrt -lpthread" >/dev/null

ASAN_OPTIONS=allocator_may_return_null=1:detect_leaks=0
export ASAN_OPTIONS

echo "== protocol fuzz (${SECS}s, seed $SEED)"
./beanstalkd -l 127.0.0.1 -p "$PORT" >/tmp/fuzz_srv.log 2>&1 &
B=$!
sleep 1
python3 "$HERE/protocol.py" "$PORT" "$SECS" "$SEED"
kill -TERM $B 2>/dev/null || true
sleep 0.5
if grep -qE "AddressSanitizer|runtime error|SUMMARY:" /tmp/fuzz_srv.log; then
    echo "SANITIZER HIT:"; head -30 /tmp/fuzz_srv.log; exit 1
fi

echo "== replay fuzz (seed $SEED)"
python3 "$HERE/replay.py" "$SEED" 25 "$ROOT/beanstalkd"

echo "fuzz: clean"
