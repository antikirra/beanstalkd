#!/bin/bash
set -euo pipefail

echo "========================================"
echo "  Beanstalkd Load Test Suite"
echo "  ASan + Valgrind + WAL Recovery + Priv Drop"
echo "========================================"

ASAN_BIN=/usr/local/bin/beanstalkd-asan
DEBUG_BIN=/usr/local/bin/beanstalkd-debug
PASS_COUNT=0
FAIL_COUNT=0

phase_result() {
    if [ "$1" = "PASS" ]; then PASS_COUNT=$((PASS_COUNT + 1))
    else FAIL_COUNT=$((FAIL_COUNT + 1)); fi
}

# Reusable protocol-correct workload
run_workload() {
    local port=$1 n=$2
    python3 << PYEOF
import socket, time
s = socket.socket()
s.settimeout(30)
s.connect(('127.0.0.1', $port))
buf = b''
def recvline():
    global buf
    while b'\r\n' not in buf:
        buf += s.recv(4096)
    i = buf.index(b'\r\n')
    line, buf = buf[:i].decode(), buf[i+2:]
    return line
def recvn(n):
    global buf
    while len(buf) < n:
        buf += s.recv(4096)
    data, buf = buf[:n], buf[n:]
    return data
t0 = time.time()
for i in range($n):
    s.sendall(b'put 1024 0 60 4\r\ntest\r\n')
    assert 'INSERTED' in recvline()
t1 = time.time()
print(f'  PUT $n: {t1-t0:.3f}s ({$n/(t1-t0):.0f} ops/s)')
for i in range($n):
    s.sendall(b'reserve-with-timeout 0\r\n')
    parts = recvline().split()
    assert parts[0] == 'RESERVED'
    recvn(int(parts[2]) + 2)
    s.sendall(f'delete {parts[1]}\r\n'.encode())
    assert 'DELETED' in recvline()
t2 = time.time()
print(f'  RESERVE+DELETE $n: {t2-t1:.3f}s ({$n/(t2-t1):.0f} ops/s)')
for t in range(10):
    s.sendall(f'use t{t}\r\n'.encode()); recvline()
    for j in range(5):
        s.sendall(b'put 0 0 60 2\r\nok\r\n'); recvline()
print('  TUBES: 50 jobs across 10 tubes')
s.close()
for i in range(30):
    c = socket.socket(); c.settimeout(5)
    c.connect(('127.0.0.1', $port))
    c.sendall(b'stats\r\n'); c.recv(8192); c.close()
print(f'  CONNECT: 30 cycles, total {time.time()-t0:.2f}s')
PYEOF
}

# Protocol-correct stats reader
read_stats_field() {
    local port=$1 field=$2
    python3 << PYEOF
import socket
s = socket.socket()
s.settimeout(10)
s.connect(('127.0.0.1', $port))
s.sendall(b'stats\r\n')
buf = b''
while b'\r\n' not in buf:
    buf += s.recv(4096)
ok_line = buf.split(b'\r\n')[0].decode()
body_len = int(ok_line.split()[1])
rest = buf[buf.index(b'\r\n')+2:]
while len(rest) < body_len:
    rest += s.recv(4096)
body = rest[:body_len].decode()
for line in body.split('\n'):
    if '$field' in line:
        print(line.split(':')[1].strip())
        break
s.close()
PYEOF
}

# ============================================================
echo ""
echo "=== PHASE 1: AddressSanitizer ==="
# ============================================================
mkdir -p /tmp/wal-p1

ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:log_path=/tmp/asan" \
    $ASAN_BIN -p 11400 -b /tmp/wal-p1 -f 50 &
PID1=$!; sleep 2

if ! kill -0 $PID1 2>/dev/null; then
    echo "FAIL: server did not start"; cat /tmp/asan.* 2>/dev/null; exit 1
fi

run_workload 11400 500
kill $PID1 2>/dev/null; wait $PID1 2>/dev/null || true

ASAN_OK=1
for f in /tmp/asan.*; do
    [ -f "$f" ] && [ -s "$f" ] && { cat "$f"; ASAN_OK=0; }
done
[ "$ASAN_OK" = "1" ] && echo "  ASan: PASS" && phase_result PASS || { echo "  ASan: FAIL"; phase_result FAIL; }

# ============================================================
echo ""
echo "=== PHASE 2: Valgrind memcheck ==="
# ============================================================
mkdir -p /tmp/wal-p2

valgrind --tool=memcheck --leak-check=full --show-leak-kinds=definite,indirect \
    --log-file=/tmp/valgrind.log \
    $DEBUG_BIN -p 11410 -b /tmp/wal-p2 -f 200 &
PID2=$!; sleep 8

if ! kill -0 $PID2 2>/dev/null; then
    echo "FAIL: valgrind server did not start"; cat /tmp/valgrind.log; exit 1
fi

run_workload 11410 50
kill $PID2 2>/dev/null; wait $PID2 2>/dev/null || true; sleep 1

echo "  Valgrind:"
grep -E "definitely lost|indirectly lost|ERROR SUMMARY" /tmp/valgrind.log || true

DEFLOST=$(grep "definitely lost:" /tmp/valgrind.log | grep -oP '\d+(?= bytes)' | head -1)
[ "${DEFLOST:-0}" = "0" ] && echo "  Valgrind: PASS" && phase_result PASS || { echo "  Valgrind: FAIL"; phase_result FAIL; }

# ============================================================
echo ""
echo "=== PHASE 3: WAL crash recovery ==="
# ============================================================
rm -rf /tmp/wal-p3; mkdir -p /tmp/wal-p3

echo "  Step 1: Put 100 durable jobs (fsync=0)"
$DEBUG_BIN -p 11420 -b /tmp/wal-p3 -f 0 &
PID3=$!; sleep 1

python3 << 'PYEOF'
import socket
s = socket.socket()
s.settimeout(10)
s.connect(('127.0.0.1', 11420))
buf = b''
def recvline():
    global buf
    while b'\r\n' not in buf:
        buf += s.recv(4096)
    i = buf.index(b'\r\n')
    line, buf = buf[:i].decode(), buf[i+2:]
    return line
for i in range(100):
    s.sendall(b'put 1024 0 300 5\r\nhello\r\n')
    assert 'INSERTED' in recvline(), f'PUT fail at {i}'
print('  100 jobs inserted OK')
s.close()
PYEOF

echo "  Step 2: kill -9 (crash)"
kill -9 $PID3; wait $PID3 2>/dev/null || true
sleep 2

echo "  Step 3: Restart from WAL on port 11421"
$DEBUG_BIN -p 11421 -b /tmp/wal-p3 -f 0 &
PID3B=$!; sleep 2

if ! kill -0 $PID3B 2>/dev/null; then
    echo "  Recovery server failed to start"
    phase_result FAIL
else
    RECOVERED=$(read_stats_field 11421 current-jobs-ready)
    echo "  Recovered: $RECOVERED / 100 jobs"

    if [ "$RECOVERED" -ge 95 ] 2>/dev/null; then
        echo "  WAL Recovery: PASS"
        phase_result PASS
    elif [ "$RECOVERED" -ge 50 ] 2>/dev/null; then
        echo "  WAL Recovery: DEGRADED"
        phase_result PASS
    else
        echo "  WAL Recovery: FAIL"
        phase_result FAIL
    fi
    kill $PID3B 2>/dev/null; wait $PID3B 2>/dev/null || true
fi

# ============================================================
echo ""
echo "=== PHASE 4: privilege drop sheds supplementary groups ==="
# ============================================================
# Hostile probe for su() in main.c (CWE-271): setuid/setgid alone do
# NOT clear the caller's supplementary groups. Start the server as
# root with deliberately poisoned supplementary groups; after
# `-u nobody` none of them may survive in /proc/<pid>/status.
# Pre-fix (no initgroups before setgid) this deterministically FAILs
# with "Groups: 1 2 3" retained.
setpriv --reuid 0 --regid 0 --groups 1,2,3 \
    $DEBUG_BIN -u nobody -p 11430 &
PID4=$!; sleep 2

if ! kill -0 $PID4 2>/dev/null; then
    echo "  Priv drop: FAIL (server did not start under -u nobody)"
    phase_result FAIL
else
    UID_LINE=$(grep '^Uid:' /proc/$PID4/status)
    GROUPS_LINE=$(grep '^Groups:' /proc/$PID4/status)
    echo "  $UID_LINE"
    echo "  $GROUPS_LINE"
    NOBODY_UID=$(id -u nobody)
    if ! echo "$UID_LINE" | grep -qw "$NOBODY_UID"; then
        echo "  Priv drop: FAIL (uid not dropped to nobody)"
        phase_result FAIL
    elif echo "$GROUPS_LINE" | sed 's/^Groups://' | grep -qwE '1|2|3'; then
        echo "  Priv drop: FAIL (root's supplementary groups retained)"
        phase_result FAIL
    else
        echo "  Priv drop: PASS"
        phase_result PASS
    fi
    kill $PID4 2>/dev/null || true; wait $PID4 2>/dev/null || true
fi

# Regression guard: an already-unprivileged `-u <self>` invocation must
# keep working. The group reset in su() is gated on euid==0 — an
# unconditional setgroups/initgroups would EPERM here and exit(34).
setpriv --reuid nobody --regid nogroup --init-groups \
    $DEBUG_BIN -u nobody -p 11431 &
PID5=$!; sleep 2

if kill -0 $PID5 2>/dev/null; then
    echo "  Non-root -u <self>: PASS"
    phase_result PASS
else
    echo "  Non-root -u <self>: FAIL (unprivileged -u nobody no longer starts)"
    phase_result FAIL
fi
kill $PID5 2>/dev/null || true; wait $PID5 2>/dev/null || true

# ============================================================
echo ""
echo "========================================"
echo "  RESULTS: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "========================================"
exit $FAIL_COUNT
