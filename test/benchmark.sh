#!/bin/bash
set -euo pipefail

UPSTREAM=${UPSTREAM:-/usr/local/bin/beanstalkd-upstream}
FORK=${FORK:-/usr/local/bin/beanstalkd-fork}
BENCH=${BENCH:-/usr/local/bin/bench}

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              beanstalkd A/B Benchmark                       ║"
echo "║   upstream vs fork — C bench client, isolated runs          ║"
echo "║   Both compiled: gcc -O2 -DNDEBUG (identical flags)         ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

stop() {
    kill $1 2>/dev/null; wait $1 2>/dev/null || true
    local tries=0
    while fuser $2/tcp >/dev/null 2>&1 && [ $tries -lt 20 ]; do
        fuser -k $2/tcp >/dev/null 2>&1 || true; sleep 0.1; tries=$((tries+1))
    done
}

# Run one C bench scenario. Args: port conns ops pipeline body
run_c() {
    $BENCH -p $1 -c $2 -n $3 -P $4 -B $5 2>/dev/null
}

# Parse bench output → "rate p50 p99 p999"
parse() {
    awk '/Rate:/{r=$2} /P50:/{p50=$3} /P99:/{p99=$3} /P999:/{p999=$3} END{print r,p50,p99,p999}'
}

# ── Run all scenarios for a binary ──────────────────────────

run_suite() {
    local label=$1 bin=$2 port=$3 extra="${4:-}"
    echo "━━━ $label ━━━"
    local w pid out parsed

    # S1: Throughput — 8 conns × 10K ops, pipeline=64, 128B
    w="/tmp/wal-$label-s1-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$(run_c $port 8 10000 64 128)
    parsed=$(echo "$out" | parse)
    eval "${label}_s1='$parsed'"
    echo "  S1 Throughput:  $(echo $parsed | awk '{printf "%s ops/s  P50=%s P99.9=%s μs", $1, $2, $4}')"
    stop $pid $port; rm -rf "$w"

    # S2: Latency — 1 conn × 5K ops, pipeline=1, 4B
    w="/tmp/wal-$label-s2-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$(run_c $port 1 5000 1 4)
    parsed=$(echo "$out" | parse)
    eval "${label}_s2='$parsed'"
    echo "  S2 Latency:     $(echo $parsed | awk '{printf "%s ops/s  P50=%s P99.9=%s μs", $1, $2, $4}')"
    stop $pid $port; rm -rf "$w"

    # S3: Large body — 8 conns × 2K ops, pipeline=16, 16KB
    w="/tmp/wal-$label-s3-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$(run_c $port 8 2000 16 16384)
    parsed=$(echo "$out" | parse)
    eval "${label}_s3='$parsed'"
    echo "  S3 Large body:  $(echo $parsed | awk '{print $1}') ops/s"
    stop $pid $port; rm -rf "$w"

    # S4: High concurrency — 32 conns × 5K ops, pipeline=32, 128B
    w="/tmp/wal-$label-s4-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$(run_c $port 32 5000 32 128)
    parsed=$(echo "$out" | parse)
    eval "${label}_s4='$parsed'"
    echo "  S4 32-conn:     $(echo $parsed | awk '{print $1}') ops/s"
    stop $pid $port; rm -rf "$w"

    # S5: Deep pipeline — 1 conn × 20K ops, pipeline=256, 128B
    w="/tmp/wal-$label-s5-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$(run_c $port 1 20000 256 128)
    parsed=$(echo "$out" | parse)
    eval "${label}_s5='$parsed'"
    echo "  S5 Deep pipe:   $(echo $parsed | awk '{print $1}') ops/s"
    stop $pid $port; rm -rf "$w"

    # S6: 500 tubes (Python)
    w="/tmp/wal-$label-s6-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    local s6rate
    s6rate=$(python3 -c "
import socket, time, threading, random
PORT=$port; random.seed(99); N_TUBES=500; JOBS_PER_TUBE=100; results=[]
def worker(bs, bsz):
    s=socket.socket(); s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    s.settimeout(120); s.connect(('127.0.0.1',PORT)); buf=b''
    def rl():
        nonlocal buf
        while b'\r\n' not in buf: buf+=s.recv(8192)
        i=buf.index(b'\r\n'); r=buf[:i]; buf=buf[i+2:]; return r
    def rn(n):
        nonlocal buf
        while len(buf)<n: buf+=s.recv(8192)
        r=buf[:n]; buf=buf[n:]; return r
    t0=time.monotonic(); ops=0
    for t in range(bs,bs+bsz):
        tube=f'tube-{t:04d}'.encode()
        s.sendall(b'use '+tube+b'\r\n'); rl()
        s.sendall(b'watch '+tube+b'\r\n'); rl()
        body_size=random.randint(16,256); body=b'x'*body_size
        put_cmd=f'put 1024 0 60 {body_size}\r\n'.encode()+body+b'\r\n'
        for _ in range(JOBS_PER_TUBE): s.sendall(put_cmd); rl(); ops+=1
        for _ in range(JOBS_PER_TUBE):
            s.sendall(b'reserve-with-timeout 0\r\n')
            parts=rl().split(); rn(int(parts[2])+2)
            s.sendall(b'delete '+parts[1]+b'\r\n'); rl(); ops+=1
        s.sendall(b'ignore '+tube+b'\r\n'); rl()
    results.append((ops, time.monotonic()-t0)); s.close()
t0=time.monotonic(); per=N_TUBES//4
threads=[threading.Thread(target=worker,args=(i*per,per)) for i in range(4)]
for t in threads: t.start()
for t in threads: t.join()
wall=time.monotonic()-t0; total=sum(r[0] for r in results)
print(f'{total/wall:.0f}')
" 2>/dev/null)
    eval "${label}_s6='$s6rate'"
    echo "  S6 500 tubes:   $s6rate ops/s"
    stop $pid $port; rm -rf "$w"

    echo "  done."
    echo ""
}

# ── Run ──────────────────────────────────────────────────────

run_suite "up" "$UPSTREAM" 11600 ""
run_suite "fk" "$FORK"     11700 ""

# ── Results ──────────────────────────────────────────────────

export up_s1 up_s2 up_s3 up_s4 up_s5 up_s6
export fk_s1 fk_s2 fk_s3 fk_s4 fk_s5 fk_s6

python3 << PYEOF
import os

def e(n): return os.environ.get(n, '0 0 0 0')
def v(s, i):
    parts = s.split()
    return parts[i] if i < len(parts) else '0'
def d(o, n, lb=False):
    try:
        of, nf = float(o), float(n)
    except: return 'N/A'
    if of == 0: return 'N/A'
    p = ((nf-of)/of)*100
    if lb: p = -p
    return f'{"+" if p > 0 else ""}{p:.1f}%'

up_s1 = e('up_s1'); fk_s1 = e('fk_s1')
up_s2 = e('up_s2'); fk_s2 = e('fk_s2')
up_s3 = e('up_s3'); fk_s3 = e('fk_s3')
up_s4 = e('up_s4'); fk_s4 = e('fk_s4')
up_s5 = e('up_s5'); fk_s5 = e('fk_s5')
up_s6 = e('up_s6'); fk_s6 = e('fk_s6')

fmt = '  {:<28s} {:>10s} {:>10s}  {:>8s}'
print()
print('╔══════════════════════════════════════════════════════════════╗')
print('║                      BENCHMARK RESULTS                      ║')
print('║          gcc -O2 -DNDEBUG, WAL enabled, fsync 50ms          ║')
print('╠══════════════════════════════════════════════════════════════╣')
print()
print(fmt.format('Scenario', 'Upstream', 'Fork', 'Δ'))
print(fmt.format('-'*28, '-'*10, '-'*10, '-'*8))

def row(name, u, f, lb=False):
    print(fmt.format(name, u, f, d(u,f,lb)))

row('S1: Throughput (ops/s)', v(up_s1,0), v(fk_s1,0))
row('  P50 latency (μs)', v(up_s1,1), v(fk_s1,1), True)
row('  P99.9 latency (μs)', v(up_s1,3), v(fk_s1,3), True)
row('S2: Latency (ops/s)', v(up_s2,0), v(fk_s2,0))
row('  P50 latency (μs)', v(up_s2,1), v(fk_s2,1), True)
row('  P99.9 latency (μs)', v(up_s2,3), v(fk_s2,3), True)
row('S3: Large body 16KB (ops/s)', v(up_s3,0), v(fk_s3,0))
row('S4: 32 connections (ops/s)', v(up_s4,0), v(fk_s4,0))
row('S5: Deep pipeline (ops/s)', v(up_s5,0), v(fk_s5,0))
row('S6: 500 tubes (ops/s)', up_s6, fk_s6)

print()
print('  S1: 8 conn × 10K put+reserve+delete, 128B body, pipeline=64')
print('  S2: 1 conn × 5K put+reserve+delete, 4B body, pipeline=1 (round-trip)')
print('  S3: 8 conn × 2K put+reserve+delete, 16KB body, pipeline=16')
print('  S4: 32 conn × 5K put+reserve+delete, 128B body, pipeline=32')
print('  S5: 1 conn × 20K put+reserve+delete, 128B body, pipeline=256')
print('  S6: 500 tubes × 100 jobs each, 4 clients, 16-256B mixed bodies')
print()
print('╚══════════════════════════════════════════════════════════════╝')
PYEOF

echo ""
echo "Done."
