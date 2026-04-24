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

# Parse bench output → "rate p50 p99 p999 p9999 max"
parse() {
    awk '/Rate:/{r=$2} /P50:/{p50=$3} /P99:/{p99=$3} /P999:/{p999=$3} /P9999:/{p9999=$3} /Max:/{m=$3} END{print r,p50,p99,p999,p9999,m}'
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

    # S8: Tail-probe — 1 conn × 100K ops, pipeline=1, 4B body (long serial RTT)
    # Investigates tail-latency regression seen in short S2. 100K samples gives
    # statistically stable P99.99 / max numbers.
    w="/tmp/wal-$label-s8-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$($BENCH -p $port -c 1 -n 100000 -P 1 -B 4 2>/dev/null)
    parsed=$(echo "$out" | parse)
    eval "${label}_s8='$parsed'"
    echo "  S8 tail-probe: $(echo $parsed | awk '{printf "%s ops/s  P50=%s P999=%s P9999=%s Max=%s μs", $1, $2, $4, $5, $6}')"
    stop $pid $port; rm -rf "$w"

    # S7: Deep-watch — 8 conns × 500 ops, pipeline=16, 128B, 500 tubes per conn
    # Worst-case for reserve fast-path: PUTs target LAST-watched tube so server
    # must scan all N watched tubes on every reserve. Tests O(watched_tubes) cost.
    w="/tmp/wal-$label-s7-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    out=$($BENCH -p $port -c 8 -n 500 -P 16 -B 128 -W 500 2>/dev/null)
    parsed=$(echo "$out" | parse)
    eval "${label}_s7='$parsed'"
    echo "  S7 deep-watch: $(echo $parsed | awk '{printf "%s ops/s  P50=%s P99.9=%s μs", $1, $2, $4}')"
    stop $pid $port; rm -rf "$w"

    # S9/S10: Durable mode (-D) — fork only. Upstream has no -D flag.
    # Measures fsync cost per op (S9) and how pipelining amortizes it (S10).
    if [ "$label" = "fk" ]; then
        # S9: -D × 1 conn × 5K × pipeline=1 × 4B — serial fsync cost.
        w="/tmp/wal-$label-s9-$$"; rm -rf "$w"; mkdir -p "$w"
        $bin $extra -D -p $port -b "$w" >/dev/null 2>&1 &
        pid=$!; sleep 1
        out=$($BENCH -p $port -c 1 -n 5000 -P 1 -B 4 2>/dev/null)
        parsed=$(echo "$out" | parse)
        eval "${label}_s9='$parsed'"
        echo "  S9 -D serial:  $(echo $parsed | awk '{printf "%s ops/s  P50=%s P99.9=%s μs", $1, $2, $4}')"
        stop $pid $port; rm -rf "$w"

        # S10: -D × 8 conn × 5K × pipeline=64 × 128B — parallel durable throughput.
        w="/tmp/wal-$label-s10-$$"; rm -rf "$w"; mkdir -p "$w"
        $bin $extra -D -p $port -b "$w" >/dev/null 2>&1 &
        pid=$!; sleep 1
        out=$($BENCH -p $port -c 8 -n 5000 -P 64 -B 128 2>/dev/null)
        parsed=$(echo "$out" | parse)
        eval "${label}_s10='$parsed'"
        echo "  S10 -D pipe:   $(echo $parsed | awk '{printf "%s ops/s  P50=%s P99.9=%s μs", $1, $2, $4}')"
        stop $pid $port; rm -rf "$w"
    else
        eval "${label}_s9='0 0 0 0 0 0'"
        eval "${label}_s10='0 0 0 0 0 0'"
        echo "  S9/S10 -D:     (upstream has no -D flag, skipped)"
    fi

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

    # S11: 10K tubes (Python) — stress tube_find hash chain walk.
    # wyhash v4 spreads 10K names across 4096 fixed buckets → avg chain ≈ 2.4.
    # Tests hypothesis 1.4 (tube hash dynamic resize).
    w="/tmp/wal-$label-s11-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    local s11rate
    s11rate=$(python3 -c "
import socket, time, threading, random
PORT=$port; random.seed(99); N_TUBES=10000; JOBS_PER_TUBE=10; results=[]
def worker(bs, bsz):
    s=socket.socket(); s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    s.settimeout(240); s.connect(('127.0.0.1',PORT)); buf=b''
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
        tube=f'mtube-{t:06d}'.encode()
        s.sendall(b'use '+tube+b'\r\n'); rl()
        s.sendall(b'watch '+tube+b'\r\n'); rl()
        body=b'x'*64
        put_cmd=b'put 1024 0 60 64\r\n'+body+b'\r\n'
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
    eval "${label}_s11='$s11rate'"
    echo "  S11 10K tubes:  $s11rate ops/s"
    stop $pid $port; rm -rf "$w"

    # S13: connection churn (Python) — open + use + put + quit, repeat.
    # Tests make_conn / connclose / conn_pool (256-slab pool → churn above cap
    # hits malloc). 4 workers × 2000 conns = 8K make_conn/s target.
    # Tests hypothesis 1.10 (pool resize).
    w="/tmp/wal-$label-s13-$$"; rm -rf "$w"; mkdir -p "$w"
    $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    pid=$!; sleep 1
    local s13rate
    s13rate=$(python3 -c "
import socket, time, threading
PORT=$port; ITERS_PER_WORKER=2000; WORKERS=4; results=[]
def worker():
    t0=time.monotonic(); ops=0
    for _ in range(ITERS_PER_WORKER):
        s=socket.socket(); s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        s.settimeout(30); s.connect(('127.0.0.1',PORT))
        s.sendall(b'put 1024 0 60 4\r\ndata\r\n')
        buf=b''
        while b'\r\n' not in buf: buf+=s.recv(4096)
        s.close(); ops+=1
    results.append((ops, time.monotonic()-t0))
t0=time.monotonic()
threads=[threading.Thread(target=worker) for _ in range(WORKERS)]
for t in threads: t.start()
for t in threads: t.join()
wall=time.monotonic()-t0; total=sum(r[0] for r in results)
print(f'{total/wall:.0f}')
" 2>/dev/null)
    eval "${label}_s13='$s13rate'"
    echo "  S13 conn churn: $s13rate conn/s"
    stop $pid $port; rm -rf "$w"

    # S12: truncate-heavy (Python) — fork-only (upstream has no truncate).
    # 1 worker: put 1000 → truncate → repeat × 50. Measures both PUT rate
    # under accumulation pressure and truncate latency with 1K-deep heaps.
    # Tests hypothesis 1.5 (truncated tube index) and lazy-reap budget.
    if [ "$label" = "fk" ]; then
        w="/tmp/wal-$label-s12-$$"; rm -rf "$w"; mkdir -p "$w"
        $bin $extra -p $port -b "$w" -f 50 >/dev/null 2>&1 &
        pid=$!; sleep 1
        local s12rate s12trunc
        read s12rate s12trunc <<< $(python3 -c "
import socket, time
PORT=$port; PUTS_PER_CYCLE=1000; CYCLES=50
s=socket.socket(); s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
s.settimeout(120); s.connect(('127.0.0.1',PORT)); buf=b''
def rl():
    global buf
    while b'\r\n' not in buf: buf+=s.recv(8192)
    i=buf.index(b'\r\n'); r=buf[:i]; buf=buf[i+2:]; return r
s.sendall(b'use trunc-bench\r\n'); rl()
put_cmd=b'put 1024 0 60 32\r\n'+b'x'*32+b'\r\n'
put_ops=0; trunc_total_us=0
t0=time.monotonic()
for _ in range(CYCLES):
    for _ in range(PUTS_PER_CYCLE):
        s.sendall(put_cmd); rl(); put_ops+=1
    tt=time.monotonic()
    s.sendall(b'truncate trunc-bench\r\n'); rl()
    trunc_total_us += (time.monotonic()-tt)*1e6
wall=time.monotonic()-t0; s.close()
print(f'{put_ops/wall:.0f} {trunc_total_us/CYCLES:.0f}')
" 2>/dev/null)
        eval "${label}_s12='$s12rate'"
        eval "${label}_s12t='$s12trunc'"
        echo "  S12 trunc-chrn: $s12rate put/s  (truncate avg=${s12trunc}μs @ 1K jobs)"
        stop $pid $port; rm -rf "$w"
    else
        eval "${label}_s12='0'"
        eval "${label}_s12t='0'"
        echo "  S12 trunc-chrn: (upstream has no truncate command, skipped)"
    fi

    echo "  done."
    echo ""
}

# ── Run ──────────────────────────────────────────────────────

run_suite "up" "$UPSTREAM" 11600 ""
run_suite "fk" "$FORK"     11700 ""

# ── Results ──────────────────────────────────────────────────

export up_s1 up_s2 up_s3 up_s4 up_s5 up_s6 up_s7 up_s8 up_s9 up_s10 up_s11 up_s12 up_s12t up_s13
export fk_s1 fk_s2 fk_s3 fk_s4 fk_s5 fk_s6 fk_s7 fk_s8 fk_s9 fk_s10 fk_s11 fk_s12 fk_s12t fk_s13

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
up_s7 = e('up_s7'); fk_s7 = e('fk_s7')
up_s8 = e('up_s8'); fk_s8 = e('fk_s8')
up_s9 = e('up_s9'); fk_s9 = e('fk_s9')
up_s10 = e('up_s10'); fk_s10 = e('fk_s10')
up_s11 = e('up_s11'); fk_s11 = e('fk_s11')
up_s12 = e('up_s12'); fk_s12 = e('fk_s12')
up_s12t = e('up_s12t'); fk_s12t = e('fk_s12t')
up_s13 = e('up_s13'); fk_s13 = e('fk_s13')

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
row('S7: Deep-watch (ops/s)', v(up_s7,0), v(fk_s7,0))
row('  P50 latency (μs)', v(up_s7,1), v(fk_s7,1), True)
row('  P99.9 latency (μs)', v(up_s7,3), v(fk_s7,3), True)
row('S8: Tail-probe (ops/s)', v(up_s8,0), v(fk_s8,0))
row('  P50 latency (μs)', v(up_s8,1), v(fk_s8,1), True)
row('  P99.9 latency (μs)', v(up_s8,3), v(fk_s8,3), True)
row('  P99.99 latency (μs)', v(up_s8,4), v(fk_s8,4), True)
row('  Max latency (μs)', v(up_s8,5), v(fk_s8,5), True)
row('S11: 10K tubes (ops/s)', up_s11, fk_s11)
row('S13: Conn churn (conn/s)', up_s13, fk_s13)
print()
print('  Truncate-heavy (fork-only, upstream has no truncate):')
row('S12: put rate under trunc',       '-', fk_s12)
row('  truncate avg (μs @ 1K jobs)',   '-', fk_s12t, True)
print()
print('  Durable mode (-D) — fork-only (upstream has no -D flag):')
row('S9: -D serial (ops/s)',          '-', v(fk_s9,0))
row('  P50 latency (μs)',             '-', v(fk_s9,1))
row('  P99.9 latency (μs)',           '-', v(fk_s9,3))
row('S10: -D pipelined (ops/s)',      '-', v(fk_s10,0))
row('  P50 latency (μs)',             '-', v(fk_s10,1))
row('  P99.9 latency (μs)',           '-', v(fk_s10,3))

# Durability cost ratios: fork durable vs fork async under same shape
def pct(num, den):
    try:
        n, d_ = float(num), float(den)
        if d_ == 0: return 'N/A'
        return f'{(n/d_)*100:.1f}%'
    except: return 'N/A'

print()
print('  Cost-of-durability (fork / fork):')
print(f'    S9/S2   serial durable  vs serial async   : {pct(v(fk_s9,0), v(fk_s2,0))}')
print(f'    S10/S1  pipelined dur.  vs pipelined async: {pct(v(fk_s10,0), v(fk_s1,0))}')
print(f'    S9 P50 / S2 P50         (how slow one op) : {pct(v(fk_s9,1), v(fk_s2,1))}')

print()
print('  S1: 8 conn × 10K put+reserve+delete, 128B body, pipeline=64')
print('  S2: 1 conn × 5K put+reserve+delete, 4B body, pipeline=1 (round-trip)')
print('  S3: 8 conn × 2K put+reserve+delete, 16KB body, pipeline=16')
print('  S4: 32 conn × 5K put+reserve+delete, 128B body, pipeline=32')
print('  S5: 1 conn × 20K put+reserve+delete, 128B body, pipeline=256')
print('  S6: 500 tubes × 100 jobs each, 4 clients, 16-256B mixed bodies')
print('  S7: 8 conn × 500 ops, 500 watched tubes each, PUTs target LAST-watched')
print('      (reserve fast-path forced to scan all 500 — measures O(watched_tubes))')
print('  S8: 1 conn × 100K ops, pipeline=1, 4B body (long serial RTT for tail study)')
print('  S9: -D × 1 conn × 5K ops, pipeline=1, 4B (serial fsync cost)')
print('  S10: -D × 8 conn × 5K ops, pipeline=64, 128B (durable under pipeline load)')
print('  S11: 10K tubes × 10 jobs each, 4 clients (tube_find hash chain at scale)')
print('  S12: put 1000 → truncate × 50 cycles (truncate cost at 1K-deep heap)')
print('  S13: open+put+quit × 2000 × 4 workers (make_conn/connclose churn)')
print()
print('╚══════════════════════════════════════════════════════════════╝')
PYEOF

echo ""
echo "Done."
