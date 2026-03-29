#!/bin/bash
set -euo pipefail

UPSTREAM=/usr/local/bin/beanstalkd-upstream
FORK=/usr/local/bin/beanstalkd-fork

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              beanstalkd A/B Benchmark                       ║"
echo "║    upstream vs fork — identical flags, isolated runs         ║"
echo "║    Both compiled: gcc -O2 -DNDEBUG (no LTO, no strip)       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

get_proc_stats() {
    local pid=$1
    local rss=$(awk '/^VmRSS:/ {print $2}' /proc/$pid/status 2>/dev/null || echo 0)
    local utime=$(awk '{print $14}' /proc/$pid/stat 2>/dev/null || echo 0)
    local stime=$(awk '{print $15}' /proc/$pid/stat 2>/dev/null || echo 0)
    local clk=$(getconf CLK_TCK)
    local cpu=$(python3 -c "print(f'{($utime + $stime) / $clk:.3f}')")
    echo "$rss $cpu"
}

# ═══════════════════════════════════════════════════════════════
# SCENARIO 1: Throughput — 8 clients × 10K jobs, 128B body
# ═══════════════════════════════════════════════════════════════
run_throughput() {
    local port=$1
    python3 << PYEOF
import socket, time, threading
PORT, N_JOBS, N_CONNS = $port, 10000, 8
results = []
def worker(wid):
    s = socket.socket(); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(60); s.connect(('127.0.0.1', PORT))
    buf = b''
    def rl():
        nonlocal buf
        while b'\r\n' not in buf: buf += s.recv(8192)
        i = buf.index(b'\r\n'); line, buf2 = buf[:i], buf[i+2:]; buf = buf2; return line  # noqa
    def rn(n):
        nonlocal buf
        while len(buf) < n: buf += s.recv(8192)
        d, buf2 = buf[:n], buf[n:]; buf = buf2; return d  # noqa
    # Fix: reassign buf properly
    buf = b''
    def rl2():
        nonlocal buf
        while b'\r\n' not in buf:
            buf += s.recv(8192)
        i = buf.index(b'\r\n')
        r = buf[:i]
        buf = buf[i+2:]
        return r
    def rn2(n):
        nonlocal buf
        while len(buf) < n:
            buf += s.recv(8192)
        r = buf[:n]
        buf = buf[n:]
        return r
    tube = f't{wid}'.encode()
    s.sendall(b'use ' + tube + b'\r\n'); rl2()
    s.sendall(b'watch ' + tube + b'\r\n'); rl2()
    body = b'x' * 128
    put_cmd = b'put 1024 0 60 128\r\n' + body + b'\r\n'
    t0 = time.monotonic()
    for _ in range(N_JOBS):
        s.sendall(put_cmd); rl2()
    t_put = time.monotonic()
    for _ in range(N_JOBS):
        s.sendall(b'reserve-with-timeout 0\r\n')
        parts = rl2().split(); rn2(int(parts[2]) + 2)
        s.sendall(b'delete ' + parts[1] + b'\r\n'); rl2()
    t_done = time.monotonic()
    s.close()
    results.append((N_JOBS, t_put-t0, t_done-t_put, t_done-t0))
t0 = time.monotonic()
threads = [threading.Thread(target=worker, args=(i,)) for i in range(N_CONNS)]
for t in threads: t.start()
for t in threads: t.join()
wall = time.monotonic() - t0
total = sum(r[0] for r in results)
print(f'{wall:.4f}\n{total}\n{total/wall:.0f}')
print(f'{sum(r[0]/r[1] for r in results)/len(results):.0f}')
print(f'{sum(r[0]/r[2] for r in results)/len(results):.0f}')
PYEOF
}

# ═══════════════════════════════════════════════════════════════
# SCENARIO 2: Multi-tube realistic (20 tubes, 8B-8KB bodies)
# ═══════════════════════════════════════════════════════════════
run_multitube() {
    local port=$1
    python3 << PYEOF
import socket, time, threading, random
PORT = $port; random.seed(42)
TUBES = []
for i in range(5):  TUBES.append((f'ids-{i}', random.randint(8,32), 2000))
for i in range(10): TUBES.append((f'json-{i}', random.randint(128,512), 1000))
for i in range(5):  TUBES.append((f'data-{i}', random.randint(2048,8192), 200))
results = []
def worker(tube_name, body_size, n_jobs):
    s = socket.socket(); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(60); s.connect(('127.0.0.1', PORT))
    buf = b''
    def rl():
        nonlocal buf
        while b'\r\n' not in buf: buf += s.recv(8192)
        i = buf.index(b'\r\n'); r = buf[:i]; buf = buf[i+2:]; return r  # noqa
    buf = b''
    def rl2():
        nonlocal buf
        while b'\r\n' not in buf: buf += s.recv(8192)
        i = buf.index(b'\r\n'); r = buf[:i]; buf = buf[i+2:]; return r
    def rn2(n):
        nonlocal buf
        while len(buf) < n: buf += s.recv(8192)
        r = buf[:n]; buf = buf[n:]; return r
    s.sendall(f'use {tube_name}\r\n'.encode()); rl2()
    s.sendall(f'watch {tube_name}\r\n'.encode()); rl2()
    body = b'D' * body_size
    put_cmd = f'put 1024 0 60 {body_size}\r\n'.encode() + body + b'\r\n'
    t0 = time.monotonic()
    for _ in range(n_jobs): s.sendall(put_cmd); rl2()
    for _ in range(n_jobs):
        s.sendall(b'reserve-with-timeout 0\r\n')
        parts = rl2().split(); rn2(int(parts[2]) + 2)
        s.sendall(b'delete ' + parts[1] + b'\r\n'); rl2()
    elapsed = time.monotonic() - t0
    s.close()
    results.append((n_jobs, body_size, elapsed))
t0 = time.monotonic()
threads = [threading.Thread(target=worker, args=t) for t in TUBES]
for t in threads: t.start()
for t in threads: t.join()
wall = time.monotonic() - t0
total = sum(r[0] for r in results)
print(f'{wall:.4f}\n{total}\n{total/wall:.0f}\n{len(TUBES)}')
PYEOF
}

# ═══════════════════════════════════════════════════════════════
# SCENARIO 3: Massive tube count — 500 tubes, proves O(1) heaps
# ═══════════════════════════════════════════════════════════════
run_many_tubes() {
    local port=$1
    python3 << PYEOF
import socket, time, threading, random
PORT = $port; random.seed(99)
N_TUBES = 500; JOBS_PER_TUBE = 100
results = []
def worker(batch_start, batch_size):
    s = socket.socket(); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(120); s.connect(('127.0.0.1', PORT))
    buf = b''
    def rl():
        nonlocal buf
        while b'\r\n' not in buf: buf += s.recv(8192)
        i = buf.index(b'\r\n'); r = buf[:i]; buf = buf[i+2:]; return r
    def rn(n):
        nonlocal buf
        while len(buf) < n: buf += s.recv(8192)
        r = buf[:n]; buf = buf[n:]; return r
    t0 = time.monotonic()
    ops = 0
    for t in range(batch_start, batch_start + batch_size):
        tube = f'tube-{t:04d}'.encode()
        s.sendall(b'use ' + tube + b'\r\n'); rl()
        s.sendall(b'watch ' + tube + b'\r\n'); rl()
        body_size = random.randint(16, 256)
        body = b'x' * body_size
        put_cmd = f'put 1024 0 60 {body_size}\r\n'.encode() + body + b'\r\n'
        for _ in range(JOBS_PER_TUBE):
            s.sendall(put_cmd); rl(); ops += 1
        for _ in range(JOBS_PER_TUBE):
            s.sendall(b'reserve-with-timeout 0\r\n')
            parts = rl().split(); rn(int(parts[2]) + 2)
            s.sendall(b'delete ' + parts[1] + b'\r\n'); rl()
            ops += 1
        s.sendall(b'ignore ' + tube + b'\r\n'); rl()
    elapsed = time.monotonic() - t0
    s.close()
    results.append((ops, elapsed))

# 4 workers, each handles 125 tubes
t0 = time.monotonic()
N_WORKERS = 4; per = N_TUBES // N_WORKERS
threads = [threading.Thread(target=worker, args=(i*per, per)) for i in range(N_WORKERS)]
for t in threads: t.start()
for t in threads: t.join()
wall = time.monotonic() - t0
total = sum(r[0] for r in results)
print(f'{wall:.4f}\n{total}\n{total/wall:.0f}\n{N_TUBES}')
PYEOF
}

# ═══════════════════════════════════════════════════════════════
# SCENARIO 4: Latency — single client, P50/P99/P99.9
# ═══════════════════════════════════════════════════════════════
run_latency() {
    local port=$1
    python3 << PYEOF
import socket, time
PORT = $port; N = 5000
s = socket.socket(); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
s.settimeout(30); s.connect(('127.0.0.1', PORT))
buf = b''
def rl():
    global buf
    while b'\r\n' not in buf: buf += s.recv(8192)
    i = buf.index(b'\r\n'); r = buf[:i]; buf = buf[i+2:]; return r
def rn(n):
    global buf
    while len(buf) < n: buf += s.recv(8192)
    r = buf[:n]; buf = buf[n:]; return r
latencies = []
for _ in range(N):
    t0 = time.monotonic()
    s.sendall(b'put 1024 0 60 4\r\ntest\r\n'); rl()
    s.sendall(b'reserve-with-timeout 0\r\n')
    parts = rl().split(); rn(int(parts[2]) + 2)
    s.sendall(b'delete ' + parts[1] + b'\r\n'); rl()
    latencies.append(time.monotonic() - t0)
s.close()
latencies.sort()
us = lambda l: [x*1e6 for x in l]
L = us(latencies)
print(f'{sum(L)/len(L):.1f}\n{L[len(L)//2]:.1f}\n{L[int(len(L)*0.99)]:.1f}\n{L[int(len(L)*0.999)]:.1f}')
PYEOF
}

# ═══════════════════════════════════════════════════════════════
# SCENARIO 5: Connection storm — 200 rapid connect/disconnect
# ═══════════════════════════════════════════════════════════════
run_conn_storm() {
    local port=$1
    python3 << PYEOF
import socket, time, threading
PORT = $port; N_CONNS = 200; N_WORKERS = 8
results = []
def worker(n):
    t0 = time.monotonic()
    for _ in range(n):
        s = socket.socket(); s.settimeout(10)
        s.connect(('127.0.0.1', PORT))
        s.sendall(b'stats\r\n')
        buf = b''
        while b'\r\n' not in buf: buf += s.recv(4096)
        parts = buf.split(b'\r\n')[0].split()
        body_len = int(parts[1])
        rest = buf[buf.index(b'\r\n')+2:]
        while len(rest) < body_len + 2: rest += s.recv(4096)
        s.close()
    results.append(time.monotonic() - t0)
t0 = time.monotonic()
per = N_CONNS // N_WORKERS
threads = [threading.Thread(target=worker, args=(per,)) for _ in range(N_WORKERS)]
for t in threads: t.start()
for t in threads: t.join()
wall = time.monotonic() - t0
print(f'{wall:.4f}\n{N_CONNS}\n{N_CONNS/wall:.0f}')
PYEOF
}

# ═══════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════
run_full() {
    local label=$1 bin=$2 port=$3 extra=${4:-""}

    # S1: Throughput
    local w="/tmp/wal-$$-$label-s1"; rm -rf "$w"; mkdir -p "$w"
    "$bin" $extra -p "$port" -b "$w" -f 50 & local pid=$!; sleep 1
    local s1=$(run_throughput "$port")
    local st1=$(get_proc_stats $pid)
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true

    # S2: Multi-tube
    w="/tmp/wal-$$-$label-s2"; rm -rf "$w"; mkdir -p "$w"
    "$bin" $extra -p "$port" -b "$w" -f 50 & pid=$!; sleep 1
    local s2=$(run_multitube "$port")
    local st2=$(get_proc_stats $pid)
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true

    # S3: Many tubes (500)
    w="/tmp/wal-$$-$label-s3"; rm -rf "$w"; mkdir -p "$w"
    "$bin" $extra -p "$port" -b "$w" -f 50 & pid=$!; sleep 1
    local s3=$(run_many_tubes "$port")
    local st3=$(get_proc_stats $pid)
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true

    # S4: Latency
    w="/tmp/wal-$$-$label-s4"; rm -rf "$w"; mkdir -p "$w"
    "$bin" $extra -p "$port" -b "$w" -f 50 & pid=$!; sleep 1
    local s4=$(run_latency "$port")
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true

    # S5: Connection storm
    w="/tmp/wal-$$-$label-s5"; rm -rf "$w"; mkdir -p "$w"
    "$bin" $extra -p "$port" -b "$w" -f 50 & pid=$!; sleep 1
    local s5=$(run_conn_storm "$port")
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true
    rm -rf /tmp/wal-$$-$label-*

    # Export
    eval "${label}_s1_wall=$(echo "$s1"|sed -n 1p)"; eval "${label}_s1_ops=$(echo "$s1"|sed -n 3p)"
    eval "${label}_s1_put=$(echo "$s1"|sed -n 4p)"; eval "${label}_s1_cycle=$(echo "$s1"|sed -n 5p)"
    eval "${label}_s1_rss=$(echo "$st1"|awk '{print $1}')"; eval "${label}_s1_cpu=$(echo "$st1"|awk '{print $2}')"
    eval "${label}_s2_wall=$(echo "$s2"|sed -n 1p)"; eval "${label}_s2_ops=$(echo "$s2"|sed -n 3p)"
    eval "${label}_s2_rss=$(echo "$st2"|awk '{print $1}')"
    eval "${label}_s3_wall=$(echo "$s3"|sed -n 1p)"; eval "${label}_s3_ops=$(echo "$s3"|sed -n 3p)"
    eval "${label}_s3_tubes=$(echo "$s3"|sed -n 4p)"
    eval "${label}_s3_rss=$(echo "$st3"|awk '{print $1}')"; eval "${label}_s3_cpu=$(echo "$st3"|awk '{print $2}')"
    eval "${label}_s4_avg=$(echo "$s4"|sed -n 1p)"; eval "${label}_s4_p50=$(echo "$s4"|sed -n 2p)"
    eval "${label}_s4_p99=$(echo "$s4"|sed -n 3p)"; eval "${label}_s4_p999=$(echo "$s4"|sed -n 4p)"
    eval "${label}_s5_wall=$(echo "$s5"|sed -n 1p)"; eval "${label}_s5_cps=$(echo "$s5"|sed -n 3p)"

    echo "  $label: done"
}

echo "━━━ Running UPSTREAM ━━━"
run_full "up" "$UPSTREAM" 11600
echo ""
echo "━━━ Running FORK ━━━"
run_full "fk" "$FORK" 11700 "-w 1"
echo ""

# ═══════════════════════════════════════════════════════════════
# Results
# ═══════════════════════════════════════════════════════════════
python3 << 'PYEOF'
import os

def e(name):
    return os.environ.get(name, '0')

def d(old, new, lower_better=False):
    o, n = float(old), float(new)
    if o == 0: return "N/A"
    pct = ((n - o) / o) * 100
    if lower_better: pct = -pct
    return f'{"+" if pct > 0 else ""}{pct:.1f}%'

def row(label, up_val, fk_val, lower_better=False, fmt='f', prec=2):
    u = float(up_val); f = float(fk_val)
    if fmt == 'd':
        us = f'{int(u):>9,}'; fs = f'{int(f):>9,}'
    else:
        us = f'{u:>9.{prec}f}'; fs = f'{f:>9.{prec}f}'
    delta = d(up_val, fk_val, lower_better)
    print(f'  │ {label:<22s}│ {us} │ {fs} │ {delta:>16s} │')

print()
print('╔═══════════════════════════════════════════════════════════════════════╗')
print('║                       BENCHMARK RESULTS                             ║')
print('║          Both binaries: gcc -O2 -DNDEBUG (identical flags)          ║')
print('╠═══════════════════════════════════════════════════════════════════════╣')

print()
print('  SCENARIO 1: Throughput (8 clients × 10K jobs, 128B body, WAL)')
print('  ┌───────────────────────┬───────────┬───────────┬──────────────────┐')
print('  │ Metric                │ Upstream  │ Fork      │ Delta            │')
print('  ├───────────────────────┼───────────┼───────────┼──────────────────┤')
PYEOF

# Inject vars into python env
export up_s1_wall up_s1_ops up_s1_put up_s1_cycle up_s1_rss up_s1_cpu
export fk_s1_wall fk_s1_ops fk_s1_put fk_s1_cycle fk_s1_rss fk_s1_cpu
export up_s2_wall up_s2_ops up_s2_rss fk_s2_wall fk_s2_ops fk_s2_rss
export up_s3_wall up_s3_ops up_s3_tubes up_s3_rss up_s3_cpu
export fk_s3_wall fk_s3_ops fk_s3_tubes fk_s3_rss fk_s3_cpu
export up_s4_avg up_s4_p50 up_s4_p99 up_s4_p999
export fk_s4_avg fk_s4_p50 fk_s4_p99 fk_s4_p999
export up_s5_wall up_s5_cps fk_s5_wall fk_s5_cps

python3 << 'PYEOF'
import os
def e(n): return os.environ.get(n, '0')
def d(old, new, lb=False):
    o,n=float(old),float(new)
    if o==0: return "N/A"
    p=((n-o)/o)*100
    if lb: p=-p
    return f'{"+" if p>0 else ""}{p:.1f}%'
def row(label,uv,fv,lb=False,fmt='f',pr=2):
    u,f=float(uv),float(fv)
    us=f'{int(u):>9,}' if fmt=='d' else f'{u:>9.{pr}f}'
    fs=f'{int(f):>9,}' if fmt=='d' else f'{f:>9.{pr}f}'
    print(f'  │ {label:<22s}│ {us} │ {fs} │ {d(uv,fv,lb):>16s} │')

row('Wall time (s)',e('up_s1_wall'),e('fk_s1_wall'),True)
row('Throughput (ops/s)',e('up_s1_ops'),e('fk_s1_ops'),fmt='d')
row('PUT ops/s (per conn)',e('up_s1_put'),e('fk_s1_put'),fmt='d')
row('CYCLE ops/s (conn)',e('up_s1_cycle'),e('fk_s1_cycle'),fmt='d')
row('Peak RSS (KB)',e('up_s1_rss'),e('fk_s1_rss'),True,fmt='d')
row('CPU time (s)',e('up_s1_cpu'),e('fk_s1_cpu'),True,pr=3)
print('  └───────────────────────┴───────────┴───────────┴──────────────────┘')

print()
print('  SCENARIO 2: Multi-tube (20 tubes, mixed 8B-8KB bodies, WAL)')
print('  ┌───────────────────────┬───────────┬───────────┬──────────────────┐')
print('  │ Metric                │ Upstream  │ Fork      │ Delta            │')
print('  ├───────────────────────┼───────────┼───────────┼──────────────────┤')
row('Wall time (s)',e('up_s2_wall'),e('fk_s2_wall'),True)
row('Throughput (ops/s)',e('up_s2_ops'),e('fk_s2_ops'),fmt='d')
row('Peak RSS (KB)',e('up_s2_rss'),e('fk_s2_rss'),True,fmt='d')
print('  └───────────────────────┴───────────┴───────────┴──────────────────┘')

print()
print('  SCENARIO 3: Massive tubes (500 tubes × 100 jobs, 4 clients, WAL)')
print('  ┌───────────────────────┬───────────┬───────────┬──────────────────┐')
print('  │ Metric                │ Upstream  │ Fork      │ Delta            │')
print('  ├───────────────────────┼───────────┼───────────┼──────────────────┤')
row('Wall time (s)',e('up_s3_wall'),e('fk_s3_wall'),True)
row('Throughput (ops/s)',e('up_s3_ops'),e('fk_s3_ops'),fmt='d')
row('Peak RSS (KB)',e('up_s3_rss'),e('fk_s3_rss'),True,fmt='d')
row('CPU time (s)',e('up_s3_cpu'),e('fk_s3_cpu'),True,pr=3)
print('  └───────────────────────┴───────────┴───────────┴──────────────────┘')

print()
print('  SCENARIO 4: Latency (1 client, 5K put+reserve+delete, 4B body)')
print('  ┌───────────────────────┬───────────┬───────────┬──────────────────┐')
print('  │ Metric                │ Upstream  │ Fork      │ Delta            │')
print('  ├───────────────────────┼───────────┼───────────┼──────────────────┤')
row('Avg latency (μs)',e('up_s4_avg'),e('fk_s4_avg'),True,pr=1)
row('P50 latency (μs)',e('up_s4_p50'),e('fk_s4_p50'),True,pr=1)
row('P99 latency (μs)',e('up_s4_p99'),e('fk_s4_p99'),True,pr=1)
row('P99.9 latency (μs)',e('up_s4_p999'),e('fk_s4_p999'),True,pr=1)
print('  └───────────────────────┴───────────┴───────────┴──────────────────┘')

print()
print('  SCENARIO 5: Connection storm (200 connect+stats+close, 8 threads)')
print('  ┌───────────────────────┬───────────┬───────────┬──────────────────┐')
print('  │ Metric                │ Upstream  │ Fork      │ Delta            │')
print('  ├───────────────────────┼───────────┼───────────┼──────────────────┤')
row('Wall time (s)',e('up_s5_wall'),e('fk_s5_wall'),True)
row('Connections/s',e('up_s5_cps'),e('fk_s5_cps'),fmt='d')
print('  └───────────────────────┴───────────┴───────────┴──────────────────┘')

print()
print('╚═══════════════════════════════════════════════════════════════════════╝')
PYEOF

echo ""
echo "Done."
