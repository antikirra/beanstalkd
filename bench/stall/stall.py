# Does memory upkeep stall the event loop, and does freed memory still
# go back to the OS?
#
# Phase 1: fill N jobs, delete every other one (a fragmented heap that
# trimming can walk but not shrink), then send a trivial command back to
# back for T seconds with -m 1 and time every reply. A reply slower than
# STALL_MS is the loop busy with something that is not a client.
# Phase 2: delete the rest except one job in every 5000 and the newest,
# stay idle, and read RSS. The survivors pin the top of the heap, so
# malloc's own shrinking cannot help: only a trim returns the megabyte
# spans between them. The memory must come back.
import json, socket, subprocess, sys, time

bin_, port, N, SIZE, T = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), float(sys.argv[5])
STALL_MS = 5.0
p = subprocess.Popen([bin_, "-l", "127.0.0.1", "-p", str(port), "-m", "1"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.5)
s = socket.create_connection(("127.0.0.1", port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

def drain(n):
    while n > 0:
        n -= s.recv(1 << 20).count(b"\r\n")

def rss_mb():
    for l in open("/proc/%d/status" % p.pid):
        if l.startswith("VmRSS"):
            return int(l.split()[1]) / 1024.0

def delete(ids):
    ids = list(ids)
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        s.sendall(b"".join(b"delete %d\r\n" % j for j in chunk)); drain(len(chunk))

put = b"put 0 0 600 %d\r\n%s\r\n" % (SIZE, b"x" * SIZE)
for i in range(0, N, 1000):
    s.sendall(put * 1000); drain(1000)
full = rss_mb()
delete(range(1, N + 1, 2))

rtt = []
end = time.monotonic() + T
while time.monotonic() < end:
    t0 = time.perf_counter(); s.sendall(b"list-tube-used\r\n"); drain(1)
    rtt.append((time.perf_counter() - t0) * 1e3)
half = rss_mb()

delete(j for j in range(2, N + 1, 2) if j % 5000 and j != N)
time.sleep(3.5)
empty = rss_mb()
p.terminate(); p.wait(10)

rtt.sort()
out = {"jobs": N, "body": SIZE, "pings": len(rtt),
       "p50_us": round(rtt[len(rtt) // 2] * 1e3, 1),
       "p999_us": round(rtt[int(len(rtt) * 0.999)] * 1e3, 1),
       "max_ms": round(rtt[-1], 2),
       "stalls": sum(1 for r in rtt if r > STALL_MS),
       "rss_full_mb": round(full, 1), "rss_half_mb": round(half, 1),
       "rss_pinned_mb": round(empty, 1)}
print(json.dumps(out))
