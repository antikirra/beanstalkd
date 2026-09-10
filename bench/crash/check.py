import socket, subprocess, time, signal, os, glob, sys
def run(durable, depth=1):
    D = "/tmp/hk"
    os.makedirs(D, exist_ok=True)
    for f in glob.glob(D+"/*"): os.remove(f)
    args = ["./beanstalkd","-l","127.0.0.1","-p","11155","-b",D]
    if durable: args.append("-D")
    p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.6)
    s = socket.create_connection(("127.0.0.1", 11155), timeout=30)
    f = s.makefile("rwb")
    acked = 0
    # depth > 1 sends a whole burst in one segment. That is the shape
    # where the server coalesces its replies, and where the durability
    # hold on the reply buffer has to outrank the pipelining one: an ack
    # that leaves before the tick's fdatasync is a job this count will
    # not find after the SIGKILL.
    total, i = 300, 0
    while i < total:
        n = min(depth, total - i)
        f.write(b"put 0 0 3600 8\r\nabcdefgh\r\n" * n); f.flush()
        for _ in range(n):
            if f.readline().startswith(b"INSERTED"): acked += 1
        i += n
    p.kill()                       # SIGKILL: no chance to flush anything
    p.wait()
    # restart and count what came back
    p = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    while True:
        try:
            s = socket.create_connection(("127.0.0.1", 11155), timeout=1)
            s.sendall(b"stats\r\n"); h = s.recv(16)
            if h.startswith(b"OK"): break
        except Exception: time.sleep(0.03)
    f = s.makefile("rwb")
    rest = s.recv(65536)
    f.write(b"stats\r\n"); f.flush()
    hdr = f.readline(); body = f.read(int(hdr.split()[1])+2).decode()
    ready = int([l.split(":")[1] for l in body.splitlines() if l.startswith("current-jobs-ready")][0])
    label = "durable (-D)" if durable else "default    "
    print(f"{label} depth {depth:2d}: acked {acked}, recovered {ready}")
    p.send_signal(signal.SIGTERM); p.wait(timeout=10)
    # Under -D an ack means the record reached the disk (invariant #14),
    # so losing one here is a real failure. Without -D the guarantee is
    # weaker by design: the page cache survives SIGKILL but not a power
    # cut, so this run is a smoke test rather than a proof.
    if durable and ready < acked:
        print(f"FAIL: -D lost {acked - ready} acked jobs")
        return 1
    return 0

rc = 0
for depth in (1, 32):
    rc |= run(False, depth) | run(True, depth)
sys.exit(rc)
