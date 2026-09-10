# Corrupt-binlog replay fuzzer: build a real WAL, mutate it, restart.
# The server must either start or refuse cleanly — never crash, and
# never trip a sanitizer.
import os, random, socket, subprocess, sys, time, signal, glob

SEED = int(sys.argv[1]); ROUNDS = int(sys.argv[2])
BIN = sys.argv[3] if len(sys.argv) > 3 else "./beanstalkd"
random.seed(SEED)
DIR = os.environ.get("FUZZ_WAL_DIR", "/tmp/wf")
PORT = 11250
env = dict(os.environ)
env["ASAN_OPTIONS"] = "allocator_may_return_null=1:detect_leaks=0"

def start(logpath):
    return subprocess.Popen([BIN, "-l", "127.0.0.1", "-p", str(PORT),
                             "-b", DIR],
                            stdout=open(logpath, "wb"), stderr=subprocess.STDOUT, env=env)

def seed_jobs():
    os.makedirs(DIR, exist_ok=True)
    for f in glob.glob(DIR + "/*"): os.remove(f)
    p = start("/tmp/wf_seed.log"); time.sleep(0.7)
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    f = s.makefile("rwb")
    for i in range(60):
        f.write(b"put 0 0 60 5\r\nabcde\r\n"); f.flush(); f.readline()
    for i in range(15):
        f.write(b"reserve\r\n"); f.flush()
        r = f.readline(); f.readline()
        f.write(b"delete " + r.split()[1] + b"\r\n"); f.flush(); f.readline()
    for i in range(8):
        f.write(b"use t%d\r\n" % i); f.flush(); f.readline()
        f.write(b"put 0 5 60 3\r\nxyz\r\n"); f.flush(); f.readline()
    s.close()
    p.send_signal(signal.SIGTERM); p.wait(timeout=10)

def mutate(path):
    data = bytearray(open(path, "rb").read())
    if not data: return
    # Sometimes relabel the file as the legacy v7 format: the v8 records
    # behind the header then drive readrec7 with bytes it never wrote,
    # which is exactly the migration path a stale binlog exercises.
    if len(data) >= 4 and random.random() < 0.3:
        data[0:4] = random.choice([7, 5, 0, 9, 255]).to_bytes(4, "little")
    for _ in range(random.randint(1, 24)):
        k = random.random()
        i = random.randrange(len(data))
        if k < 0.5:
            data[i] = random.getrandbits(8)
        elif k < 0.7:
            n = min(len(data) - i, random.randint(1, 64))
            for j in range(n): data[i + j] = 0
        elif k < 0.85:
            n = min(len(data) - i, random.randint(1, 64))
            for j in range(n): data[i + j] = random.getrandbits(8)
        else:
            del data[i:]
            break
    open(path, "wb").write(bytes(data))

bad = 0
for r in range(ROUNDS):
    seed_jobs()
    for f in glob.glob(DIR + "/binlog.*"):
        if random.random() < 0.8: mutate(f)
    log = "/tmp/wf_run.log"
    p = start(log)
    time.sleep(0.9)
    alive = p.poll() is None
    ok = False
    if alive:
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=2)
            s.sendall(b"stats\r\n"); ok = s.recv(32).startswith(b"OK ")
            s.close()
        except Exception:
            ok = False
        p.send_signal(signal.SIGTERM)
        try: p.wait(timeout=10)
        except Exception: p.kill()
    else:
        rc = p.returncode
        # a clean refusal is allowed; a crash is not
        ok = rc in (0, 1, 111)
        if not ok: print(f"round {r}: exited rc={rc}")
    out = open(log, "rb").read().decode("utf8", "replace")
    if "AddressSanitizer" in out or "runtime error" in out or "SUMMARY:" in out:
        print(f"round {r}: SANITIZER HIT")
        print("\n".join(out.splitlines()[:25]))
        bad += 1
        break
    if not ok:
        print(f"round {r}: server unusable")
        print("\n".join(out.splitlines()[-12:]))
        bad += 1
        break
print(f"rounds={ROUNDS} bad={bad}")
sys.exit(1 if bad else 0)
