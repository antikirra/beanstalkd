# Drive every command the protocol has at a server built with
# AddressSanitizer, then let it exit cleanly so LeakSanitizer can speak.
import socket, subprocess, sys, time, os, glob, tempfile, shutil, signal

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 11810
D = tempfile.mkdtemp()
env = dict(os.environ, ASAN_OPTIONS="detect_leaks=1:allocator_may_return_null=1")
p = subprocess.Popen(["./beanstalkd", "-l", "127.0.0.1", "-p", str(PORT),
                      "-b", D, "-s", "65536", "-z", "70000"],
                     env=env, stderr=subprocess.PIPE)
time.sleep(0.9)

def conn():
    s = socket.create_connection(("127.0.0.1", PORT), timeout=10)
    return s, s.makefile("rwb")

def line(f, c):
    f.write(c); f.flush(); return f.readline()

s, f = conn()
# tubes: use / watch / ignore, including names that get created and dropped
for i in range(40):
    line(f, b"use tube-%d\r\n" % i)
    line(f, b"watch tube-%d\r\n" % i)
for i in range(39):
    line(f, b"ignore tube-%d\r\n" % i)

# the job life cycle, every branch of it
ids = []
line(f, b"use default\r\n")
for i in range(300):
    r = line(f, b"put 0 0 60 8\r\nabcdefgh\r\n")
    ids.append(int(r.split()[1]))
line(f, b"watch default\r\n")
for i in range(60):
    r = line(f, b"reserve\r\n"); f.readline()
    jid = int(r.split()[1])
    if i % 4 == 0:   line(f, b"release %d 5 0\r\n" % jid)
    elif i % 4 == 1: line(f, b"bury %d 0\r\n" % jid)
    elif i % 4 == 2: line(f, b"touch %d\r\n" % jid); line(f, b"delete %d\r\n" % jid)
    else:            line(f, b"delete %d\r\n" % jid)
line(f, b"kick 20\r\n")
line(f, b"peek-ready\r\n"); f.readline()
line(f, b"peek-buried\r\n")
line(f, b"peek-delayed\r\n")
line(f, b"pause-tube default 0\r\n")

# stats bodies (these allocate and format)
for cmd in (b"stats\r\n", b"list-tubes\r\n", b"list-tubes-watched\r\n",
            b"list-tube-used\r\n", b"stats-tube default\r\n"):
    hdr = line(f, cmd)
    if hdr.startswith(b"OK"):
        f.read(int(hdr.split()[1]) + 2)
hdr = line(f, b"stats-job %d\r\n" % ids[-1])
if hdr.startswith(b"OK"):
    f.read(int(hdr.split()[1]) + 2)

# refusal paths: too big, malformed, over-long line, unknown verb.
# The oversized put has to send its body: the protocol answers
# JOB_TOO_BIG only after reading and discarding it, which is the
# bit-bucket path and worth walking.
f.write(b"put 0 0 60 70001\r\n" + b"x" * 70001 + b"\r\n"); f.flush()
f.readline()
line(f, b"put x y z\r\n")
line(f, b"a" * 400 + b"\r\n")
line(f, b"nosuchcommand\r\n")

# a pipelined burst, the coalescing path
f.write(b"".join(b"put 0 0 60 3\r\nabc\r\n" for _ in range(64))); f.flush()
for _ in range(64): f.readline()

# many short-lived connections, including ones that say nothing
for i in range(150):
    s2, f2 = conn()
    if i % 3:
        line(f2, b"stats-tube default\r\n")
    s2.close()

# a worker parked on reserve when the server goes down
s3, f3 = conn()
f3.write(b"reserve-with-timeout 30\r\n"); f3.flush()
time.sleep(0.3)

p.send_signal(signal.SIGTERM)
try:
    p.wait(timeout=60)
except subprocess.TimeoutExpired:
    p.kill(); print("server did not exit"); sys.exit(1)
err = p.stderr.read().decode(errors="replace")
shutil.rmtree(D, ignore_errors=True)

if "LeakSanitizer" in err or "detected memory leaks" in err:
    print(err[-4000:])
    print("RESULT: FAIL - leaks reported")
    sys.exit(1)
print("RESULT: PASS - no leaks after a clean exit")
sys.exit(0)
