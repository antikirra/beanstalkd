# Do binlogs stay bounded when a few long-lived jobs sit buried/delayed
# while thousands of short-lived ones churn past them?  (upstream #599)
import socket, subprocess, time, os, glob, sys, tempfile, shutil

port = 11801
D = tempfile.mkdtemp()
p = subprocess.Popen(["./beanstalkd","-l","127.0.0.1","-p",str(port),
                      "-b",D,"-s","65536"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.8)
s = socket.create_connection(("127.0.0.1", port), timeout=10)
f = s.makefile("rwb")

def cmd(c):
    f.write(c); f.flush(); return f.readline()

# long-lived anchors: two buried, two far-future delayed
for i in range(2):
    cmd(b"put 0 0 3600 4\r\nanch\r\n")
    line = cmd(b"reserve\r\n"); f.readline()
    jid = int(line.split()[1])
    cmd(b"bury %d 0\r\n" % jid)
for i in range(2):
    cmd(b"put 0 86400 3600 4\r\ndlay\r\n")

def sizes():
    fs = sorted(glob.glob(D + "/binlog.*"))
    return len(fs), sum(os.path.getsize(x) for x in fs)

n0, b0 = sizes()
print(f"after anchors: {n0} binlogs, {b0} bytes")

for rnd in range(6):
    for i in range(500):
        cmd(b"put 0 0 3600 8\r\nabcdefgh\r\n")
        line = cmd(b"reserve\r\n"); f.readline()
        jid = int(line.split()[1])
        cmd(b"delete %d\r\n" % jid)
    n, b = sizes()
    print(f"round {rnd+1}: {n} binlogs, {b} bytes")

n1, b1 = sizes()
p.terminate(); p.wait(timeout=10); shutil.rmtree(D, ignore_errors=True)
# 3000 put/delete cycles past four pinned jobs must not leave the binlog
# growing without bound.
ok = n1 <= max(4, n0 + 2)
print("RESULT:", "PASS - bounded" if ok else f"FAIL - grew {n0} -> {n1} files")
sys.exit(0 if ok else 1)
