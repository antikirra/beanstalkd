import socket, subprocess, time, signal, os, glob
D = "/tmp/sd"
os.makedirs(D, exist_ok=True)
for f in glob.glob(D + "/*"): os.remove(f)
p = subprocess.Popen(["./beanstalkd","-l","127.0.0.1","-p","11205","-b",D,"-s","65536"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.6)
s = socket.create_connection(("127.0.0.1", 11205), timeout=5)
f = s.makefile("rwb")
for i in range(5):
    f.write(b"put 0 0 3600 4\r\nabcd\r\n"); f.flush(); f.readline()
size_running = os.path.getsize(D + "/binlog.1")

# Let the server go fully idle first, then time the shutdown. An idle
# server parks in epoll for as long as its next timer allows (the
# malloc_trim cadence, 60s by default), so "did SIGTERM get acted on
# promptly" is a real end-to-end question and this is the only place
# that asks it.
#
# What it does NOT prove, checked by mutating srv_wake to a no-op and
# watching this still pass in 4ms: the eventfd wake-up. A signal
# delivered to the thread sitting in epoll_pwait interrupts it with
# EINTR all by itself, and the fsync thread starts with every signal
# blocked, so SIGTERM always lands on that thread. srv_wake is for the
# window this cannot reach — a signal arriving after srvserve tests the
# flag and before it enters the syscall, where there is no syscall to
# interrupt yet — and for qemu/Rosetta, where the delivery never
# happens at all. That window is covered by a unit test that injects
# the signal into it directly; this bench only guards the ordinary
# path.
time.sleep(1.0)
t0 = time.time()
p.send_signal(signal.SIGTERM); p.wait(timeout=10)
shutdown_s = time.time() - t0
size_after = os.path.getsize(D + "/binlog.1")
print(f"binlog while running: {size_running} bytes; after clean exit: {size_after}")
print(f"SIGTERM honoured while parked in epoll: {shutdown_s:.3f}s")
assert shutdown_s < 2.0, (
    f"an idle server took {shutdown_s:.1f}s to act on SIGTERM; the wake "
    "eventfd is not doing its job and the flag waited out the epoll park")

# and the jobs must still replay
p = subprocess.Popen(["./beanstalkd","-l","127.0.0.1","-p","11206","-b",D,"-s","65536"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(0.6)
s = socket.create_connection(("127.0.0.1", 11206), timeout=5)
f = s.makefile("rwb")
f.write(b"stats\r\n"); f.flush()
hdr = f.readline(); body = f.read(int(hdr.split()[1]) + 2).decode()
ready = [l for l in body.splitlines() if "current-jobs-ready" in l]
print("after restart:", ready[0].strip())
p.send_signal(signal.SIGTERM); p.wait(timeout=10)
