import socket, random, sys, time, os, signal, subprocess

PORT = int(sys.argv[1]); SECONDS = float(sys.argv[2]); SEED = int(sys.argv[3])
random.seed(SEED)

VERBS = [b"put", b"reserve", b"reserve-with-timeout", b"reserve-job", b"delete",
         b"release", b"bury", b"kick", b"kick-job", b"touch", b"watch", b"ignore",
         b"use", b"peek", b"peek-ready", b"peek-delayed", b"peek-buried",
         b"list-tubes", b"list-tube-used", b"list-tubes-watched", b"stats",
         b"stats-job", b"stats-tube", b"pause-tube", b"quit"]
SEPS  = [b" ", b"", b"\t", b"  ", b"\x00", b"-", b"Z", b"\r", b"\n", b"\xff"]
NUMS  = [b"0", b"1", b"-1", b"4294967295", b"4294967296", b"18446744073709551615",
         b"99999999999999999999", b"0x10", b"+7", b" 7", b"1e3", b"", b"007"]
NAMES = [b"default", b"a"*200, b"a"*201, b"a"*300, b"", b"a b", b"a\x00b",
         b"\xc3\xa9", b"-lead", b"ok-name", b"+", b"a"*64]

def frag():
    r = random.random()
    if r < 0.45:
        v = random.choice(VERBS)
        parts = [v]
        for _ in range(random.randint(0, 4)):
            parts.append(random.choice(SEPS))
            parts.append(random.choice(NUMS + NAMES))
        return b"".join(parts) + random.choice([b"\r\n", b"\n", b"\r", b""])
    if r < 0.6:
        n = random.randint(0, 500)
        return bytes(random.getrandbits(8) for _ in range(n))
    if r < 0.8:
        body = bytes(random.getrandbits(8) for _ in range(random.randint(0, 40)))
        return b"put 0 0 60 " + str(random.choice([0, 1, len(body), 999999, -1])).encode() \
               + b"\r\n" + body + b"\r\n"
    if r < 0.9:
        # Command lines around the two length boundaries that matter.
        # LINE_BUF_SIZE (224) is where the protocol stops accepting a
        # line; CMD_BUF_SIZE (1024) is where one stops ARRIVING whole,
        # so the answer has to come from the discard path instead. The
        # server must say BAD_FORMAT on both sides of both edges and
        # never confuse a long line with the commands behind it.
        n = random.choice([1, 222, 223, 224, 225, 226, 400,
                           1021, 1022, 1023, 1024, 1025, 2000])
        return b"a"*n + random.choice([b"\r\n", b"\n", b""])
    if r < 0.97:
        # One pipelined burst in ONE send: the shape the server
        # coalesces replies for. A reserve in the middle blocks the rest
        # of the burst until something unblocks it, which is the run
        # queue's whole reason to exist.
        out = []
        for _ in range(random.randint(2, 40)):
            out.append(random.choice([
                b"put 0 0 60 3\r\nabc\r\n",
                b"delete " + random.choice(NUMS) + b"\r\n",
                b"reserve\r\n",
                b"reserve-with-timeout 0\r\n",
                b"list-tubes\r\n",
                b"peek-ready\r\n",
                b"stats\r\n",
                b"use " + random.choice(NAMES) + b"\r\n",
                b"a"*random.randint(200, 1100) + b"\r\n",
            ]))
        return b"".join(out)
    return random.choice([b"\r\n", b"\n"*random.randint(1, 5),
                          b"a"*random.randint(1, 400) + b"\r\n"])

sent = 0
deadline = time.time() + SECONDS
while time.time() < deadline:
    try:
        s = socket.create_connection(("127.0.0.1", PORT), timeout=2)
        s.setblocking(False)
        for _ in range(random.randint(1, 40)):
            try:
                s.sendall(frag()); sent += 1
            except (BlockingIOError, OSError):
                break
            try: s.recv(65536)
            except (BlockingIOError, socket.timeout): pass
            except OSError: break
        s.close()
    except (ConnectionResetError, BrokenPipeError, socket.timeout, OSError):
        continue
print(f"sent {sent} fragments")
# the server must still answer
try:
    s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
    s.sendall(b"stats\r\n")
    r = s.recv(64)
    print("server alive:", r.startswith(b"OK "))
except Exception as e:
    print("server DEAD:", e)
    sys.exit(1)
