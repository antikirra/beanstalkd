// Hostile tests for v8 WAL CRC32C.
//
// Goal: prove that silent data corruption in binlog files is now
// detected on recovery and the corrupted record is rejected. Each
// integration test below:
//   1. forks a server with a persistent binlog
//   2. inserts jobs so at least one full record lives on disk
//   3. SIGTERMs the server (graceful shutdown writes records)
//   4. flips bytes on disk at a specific offset
//   5. restarts the server, verifies recovery outcome
//
// Unit tests cover wal_crc32c directly with RFC 3720 / iSCSI standard
// test vectors and stress the SSE4.2 8-byte loop vs tail boundary.

#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <dirent.h>
#include <errno.h>


//
// --- wal_crc32c unit tests: validate against RFC 3720 / iSCSI spec ---
//
// These are the canonical CRC32C test vectors. If the SSE4.2 hardware
// path produces different values, this file will never compile-link
// cleanly against file.c/walg.c and silent corruption would pass through.
//

static uint32
crc(const char *s, size_t n)
{
    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, s, n);
    return c ^ WAL_CRC32C_XOR;
}

void
cttest_crc32c_known_vectors()
{
    // Empty string — the identity case. Any implementation bug in
    // init/final XOR tends to trip here.
    assertf(crc("", 0) == 0x00000000u,
            "empty CRC32C: expected 0x00000000, got 0x%08x", crc("", 0));

    // Single byte — proves the tail loop works.
    assertf(crc("a", 1) == 0xC1D04330u,
            "CRC32C(\"a\"): expected 0xC1D04330, got 0x%08x", crc("a", 1));

    // "123456789" is the de-facto standard test vector (RFC 3720,
    // Appendix B.4). 9 bytes = 1 full u64 load + 1 tail byte — proves
    // the 8-byte loop boundary.
    assertf(crc("123456789", 9) == 0xE3069283u,
            "CRC32C(\"123456789\"): expected 0xE3069283, got 0x%08x",
            crc("123456789", 9));

    // 43-byte pangram — 5 full u64 loads + 3 tail bytes.
    assertf(crc("The quick brown fox jumps over the lazy dog", 43)
            == 0x22620404u,
            "CRC32C(pangram): expected 0x22620404, got 0x%08x",
            crc("The quick brown fox jumps over the lazy dog", 43));

    // RFC 3720 B.4 vectors. Every length above leaves a tail (0, 1, 1, 3
    // bytes), so nothing yet exercises the u64 main loop alone: these are
    // exactly 4 iterations with no tail at all. The all-zero and all-ones
    // patterns are also the two that a wrong init or final XOR cannot
    // hide behind.
    char zeros[32];
    memset(zeros, 0x00, sizeof zeros);
    assertf(crc(zeros, sizeof zeros) == 0x8A9136AAu,
            "CRC32C(32 zero bytes): expected 0x8A9136AA, got 0x%08x",
            crc(zeros, sizeof zeros));

    char ones[32];
    memset(ones, 0xFF, sizeof ones);
    assertf(crc(ones, sizeof ones) == 0x62A8AB43u,
            "CRC32C(32 0xFF bytes): expected 0x62A8AB43, got 0x%08x",
            crc(ones, sizeof ones));

    // 0x00..0x1F and its reverse: same multiset of bytes, different
    // order. A checksum that accumulates without regard to position
    // (a sum, an xor) gives one answer for both.
    char up[32], down[32];
    for (int i = 0; i < 32; i++) {
        up[i] = (char)i;
        down[i] = (char)(31 - i);
    }
    assertf(crc(up, sizeof up) == 0x46DD794Eu,
            "CRC32C(0x00..0x1F): expected 0x46DD794E, got 0x%08x",
            crc(up, sizeof up));
    assertf(crc(down, sizeof down) == 0x113FDB5Cu,
            "CRC32C(0x1F..0x00): expected 0x113FDB5C, got 0x%08x",
            crc(down, sizeof down));
}


void
cttest_crc32c_chunked_equals_monolithic()
{
    // Incremental CRC (used by filewrjobshort/filewrjobfull — multiple
    // wal_crc32c calls for namelen, name, jr, body separately) must be
    // bit-identical to a single call over the concatenated buffer.
    // This proves the intermediate state handoff is correct.
    const char *msg = "The quick brown fox jumps over the lazy dog";
    size_t n = strlen(msg);

    uint32 mono = WAL_CRC32C_INIT;
    mono = wal_crc32c(mono, msg, n);
    mono ^= WAL_CRC32C_XOR;

    // Anchor the reference. Comparing the implementation against itself
    // is satisfied by any accumulator: swap the init/final constants or
    // the polynomial and every chunking below moves with the monolith,
    // still equal, still green.
    assertf(mono == 0x22620404u,
            "the reference CRC32C must be the standard value: expected "
            "0x22620404, got 0x%08x", mono);

    // Three calls, two state handoffs — the writer folds namelen, name,
    // Jobrec and body in four. A handoff that only survives one boundary
    // passes the two-call form below.
    uint32 three = WAL_CRC32C_INIT;
    three = wal_crc32c(three, msg, 7);
    three = wal_crc32c(three, msg + 7, 13);
    three = wal_crc32c(three, msg + 20, n - 20);
    three ^= WAL_CRC32C_XOR;
    assertf(three == mono,
            "three-chunk CRC32C must equal the monolith: got 0x%08x, "
            "want 0x%08x", three, mono);

    // Split at every boundary 0..n and verify all match.
    for (size_t split = 0; split <= n; split++) {
        uint32 c = WAL_CRC32C_INIT;
        c = wal_crc32c(c, msg,         split);
        c = wal_crc32c(c, msg + split, n - split);
        c ^= WAL_CRC32C_XOR;
        assertf(c == mono,
                "chunked CRC32C mismatch at split=%zu: got 0x%08x, want 0x%08x",
                split, c, mono);
    }
}


void
cttest_crc32c_64kb_body()
{
    // Maximum legal job body = 64 KiB. Verify the SSE4.2 loop computes
    // the whole buffer without overflow or truncation. We also compare
    // a deterministic seed'd fill vs. a single-bit flip — the two CRCs
    // MUST differ for every possible flipped position, otherwise we
    // have a collision where corruption would go undetected.
    size_t n = 65536;
    char *buf = calloc(n, 1);
    assertf(buf != NULL, "calloc 64KiB");

    // Deterministic fill. No srand — use a simple LCG for reproducibility.
    uint32 seed = 0xDEADBEEF;
    for (size_t i = 0; i < n; i++) {
        seed = seed * 1664525u + 1013904223u;
        buf[i] = (char)(seed >> 24);
    }

    uint32 baseline = WAL_CRC32C_INIT;
    baseline = wal_crc32c(baseline, buf, n);
    baseline ^= WAL_CRC32C_XOR;

    // Known answer for this exact buffer, computed independently from
    // the Castagnoli definition (reflected poly 0x82F63B78, init and
    // final XOR 0xFFFFFFFF) rather than from this implementation.
    // Without it, "the CRC changed when I flipped a bit" is satisfied by
    // a byte sum or an Adler accumulator — every guarantee CRC32C was
    // chosen for (burst-error detection, Hamming distance on 64 KiB)
    // would be gone with the test still green.
    assertf(baseline == 0x9864C70Du,
            "CRC32C of the 64KiB LCG buffer: expected 0x9864C70D, got 0x%08x",
            baseline);

    // Flip a single bit in three representative positions (head, body,
    // tail). CRC32C mathematically guarantees single-bit flip detection
    // for messages up to (polynomial_length * 2^31) bits — far beyond 64 KiB.
    size_t positions[] = { 0, n/2, n-1 };
    for (size_t i = 0; i < 3; i++) {
        buf[positions[i]] ^= 0x01;
        uint32 flipped = WAL_CRC32C_INIT;
        flipped = wal_crc32c(flipped, buf, n);
        flipped ^= WAL_CRC32C_XOR;
        assertf(flipped != baseline,
                "single-bit flip at pos %zu failed to change CRC32C", positions[i]);
        buf[positions[i]] ^= 0x01;
    }

    free(buf);
}


//
// --- integration test infrastructure: fork server + corrupt binlog ---
//
// This mirrors testserv.c / testserv2.c's server-fork pattern. We
// duplicate the helpers because ct links all test objects together
// but each source file has its own static helpers.
//

static int walsrvpid;
static int64 waltimeout = 5000000000LL;

static void
wal_killsrv(void)
{
    if (!walsrvpid) return;
    kill(walsrvpid, SIGTERM);
    waitpid(walsrvpid, 0, 0);
    walsrvpid = 0;
}

static void
wal_sigexit(int sig)
{
    UNUSED_PARAMETER(sig);
    exit(0);
}

static int
wal_startsrv(void)
{
    struct sockaddr_in addr;

    srv.sock.fd = make_server_socket("127.0.0.1", "0");
    if (srv.sock.fd == -1) { twarnx("make_server_socket"); exit(1); }

    socklen_t len = sizeof(addr);
    if (getsockname(srv.sock.fd, (struct sockaddr *)&addr, &len) == -1) {
        twarnx("getsockname"); exit(1);
    }
    int port = ntohs(addr.sin_port);

    walsrvpid = fork();
    if (walsrvpid < 0) { twarn("fork"); exit(1); }
    if (walsrvpid > 0) {
        atexit(wal_killsrv);
        usleep(100000);
        // The listening socket lives in the PARENT, so a child that died
        // during replay still completes connect() and every later
        // failure surfaces as an unexplained read timeout. Reap it here
        // instead: a replay that abort()s or exit()s is a defect in its
        // own right, and must never be mistaken for a record that was
        // legitimately rejected.
        int st = 0;
        if (waitpid(walsrvpid, &st, WNOHANG) == walsrvpid) {
            int code = WIFEXITED(st) ? WEXITSTATUS(st) : -1;
            int sig = WIFSIGNALED(st) ? WTERMSIG(st) : 0;
            walsrvpid = 0;
            assertf(0, "server died during startup/replay: exit=%d signal=%d",
                    code, sig);
        }
        return port;
    }

    // child
    struct sigaction sa = { .sa_handler = wal_sigexit, .sa_flags = 0 };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, 0);

    prot_init();
    srv_acquire_wal(&srv);
    srvserve(&srv);
    exit(1); // unreachable
}

static int
wal_dial(int port)
{
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    inet_aton("127.0.0.1", &addr.sin_addr);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) { twarn("socket"); exit(1); }

    int flags = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(int));

    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) == -1) {
        twarn("connect"); exit(1);
    }
    return fd;
}

static void
wal_send(int fd, const char *s)
{
    size_t n = strlen(s);
    while (n > 0) {
        ssize_t w = write(fd, s, n);
        if (w <= 0) { twarn("write"); exit(1); }
        s += w;
        n -= (size_t)w;
    }
}

static char *
wal_readline(int fd)
{
    static char buf[4096];
    size_t i = 0;
    char c = 0, p = 0;
    fd_set rfd;
    struct timeval tv;

    for (;;) {
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        tv.tv_sec = waltimeout / 1000000000;
        tv.tv_usec = (waltimeout / 1000) % 1000000;
        int r = select(fd+1, &rfd, NULL, NULL, &tv);
        if (r <= 0) { fputs("wal_readline timeout\n", stderr); exit(8); }
        r = read(fd, &c, 1);
        if (r <= 0) break;
        if (i < sizeof(buf)-1) buf[i++] = c;
        if (p == '\r' && c == '\n') break;
        p = c;
    }
    buf[i] = '\0';
    return buf;
}

static void
wal_ckline(int fd, const char *want)
{
    char *got = wal_readline(fd);
    assertf(strcmp(want, got) == 0, "expected %s, got %s", want, got);
}

// Return path to binlog.N inside ctdir(). Caller must free.
static char *
wal_binlog_path(int n)
{
    char *path = malloc(512);
    assertf(path != NULL, "malloc");
    snprintf(path, 512, "%s/binlog.%d", ctdir(), n);
    return path;
}

// Locate binlog.N files, find the lowest-numbered one that exists.
// Returns N or -1 if none. Needed because after graceful shutdown the
// "current" binlog may be binlog.1 or binlog.2 depending on activity.
static int
wal_first_binlog(void)
{
    DIR *d = opendir(ctdir());
    if (!d) return -1;
    struct dirent *e;
    int best = -1;
    while ((e = readdir(d)) != NULL) {
        int n;
        if (sscanf(e->d_name, "binlog.%d", &n) == 1) {
            if (best == -1 || n < best) best = n;
        }
    }
    closedir(d);
    return best;
}

// Flip the lowest bit of one byte at offset `off` in binlog.N.
static void
wal_flip_byte(int binlog_n, off_t off)
{
    char *path = wal_binlog_path(binlog_n);
    int fd = open(path, O_RDWR);
    assertf(fd >= 0, "open %s: %s", path, strerror(errno));

    unsigned char b;
    ssize_t r = pread(fd, &b, 1, off);
    assertf(r == 1, "pread %s @%lld", path, (long long)off);

    b ^= 0x01;
    r = pwrite(fd, &b, 1, off);
    assertf(r == 1, "pwrite %s @%lld", path, (long long)off);

    close(fd);
    free(path);
}

// Common setup — use a ctdir binlog with reasonable filesize.
static void
wal_setup(void)
{
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.filesize = 4096;
    srv.wal.wantsync = 1;
    srv.wal.syncrate = 0;
    job_data_size_limit = 1024;
}


//
// --- Integration tests: silent corruption detection ---
//

// Flip a byte in the body portion of a full record. CRC32C must
// reject the record; the job must NOT reappear after restart.
void
cttest_wal_v8_body_flip()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 5\r\n");
    wal_send(fd, "hello\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    // Body begins after: version(4) + namelen(4) + "default"(7) + Jobrec(80)
    // = 95 bytes. Flip the first body byte.
    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");
    wal_flip_byte(n, 4 + 4 + 7 + 80);

    // Restart — the corrupt record must be rejected.
    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Flip a byte inside the Jobrec (high-impact corruption of structural
// metadata like pri, delay, ttr, state). Must be rejected.
void
cttest_wal_v8_jobrec_flip()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 3\r\n");
    wal_send(fd, "abc\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    // Jobrec begins at offset 4 + 4 + 7 = 15 (after version + namelen + "default").
    // Flip a byte 10 bytes into Jobrec — somewhere in jr.pri or jr.delay.
    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");
    wal_flip_byte(n, 4 + 4 + 7 + 10);

    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Flip a byte in the CRC trailer itself. Symmetric: a bad CRC over
// otherwise-valid data must still be rejected.
void
cttest_wal_v8_crc_footer_flip()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 2\r\n");
    wal_send(fd, "qz\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    // CRC trailer is the last 4 bytes of the full record. Body size = 2+2 = 4.
    // Record = namelen(4) + "default"(7) + Jobrec(80) + body(4) + crc(4) = 99 bytes.
    // Preceded by version(4). So CRC starts at 4+95 = 99, last byte at 102.
    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");
    wal_flip_byte(n, 4 + 4 + 7 + 80 + 4); // first byte of CRC

    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Delete every binlog.N in ctdir so a hand-built fixture is the only
// file walscandir will find.
static void
wal_unlink_binlogs(void)
{
    DIR *d = opendir(ctdir());
    assertf(d != NULL, "opendir %s: %s", ctdir(), strerror(errno));
    struct dirent *e;
    char path[512];
    while ((e = readdir(d)) != NULL) {
        int n;
        if (sscanf(e->d_name, "binlog.%d", &n) == 1) {
            snprintf(path, sizeof path, "%s/%s", ctdir(), e->d_name);
            unlink(path);
        }
    }
    closedir(d);
}


// Hand-craft binlog.1 holding ONE v8 full record whose bytes are
// entirely self-consistent — a legal 6-byte tube name, a well-formed
// Jobrec, a body of the size the Jobrec declares — but whose CRC trailer
// was taken over a DIFFERENT namelen. Nothing about the record is
// mis-shaped, so the parser sails through it; only a checksum that
// actually covers namelen can tell it apart from a record a writer
// produced.
static void
wal_write_v8_namelen_crc_fixture(void)
{
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assertf(bfd >= 0, "create v8 namelen fixture");

    int ver8 = Walver;
    assertf(write(bfd, &ver8, sizeof ver8) == (ssize_t)sizeof ver8,
            "write v8 header");

    int disk_namelen = 6;      // what the record on disk says: "defaul"
    int crc_namelen = 7;       // what the checksum was taken over
    Jobrec jr = {0};
    jr.id = 1;
    jr.ttr = 120000000000LL;
    jr.body_size = 5;          // "abc" + "\r\n"
    jr.created_at = 1;
    jr.state = Ready;

    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &crc_namelen, sizeof crc_namelen);
    c = wal_crc32c(c, "defaul", 6);
    c = wal_crc32c(c, &jr, sizeof jr);
    c = wal_crc32c(c, "abc\r\n", 5);
    c ^= WAL_CRC32C_XOR;
    unsigned char trailer[4] = {
        (unsigned char)(c      ),
        (unsigned char)(c >>  8),
        (unsigned char)(c >> 16),
        (unsigned char)(c >> 24),
    };

    assertf(write(bfd, &disk_namelen, sizeof disk_namelen)
            == (ssize_t)sizeof disk_namelen, "namelen");
    assertf(write(bfd, "defaul", 6) == 6, "tube name");
    assertf(write(bfd, &jr, sizeof jr) == (ssize_t)sizeof jr, "jobrec");
    assertf(write(bfd, "abc\r\n", 5) == 5, "body");
    assertf(write(bfd, trailer, 4) == 4, "crc trailer");

    char zeros[512] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);
}


// Flip a byte in namelen. Without CRC covering namelen, this would
// mis-parse the record and likely corrupt recovery silently. With v8
// CRC covering namelen, the record must be rejected.
void
cttest_wal_v8_namelen_flip()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 2\r\n");
    wal_send(fd, "xy\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    // namelen is the 4 bytes right after the version header.
    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");
    wal_flip_byte(n, 4); // first byte of namelen

    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();

    // The flip above desynchronises the parse by one byte, so job 1 is
    // unreachable whether or not any checksum was consulted — the
    // assertion is true by construction and cannot see the CRC at all.
    // Ask the real question instead: a record that parses PERFECTLY,
    // byte for byte what a writer would emit, except that its trailer
    // was taken over a different namelen. It is accepted the moment
    // namelen leaves the checksummed range, or the moment the
    // comparison stops rejecting mismatches.
    wal_unlink_binlogs();
    wal_write_v8_namelen_crc_fixture();

    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");

    // ...and the reader must not have taken the record's word for its
    // tube either: a rejected record leaves no trace in the namespace.
    wal_send(fd, "stats-tube defaul\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Zero out the CRC trailer — simulates a partial-write crash where
// the footer wasn't flushed before the file's fallocated tail. readrec
// reads four zero bytes as the stored CRC, compares against the computed
// CRC, and must reject on mismatch.
void
cttest_wal_v8_truncated_crc()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 4\r\n");
    wal_send(fd, "trnc\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");

    // Record layout: version(4) + namelen(4) + "default"(7) + Jobrec(80) + body(6) + crc(4)
    // body_size = 4 (trnc) + 2 (\r\n trailer) = 6. CRC starts at offset 4+4+7+80+6 = 101.
    off_t crc_off = 4 + 4 + 7 + 80 + 6;
    char *path = wal_binlog_path(n);
    int bfd = open(path, O_RDWR);
    assertf(bfd >= 0, "open %s", path);
    unsigned char zeros[4] = {0, 0, 0, 0};
    assertf(pwrite(bfd, zeros, 4, crc_off) == 4, "pwrite zero-crc");
    close(bfd);
    free(path);

    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Round-trip sanity: without corruption, the record must be recovered
// after restart. This catches cases where the CRC compute/verify is
// wired wrong and always returns false negative.
void
cttest_wal_v8_roundtrip_clean()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 42 0 120 11\r\n");
    wal_send(fd, "hello world\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    // Restart, no modification — job must survive.
    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "peek 1\r\n");
    wal_ckline(fd, "FOUND 1 11\r\n");
    wal_ckline(fd, "hello world\r\n");

    // The id and the body are two fields out of a Jobrec of fourteen.
    // Everything the put declared has to come back, or "recovered" means
    // "the bytes are there and the job is a stranger": a wrong priority
    // re-orders the ready queue, a wrong ttr changes when the job is
    // released back, a wrong tube hands it to the wrong workers.
    wal_send(fd, "stats-job 1\r\n");
    char *ok = wal_readline(fd);
    assertf(strncmp(ok, "OK ", 3) == 0,
            "stats-job must answer OK after replay, got %s", ok);
    char *yaml = wal_readline(fd);
    assertf(strstr(yaml, "\npri: 42\n"),
            "priority 42 must survive replay, got:\n%s", yaml);
    assertf(strstr(yaml, "\nttr: 120\n"),
            "ttr 120 must survive replay, got:\n%s", yaml);
    assertf(strstr(yaml, "\ndelay: 0\n"),
            "delay 0 must survive replay, got:\n%s", yaml);
    assertf(strstr(yaml, "\nstate: ready\n"),
            "a replayed ready job must come back ready, got:\n%s", yaml);
    assertf(strstr(yaml, "\ntube: \"default\"\n"),
            "the tube must survive replay, got:\n%s", yaml);
    wal_killsrv();
}


// Drop a hand-crafted v7 binlog (Walver7 header + zero padding that
// parses as "no records") into the directory. A new binary must
// dispatch to readrec7 via fileread's switch, accept the file without
// error, and start a new binlog.2 with Walver=8. This locks the
// forward-compat path for legacy files.
void
cttest_wal_v7_header_dispatch()
{
    wal_setup();

    // Write binlog.1 with the v7 header and ONE real v7 full record, then
    // zeros. A header followed by nothing but padding is a file with
    // deliberately zero records: "dispatch reached readrec7" would then
    // be attested only by the server not crashing, and a readrec7 stubbed
    // to `return 0` would pass. The record makes the dispatch prove
    // itself — it has to come back out.
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT, 0600);
    assertf(bfd >= 0, "create v7 fixture");
    int ver7 = 7;
    assertf(write(bfd, &ver7, sizeof ver7) == sizeof ver7, "write v7 header");

    int nl = 7;
    Jobrec jr = {0};
    jr.id = 1;
    jr.ttr = 120000000000LL; // 120s in ns
    jr.body_size = 5;        // "leg" + "\r\n"
    jr.created_at = 1;
    jr.state = Ready;
    assertf(write(bfd, &nl, sizeof nl) == (ssize_t)sizeof nl, "v7 namelen");
    assertf(write(bfd, "default", 7) == 7, "v7 tube name");
    assertf(write(bfd, &jr, sizeof jr) == (ssize_t)sizeof jr, "v7 jobrec");
    assertf(write(bfd, "leg\r\n", 5) == 5, "v7 body");

    // Pad to a realistic filesize with zeros so walscandir sees it.
    char zeros[1024] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);

    // Start a new server — it must accept the v7 file without crash.
    int port = wal_startsrv();
    int fd = wal_dial(port);

    // The legacy record must have been PARSED, not merely skipped: the
    // job, its body and its tube all come back.
    wal_send(fd, "peek 1\r\n");
    wal_ckline(fd, "FOUND 1 3\r\n");
    wal_ckline(fd, "leg\r\n");

    // Server is up: can put and stats normally. Job 1 came from the v7
    // file, so the fresh job is number 2 — the id counter must have been
    // advanced past the replayed record.
    wal_send(fd, "put 0 0 60 3\r\n");
    wal_send(fd, "hey\r\n");
    wal_ckline(fd, "INSERTED 2\r\n");
    wal_killsrv();

    // A new binlog (binlog.2) must exist and carry v8 header.
    char *p2 = wal_binlog_path(2);
    int b2 = open(p2, O_RDONLY);
    assertf(b2 >= 0, "open binlog.2");
    int ver = 0;
    assertf(read(b2, &ver, sizeof ver) == sizeof ver, "read binlog.2 header");
    close(b2);
    free(p2);
    assertf(ver == 8, "new binlog must be v8, got %d", ver);
}


// Verify that the newly-created binlog file carries Walver=8 in its
// header. This locks the on-disk version at the reader/writer boundary.
void
cttest_wal_v8_header_byte()
{
    wal_setup();

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "put 0 0 120 1\r\n");
    wal_send(fd, "x\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
    wal_killsrv();

    int n = wal_first_binlog();
    assertf(n >= 0, "no binlog found");

    char *path = wal_binlog_path(n);
    int bfd = open(path, O_RDONLY);
    assertf(bfd >= 0, "open");
    int ver = 0;
    assertf(read(bfd, &ver, sizeof ver) == sizeof ver, "read header");
    close(bfd);
    free(path);

    assertf(ver == 8, "binlog version header: expected 8, got %d", ver);

    // The version byte only matters because it routes the reader. Pinning
    // the constant while never asking the reader to parse what this
    // writer just emitted leaves the writer/reader boundary — the thing
    // the header exists to keep aligned — untested. Restart and require
    // the record back.
    port = wal_startsrv();
    fd = wal_dial(port);
    wal_send(fd, "peek 1\r\n");
    wal_ckline(fd, "FOUND 1 1\r\n");
    wal_ckline(fd, "x\r\n");
    wal_killsrv();
}


// Hand-craft a v7 binlog.1: [int32 ver=7] [full record for job 1 in
// "default": namelen=7, name, Jobrec{body_size=5}, body "abc\r\n"]
// [short record for job 1: namelen=0, Jobrec{state=Buried,
// body_size=short_body_size}] [zero padding -> clean EOF]. v7 carries
// no CRC trailer, so the short record's body_size field is whatever
// the disk says — the reader must cross-check it itself.
static void
wal_write_v7_short_record_fixture(int32 short_body_size)
{
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assertf(bfd >= 0, "create v7 fixture");

    int ver7 = 7;
    assertf(write(bfd, &ver7, sizeof ver7) == (ssize_t)sizeof ver7,
            "write v7 header");

    // Full record: establishes job 1 with a 5-byte body allocation.
    int nl = 7;
    Jobrec jr = {0};
    jr.id = 1;
    jr.pri = 0;
    jr.delay = 0;
    jr.ttr = 120000000000LL; // 120s in ns
    jr.body_size = 5;        // "abc" + "\r\n"
    jr.created_at = 1;
    jr.state = Ready;
    assertf(write(bfd, &nl, sizeof nl) == (ssize_t)sizeof nl, "full namelen");
    assertf(write(bfd, "default", 7) == 7, "tube name");
    assertf(write(bfd, &jr, sizeof jr) == (ssize_t)sizeof jr, "full jobrec");
    assertf(write(bfd, "abc\r\n", 5) == 5, "full body");

    // Short record: state update for job 1. The writer always snapshots
    // the live j->r, so a legit short record repeats body_size=5; the
    // hostile variant plants a different value.
    int nl0 = 0;
    Jobrec jrs = jr;
    jrs.state = Buried;
    jrs.body_size = short_body_size;
    assertf(write(bfd, &nl0, sizeof nl0) == (ssize_t)sizeof nl0,
            "short namelen");
    assertf(write(bfd, &jrs, sizeof jrs) == (ssize_t)sizeof jrs,
            "short jobrec");

    // Zero padding parses as namelen=0 + all-zero Jobrec -> jr.id==0
    // -> clean EOF for readrec7.
    char zeros[512] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);
}


// v7 short records (namelen==0) used to skip ALL body_size validation:
// `j->r = jr` copied a corrupt size straight into the live job while
// the real allocation stayed 5 bytes. Consequences of replaying it:
// (a) peek/reserve would send r.body_size bytes from the 5-byte heap
// buffer to the client (OOB read), and (b) job_free would file the
// 5-byte slab into pool_class(60000) — a later 64KB PUT reusing that
// slab overflows it. The reader must reject the record; the corrupt
// state update takes the job down with it (same contract as a v8 CRC
// mismatch on a full record).
void
cttest_wal_v7_short_record_body_size_corruption_rejected()
{
    wal_setup();
    wal_write_v7_short_record_fixture(60000); // full record said 5

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Companion compatibility guard: a LEGIT v7 short record — body_size
// equal to the full record's, exactly as every writer (v7 and v8)
// produces — must still replay, preserving both the buried state and
// the body bytes. If the mismatch check ever rejects equal sizes,
// this fails: the fix may only kill corrupt records, never real ones.
void
cttest_wal_v7_short_record_legit_still_replays()
{
    wal_setup();
    wal_write_v7_short_record_fixture(5); // matches the full record

    int port = wal_startsrv();
    int fd = wal_dial(port);
    // The short record buried job 1; body must be intact ("abc" = 3
    // bytes net of the \r\n trailer).
    wal_send(fd, "peek-buried\r\n");
    wal_ckline(fd, "FOUND 1 3\r\n");
    wal_ckline(fd, "abc\r\n");
    wal_killsrv();
}


// Hand-craft a v7 binlog.1 with TWO full records for job 1: the first
// establishes a 5-byte allocation, the second re-states the same job
// (as a v7-era compaction/migration would) with body_size =
// second_body_size. v7 carries no CRC trailer, so the reader's own
// full-record body_size cross-check is the only thing keeping a
// mismatch out of j->r — where job_free would pool the small body slab
// under the bogus larger size class and a later allocate_job of that
// class would overflow it.
static void
wal_write_v7_full_record_fixture(int32 second_body_size)
{
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assertf(bfd >= 0, "create v7 fixture");

    int ver7 = 7;
    assertf(write(bfd, &ver7, sizeof ver7) == (ssize_t)sizeof ver7,
            "write v7 header");

    // Full record 1: establishes job 1 with a 5-byte body allocation.
    int nl = 7;
    Jobrec jr = {0};
    jr.id = 1;
    jr.pri = 0;
    jr.delay = 0;
    jr.ttr = 120000000000LL; // 120s in ns
    jr.body_size = 5;        // "abc" + "\r\n"
    jr.created_at = 1;
    jr.state = Ready;
    assertf(write(bfd, &nl, sizeof nl) == (ssize_t)sizeof nl, "full namelen");
    assertf(write(bfd, "default", 7) == 7, "tube name");
    assertf(write(bfd, &jr, sizeof jr) == (ssize_t)sizeof jr, "full jobrec");
    assertf(write(bfd, "abc\r\n", 5) == 5, "full body");

    // Full record 2: same job, same tube. The body is 'X'-padded with a
    // \r\n trailer so any body_size >= 2 stays well-formed.
    char body[64];
    memset(body, 'X', sizeof body);
    body[second_body_size-2] = '\r';
    body[second_body_size-1] = '\n';
    Jobrec jr2 = jr;
    jr2.body_size = second_body_size;
    assertf(write(bfd, &nl, sizeof nl) == (ssize_t)sizeof nl, "full2 namelen");
    assertf(write(bfd, "default", 7) == 7, "tube name 2");
    assertf(write(bfd, &jr2, sizeof jr2) == (ssize_t)sizeof jr2,
            "full2 jobrec");
    assertf(write(bfd, body, second_body_size) == (ssize_t)second_body_size,
            "full2 body");

    // Zero padding parses as namelen=0 + all-zero Jobrec -> jr.id==0
    // -> clean EOF for readrec7.
    char zeros[512] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);
}


// v7 full records for an existing job used to run their "size changed"
// check AFTER `j->r = jr`: a corrupt second full record with a larger
// body_size was copied into the live job first, and the Error path then
// freed the job by the bogus size, poisoning the size-class pool. The
// check now fires BEFORE the assignment; the corrupt re-state takes the
// job down (same contract as the v7 short-record corruption test).
void
cttest_wal_v7_full_record_body_size_corruption_rejected()
{
    wal_setup();
    wal_write_v7_full_record_fixture(40); // full record 1 said 5

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Companion compatibility guard: a LEGIT repeated full record — same
// body_size as the first, exactly what compaction/migration writes —
// must still replay and overwrite the body. If the check ever rejects
// equal sizes, migration replay breaks.
void
cttest_wal_v7_full_record_legit_still_replays()
{
    wal_setup();
    wal_write_v7_full_record_fixture(5); // "XXX\r\n" net of trailer: "XXX"

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "peek 1\r\n");
    wal_ckline(fd, "FOUND 1 3\r\n");
    wal_ckline(fd, "XXX\r\n");
    wal_killsrv();
}


// Write one v8 full record for job 1 into bfd with a VALID CRC32C
// trailer. Used to drive the v8 full-record body_size cross-check,
// which sits behind the CRC gate and can only be reached with a
// well-formed (but semantically hostile) record.
static void
wal_write_v8_full_record(int bfd, int32 body_size, const char *body)
{
    int nl = 7;
    Jobrec jr = {0};
    jr.id = 1;
    jr.pri = 0;
    jr.delay = 0;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 1;
    jr.state = Ready;

    uint32 crc = WAL_CRC32C_INIT;
    crc = wal_crc32c(crc, &nl, sizeof nl);
    crc = wal_crc32c(crc, "default", 7);
    crc = wal_crc32c(crc, &jr, sizeof jr);
    crc = wal_crc32c(crc, body, body_size);
    crc ^= WAL_CRC32C_XOR;
    unsigned char trailer[4] = {
        (unsigned char)(crc      ),
        (unsigned char)(crc >>  8),
        (unsigned char)(crc >> 16),
        (unsigned char)(crc >> 24),
    };

    assertf(write(bfd, &nl, sizeof nl) == (ssize_t)sizeof nl, "namelen");
    assertf(write(bfd, "default", 7) == 7, "tube name");
    assertf(write(bfd, &jr, sizeof jr) == (ssize_t)sizeof jr, "jobrec");
    assertf(write(bfd, body, body_size) == (ssize_t)body_size, "body");
    assertf(write(bfd, trailer, 4) == 4, "crc trailer");
}


// Hand-craft a v8 binlog.1: header + two full records for job 1 (first
// body_size 5, second second_body_size), both with valid CRC trailers.
static void
wal_write_v8_full_record_fixture(int32 second_body_size)
{
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assertf(bfd >= 0, "create v8 fixture");

    int ver8 = Walver;
    assertf(write(bfd, &ver8, sizeof ver8) == (ssize_t)sizeof ver8,
            "write v8 header");

    wal_write_v8_full_record(bfd, 5, "abc\r\n");

    char body[64];
    memset(body, 'X', sizeof body);
    body[second_body_size-2] = '\r';
    body[second_body_size-1] = '\n';
    wal_write_v8_full_record(bfd, second_body_size, body);

    char zeros[512] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);
}


// v8 full record with a valid CRC but a body_size that contradicts the
// job's existing allocation. The CRC gate passes, so the semantic
// cross-check (defense in depth against writer bugs) must reject the
// record BEFORE `j->r = jr`, and the job must not survive replay.
void
cttest_wal_v8_full_record_body_size_mismatch_rejected()
{
    wal_setup();
    wal_write_v8_full_record_fixture(40); // full record 1 said 5

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "stats-job 1\r\n");
    wal_ckline(fd, "NOT_FOUND\r\n");
    wal_killsrv();
}


// Legit v8 repeated full record (same body_size, valid CRC) must still
// replay and overwrite the body — the migration-replay contract.
void
cttest_wal_v8_full_record_legit_still_replays()
{
    wal_setup();
    wal_write_v8_full_record_fixture(5);

    int port = wal_startsrv();
    int fd = wal_dial(port);
    wal_send(fd, "peek 1\r\n");
    wal_ckline(fd, "FOUND 1 3\r\n");
    wal_ckline(fd, "XXX\r\n");
    wal_killsrv();
}
