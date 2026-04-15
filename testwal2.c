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

    // Write binlog.1 with just the v7 header, rest is all zeros —
    // readrec7 will read zeros as namelen=0 + all-zero Jobrec, see
    // jr.id == 0, return 0 (EOF). No records, no errors.
    char *path = wal_binlog_path(1);
    int bfd = open(path, O_WRONLY | O_CREAT, 0600);
    assertf(bfd >= 0, "create v7 fixture");
    int ver7 = 7;
    assertf(write(bfd, &ver7, sizeof ver7) == sizeof ver7, "write v7 header");
    // Pad to a realistic filesize with zeros so walscandir sees it.
    char zeros[1024] = {0};
    assertf(write(bfd, zeros, sizeof zeros) == (ssize_t)sizeof zeros, "pad");
    close(bfd);
    free(path);

    // Start a new server — it must accept the v7 file without crash.
    int port = wal_startsrv();
    int fd = wal_dial(port);

    // Server is up: can put and stats normally.
    wal_send(fd, "put 0 0 60 3\r\n");
    wal_send(fd, "hey\r\n");
    wal_ckline(fd, "INSERTED 1\r\n");
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
}
