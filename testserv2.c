#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <errno.h>

/* Reuse testserv.c infrastructure — these are extern because ct
 * links all test objects together. We declare the helpers we need. */
static int srvpid2;
static int64 timeout2 = 5000000000LL;

static int
diallocal(int port)
{
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };
    inet_aton("127.0.0.1", &addr.sin_addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) { perror("socket"); exit(1); }
    int flags = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(int));
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) == -1) {
        perror("connect"); exit(1);
    }
    return fd;
}

static void
killsrv2(void)
{
    if (!srvpid2) return;
    kill(srvpid2, SIGTERM);
    waitpid(srvpid2, 0, 0);
    srvpid2 = 0;
}

static void
exit2(int sig)
{
    UNUSED_PARAMETER(sig);
    exit(0);
}

static int
startsrv(void)
{
    struct sockaddr_in addr;

    srv.sock.fd = make_server_socket("127.0.0.1", "0");
    if (srv.sock.fd == -1) exit(1);

    socklen_t len = sizeof(addr);
    getsockname(srv.sock.fd, (struct sockaddr *)&addr, &len);
    int port = ntohs(addr.sin_port);

    srvpid2 = fork();
    if (srvpid2 < 0) exit(1);
    if (srvpid2 > 0) {
        atexit(killsrv2);
        usleep(100000);
        return port;
    }

    struct sigaction sa = { .sa_handler = exit2, .sa_flags = 0 };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, 0);
    prot_init();
    srv_acquire_wal(&srv);
    srvserve(&srv);
    exit(1);
}

static char *
rd(int fd)
{
    char c = 0, p = 0;
    static char buf[4096];
    fd_set rfd;
    struct timeval tv;
    size_t i = 0;
    for (;;) {
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        tv.tv_sec = timeout2 / 1000000000;
        tv.tv_usec = (timeout2/1000) % 1000000;
        int r = select(fd+1, &rfd, NULL, NULL, &tv);
        if (r <= 0) { fputs("timeout\n", stderr); exit(8); }
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
snd(int fd, char *s)
{
    int n = strlen(s);
    while (n > 0) {
        int w = write(fd, s, n);
        if (w <= 0) { perror("write"); exit(1); }
        s += w;
        n -= w;
    }
}

static void
ck(int fd, char *expect)
{
    char *got = rd(fd);
    assertf(strcmp(expect, got) == 0, "expected \"%s\", got \"%s\"", expect, got);
}

static void
cksub(int fd, char *sub)
{
    char *got = rd(fd);
    assertf(strstr(got, sub), "\"%s\" not found in \"%s\"", sub, got);
}

// WAL-enabled server: start, kill, restart with same WAL dir.
static int
startsrv_wal(void)
{
    srv.wal.use = 1;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;
    return startsrv();
}

static int
restartsrv_wal(void)
{
    killsrv2();
    close(srv.sock.fd);
    srv.sock.fd = -1;
    return startsrv_wal();
}

/* ============================================================
 * ANGRY PROTOCOL TESTS — hostile to the code
 * ============================================================ */

void
cttest_kick_negative_rejected()
{
    int port = startsrv();
    int fd = diallocal(port);
    /* kick -1 was accepting via strtoul wraparound — bug #11 fixed */
    snd(fd, "kick -1\r\n");
    ck(fd, "BAD_FORMAT\r\n");
}

void
cttest_kick_trailing_garbage_rejected()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "kick 5 GARBAGE\r\n");
    ck(fd, "BAD_FORMAT\r\n");
}

void
cttest_kick_zero()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "kick 0\r\n");
    ck(fd, "KICKED 0\r\n");
}

void
cttest_reserve_timeout_trailing_garbage()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "reserve-with-timeout 5 junk\r\n");
    ck(fd, "BAD_FORMAT\r\n");
}

void
cttest_put_zero_body()
{
    int port = startsrv();
    int fd = diallocal(port);
    /* body_size=0 means client sends 0 bytes of body + \r\n trailer */
    snd(fd, "put 0 0 1 0\r\n\r\n");
    cksub(fd, "INSERTED");
}

static void
readstats(int fd, char *body, int bodysz)
{
    char *line = rd(fd); /* "OK <len>\r\n" */
    assertf(strstr(line, "OK"), "stats must return OK, got: %s", line);
    int blen = atoi(line + 3);
    assertf(blen > 0 && blen < bodysz, "body len %d out of range", blen);
    int got = 0;
    while (got < blen) {
        int r = read(fd, body + got, blen - got);
        if (r <= 0) break;
        got += r;
    }
    body[got] = '\0';
}

void
cttest_stats_after_put_delete()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192];

    /* put 3 jobs */
    snd(fd, "put 0 0 60 4\r\ntest\r\n");
    cksub(fd, "INSERTED");
    snd(fd, "put 0 0 60 4\r\ntest\r\n");
    cksub(fd, "INSERTED");
    snd(fd, "put 0 0 60 4\r\ntest\r\n");
    cksub(fd, "INSERTED");

    snd(fd, "stats\r\n");
    readstats(fd, body, sizeof body);
    assertf(strstr(body, "current-jobs-ready: 3"), "must have 3 ready, body:\n%s", body);
    assertf(strstr(body, "cmd-put: 3"), "cmd-put must be 3");
    assertf(strstr(body, "total-jobs: 3"), "total-jobs must be 3");
}

void
cttest_release_cycle()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 100 0 60 4\r\ntest\r\n");
    cksub(fd, "INSERTED");

    /* reserve and release 10 times */
    int i;
    for (i = 0; i < 10; i++) {
        snd(fd, "reserve-with-timeout 0\r\n");
        cksub(fd, "RESERVED 1");
        char tmp[256];
        { ssize_t n_ = read(fd, tmp, sizeof tmp); (void)n_; }
        snd(fd, "release 1 100 0\r\n");
        ck(fd, "RELEASED\r\n");
    }

    /* job must still exist with same id */
    snd(fd, "peek 1\r\n");
    cksub(fd, "FOUND 1");
    char tmp[256];
    { ssize_t n_ = read(fd, tmp, sizeof tmp); (void)n_; }

    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
}

void
cttest_bury_kick_cycle()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 3\r\nabc\r\n");
    cksub(fd, "INSERTED");

    int i;
    for (i = 0; i < 5; i++) {
        snd(fd, "reserve-with-timeout 0\r\n");
        cksub(fd, "RESERVED");
        char tmp[256];
        { ssize_t n_ = read(fd, tmp, sizeof tmp); (void)n_; }
        snd(fd, "bury 1 0\r\n");
        ck(fd, "BURIED\r\n");

        snd(fd, "kick 1\r\n");
        ck(fd, "KICKED 1\r\n");
    }

    /* after 5 cycles, job is ready, body intact */
    snd(fd, "peek-ready\r\n");
    cksub(fd, "FOUND 1 3");
    char body[32];
    { ssize_t n_ = read(fd, body, sizeof body); (void)n_; }
    assertf(memcmp(body, "abc\r\n", 5) == 0, "body must be preserved after bury/kick cycles");
}

void
cttest_delete_nonexistent()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "delete 99999\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_use_watch_ignore_sequence()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");

    snd(fd, "watch alpha\r\n");
    ck(fd, "WATCHING 2\r\n");

    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    /* cannot ignore last tube */
    snd(fd, "ignore alpha\r\n");
    ck(fd, "NOT_IGNORED\r\n");

    snd(fd, "list-tube-used\r\n");
    ck(fd, "USING alpha\r\n");

    /* put into alpha, reserve from alpha */
    snd(fd, "put 0 0 60 2\r\nhi\r\n");
    cksub(fd, "INSERTED");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED");
    char tmp[256];
    { ssize_t n_ = read(fd, tmp, sizeof tmp); (void)n_; }
}

void
cttest_put_bad_formats()
{
    int port = startsrv();
    int fd = diallocal(port);

    /* "put" without space is unknown command (CMD_PUT = "put ") */
    snd(fd, "put\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    snd(fd, "put 0\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    snd(fd, "put 0 0\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    snd(fd, "put 0 0 0\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    /* non-numeric */
    snd(fd, "put abc 0 0 1\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    /* trailing garbage */
    snd(fd, "put 0 0 0 1 extra\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    /* after all bad formats, connection must still work */
    snd(fd, "put 0 0 1 1\r\n");
    snd(fd, "X\r\n");
    cksub(fd, "INSERTED");
}

void
cttest_empty_line()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    /* connection still alive */
    snd(fd, "put 0 0 1 1\r\nZ\r\n");
    cksub(fd, "INSERTED");
}

void
cttest_stats_tube_nonexistent()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "stats-tube nonexistent\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_pause_tube_nonexistent()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "pause-tube nonexistent 10\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_reserve_job_already_reserved_v2()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\nX\r\n");
    cksub(fd, "INSERTED");

    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED");
    char tmp[256];
    { ssize_t n_ = read(fd, tmp, sizeof tmp); (void)n_; }

    /* reserve-job on already-reserved job must fail */
    snd(fd, "reserve-job 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

/* ============================================================
 * HOSTILE WAL TESTS — attack data durability guarantees
 * ============================================================ */

// Helper: check one stats-job field. Sends stats-job, reads OK line,
// then reads YAML body and checks for substring.
static void
ckstatjob(int fd, int id, char *sub)
{
    char cmd[64];
    snprintf(cmd, sizeof cmd, "stats-job %d\r\n", id);
    snd(fd, cmd);
    cksub(fd, "OK ");        // consume "OK <bytes>\r\n"
    cksub(fd, sub);          // check YAML body
}

// #597: release with delay=0 must persist new priority to WAL.
// Without fix, !!delay evaluates to 0, WAL write is skipped,
// and the new priority is lost on restart.
void
cttest_wal_release_delay0_persists_priority()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 120 4\r\ntest\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);

    // release with NEW priority 999 and delay=0
    snd(fd, "release 1 999 0\r\n");
    ck(fd, "RELEASED\r\n");

    ckstatjob(fd, 1, "\npri: 999\n");
    ckstatjob(fd, 1, "\nreleases: 1\n");

    close(fd);

    // CRASH AND RESTART — the moment of truth
    port = restartsrv_wal();
    fd = diallocal(port);

    // priority MUST survive restart
    ckstatjob(fd, 1, "\npri: 999\n");
    ckstatjob(fd, 1, "\nreleases: 1\n");
}

// #668: bury_ct must NOT double-increment on WAL replay.
// Without fix, each restart increments bury_ct by 1.
void
cttest_wal_bury_ct_stable_across_restarts()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 120 4\r\ntest\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);

    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");

    ckstatjob(fd, 1, "\nburies: 1\n");

    close(fd);

    // First restart
    port = restartsrv_wal();
    fd = diallocal(port);

    ckstatjob(fd, 1, "\nburies: 1\n"); // must be 1, not 2

    close(fd);

    // Second restart — stress the invariant
    port = restartsrv_wal();
    fd = diallocal(port);

    ckstatjob(fd, 1, "\nburies: 1\n"); // must still be 1, not 3
}

// #668: buried jobs must preserve their burial ORDER across WAL replay.
// Without fix, order was based on creation time, not burial time.
void
cttest_wal_buried_order_preserved_after_restart()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 120 1\r\nA\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 120 1\r\nB\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 120 1\r\nC\r\n");
    ck(fd, "INSERTED 3\r\n");

    // reserve all 3
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 1\r\n");
    rd(fd);
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 3 1\r\n");
    rd(fd);

    // bury in order: 3 (C), then 1 (A), then 2 (B)
    snd(fd, "bury 3 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "bury 2 0\r\n");
    ck(fd, "BURIED\r\n");

    // peek-buried must return first buried: job 3
    snd(fd, "peek-buried\r\n");
    cksub(fd, "FOUND 3 1\r\n");
    rd(fd);

    close(fd);

    // RESTART
    port = restartsrv_wal();
    fd = diallocal(port);

    // peek-buried MUST still return job 3 (first buried)
    snd(fd, "peek-buried\r\n");
    cksub(fd, "FOUND 3 1\r\n");
    rd(fd);

    // kick 1 — removes job 3 from buried
    snd(fd, "kick 1\r\n");
    ck(fd, "KICKED 1\r\n");

    // next peek-buried must be job 1 (second buried)
    snd(fd, "peek-buried\r\n");
    cksub(fd, "FOUND 1 1\r\n");
    rd(fd);

    // kick 1 — removes job 1
    snd(fd, "kick 1\r\n");
    ck(fd, "KICKED 1\r\n");

    // next must be job 2 (third buried)
    snd(fd, "peek-buried\r\n");
    cksub(fd, "FOUND 2 1\r\n");
    rd(fd);
}

// #597 stress: release-with-delay=0 across multiple priority changes.
void
cttest_wal_release_delay0_multi_priority_changes()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 120 4\r\ntest\r\n");
    ck(fd, "INSERTED 1\r\n");

    // cycle: reserve → release with new priority (delay=0) × 5
    uint32_t priorities[] = {100, 200, 50, 999, 42};
    for (int i = 0; i < 5; i++) {
        snd(fd, "reserve-with-timeout 0\r\n");
        cksub(fd, "RESERVED 1 4\r\n");
        rd(fd);

        char cmd[64];
        snprintf(cmd, sizeof cmd, "release 1 %u 0\r\n", priorities[i]);
        snd(fd, cmd);
        ck(fd, "RELEASED\r\n");
    }

    ckstatjob(fd, 1, "\npri: 42\n");
    ckstatjob(fd, 1, "\nreleases: 5\n");

    close(fd);

    // restart — verify last priority survives
    port = restartsrv_wal();
    fd = diallocal(port);

    ckstatjob(fd, 1, "\npri: 42\n");
    ckstatjob(fd, 1, "\nreleases: 5\n");
}


/* ============================================================
 * TRUNCATE COMMAND TESTS
 * ============================================================ */

void
cttest_truncate_basic()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use test\r\n");
    ck(fd, "USING test\r\n");
    snd(fd, "put 0 0 60 4\r\naaaa\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 4\r\nbbbb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 60 4\r\ncccc\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "truncate test\r\n");
    ck(fd, "TRUNCATED 3\r\n");

    snd(fd, "watch test\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_not_found()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "truncate nonexistent\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "truncate \r\n");
    ck(fd, "BAD_FORMAT\r\n");
}

void
cttest_truncate_trailing_garbage()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "use test\r\n");
    ck(fd, "USING test\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "truncate test extra\r\n");
    ck(fd, "BAD_FORMAT\r\n");
}

void
cttest_truncate_empty_tube()
{
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "use empty\r\n");
    ck(fd, "USING empty\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch empty\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
    snd(fd, "truncate empty\r\n");
    ck(fd, "TRUNCATED 0\r\n");
}

void
cttest_truncate_preserves_new_jobs()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use myq\r\n");
    ck(fd, "USING myq\r\n");
    snd(fd, "put 0 0 60 3\r\nold\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 3\r\nold\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "truncate myq\r\n");
    ck(fd, "TRUNCATED 2\r\n");

    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "watch myq\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 3 3\r\n");
    cksub(fd, "new\r\n");
}

void
cttest_truncate_with_delayed()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use dq\r\n");
    ck(fd, "USING dq\r\n");
    snd(fd, "put 0 3600 60 4\r\ndata\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 3600 60 4\r\ndata\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "truncate dq\r\n");
    ck(fd, "TRUNCATED 2\r\n");

    snd(fd, "watch dq\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_with_buried()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use bq\r\n");
    ck(fd, "USING bq\r\n");
    snd(fd, "put 0 0 60 4\r\ndata\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch bq\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");

    snd(fd, "truncate bq\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "kick 100\r\n");
    ck(fd, "KICKED 0\r\n");
}

void
cttest_truncate_reserved_released_is_dead()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use rq\r\n");
    ck(fd, "USING rq\r\n");
    snd(fd, "put 0 0 60 4\r\ndata\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch rq\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);

    snd(fd, "truncate rq\r\n");
    ck(fd, "TRUNCATED 0\r\n");

    snd(fd, "release 1 0 0\r\n");
    ck(fd, "RELEASED\r\n");

    snd(fd, "stats-tube rq\r\n");
    cksub(fd, "OK ");
    cksub(fd, "current-jobs-ready: 0\n");
}

void
cttest_truncate_other_tube_unaffected()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");
    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "use beta\r\n");
    ck(fd, "USING beta\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "truncate alpha\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "watch beta\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 1\r\n");
    cksub(fd, "b\r\n");
}

void
cttest_truncate_double()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use dbl\r\n");
    ck(fd, "USING dbl\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate dbl\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "truncate dbl\r\n");
    ck(fd, "TRUNCATED 0\r\n");
}

void
cttest_wal_truncate_persists()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "use wt\r\n");
    ck(fd, "USING wt\r\n");
    snd(fd, "put 0 0 60 4\r\ndata\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 4\r\nmore\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "truncate wt\r\n");
    ck(fd, "TRUNCATED 2\r\n");

    close(fd);
    port = restartsrv_wal();
    fd = diallocal(port);

    snd(fd, "watch wt\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_wal_truncate_new_jobs_survive()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "use ws\r\n");
    ck(fd, "USING ws\r\n");
    snd(fd, "put 0 0 60 3\r\nold\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate ws\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 2\r\n");

    close(fd);
    port = restartsrv_wal();
    fd = diallocal(port);

    snd(fd, "watch ws\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 3\r\n");
    cksub(fd, "new\r\n");
}

void
cttest_truncate_put_into_truncated_tube()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use tput\r\n");
    ck(fd, "USING tput\r\n");
    snd(fd, "put 0 0 60 3\r\nold\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate tput\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 60 4\r\nnew2\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "watch tput\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 3\r\n");
    cksub(fd, "new\r\n");

    snd(fd, "delete 2\r\n");
    ck(fd, "DELETED\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 3 4\r\n");
    cksub(fd, "new2\r\n");
}

void
cttest_truncate_stats_cmd_counter()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use st\r\n");
    ck(fd, "USING st\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate st\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "stats\r\n");
    cksub(fd, "OK ");
    cksub(fd, "cmd-truncate: 1\n");
}

void
cttest_truncate_kick_dead_buried()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use kb\r\n");
    ck(fd, "USING kb\r\n");
    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "watch kb\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 1\r\n");
    rd(fd);
    snd(fd, "bury 2 0\r\n");
    ck(fd, "BURIED\r\n");

    snd(fd, "truncate kb\r\n");
    ck(fd, "TRUNCATED 2\r\n");

    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 3 3\r\n");
    cksub(fd, "new\r\n");
}

void
cttest_truncate_pause_interaction()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use pi\r\n");
    ck(fd, "USING pi\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "pause-tube pi 3600\r\n");
    ck(fd, "PAUSED\r\n");

    snd(fd, "truncate pi\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "pause-tube pi 0\r\n");
    ck(fd, "PAUSED\r\n");

    snd(fd, "watch pi\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 2\r\n");
    cksub(fd, "RESERVED 2 3\r\n");
    cksub(fd, "new\r\n");
}

void
cttest_truncate_drain_mode()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use dr\r\n");
    ck(fd, "USING dr\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate dr\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    snd(fd, "watch dr\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_peek_ready_hides_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use pk\r\n");
    ck(fd, "USING pk\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");

    // Pipeline: truncate + peek-ready in one write.
    // No prottick between them, so the zombie is still in the heap.
    snd(fd, "truncate pk\r\npeek-ready\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_peek_job_hides_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use pj\r\n");
    ck(fd, "USING pj\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");

    // Pipeline: truncate + peek <id> in one write.
    snd(fd, "truncate pj\r\npeek 1\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_reserve_job_blocked()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use rj\r\n");
    ck(fd, "USING rj\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");

    // Pipeline: truncate + reserve-job in one write.
    snd(fd, "truncate rj\r\nreserve-job 1\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_fast_reserve_skips_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use fr\r\n");
    ck(fd, "USING fr\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");

    // Pipeline: truncate + watch + reserve in one write.
    // Fast-path reserve must skip the truncated tube.
    snd(fd, "truncate fr\r\nwatch fr\r\nreserve-with-timeout 0\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "WATCHING 2\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_peek_delayed_hides_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use pd\r\n");
    ck(fd, "USING pd\r\n");
    snd(fd, "put 0 3600 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "truncate pd\r\npeek-delayed\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_peek_buried_hides_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use pb\r\n");
    ck(fd, "USING pb\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch pb\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 3\r\n");
    rd(fd);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");

    snd(fd, "truncate pb\r\npeek-buried\r\n");
    ck(fd, "TRUNCATED 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

void
cttest_truncate_delete_reserved_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use dr\r\n");
    ck(fd, "USING dr\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch dr\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 3\r\n");
    rd(fd);

    snd(fd, "truncate dr\r\n");
    ck(fd, "TRUNCATED 0\r\n");

    // Explicit delete of a reserved purge-eligible job must work.
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
}

void
cttest_truncate_release_with_delay_kills_zombie()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use rd\r\n");
    ck(fd, "USING rd\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch rd\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 3\r\n");
    rd(fd);

    snd(fd, "truncate rd\r\n");
    ck(fd, "TRUNCATED 0\r\n");

    // Release with delay: enqueue_job intercept catches purge-eligible.
    snd(fd, "release 1 0 3600\r\n");
    ck(fd, "RELEASED\r\n");

    // Job should NOT appear in delay queue.
    snd(fd, "stats-tube rd\r\n");
    cksub(fd, "OK ");
    cksub(fd, "current-jobs-delayed: 0\n");
}

void
cttest_truncate_multi_tube_isolation()
{
    int port = startsrv();
    int fd = diallocal(port);

    // Create jobs across 3 tubes.
    snd(fd, "use t1\r\n");
    ck(fd, "USING t1\r\n");
    snd(fd, "put 0 0 60 2\r\nt1\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "use t2\r\n");
    ck(fd, "USING t2\r\n");
    snd(fd, "put 0 0 60 2\r\nt2\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "use t3\r\n");
    ck(fd, "USING t3\r\n");
    snd(fd, "put 0 0 60 2\r\nt3\r\n");
    ck(fd, "INSERTED 3\r\n");

    // Truncate only t2.
    snd(fd, "truncate t2\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    // t1 and t3 must still have their jobs.
    snd(fd, "use t1\r\n");
    ck(fd, "USING t1\r\n");
    snd(fd, "peek-ready\r\n");
    cksub(fd, "FOUND 1 2\r\n");
    rd(fd);

    snd(fd, "use t3\r\n");
    ck(fd, "USING t3\r\n");
    snd(fd, "peek-ready\r\n");
    cksub(fd, "FOUND 3 2\r\n");
    rd(fd);

    // t2 must be empty.
    snd(fd, "use t2\r\n");
    ck(fd, "USING t2\r\n");
    snd(fd, "watch t2\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_bury_then_kick_new()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use bk\r\n");
    ck(fd, "USING bk\r\n");
    snd(fd, "put 0 0 60 3\r\nold\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch bk\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 3\r\n");
    rd(fd);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");

    snd(fd, "truncate bk\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    // Put a NEW job, bury it, then kick.
    snd(fd, "put 0 0 60 3\r\nnew\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 3\r\n");
    rd(fd);
    snd(fd, "bury 2 0\r\n");
    ck(fd, "BURIED\r\n");

    // Kick should get the NEW buried job, not the old dead one.
    snd(fd, "kick 100\r\n");
    ck(fd, "KICKED 1\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 2 3\r\n");
    cksub(fd, "new\r\n");
}

void
cttest_wal_truncate_cross_tube_safe()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    // Tube alpha gets job 1.
    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");
    snd(fd, "put 0 0 60 5\r\nalpha\r\n");
    ck(fd, "INSERTED 1\r\n");

    // Tube beta gets job 2, then truncated.
    snd(fd, "use beta\r\n");
    ck(fd, "USING beta\r\n");
    snd(fd, "put 0 0 60 4\r\nbeta\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "truncate beta\r\n");
    ck(fd, "TRUNCATED 1\r\n");

    // Restart.
    close(fd);
    port = restartsrv_wal();
    fd = diallocal(port);

    // Alpha's job must survive (cross-tube safety).
    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");
    snd(fd, "watch alpha\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 5\r\n");
    cksub(fd, "alpha\r\n");

    // Beta's job must be gone.
    snd(fd, "watch beta\r\n");
    ck(fd, "WATCHING 3\r\n");
    snd(fd, "ignore alpha\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

void
cttest_truncate_connclose_reserved()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use cc\r\n");
    ck(fd, "USING cc\r\n");
    snd(fd, "put 0 0 60 3\r\nfoo\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch cc\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 3\r\n");
    rd(fd);

    snd(fd, "truncate cc\r\n");
    ck(fd, "TRUNCATED 0\r\n");

    // Close connection with reserved purge-eligible job.
    // Must not crash (use-after-free fix in enqueue_reserved_jobs).
    close(fd);

    // Verify server is still alive by connecting again.
    int fd2 = diallocal(port);
    snd(fd2, "stats\r\n");
    cksub(fd2, "OK ");
}

// ============================================================
// -c MAXCONN: hostile checks. Reject must happen at accept layer
// without ever creating a Conn — verify by counting accepted
// stats and probing a third connection.
// ============================================================

// Helper: read whatever the server sends, treat EOF as the signal
// that the server slammed the door. Returns 1 if conn looks open
// (got bytes back), 0 if conn was closed/reset before any reply.
static int
probe_conn_alive(int fd)
{
    // Send something cheap. If server already closed pre-write,
    // EPIPE will surface here or on the read.
    ssize_t w = write(fd, "stats\r\n", 7);
    if (w < 0) return 0;
    char buf[64];
    fd_set rfd;
    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    int r = select(fd + 1, &rfd, NULL, NULL, &tv);
    if (r <= 0) return 0;
    ssize_t n = read(fd, buf, sizeof buf);
    return n > 0;
}

void
cttest_maxconn_rejects_excess()
{
    srv.maxconn = 2;
    int port = startsrv();

    int a = diallocal(port);
    int b = diallocal(port);

    // First two must be live: send stats, expect a reply.
    assertf(probe_conn_alive(a), "1st conn must be accepted");
    assertf(probe_conn_alive(b), "2nd conn must be accepted");

    // Third conn: kernel completes the handshake (it's in accept
    // backlog), but the server's accept4 + close cycle must hand
    // back EOF to our read.
    int c = diallocal(port);
    assertf(!probe_conn_alive(c),
        "3rd conn must be rejected (server-side close)");

    close(a); close(b); close(c);
}

// After a slot frees up, the server must accept again — limit is
// dynamic, not a one-shot trip.
void
cttest_maxconn_recovers_after_close()
{
    srv.maxconn = 1;
    int port = startsrv();

    int a = diallocal(port);
    assertf(probe_conn_alive(a), "1st conn must be accepted");

    int b = diallocal(port);
    assertf(!probe_conn_alive(b), "2nd conn must be rejected at limit");
    close(b);

    close(a);

    // Slot must be reclaimed in cur_conn_ct on connclose; new conn ok.
    // Small sleep to let the server drain the close event.
    usleep(50000);
    int c = diallocal(port);
    assertf(probe_conn_alive(c),
        "after slot frees, server must accept again");
    close(c);
}

// Default (maxconn=0) must allow many connections — preserves the
// upstream "no limit on upgrade" contract. 32 is enough to detect
// any accidental cap that crept in.
void
cttest_maxconn_default_unlimited()
{
    srv.maxconn = 0;
    int port = startsrv();

    int fds[32];
    for (int i = 0; i < 32; i++) {
        fds[i] = diallocal(port);
        assertf(probe_conn_alive(fds[i]),
            "conn %d must succeed with maxconn=0", i);
    }
    for (int i = 0; i < 32; i++) close(fds[i]);
}

// ============================================================
// -I IDLE_TIMEOUT: hostile checks. Idle = STATE_WANT_COMMAND
// with no reserved jobs and no pending reserve. A worker
// blocked on reserve-with-timeout is NOT idle.
// ============================================================

// Wait up to `to_ms` ms and return whether the conn was server-closed
// (read returns 0 = EOF). 1 = closed by server, 0 = still open.
static int
wait_for_server_close(int fd, int to_ms)
{
    fd_set rfd;
    struct timeval tv = { .tv_sec = to_ms / 1000,
                          .tv_usec = (to_ms % 1000) * 1000 };
    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    int r = select(fd + 1, &rfd, NULL, NULL, &tv);
    if (r <= 0) return 0;
    char buf[64];
    ssize_t n = read(fd, buf, sizeof buf);
    return n == 0; // EOF means server closed
}

// Drain whatever bytes are buffered on fd within tail_ms of quiet,
// then return. Used after a stats reply where we don't care about
// the body — only that the conn is back in STATE_WANT_COMMAND.
static void
drain_quiet(int fd, int tail_ms)
{
    char buf[4096];
    for (;;) {
        fd_set rfd;
        struct timeval tv = { .tv_sec = 0, .tv_usec = tail_ms * 1000 };
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        int r = select(fd + 1, &rfd, NULL, NULL, &tv);
        if (r <= 0) return;
        ssize_t n = read(fd, buf, sizeof buf);
        if (n <= 0) return;
    }
}

void
cttest_idle_timeout_closes_silent_conn()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // Send one command to ensure the server-side conn is fully wired.
    snd(fd, "stats\r\n");
    cksub(fd, "OK ");
    drain_quiet(fd, 100);

    // Now sit silent; server must close us within ~2s.
    assertf(wait_for_server_close(fd, 2500),
        "server must close conn after idle_timeout");
    close(fd);
}

void
cttest_idle_timeout_does_not_close_busy_conn()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // Hammer commands continuously for ~1.5s. last_activity_at must
    // refresh on each command boundary so idle never fires.
    for (int i = 0; i < 6; i++) {
        snd(fd, "stats\r\n");
        cksub(fd, "OK ");
        drain_quiet(fd, 50);
        usleep(250000); // 0.25s between cmds — well under 1s timeout
    }
    // Conn must still be alive.
    assertf(probe_conn_alive(fd),
        "active conn must survive idle_timeout");
    close(fd);
}

void
cttest_idle_timeout_does_not_close_waiting_reserve()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // Block on reserve-with-timeout 3 (longer than idle_timeout).
    // The conn is "waiting" — must not be idle-closed before the
    // protocol-level reserve timeout fires.
    snd(fd, "reserve-with-timeout 3\r\n");
    // Server will reply TIMED_OUT after ~3s; if -I (1s) wrongly fired
    // first, we'd see EOF (read returns 0) instead.
    char *got = rd(fd);
    assertf(strcmp(got, "TIMED_OUT\r\n") == 0,
        "blocked reserve must produce TIMED_OUT, got [%s]", got);
    close(fd);
}

void
cttest_idle_timeout_does_not_close_reserved_holder()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    // Conn A: producer, puts a job with TTR=10.
    int a = diallocal(port);
    snd(a, "put 0 0 10 3\r\nfoo\r\n");
    cksub(a, "INSERTED");

    // Conn B: worker, reserves the job and then sits silent. The conn
    // is no longer "waiting" but holds a reserved job — must not be
    // idle-closed (closing would auto-release the job and confuse
    // workers that legitimately hold long-TTR jobs).
    int b = diallocal(port);
    snd(b, "reserve-with-timeout 1\r\n");
    cksub(b, "RESERVED");
    rd(b); // body line

    // Wait > idle_timeout; B must still be alive.
    usleep(1500000);
    assertf(probe_conn_alive(b),
        "conn holding a reserved job must not be idle-closed");
    close(a); close(b);
}

void
cttest_idle_timeout_default_off_keeps_conn()
{
    srv.idle_timeout = 0; // disabled
    int port = startsrv();

    int fd = diallocal(port);
    snd(fd, "stats\r\n");
    cksub(fd, "OK ");
    drain_quiet(fd, 100);

    // Sit silent for 2s; without -I, server must keep the conn open.
    int closed = wait_for_server_close(fd, 2000);
    assertf(!closed,
        "default (idle_timeout=0) must not close idle conns");
    close(fd);
}

// REGRESSION: after a put completes via the reply() fast path, the
// conn transitions WANT_DATA → WANT_COMMAND. During WANT_DATA the
// idle deadline that h_accept scheduled may FIRE (prottick removes
// it from srv.conns; conn_timeout sees state != WANT_COMMAND so
// it no-ops; connsched at conn_timeout's tail removes it again
// because conntickat returns 0 for non-eligible states). The conn
// is now OUT of the heap. Reply()'s fast path must put it back,
// otherwise the client puts and sits silent and never gets reaped.
//
// To trigger this, we MUST wait > idle_timeout while in WANT_DATA
// so the heap entry actually fires. A short usleep wouldn't expose
// the bug — the conn would stay in the heap with its original
// idle deadline, and reply()'s c->in_conns check would catch it.
void
cttest_idle_timeout_after_put_still_fires()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    snd(fd, "put 0 0 60 4\r\n");   // header — server enters WANT_DATA
    usleep(1500000);                // > idle_timeout: heap entry fires & is removed
    snd(fd, "test\r\n");            // body completes the put
    cksub(fd, "INSERTED");
    drain_quiet(fd, 50);

    // Sit silent after INSERTED. Server must close us within ~2s.
    // If reply() forgot to reschedule with -I on, the conn would be
    // OUT of srv.conns (idle deadline already fired during WANT_DATA)
    // and would never be reaped.
    assertf(wait_for_server_close(fd, 2500),
        "server must idle-close after a completed slow put");
    close(fd);
}

// A client mid-PUT (STATE_WANT_DATA) is intentionally NOT subject to
// -I: the conn_timeout idle gate requires STATE_WANT_COMMAND. Locks
// in this scoping decision so a future "broaden -I to all states"
// patch is forced to update this test deliberately.
void
cttest_idle_timeout_does_not_close_slow_put_body()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // Send the PUT line but only part of the body. Server transitions
    // to STATE_WANT_DATA. Wait > idle_timeout, then complete the body.
    snd(fd, "put 0 0 60 10\r\n");
    usleep(1500000); // 1.5s — well past idle deadline
    snd(fd, "0123456789\r\n");
    cksub(fd, "INSERTED");
    close(fd);
}

// Hot-path regression. The reply() fast path sets STATE_WANT_COMMAND
// directly, bypassing conn_want_command. Forgetting to refresh
// last_activity_at there would let the idle timer fire while the
// client is busy — a kill-the-server-while-it's-working bug.
//
// We MUST use a STATE_SEND_WORD reply (not stats — that's SEND_JOB
// which routes through conn_want_command and would mask the bug).
// `put` returns "INSERTED <id>\r\n" via the SEND_WORD fast path,
// followed by `delete` which also returns "DELETED\r\n" via SEND_WORD.
// Both go through reply()'s fast path on success.
void
cttest_idle_timeout_busy_pipeline_survives()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // 6 cycles × 0.5s = 3s wall time, well past idle_timeout.
    // Each cycle puts and deletes a job — both replies go through
    // the SEND_WORD fast path. Without the last_activity_at refresh,
    // the conn would be reaped around cycle 2-3.
    for (int i = 0; i < 6; i++) {
        char cmd[64];
        snprintf(cmd, sizeof cmd, "put 0 0 60 1\r\nx\r\n");
        snd(fd, cmd);
        char *got = rd(fd);
        assertf(strncmp(got, "INSERTED ", 9) == 0,
            "expected INSERTED, got [%s]", got);
        int id = atoi(got + 9);
        snprintf(cmd, sizeof cmd, "delete %d\r\n", id);
        snd(fd, cmd);
        ck(fd, "DELETED\r\n");
        usleep(500000); // 0.5s
    }
    assertf(probe_conn_alive(fd),
        "pipelined client must survive idle_timeout via fast-path refresh");
    close(fd);
}

// Hostile: the first conn that never sends a byte. Without the
// connsched in h_accept, this conn would never enter srv.conns and
// the idle timer would silently miss it. Catches that regression.
void
cttest_idle_timeout_closes_silent_at_accept()
{
    srv.idle_timeout = 1000000000LL; // 1s
    int port = startsrv();

    int fd = diallocal(port);
    // Do NOT send anything. Server must still reap us.
    assertf(wait_for_server_close(fd, 2500),
        "server must idle-close a conn that never sent a byte");
    close(fd);
}

// ============================================================
// -H HTTP health endpoint. Detection runs before which_cmd, so
// any non-GET/non-HEAD command must keep the upstream behaviour
// 1:1. Drain mode flips 200 → 503; -H off makes GET an
// UNKNOWN_COMMAND like any other unknown verb.
// ============================================================

// Read up to `cap` bytes within `to_ms` ms; returns total read.
static int
read_until_close(int fd, char *out, int cap, int to_ms)
{
    int total = 0;
    while (total < cap) {
        fd_set rfd;
        struct timeval tv = { .tv_sec = to_ms / 1000,
                              .tv_usec = (to_ms % 1000) * 1000 };
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        int r = select(fd + 1, &rfd, NULL, NULL, &tv);
        if (r <= 0) break;
        ssize_t n = read(fd, out + total, cap - total);
        if (n <= 0) break;
        total += n;
    }
    out[total] = 0;
    return total;
}

void
cttest_http_health_get_200_with_flag()
{
    srv.http_health = 1;
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "GET /health HTTP/1.0\r\n\r\n");

    char buf[1024];
    int n = read_until_close(fd, buf, sizeof buf - 1, 1000);
    assertf(n > 0, "must receive an HTTP response");
    assertf(strncmp(buf, "HTTP/1.0 200 OK\r\n", 17) == 0,
        "must start with HTTP/1.0 200 OK, got [%s]", buf);
    assertf(strstr(buf, "Content-Length: 2") != NULL,
        "must declare Content-Length: 2, got [%s]", buf);
    assertf(strstr(buf, "Connection: close") != NULL,
        "must include Connection: close, got [%s]", buf);
    assertf(strstr(buf, "\r\n\r\nok") != NULL,
        "body must be \"ok\", got [%s]", buf);
    close(fd);
}

void
cttest_http_health_head_200_no_body()
{
    srv.http_health = 1;
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "HEAD /health HTTP/1.0\r\n\r\n");

    char buf[1024];
    int n = read_until_close(fd, buf, sizeof buf - 1, 1000);
    assertf(n > 0, "must receive a response");
    assertf(strncmp(buf, "HTTP/1.0 200 OK\r\n", 17) == 0,
        "HEAD must return 200, got [%s]", buf);
    assertf(strstr(buf, "Content-Length: 2") != NULL,
        "HEAD must keep Content-Length header, got [%s]", buf);
    // Body must be absent: response ends with the empty-line CRLF.
    assertf(buf[n - 1] == '\n' && buf[n - 2] == '\r'
            && buf[n - 3] == '\n' && buf[n - 4] == '\r',
        "HEAD must end at empty-line CRLF (no body), got [%s]", buf);
    close(fd);
}

// 503 when draining. enter_drain_mode flips the static drain_mode in
// the parent BEFORE fork; the child inherits it. Same pattern as
// cttest_put_in_drain in testserv.c — avoids wiring SIGUSR1 into the
// test server (default SIGUSR1 action is Term).
void
cttest_http_health_503_when_draining()
{
    srv.http_health = 1;
    enter_drain_mode(SIGUSR1); // sets static drain_mode=1 pre-fork
    int port = startsrv();

    int fd = diallocal(port);
    snd(fd, "GET /health HTTP/1.0\r\n\r\n");
    char buf[1024];
    int n = read_until_close(fd, buf, sizeof buf - 1, 1000);
    assertf(n > 0, "must receive a 503 response");
    assertf(strncmp(buf, "HTTP/1.0 503 ", 13) == 0,
        "drain mode must produce 503, got [%s]", buf);
    assertf(strstr(buf, "Content-Length: 8") != NULL,
        "503 body length must be 8, got [%s]", buf);
    assertf(strstr(buf, "draining") != NULL,
        "drain body must be \"draining\", got [%s]", buf);
    close(fd);
}

// Symmetric for HEAD: drain mode → 503 status line + headers, no body.
void
cttest_http_health_head_503_when_draining()
{
    srv.http_health = 1;
    enter_drain_mode(SIGUSR1);
    int port = startsrv();

    int fd = diallocal(port);
    snd(fd, "HEAD /health HTTP/1.0\r\n\r\n");
    char buf[1024];
    int n = read_until_close(fd, buf, sizeof buf - 1, 1000);
    assertf(n > 0, "must receive a 503 response");
    assertf(strncmp(buf, "HTTP/1.0 503 ", 13) == 0,
        "HEAD in drain must produce 503, got [%s]", buf);
    assertf(strstr(buf, "draining") == NULL,
        "HEAD must NOT include body even in drain, got [%s]", buf);
    close(fd);
}

// -H off: GET must hit the standard unknown-command path so
// existing operators see no behaviour drift on upgrade.
void
cttest_http_health_off_get_is_unknown()
{
    srv.http_health = 0;
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "GET /health HTTP/1.0\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");
    close(fd);
}

// "GETS" or "GET" without the trailing space must NOT be hijacked
// — strict prefix match. UNKNOWN_COMMAND is the correct reply.
void
cttest_http_health_strict_prefix()
{
    srv.http_health = 1;
    int port = startsrv();

    int a = diallocal(port);
    snd(a, "GETS\r\n"); // 5 chars, no space after GET
    ck(a, "UNKNOWN_COMMAND\r\n");

    int b = diallocal(port);
    snd(b, "GET\r\n"); // 4 chars, "GET" then \r\n — no space
    ck(b, "UNKNOWN_COMMAND\r\n");

    int d = diallocal(port);
    snd(d, "HEADS\r\n");
    ck(d, "UNKNOWN_COMMAND\r\n");

    close(a); close(b); close(d);
}

// Real-world HTTP probes (kubelet, curl) send the request line AND
// headers in one TCP packet:
//   GET / HTTP/1.1\r\nHost: x\r\nUser-Agent: kube-probe\r\n\r\n
// The server's pipeline loop must NOT iterate past the GET line. If
// it did, each subsequent header would be answered as UNKNOWN_COMMAND
// and the kubelet probe would see garbage after the 200 OK. The
// guarantee is delivered by cmd_data_ready's state == STATE_WANT_COMMAND
// gate; STATE_CLOSE breaks the loop after the HTTP reply.
void
cttest_http_health_kubelet_style_request()
{
    srv.http_health = 1;
    int port = startsrv();
    int fd = diallocal(port);

    // Send the entire HTTP/1.1 request in one write — request line
    // plus three headers plus the empty terminator line.
    snd(fd,
        "GET /healthz HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "User-Agent: kube-probe/1.28\r\n"
        "Connection: close\r\n"
        "\r\n");

    char buf[2048];
    int n = read_until_close(fd, buf, sizeof buf - 1, 1000);
    assertf(n > 0, "must receive a response");
    // Exactly ONE HTTP/1.0 status line. The body is "ok" (2 bytes).
    // No UNKNOWN_COMMAND lines from header-misinterpretation.
    assertf(strncmp(buf, "HTTP/1.0 200 OK\r\n", 17) == 0,
        "must start with single 200 OK, got [%s]", buf);
    assertf(strstr(buf, "UNKNOWN_COMMAND") == NULL,
        "headers must not be misinterpreted as beanstalk cmds, got [%s]",
        buf);
    // Response must end at "ok" — no trailing UNKNOWN_COMMAND noise.
    int len = (int)strlen(buf);
    assertf(buf[len - 2] == 'o' && buf[len - 1] == 'k',
        "response must end with body \"ok\", got tail [...%s]",
        buf + (len > 8 ? len - 8 : 0));
    close(fd);
}

// After replying, the server MUST close the conn. Connection: close
// is advisory for the client; the real guarantee is that we read EOF.
void
cttest_http_health_closes_after_reply()
{
    srv.http_health = 1;
    int port = startsrv();
    int fd = diallocal(port);
    snd(fd, "GET / HTTP/1.0\r\n\r\n");

    char buf[1024];
    read_until_close(fd, buf, sizeof buf - 1, 1000);
    // Now the read above must have hit EOF for read_until_close to
    // return without filling cap. Probe again — must be EOF immediately.
    char tail[16];
    ssize_t n = read(fd, tail, sizeof tail);
    assertf(n == 0, "server must close conn after HTTP reply, read=%zd", n);
    close(fd);
}

