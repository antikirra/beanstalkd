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

