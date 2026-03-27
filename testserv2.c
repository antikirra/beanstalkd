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

void
cttest_shard_wal_write_and_recover()
{
    /* Write jobs across multiple tubes (hitting different WAL shards),
     * kill the server, restart, verify all jobs survive. */
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 0;
    srv.nshards = 4;

    int port = startsrv();
    int fd = diallocal(port);

    /* Put 5 jobs in each of 4 tubes = 20 jobs across shards. */
    int t, i;
    for (t = 0; t < 4; t++) {
        char cmd[64];
        snprintf(cmd, sizeof(cmd), "use shard-tube-%d\r\n", t);
        snd(fd, cmd);
        cksub(fd, "USING");
        for (i = 0; i < 5; i++) {
            snd(fd, "put 0 0 60 4\r\ntest\r\n");
            cksub(fd, "INSERTED");
        }
    }
    close(fd);

    /* Kill server, restart with same WAL dir and shard count. */
    killsrv2();
    srv.nshards = 4;
    port = startsrv();
    fd = diallocal(port);

    /* Verify all 20 jobs recovered: watch each tube and reserve. */
    for (t = 0; t < 4; t++) {
        char cmd[64];
        snprintf(cmd, sizeof(cmd), "watch shard-tube-%d\r\n", t);
        snd(fd, cmd);
        cksub(fd, "WATCHING");
    }
    for (i = 0; i < 20; i++) {
        snd(fd, "reserve-with-timeout 0\r\n");
        cksub(fd, "RESERVED");
        rd(fd); /* drain body */
    }
    /* 21st reserve must timeout — all jobs consumed. */
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}
