#include "ct/ct.h"
#include "dat.h"
#include "testinject.h"
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
static int srvport2;   // last port startsrv() handed out; see nudge_srv2
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

// See the nudge_srv note in testserv.c, caveat included: ending the
// epoll park was the third theory for the SIGTERM timeouts that show up
// under TSan, and measurement refuted it like the two before. The nudge
// stays because it is free on the normal path and cannot weaken a check
// — a server that truly ignored SIGTERM would still never exit.
static void
nudge_srv2(void)
{
    if (srvport2 <= 0) return;
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(srvport2),
    };
    inet_aton("127.0.0.1", &addr.sin_addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) return;
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) == 0) {
        ssize_t w = write(fd, "quit\r\n", 6);
        (void)w;
    }
    close(fd);
}

static void
killsrv2(void)
{
    if (!srvpid2) return;
    kill(srvpid2, SIGTERM);
    // Bounded, for the same reason as testserv.c's kill_srvpid: a plain
    // waitpid turns a server that will not stop into a suite that hangs
    // with no output and no failing test name. This one also runs from
    // atexit, so it cannot assert — kill and carry on.
    for (int i = 0; i < 1000; i++) {
        if (i == 2 || i == 20 || i == 200)
            nudge_srv2();
        pid_t r = waitpid(srvpid2, 0, WNOHANG);
        if (r == srvpid2) { srvpid2 = 0; srvport2 = 0; return; }
        if (r == -1 && errno != EINTR) break;
        usleep(10000);
    }
    twarnx("test server pid %d did not exit on SIGTERM within 10s; killing",
           (int)srvpid2);
    kill(srvpid2, SIGKILL);
    waitpid(srvpid2, 0, 0);
    srvpid2 = 0;
    srvport2 = 0;
}

// SIGTERM in the forked test server. Not exit(): that is not
// async-signal-safe (atexit handlers, stdio flush, and under a
// sanitizer the runtime's own teardown) and it deadlocked under TSan,
// hanging the whole suite on the parent's waitpid. Same shape as the
// real server: flag, wake the loop through the eventfd, let srvserve
// return and exit from ordinary context.
static void
exit2(int sig)
{
    UNUSED_PARAMETER(sig);
    atomic_store_explicit(&shutdown_requested, 1, memory_order_relaxed);
    srv_wake();
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

    // Ready pipe instead of a fixed sleep. The listening socket exists
    // before the fork, so a successful connect() proves nothing — the
    // kernel queues it whether or not the child is serving yet. The
    // child tells us directly, which is both exact and ~100ms faster
    // per test that starts a server (there are over a hundred).
    // socketpair + send/recv, not pipe + write/read: the fault injector
    // wraps write(), so a ready byte written with write() becomes the
    // server's first wrapped write and silently eats a fault armed at
    // skip 0. See the longer note in testserv.c.
    int ready[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, ready) != 0) exit(1);

    srvpid2 = fork();
    if (srvpid2 < 0) exit(1);
    if (srvpid2 > 0) {
        close(ready[1]);
        atexit(killsrv2);
        char go;
        ssize_t n = recv(ready[0], &go, 1, 0);
        close(ready[0]);
        // n == 0 means the child died before it was serving: its end of
        // the pipe closed on exit.
        assertf(n == 1, "setup: the server did not come up");
        srvport2 = port;
        return port;
    }

    close(ready[0]);
    // Before the handlers: srv_wake() does nothing until the eventfd
    // exists, and a signal landing while srvserve is parked in epoll
    // would then sit on its flag until the next natural wake-up.
    if (srv_wake_init() == -1) {
        twarn("srv_wake_init");
        exit(111);
    }
    struct sigaction sa = { .sa_handler = exit2, .sa_flags = 0 };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, 0);
    // The real server installs this in main(); without it SIGUSR1 kills
    // the test server outright instead of putting it into drain mode,
    // and every drain test times out against a corpse.
    struct sigaction su = { .sa_handler = enter_drain_mode, .sa_flags = 0 };
    sigemptyset(&su.sa_mask);
    sigaction(SIGUSR1, &su, 0);
    prot_init();
    srv_acquire_wal(&srv);
    // Retry the handshake: a test may have armed a write fault before
    // the fork (the table is inherited), and this write must not be the
    // one that spends it. EINTR is the ordinary reason to retry.
    for (;;) {
        ssize_t w = send(ready[1], "1", 1, 0);
        if (w == 1)
            break;
        if (w == -1 && (errno == EINTR || errno == EAGAIN
                        || errno == EWOULDBLOCK))
            continue;
        break;   // the parent's read sees EOF and fails the setup
    }
    close(ready[1]);
    srvserve(&srv);
    // exit(), not _exit(). exit() in a process forked out of a
    // sanitizer-instrumented parent IS a real hazard — the child
    // inherits the runtime's locks without the threads that would
    // release them — and this was _exit for a while because of it. Two
    // things settled it back:
    //
    //   the hazard is not what makes cttest_binlog_empty_exit time out
    //   under TSan (switching to _exit did not move the rate), and
    //
    //   _exit costs the coverage of every forked server, which is most
    //   of prot.c: 82.2% with exit(), 39.9% with _exit plus a
    //   weakly-declared __gcov_dump. The weak declaration is why — a
    //   weak REFERENCE does not pull the defining member out of
    //   libgcov, so the pointer stays null and the dump is skipped in
    //   silence. That is issue #443's reason for exit() being here.
    //
    // A speculative fix that missed its target and halved a measurement
    // is not a fix.
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

/* protocol.txt (delete): a job can be deleted while it is ready, delayed
 * or buried, or while it is reserved by this client. The delayed case is
 * the one with a second half: the delay timer is still armed when the
 * job goes away, so a delete that only unlinks the job from the ready
 * path leaves prottick to promote a freed job when the delay expires. */
void
cttest_delete_delayed_job_never_becomes_ready()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 1 60 1\r\nd\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");

    // Outlive the delay, then ask for the job the only two ways the
    // protocol offers. Neither may produce it, and the server must
    // still be answering at all.
    usleep(1300000);

    snd(fd, "peek-delayed\r\n");
    ck(fd, "NOT_FOUND\r\n");

    snd(fd, "peek-ready\r\n");
    ck(fd, "NOT_FOUND\r\n");

    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
}

/* ---------- partial writes ---------- */

/* A socket buffer that takes only part of a reply is not an error: the
 * kernel does it whenever the peer is slow, and the retry path in
 * reply() is what makes the rest arrive. Nothing exercised it before,
 * so a retry that resent the whole line (duplicate bytes) or dropped
 * the remainder (a client waiting forever) would have gone unnoticed.
 *
 * fault_set_short is armed BEFORE startsrv so the forked server
 * inherits the armed table; the child's very first reply is the one
 * that gets truncated. */
void
cttest_short_write_reply_arrives_whole_and_once()
{
    fault_set_short(FAULT_WRITE, 0, 3);   // first reply: only 3 bytes land
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");

    // ...and the connection is still usable afterwards, in order.
    snd(fd, "list-tube-used\r\n");
    ck(fd, "USING alpha\r\n");
    fault_clear_all();
}


/* The other half of the same story: the socket accepts NOTHING and the
 * kernel says EAGAIN. That is not an error either — it means "ask me
 * again when the socket is writable" — so the reply must be parked with
 * epoll armed for write and delivered whole once it is. Treating EAGAIN
 * as failure would close a healthy connection; ignoring it would drop
 * the reply. */
void
cttest_eagain_on_the_first_write_still_delivers_the_reply()
{
    // The fault table is armed before the fork, so BOTH sides inherit a
    // countdown of 1 — including this process, whose next write is the
    // command being sent. Spend the parent's copy on a harmless
    // descriptor first; the server's copy is still armed for its reply.
    int sink = open("/dev/null", O_WRONLY);
    assertf(sink > 2, "setup: /dev/null must land above the wrapped fds");
    fault_set(FAULT_WRITE, 0, EAGAIN);
    int port = startsrv();
    ssize_t spent = write(sink, "x", 1);
    assertf(spent == -1 && errno == EAGAIN,
            "setup: the parent's copy of the injection must be spent here");
    close(sink);

    int fd = diallocal(port);

    snd(fd, "use gamma\r\n");
    ck(fd, "USING gamma\r\n");

    snd(fd, "list-tube-used\r\n");
    ck(fd, "USING gamma\r\n");
    fault_clear_all();
}

/* Same hazard on the job reply, which goes out as header+body through
 * writev: a short vector must resume at the right offset in the right
 * buffer, or the body is spliced into the header or sent twice. */
void
cttest_short_writev_job_reply_keeps_header_and_body_intact()
{
    fault_set_short(FAULT_WRITEV, 0, 4);  // first writev: 4 bytes land
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 11\r\nhello world\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve\r\n");
    ck(fd, "RESERVED 1 11\r\n");
    ck(fd, "hello world\r\n");

    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
    fault_clear_all();
}

/* A reply truncated to a single byte is the extreme of the same path:
 * it forces the retry to run several times before the line is out. */
void
cttest_one_byte_writes_still_deliver_every_reply_in_order()
{
    fault_set_short(FAULT_WRITE, 0, 1);
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use beta\r\n");
    ck(fd, "USING beta\r\n");
    snd(fd, "watch beta\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");
    fault_clear_all();
}

/* A worker that shuts down its write side while parked on `reserve` is
 * saying "no more commands", not "goodbye": it is still waiting for the
 * job it asked for. protocol.txt gives reserve two outcomes, RESERVED
 * or TIMED_OUT, so the half-close has to resolve into one of them
 * rather than leaving the conn parked in the waiting set forever —
 * where it also keeps its tube's current-waiting count inflated. */
void
cttest_half_close_while_waiting_on_reserve_ends_the_wait()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "reserve-with-timeout 30\r\n");
    // No job will ever arrive; announce end-of-commands instead.
    assertf(shutdown(fd, SHUT_WR) == 0, "setup: half-close must succeed");

    ck(fd, "TIMED_OUT\r\n");
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

// A delayed job's whole promise is the instant it becomes ready, and
// that instant has to survive a restart. The WAL stores an absolute
// deadline, so replay must put the job back on the delay heap with the
// time it had left — not ready immediately (the delay is lost, workers
// see the job early) and not delayed all over again from zero (the
// delay is doubled).
void
cttest_wal_delayed_job_keeps_its_deadline_across_a_restart()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 2 120 4\r\nlate\r\n");
    ck(fd, "INSERTED 1\r\n");
    ckstatjob(fd, 1, "\nstate: delayed\n");
    close(fd);

    port = restartsrv_wal();
    fd = diallocal(port);

    // Still delayed, and still not reservable.
    ckstatjob(fd, 1, "\nstate: delayed\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");

    // ...and it does arrive. 4s covers the 2s delay plus replay, and
    // the reserve blocks rather than polling, so this does not race.
    snd(fd, "reserve-with-timeout 4\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);
}

// A reservation belongs to a connection, and no connection survives a
// restart. protocol.txt describes reserved as a state a job leaves when
// its TTR expires, so a job whose reserver is gone must come back
// ready and reservable — not stuck reserved by nobody, which no client
// could ever release.
void
cttest_wal_reserved_job_comes_back_ready_after_a_restart()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 3600 4\r\nheld\r\n");   // long TTR: it cannot lapse
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);
    ckstatjob(fd, 1, "\nstate: reserved\n");
    close(fd);

    port = restartsrv_wal();
    fd = diallocal(port);

    ckstatjob(fd, 1, "\nstate: ready\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    rd(fd);
}

// Ids must keep climbing across a restart. If replay left the counter
// where a fresh start puts it, the next put would reuse an id a
// recovered job already holds: two live jobs with one id, and `delete
// <id>` becomes a coin flip. (job.c derives the counter from the
// largest id it replays; this is that promise seen from the wire.)
void
cttest_wal_job_ids_keep_climbing_after_a_restart()
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 120 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 120 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 120 1\r\nc\r\n");
    ck(fd, "INSERTED 3\r\n");

    // Delete the middle one: the counter must follow the HIGHEST id
    // ever used, not the number of jobs that survived.
    snd(fd, "delete 2\r\n");
    ck(fd, "DELETED\r\n");
    close(fd);

    port = restartsrv_wal();
    fd = diallocal(port);

    snd(fd, "put 0 0 120 1\r\nd\r\n");
    ck(fd, "INSERTED 4\r\n");

    // And the survivors are still addressable under their own ids.
    ckstatjob(fd, 1, "\nid: 1\n");
    ckstatjob(fd, 3, "\nid: 3\n");
    snd(fd, "stats-job 2\r\n");
    ck(fd, "NOT_FOUND\r\n");
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


// Protocol conformance: dispatch must strictly match the verb prefix.
// Garbage bytes in place of a real command prefix should yield
// UNKNOWN_COMMAND, not be misdispatched as the real verb.
void
cttest_prot_dispatch_strict_prefix()
{
    int port = startsrv();
    int fd = diallocal(port);

    // peek must be exactly "peek ", not "pe?? ". Before the fix, any
    // "pe?? <id>\r\n" dispatched to OP_PEEKJOB.
    snd(fd, "pexk 1\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // reserve-with-timeout needs full prefix. Before the fix, any
    // "r??????-w*" dispatched and replied BAD_FORMAT.
    snd(fd, "reserve-wtf 1\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // quit must be exactly "quit\r\n" (6 bytes). Before the fix,
    // "quitXXX\r\n" closed the connection.
    snd(fd, "quitXXX\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // kick must be "kick N". "kxxk 1" must fail.
    snd(fd, "kxxk 1\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // list-tubes-watched needs exact length-20 form.
    snd(fd, "list-tubesXXXXXXXX\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // "truncate" existed in this fork and was removed: the verb must be
    // unknown again (upstream wire contract).
    snd(fd, "truncate foo\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // "touch" still owns the 't' branch: it must not be shadowed by the
    // removed truncate dispatch.
    snd(fd, "touch 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

// ============================================================
// CRLF injection (upstream issue #669, CVSS 7.5). The forged-reply
// vector needs a protocol field containing \r or \n to be accepted
// and echoed; our parser's valid_name_char[] charset excludes every
// control byte, so names with bare CR/LF must be rejected outright,
// and an embedded CRLF must behave as plain pipelining (1 reply per
// command, never a desync).
// ============================================================

void
cttest_crlf_injection_rejected()
{
    int port = startsrv();
    int fd = diallocal(port);

    // Bare CR inside a tube name: rejected, single BAD_FORMAT.
    snd(fd, "use a\rb\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    // Bare LF inside a tube name: rejected.
    snd(fd, "watch a\nb\r\n");
    ck(fd, "BAD_FORMAT\r\n");

    // Embedded NUL: caught by the memchr guard, not silently truncated
    // into a valid name.
    {
        const char raw[] = "use a\0b\r\n";
        size_t n = sizeof raw - 1;
        const char *p = raw;
        while (n > 0) {
            int w = write(fd, p, n);
            if (w <= 0) { perror("write"); exit(1); }
            p += w;
            n -= w;
        }
    }
    ck(fd, "BAD_FORMAT\r\n");

    // CRLF terminates the command (protocol-defined line split, i.e.
    // ordinary pipelining): "use a" succeeds as its own command, the
    // leftover "b" is unknown. Replies stay 1:1 with commands.
    snd(fd, "use a\r\nb\r\n");
    ck(fd, "USING a\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // #669's PoC is exactly this: two puts in one TCP send. Both are
    // queued as separate commands and each gets its own ack — nothing
    // is "injected" into another client's stream.
    snd(fd, "put 0 0 60 5\r\nHello\r\nput 0 0 60 5\r\nHello\r\n");
    cksub(fd, "INSERTED ");
    cksub(fd, "INSERTED ");

    // The conn is still in sync afterwards: a follow-up stats-tube on
    // the injected-adjacent tube answers on its own line.
    snd(fd, "stats-tube default\r\n");
    cksub(fd, "OK ");
}

// ============================================================
// -c MAXCONN: hostile checks. Reject must happen at accept layer
// without ever creating a Conn — verify by counting accepted
// stats and probing a third connection.
// ============================================================

// Defined below with the -I tests; declared here because the -c tests
// need a REAL end-of-file (not merely "nothing arrived") to tell a
// server-side close apart from a half-open hang, and need to drain a
// stats body before reusing the conn.
static int  wait_for_server_close(int fd, int to_ms);
static void drain_quiet(int fd, int tail_ms);

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

    // The refusal must reach the client rather than leaving it hanging
    // on a descriptor nobody serves. The window has to clear
    // TCP_DEFER_ACCEPT (net.c): the kernel withholds a connection that
    // has sent nothing for about a second, so the server cannot accept
    // — let alone reject — this conn any sooner.
    assertf(wait_for_server_close(c, 3000),
        "rejected conn must see the close, not a half-open hang");

    assertf(!probe_conn_alive(c),
        "3rd conn must be rejected (server-side close)");

    // The reject must happen before a Conn exists. total-connections
    // counts every Conn ever made, so a client turned away at the accept
    // layer must leave it at 2; current-connections must still show the
    // two survivors.
    drain_quiet(a, 100);
    snd(a, "stats\r\n");
    char body[8192];
    readstats(a, body, sizeof body);
    assertf(strstr(body, "current-connections: 2\n"),
        "the two accepted conns must be the only ones counted, body:\n%s", body);
    assertf(strstr(body, "total-connections: 2\n"),
        "a conn rejected at the accept layer must never become a Conn, body:\n%s",
        body);
    drain_quiet(a, 100);

    // EOF on the third conn is also what a dead server produces. The
    // survivors prove the door was slammed on one client, not on all.
    // Both sockets are drained first: bytes still sitting in the kernel
    // buffer would answer for a server that has already gone.
    drain_quiet(b, 100);
    assertf(probe_conn_alive(a),
        "1st conn must still be served after a rejection");
    assertf(probe_conn_alive(b),
        "2nd conn must still be served after a rejection");

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
    // Silence is not a rejection: a server that accepted the fd and then
    // forgot to close it looks exactly the same from here, while leaking
    // one descriptor per turned-away client until EMFILE. Read before
    // writing so the close arrives as a FIN rather than an RST.
    // 3s, not 1s: TCP_DEFER_ACCEPT holds a silent connection back for
    // about a second before the server ever sees it (net.c).
    assertf(wait_for_server_close(b, 3000),
        "conn rejected at the limit must see the close, not a half-open hang");
    assertf(!probe_conn_alive(b), "2nd conn must be rejected at limit");
    close(b);

    close(a);

    // Slot must be reclaimed in cur_conn_ct on connclose; new conn ok.
    // Small sleep to let the server drain the close event.
    usleep(50000);
    int c = diallocal(port);
    assertf(probe_conn_alive(c),
        "after slot frees, server must accept again");

    // Closing ONE conn must free exactly ONE slot, and the client that
    // was turned away must not have consumed one at all: c is the second
    // Conn this server ever made, and the only one alive.
    drain_quiet(c, 100);
    snd(c, "stats\r\n");
    char body[8192];
    readstats(c, body, sizeof body);
    assertf(strstr(body, "current-connections: 1\n"),
        "exactly one slot must be occupied after the swap, body:\n%s", body);
    assertf(strstr(body, "total-connections: 2\n"),
        "the rejected client must not have consumed a Conn, body:\n%s", body);

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

    // Dial the whole burst BEFORE talking to any of them: the conns pile
    // into the accept backlog together instead of each getting its own
    // readiness event and its own round trip.
    enum { NCONN = 32 };
    int fds[NCONN];
    for (int i = 0; i < NCONN; i++)
        fds[i] = diallocal(port);

    // "Some bytes came back" is satisfied by a server answering garbage.
    // Each conn gets a command whose correct reply is unique to it, so a
    // crossed or truncated stream is a failure, not a pass.
    for (int i = 0; i < NCONN; i++) {
        char cmd[64], want[64];
        snprintf(cmd, sizeof cmd, "use burst-%d\r\n", i);
        snprintf(want, sizeof want, "USING burst-%d\r\n", i);
        snd(fds[i], cmd);
        ck(fds[i], want);
    }

    // All NCONN must be open AT ONCE — the count the server reports is
    // the direct statement, not an inference from N successful dials.
    char body[8192];
    char want[64];
    snd(fds[0], "stats\r\n");
    readstats(fds[0], body, sizeof body);
    snprintf(want, sizeof want, "current-connections: %d\n", NCONN);
    assertf(strstr(body, want),
        "maxconn=0 must hold all %d conns simultaneously, body:\n%s",
        NCONN, body);

    for (int i = 0; i < NCONN; i++) close(fds[i]);
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
    // EOF is the clean signal; ECONNRESET is the same refusal seen
    // through a socket that still had unread bytes when the server
    // closed it. Both say "the server is not serving this conn".
    return n == 0 || (n == -1 && errno == ECONNRESET);
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
    // Do NOT send anything. The deadline is one full idle_timeout away,
    // so the conn must survive comfortably inside it: an upper bound
    // alone is equally happy with a server that closes every conn the
    // instant it accepts it.
    assertf(!wait_for_server_close(fd, 500),
        "conn must not be closed before its idle deadline");

    // Server must still reap us.
    assertf(wait_for_server_close(fd, 2500),
        "server must idle-close a conn that never sent a byte");

    // EOF is also what a dead server delivers. A fresh client proves the
    // conn was reaped, not that the process fell over.
    int probe = diallocal(port);
    assertf(probe_conn_alive(probe),
        "server must still accept and serve clients after reaping an idle conn");
    close(probe);
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


// #P1 regression: 7-byte commands starting with 's' other than "stats"
// must NOT dispatch as OP_STATS. The old which_cmd dispatched on the
// length+offset alone, leaking the global stats block for "sleep\r\n",
// "steal\r\n", "stash\r\n" etc. Also exercise the "stats-" prefix
// tightening for "stats-job" and "stats-tube".
void
cttest_which_cmd_stats_prefix_strict()
{
    int port = startsrv();
    int fd = diallocal(port);

    // Seven-byte 's-prefix' impostors: every must reply UNKNOWN_COMMAND.
    const char *bogus[] = {
        "sleep\r\n",
        "steal\r\n",
        "stash\r\n",
        "slxxx\r\n",
        "sXXXXX\r\n",     // 8 bytes — falls through via length gate anyway
    };
    for (size_t i = 0; i < sizeof bogus / sizeof *bogus; i++) {
        snd(fd, (char *)bogus[i]);
        ck(fd, "UNKNOWN_COMMAND\r\n");
    }

    // Trailing-garbage variants of the real OP_STATSJOB / OP_STATS_TUBE
    // prefixes: must also be rejected. Missing '-' separator at offset 5
    // is the exact path that used to be loose.
    snd(fd, "sXXXXXjYYY\r\n");  // cmd[6]='j' but no "stats-" prefix
    ck(fd, "UNKNOWN_COMMAND\r\n");
    snd(fd, "sXXXXXtYYY\r\n");  // cmd[6]='t' but no "stats-" prefix
    ck(fd, "UNKNOWN_COMMAND\r\n");

    // Positive control: real "stats\r\n" still works. Only the first
    // reply line is checked — do_list_tubes / do_stats follow with a
    // YAML body that does NOT terminate with \r\n, which would hang the
    // line-based rd() helper.
    snd(fd, "stats\r\n");
    char *reply = rd(fd);
    assertf(strncmp(reply, "OK ", 3) == 0,
            "stats must still work, got [%s]", reply);

    close(fd);
}


// #P2 regression: 12-byte 'l...s\r\n' commands must require the literal
// "list-tubes" prefix. The old dispatch accepted any byte soup with
// cmd[9]=='s' and leaked the whole tube namespace.
void
cttest_which_cmd_list_tubes_prefix_strict()
{
    int port = startsrv();
    int fd = diallocal(port);

    const char *bogus[] = {
        "l12345678s\r\n",
        "looking-us\r\n",
        "l-banned-s\r\n",
        "lXXXXXXXXs\r\n",
    };
    for (size_t i = 0; i < sizeof bogus / sizeof *bogus; i++) {
        snd(fd, (char *)bogus[i]);
        ck(fd, "UNKNOWN_COMMAND\r\n");
    }

    // Positive control: real "list-tubes\r\n" still works. As above,
    // only inspect the first line of the reply.
    snd(fd, "list-tubes\r\n");
    char *reply = rd(fd);
    assertf(strncmp(reply, "OK ", 3) == 0,
            "list-tubes must still work, got [%s]", reply);

    close(fd);
}


// #S1 regression: after reserve-with-timeout TIMED_OUT, the reply fast
// path must re-arm epoll for read ('r'). The conn entered STATE_WAIT
// with c->rw='h' (hangup-only); conn_timeout then fires the TIMED_OUT
// reply via reply() fast path with state=STATE_WANT_COMMAND, but the
// old code never switched the socket interest back to EPOLLIN. The
// next command sat in the kernel buffer and epoll_wait never reported
// it — the client hung until closing.
//
// We drive the regression directly: reserve-with-timeout 1 → wait for
// TIMED_OUT → send stats and require a response. With the regression,
// rd() would timeout (timeout2=5s) and fail the test.
void
cttest_reserve_timeout_then_next_command_does_not_hang()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "watch s1tube\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    // 1-second reserve with no jobs in the tube — must produce TIMED_OUT.
    snd(fd, "reserve-with-timeout 1\r\n");
    ck(fd, "TIMED_OUT\r\n");

    // The moment of truth: can the conn still receive a command?
    // On the S1 regression, the socket events mask stays at 'h' and
    // rd() blocks until timeout2 (5s).
    snd(fd, "stats\r\n");
    char *reply = rd(fd);
    assertf(strncmp(reply, "OK ", 3) == 0,
            "stats after TIMED_OUT must still work, got [%s]", reply);

    close(fd);
}


// #S1 sister regression: DEADLINE_SOON — like TIMED_OUT — is emitted
// from conn_timeout via reply_msg() and follows the same fast-path
// under STATE_WAIT. If the re-arm branch were specific to TIMED_OUT,
// DEADLINE_SOON would leak the same hang. Lock the property end-to-end.
//
// Timing: put ttr=1, reserve → c holds a Reserved job whose deadline
// is ~now+1s. Sleep so that <1s remains (inside SAFETY_MARGIN). A new
// reserve-with-timeout enters wait_for_job which sees conndeadlinesoon
// and fires DEADLINE_SOON immediately.
void
cttest_deadline_soon_then_next_command_does_not_hang()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "use s1ds\r\n");
    ck(fd, "USING s1ds\r\n");
    snd(fd, "watch s1ds\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    snd(fd, "put 0 0 1 4\r\nsoon\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    cksub(fd, "RESERVED 1 4\r\n");
    cksub(fd, "soon\r\n");

    // 500ms closes the gap so that remaining ttr (~500ms) < the 1s
    // SAFETY_MARGIN. The next reserve must immediately receive
    // DEADLINE_SOON, not wait for timeout or ttr expiry.
    usleep(500000);
    snd(fd, "reserve-with-timeout 5\r\n");
    ck(fd, "DEADLINE_SOON\r\n");

    // The S1 re-arm path must put the socket back on EPOLLIN; a
    // regression here would stall this next command until connclose.
    snd(fd, "stats\r\n");
    char *reply = rd(fd);
    assertf(strncmp(reply, "OK ", 3) == 0,
            "stats after DEADLINE_SOON must still work, got [%s]", reply);

    close(fd);
}


/* ============================================================
 * DURABLE (-D) GROUP-COMMIT WIRE TESTS — hostile to the code
 * ============================================================ */

// Server in durable mode: -D semantics (durable_sync=1 implies -F /
// wantsync=0; group commit per invariant #16). Caller sets srv.wal.dir.
static int
startsrv_durable(void)
{
    srv.wal.use = 1;
    srv.wal.durable_sync = 1;
    srv.wal.wantsync = 0; // -D implies -F: walcommit owns the fdatasync
    srv.wal.syncrate = 0;
    return startsrv();
}

// >4KB of acks buffered on ONE conn in ONE tick. The reply() defer hook
// used to handle the dur_reply_buf overflow by dropping the buffered
// acks and sending only the current line — up to 4KB of replies (~300
// INSERTED lines) silently vanished and the client's reply stream
// desynced forever. Every one of the 900 INSERTED acks must arrive, in
// id order.
void
cttest_dur_pipeline_acks_over_4kb_not_dropped()
{
    srv.wal.dir = ctdir();
    int port = startsrv_durable();
    int fd = diallocal(port);

    enum { NPUT = 900 };
    static const char put_cmd[] = "put 0 0 0 1\r\nx\r\n"; // 16 bytes
    static char burst[NPUT * (sizeof put_cmd - 1) + 1];
    size_t blen = 0;
    for (int i = 0; i < NPUT; i++) {
        memcpy(burst + blen, put_cmd, sizeof put_cmd - 1);
        blen += sizeof put_cmd - 1;
    }
    burst[blen] = '\0';
    // One pipelined burst: the kernel queues it faster than the server
    // dispatches 224-byte command-buffer rounds, so the acks accumulate
    // in dur_reply_buf within a tick and cross the 4096 soft cap.
    snd(fd, burst);

    for (int i = 1; i <= NPUT; i++) {
        char exp[64];
        snprintf(exp, sizeof exp, "INSERTED %d\r\n", i);
        ck(fd, exp);
    }

    close(fd);
}


// Reply ORDER under -D: a WAL-dirty command (delete → ack deferred for
// the group commit) pipelined with a STATE_SEND_JOB command
// (peek-ready → immediate writev). The job reply used to bypass the
// deferred ack buffer entirely: the client saw FOUND+body BEFORE
// DELETED — positional clients attribute both replies to the wrong
// commands. Wire contract: replies arrive in command order.
void
cttest_dur_delete_then_peek_replies_in_command_order()
{
    srv.wal.dir = ctdir();
    int port = startsrv_durable();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");

    // One pipelined burst, dispatched in a single tick.
    snd(fd, "delete 1\r\npeek-ready\r\n");
    ck(fd, "DELETED\r\n");
    ck(fd, "FOUND 2 1\r\n");
    ck(fd, "b\r\n"); // job body line

    close(fd);
}

// Mid-PUT state vs the end-of-tick durable flush. A pipelined
// "delete N\r\nput <hdr>\r\n" whose body lags one packet leaves the
// conn in STATE_WANT_DATA when dur_flush_all runs. The old flush
// unconditionally reset the conn to STATE_WANT_COMMAND, so the body
// bytes that arrived next were parsed as a COMMAND LINE — job-body
// content injected into the command stream (and the half-read in_job
// leaked). The body must complete the put normally.
void
cttest_dur_flush_does_not_clobber_mid_put_body()
{
    srv.wal.dir = ctdir();
    int port = startsrv_durable();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");

    // WAL-dirty delete (deferred ack) + put header; body intentionally
    // held back so the durable flush runs while the put is mid-body.
    snd(fd, "delete 1\r\nput 0 0 60 10\r\n");
    ck(fd, "DELETED\r\n");
    usleep(100000); // tick boundary: flush fires with conn in WANT_DATA
    snd(fd, "0123456789\r\n");
    ck(fd, "INSERTED 2\r\n"); // old code: body parsed as a command → UNKNOWN_COMMAND

    close(fd);
}

// The worker idiom under -D: "delete N\r\nreserve\r\n" in ONE pipelined
// burst. The delete's ack is deferred for the group commit; the
// RESERVED+body reply is deferred BEHIND it (reply() SEND_JOB hook) and
// the fd is parked out of epoll until the end-of-tick flush re-arms
// 'w'. Attack: if the flush forgets the re-arm, the parked fd produces
// no events and the client waits for RESERVED forever — this test then
// dies in rd()'s 5s timeout. Also kills any reorder (RESERVED before
// DELETED) and any ack-bytes-into-body interleave.
void
cttest_dur_worker_idiom_delete_reserve_one_burst()
{
    srv.wal.dir = ctdir();
    int port = startsrv_durable();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");

    snd(fd, "delete 1\r\nreserve\r\n");
    ck(fd, "DELETED\r\n");
    ck(fd, "RESERVED 2 1\r\n");
    ck(fd, "b\r\n");

    // The conn must stay fully usable after the deferred-FSM round trip
    // (epoll re-armed 'r' by conn_want_command).
    snd(fd, "delete 2\r\n");
    ck(fd, "DELETED\r\n");

    close(fd);
}

// Commands pipelined BEHIND a deferred job reply. The defer halts
// h_conn's dispatch loop at the reserve (STATE_SEND_JOB), leaving
// "delete 2\r\nreserve\r\n" unparsed in c->cmd. No further client bytes
// ever arrive, so a level-triggered 'r' wake-up cannot save a broken
// resume: only the SEND_JOB FSM's conn_want_command path can revive the
// parser. A regression there deadlocks this test at the 4th reply. Two
// full defer cycles in one burst also prove the conn re-enters the
// batch cleanly after a deferred round.
void
cttest_dur_deferred_job_reply_resumes_pipeline()
{
    srv.wal.dir = ctdir();
    int port = startsrv_durable();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 60 1\r\nc\r\n");
    ck(fd, "INSERTED 3\r\n");

    // One burst: delete 1 (ack deferred) + reserve (job 2, reply
    // deferred, pipeline halted) + delete 2 (reserved by this conn —
    // legal; must be parsed only after the deferred FSM drains) +
    // reserve (job 3, second defer cycle).
    snd(fd, "delete 1\r\nreserve\r\ndelete 2\r\nreserve\r\n");
    ck(fd, "DELETED\r\n");
    ck(fd, "RESERVED 2 1\r\n");
    ck(fd, "b\r\n");
    ck(fd, "DELETED\r\n");
    ck(fd, "RESERVED 3 1\r\n");
    ck(fd, "c\r\n");

    close(fd);
}

/* ============================================================
 * ANGRY TESTS — prot.c command surface (which_cmd, dispatch_cmd,
 * do_stats/fmt_stats*, read_* parsers, is_valid_tube, reply_*,
 * enqueue_incoming_job, _skip/fill_extra_data, conn_timeout).
 *
 * Contract sources: doc/protocol.txt and README.md invariant #13
 * ("strict literal prefix dispatch") / the -I row of the flag table.
 * Nothing here asserts observed behaviour: every expectation is read
 * off one of those two documents.
 * ============================================================ */

/* Reads `nlines` CRLF-terminated reply lines and returns them
 * concatenated, so a whole reply stream can be pinned by ONE
 * comparison instead of a chain of per-line assertions. */
static char *
agy_lines(int fd, int nlines)
{
    static char out[16384];
    size_t o = 0;
    int i;

    out[0] = '\0';
    for (i = 0; i < nlines; i++) {
        char *l = rd(fd);
        size_t n = strlen(l);
        if (o + n + 1 < sizeof out) {
            memcpy(out + o, l, n);
            o += n;
            out[o] = '\0';
        }
    }
    return out;
}

/* Reads exactly n bytes (or fewer on timeout/EOF); returns the count.
 * Needed wherever the payload may contain CRLF or NUL and the
 * line-oriented rd() would stop early or truncate. */
static int
agy_readn(int fd, char *out, int n)
{
    int total = 0;

    while (total < n) {
        fd_set rfd;
        struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        if (select(fd + 1, &rfd, NULL, NULL, &tv) <= 0)
            break;
        ssize_t r = read(fd, out + total, (size_t)(n - total));
        if (r <= 0)
            break;
        total += (int)r;
    }
    return total;
}

/* Writes n raw bytes; unlike snd() it does not stop at a NUL. */
static void
agy_write(int fd, const char *b, int n)
{
    while (n > 0) {
        ssize_t w = write(fd, b, (size_t)n);
        if (w <= 0) { perror("write"); exit(1); }
        b += w;
        n -= (int)w;
    }
}

/* Sends cmd, then reads the "OK <n>\r\n" line, exactly n body bytes and
 * the two bytes that must follow them. Returns n, or -1 when the reply
 * was not an OK line (the line is then copied into body so the failure
 * message can show it). tail must hold 3 bytes. */
static int
agy_okbody(int fd, char *cmd, char *body, int cap, char *tail)
{
    int n, got;
    char *line;

    snd(fd, cmd);
    line = rd(fd);
    if (strncmp(line, "OK ", 3) != 0) {
        snprintf(body, (size_t)cap, "%s", line);
        return -1;
    }
    n = atoi(line + 3);
    if (n < 0 || n >= cap)
        return -1;
    got = agy_readn(fd, body, n);
    body[got] = '\0';
    if (got != n)
        return -1;
    if (tail) {
        int t = agy_readn(fd, tail, 2);
        tail[t < 0 ? 0 : t] = '\0';
    }
    return n;
}

/* Decimal value of a "<key>: <n>" line in a YAML stats body, or -1 when
 * the key is absent. The "\n" prefix keeps "cmd-list-tubes" from
 * matching the "cmd-list-tubes-watched" line. */
static long long
agy_statfield(const char *body, const char *key)
{
    char pat[96];
    const char *p;

    snprintf(pat, sizeof pat, "\n%s: ", key);
    p = strstr(body, pat);
    if (!p)
        return -1;
    return strtoll(p + strlen(pat), NULL, 10);
}


/* 1 when the peer has closed within to_ms, 0 when it is still open or
 * still talking. */
static int
agy_saw_eof(int fd, int to_ms)
{
    fd_set rfd;
    struct timeval tv = { .tv_sec = to_ms / 1000,
                          .tv_usec = (to_ms % 1000) * 1000 };
    char b[64];

    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    if (select(fd + 1, &rfd, NULL, NULL, &tv) <= 0)
        return 0;
    return read(fd, b, sizeof b) == 0;
}

/* Reads one CRLF-terminated line within to_ms, or returns "" when the
 * server said nothing at all. Unlike rd() this reports silence as a
 * value the assertion can name, instead of aborting the process with a
 * bare "timeout" — a reply that never arrives is itself the finding in
 * the tests below. */
static char *
agy_line_or_silence(int fd, int to_ms)
{
    static char line[8192];
    size_t i = 0;
    char c = 0, prev = 0;

    line[0] = '\0';
    for (;;) {
        fd_set rfd;
        struct timeval tv = { .tv_sec = to_ms / 1000,
                              .tv_usec = (to_ms % 1000) * 1000 };
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        if (select(fd + 1, &rfd, NULL, NULL, &tv) <= 0)
            break;
        if (read(fd, &c, 1) <= 0)
            break;
        if (i < sizeof line - 1)
            line[i++] = c;
        if (prev == '\r' && c == '\n')
            break;
        prev = c;
    }
    line[i] = '\0';
    return line;
}

/* Invariant #2 (doc/invariants.md): the global and per-tube reserved
 * counters move together. Nothing recomputes them, so a single missed
 * pair is permanent — `stats` reports reservations that do not exist
 * for the rest of the process's life. This walks a job through every
 * transition that touches the pair (reserve, release, reserve again,
 * bury, kick, reserve, delete) across two tubes and checks the sum
 * after each one. */
void
cttest_reserved_counters_stay_paired_across_every_transition()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    // One job, in alpha. beta is watched but stays empty, so the sum
    // below has a second term that must remain 0 — a global counter
    // moved without its tube's shows up as either side drifting.
    snd(fd, "use alpha\r\n");     ck(fd, "USING alpha\r\n");
    snd(fd, "put 0 0 120 1\r\na\r\n"); ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch alpha\r\n");   ck(fd, "WATCHING 2\r\n");
    snd(fd, "watch beta\r\n");    ck(fd, "WATCHING 3\r\n");
    snd(fd, "ignore default\r\n"); ck(fd, "WATCHING 2\r\n");

    // After each step: global current-jobs-reserved must equal the sum
    // of the two tubes' own counts.
    struct { const char *what; const char *cmd; const char *want; } steps[] = {
        { "after the first reserve",  "reserve\r\n",          NULL },
        { "after releasing it",       "release 1 0 0\r\n",    "RELEASED\r\n" },
        { "after reserving again",    "reserve\r\n",          NULL },
        { "after burying",            "bury 1 0\r\n",         "BURIED\r\n" },
        { "after kicking it back",    "kick 1\r\n",           "KICKED 1\r\n" },
        { "after the last reserve",   "reserve\r\n",          NULL },
        { "after deleting",           "delete 1\r\n",         "DELETED\r\n" },
    };

    for (size_t i = 0; i < sizeof steps / sizeof *steps; i++) {
        snd(fd, (char *)steps[i].cmd);
        if (steps[i].want) {
            ck(fd, (char *)steps[i].want);
        } else {
            cksub(fd, "RESERVED ");
            rd(fd);                 // body
        }

        int n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
        assertf(n > 0, "setup: stats must answer %s", steps[i].what);
        long long total = agy_statfield(body, "current-jobs-reserved");

        n = agy_okbody(fd, "stats-tube alpha\r\n", body, sizeof body, tail);
        assertf(n > 0, "setup: stats-tube alpha must answer");
        long long a = agy_statfield(body, "current-jobs-reserved");

        n = agy_okbody(fd, "stats-tube beta\r\n", body, sizeof body, tail);
        assertf(n > 0, "setup: stats-tube beta must answer");
        long long b = agy_statfield(body, "current-jobs-reserved");

        assertf(total == a + b,
                "%s: the global reserved count must be the sum of the "
                "tubes' (global=%lld, alpha=%lld, beta=%lld)",
                steps[i].what, total, a, b);
    }
}


/* protocol.txt: a put whose body exceeds max-job-size gets JOB_TOO_BIG.
 * "Exceeds" is the whole question — the limit itself must be usable, or
 * every client that sizes its payload to the documented maximum is
 * rejected, and a limit one byte lower than advertised is a limit
 * nobody can discover except by being refused. Pin both sides. */
void
cttest_a_body_of_exactly_max_job_size_is_accepted_and_one_more_is_not()
{
    job_data_size_limit = 64;          // small enough to write by hand
    int port = startsrv();
    int fd = diallocal(port);

    static char cmd[128], body[128];
    memset(body, 'x', 64);

    int n = snprintf(cmd, sizeof cmd, "put 0 0 120 64\r\n");
    assertf(n > 0, "setup: command did not fit");
    snd(fd, cmd);
    memcpy(cmd, body, 64);
    cmd[64] = '\r'; cmd[65] = '\n'; cmd[66] = 0;
    snd(fd, cmd);
    ck(fd, "INSERTED 1\r\n");

    // One byte more is over the line.
    snd(fd, "put 0 0 120 65\r\n");
    memcpy(cmd, body, 64);
    cmd[64] = 'x'; cmd[65] = '\r'; cmd[66] = '\n'; cmd[67] = 0;
    snd(fd, cmd);
    ck(fd, "JOB_TOO_BIG\r\n");

    // The refusal consumed exactly the body it announced: the next
    // command is read as a command, not as leftovers.
    snd(fd, "stats-job 1\r\n");
    cksub(fd, "OK ");
    rd(fd);
}

/* protocol.txt documents a cmd-<name> counter for every command, and
 * each is "the cumulative number of <name> commands". A command that
 * forgets to bump its own counter leaves a field permanently at zero —
 * a monitoring dashboard reading it sees a command nobody ever issues.
 * Nothing checked the set as a whole, so this drives every verb once
 * and then insists no cmd-* field is still zero. */
void
cttest_every_command_counts_itself_in_stats()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    char cmd[128];

    snd(fd, "use ctr\r\n");                ck(fd, "USING ctr\r\n");
    snd(fd, "put 0 0 120 1\r\na\r\n");     ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch ctr\r\n");              ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");         ck(fd, "WATCHING 1\r\n");
    snd(fd, "reserve\r\n");                cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "touch 1\r\n");                ck(fd, "TOUCHED\r\n");
    snd(fd, "release 1 0 0\r\n");          ck(fd, "RELEASED\r\n");
    snd(fd, "reserve-with-timeout 1\r\n"); cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "bury 1 0\r\n");               ck(fd, "BURIED\r\n");
    snd(fd, "kick-job 1\r\n");             ck(fd, "KICKED\r\n");
    snd(fd, "reserve\r\n");                cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "bury 1 0\r\n");               ck(fd, "BURIED\r\n");
    snd(fd, "kick 1\r\n");                 ck(fd, "KICKED 1\r\n");
    snd(fd, "peek 1\r\n");                 cksub(fd, "FOUND 1 "); rd(fd);
    snd(fd, "reserve-job 1\r\n");          cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "release 1 0 0\r\n");          ck(fd, "RELEASED\r\n");

    // peek-* answer FOUND + body, not OK + YAML.
    snd(fd, "peek-ready\r\n");             cksub(fd, "FOUND 1 "); rd(fd);
    snd(fd, "put 0 60 120 1\r\nb\r\n");   ck(fd, "INSERTED 2\r\n");
    snd(fd, "peek-delayed\r\n");           cksub(fd, "FOUND 2 "); rd(fd);
    snd(fd, "reserve\r\n");                cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "bury 1 0\r\n");               ck(fd, "BURIED\r\n");
    snd(fd, "peek-buried\r\n");            cksub(fd, "FOUND 1 "); rd(fd);
    assertf(agy_okbody(fd, "list-tubes\r\n", body, sizeof body, tail) > 0,
            "setup: list-tubes must answer");
    snd(fd, "list-tube-used\r\n");        ck(fd, "USING ctr\r\n");
    assertf(agy_okbody(fd, "list-tubes-watched\r\n", body, sizeof body, tail) > 0,
            "setup: list-tubes-watched must answer");
    int n = snprintf(cmd, sizeof cmd, "stats-job 1\r\n");
    assertf(n > 0 && agy_okbody(fd, cmd, body, sizeof body, tail) > 0,
            "setup: stats-job must answer");
    assertf(agy_okbody(fd, "stats-tube ctr\r\n", body, sizeof body, tail) > 0,
            "setup: stats-tube must answer");
    snd(fd, "pause-tube ctr 0\r\n");       ck(fd, "PAUSED\r\n");
    snd(fd, "kick 1\r\n");                 ck(fd, "KICKED 1\r\n");
    snd(fd, "delete 1\r\n");               ck(fd, "DELETED\r\n");

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats must answer");

    // Every cmd-* line must now be above zero. The names come from the
    // reply itself, so a command added later is covered the moment it
    // reports a counter.
    const char *p = body;
    int checked = 0;
    while ((p = strstr(p, "cmd-")) != NULL) {
        char name[64];
        const char *colon = strchr(p, ':');
        assertf(colon != NULL, "malformed stats line at: %.20s", p);
        size_t len = (size_t)(colon - p);
        assertf(len < sizeof name, "stats field name too long");
        memcpy(name, p, len);
        name[len] = 0;
        long long v = agy_statfield(body, name);
        assertf(v > 0,
                "%s is still 0 after every command was issued: either the "
                "command does not count itself, or this test does not "
                "issue it", name);
        checked++;
        p = colon;
    }
    assertf(checked >= 20,
            "stats must report a counter for every command; only %d "
            "cmd-* fields were found", checked);
}

/* protocol.txt: "Tubes are created on demand whenever they are
 * referenced. If a tube is empty ... and no client refers to it, it
 * will be deleted." A server that keeps every tube a client ever named
 * leaks one Tube (640 bytes plus its heaps and waiting set) per name —
 * and job ids are visible to clients, so tube names often are too:
 * per-customer or per-day names are ordinary usage. Walk a tube through
 * every way it can be referenced and check it goes away when the last
 * reference does. */
void
cttest_an_empty_unreferenced_tube_is_reclaimed()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    int n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats must answer");
    long long base = agy_statfield(body, "current-tubes");
    assertf(base >= 1, "setup: the default tube must exist, got %lld", base);

    // Referenced by `use`: it exists while it is the used tube.
    snd(fd, "use ephemeral\r\n");
    ck(fd, "USING ephemeral\r\n");
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-tubes") == base + 1,
            "a used tube must exist: current-tubes is %lld, want %lld",
            agy_statfield(body, "current-tubes"), base + 1);

    // Point the connection elsewhere: nothing refers to it any more and
    // it holds no jobs, so it must go.
    snd(fd, "use default\r\n");
    ck(fd, "USING default\r\n");
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-tubes") == base,
            "an empty tube nobody uses must be reclaimed: current-tubes "
            "is %lld, want %lld", agy_statfield(body, "current-tubes"), base);

    // A tube holding a job stays, even with nobody using or watching it.
    snd(fd, "use holder\r\n");            ck(fd, "USING holder\r\n");
    snd(fd, "put 0 0 3600 1\r\nx\r\n");   ck(fd, "INSERTED 1\r\n");
    snd(fd, "use default\r\n");           ck(fd, "USING default\r\n");
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-tubes") == base + 1,
            "a tube holding a ready job must survive: current-tubes is %lld",
            agy_statfield(body, "current-tubes"));

    // Take the job away and it goes with it.
    snd(fd, "watch holder\r\n");          ck(fd, "WATCHING 2\r\n");
    snd(fd, "reserve\r\n");               cksub(fd, "RESERVED 1 "); rd(fd);
    snd(fd, "delete 1\r\n");              ck(fd, "DELETED\r\n");
    snd(fd, "ignore holder\r\n");         ck(fd, "WATCHING 1\r\n");
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-tubes") == base,
            "once the job is deleted and the tube unwatched it must be "
            "reclaimed: current-tubes is %lld, want %lld",
            agy_statfield(body, "current-tubes"), base);
}

/* protocol.txt: pause-tube "can delay any new job being reserved for a
 * given time", and stats-tube reports `pause` and `pause-time-left`. A
 * paused tube is not an empty one: its jobs stay ready and stay
 * counted, they are simply not handed out. A client watching
 * current-jobs-ready to decide whether to scale workers must not be
 * told the queue drained when it was only paused. */
void
cttest_a_paused_tube_still_reports_its_ready_jobs()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    snd(fd, "use paused\r\n");            ck(fd, "USING paused\r\n");
    snd(fd, "put 0 0 3600 1\r\nx\r\n");   ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch paused\r\n");          ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");        ck(fd, "WATCHING 1\r\n");

    snd(fd, "pause-tube paused 30\r\n");  ck(fd, "PAUSED\r\n");

    int n = agy_okbody(fd, "stats-tube paused\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats-tube must answer");

    assertf(agy_statfield(body, "current-jobs-ready") == 1,
            "a paused tube keeps its ready jobs: current-jobs-ready is %lld",
            agy_statfield(body, "current-jobs-ready"));
    assertf(agy_statfield(body, "pause") == 30,
            "the tube must report the pause it was given, got %lld",
            agy_statfield(body, "pause"));
    long long left = agy_statfield(body, "pause-time-left");
    assertf(left > 0 && left <= 30,
            "pause-time-left must be a remaining slice of the pause, got "
            "%lld", left);

    // And the job really is withheld.
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");

    // Lifting the pause hands it over immediately.
    snd(fd, "pause-tube paused 0\r\n");   ck(fd, "PAUSED\r\n");
    snd(fd, "reserve-with-timeout 1\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);

    n = agy_okbody(fd, "stats-tube paused\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "pause-time-left") == 0,
            "a lifted pause must report no time left, got %lld",
            agy_statfield(body, "pause-time-left"));
}

/* protocol.txt: touch "allows a worker to request more time to work on
 * a job ... it will reset the time left for the job to run to its
 * original TTR". Existing tests only cover the ways touch can be
 * refused (BAD_FORMAT, NOT_FOUND) — the thing it exists to do was
 * never checked. A touch that answered TOUCHED without moving the
 * deadline loses the job to a TTR timeout while a worker is still
 * running it, which is exactly the failure the command prevents. */
void
cttest_touch_resets_the_time_left_to_the_full_ttr()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    snd(fd, "put 0 0 4 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);

    // Let a visible slice of the TTR go by.
    usleep(2100000);

    int n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats-job must answer");
    long long before = agy_statfield(body, "time-left");
    assertf(before <= 2,
            "setup: about half the 4s TTR should have gone, time-left is "
            "%lld", before);

    snd(fd, "touch 1\r\n");
    ck(fd, "TOUCHED\r\n");

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: the second stats-job must answer");
    long long after = agy_statfield(body, "time-left");

    assertf(after > before,
            "touch must move the deadline out: time-left went %lld -> %lld",
            before, after);
    assertf(after >= 3,
            "touch resets to the original TTR (4s), so time-left must be "
            "back near it, got %lld", after);

    // And the job is still this client's: the reservation was extended,
    // not replaced.
    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "reserves") == 1,
            "touch must not count as a new reservation, reserves is %lld",
            agy_statfield(body, "reserves"));
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
}

/* protocol.txt (stats-job): "timeouts" is the number of times this job
 * has timed out during a reservation, and a job whose TTR elapses goes
 * back to the ready queue. Both halves matter to a worker: the job must
 * become available again, and the counter is how an operator sees jobs
 * that keep being abandoned. Neither was checked — `timeouts` appears
 * nowhere in the suite, and the global `job-timeouts` alongside it is
 * the same number aggregated. */
void
cttest_a_lapsed_ttr_requeues_the_job_and_counts_the_timeout()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    snd(fd, "put 0 0 1 1\r\nx\r\n");     // 1s TTR
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);

    int n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "timeouts") == 0,
            "setup: a fresh reservation has not timed out yet, got %lld",
            agy_statfield(body, "timeouts"));

    // Walk away for longer than the TTR without touching or deleting.
    usleep(1600000);

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats-job must answer after the TTR lapsed");
    assertf(agy_statfield(body, "timeouts") == 1,
            "a lapsed TTR must count exactly one timeout for the job, got "
            "%lld", agy_statfield(body, "timeouts"));

    const char *state = strstr(body, "\nstate: ");
    assertf(state && strncmp(state, "\nstate: ready\n", 14) == 0,
            "a job whose TTR lapsed must be back in the ready queue");

    // The global counter is the same event, aggregated.
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "job-timeouts") >= 1,
            "the server-wide job-timeouts must have counted it, got %lld",
            agy_statfield(body, "job-timeouts"));

    // ...and it really is reservable again, by anyone.
    snd(fd, "reserve-with-timeout 1\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);
}


/* protocol.txt (kick): "applies only to the currently used tube". The
 * buried-before-delayed half of that sentence is covered by
 * cttest_kick_moves_only_buried_jobs_while_any_are_buried and its
 * delayed counterpart; the tube SCOPE was not. kick is how an operator
 * drains a bury queue after fixing whatever caused it, and reaching
 * into another tube would put work back in flight that nobody asked
 * for — in a tube whose owner is not even connected. */
void
cttest_kick_reaches_only_the_used_tube()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    // Tube A: one buried job and one delayed job.
    snd(fd, "use ka\r\n");                ck(fd, "USING ka\r\n");
    snd(fd, "put 0 0 3600 1\r\nb\r\n");  ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 300 3600 1\r\nd\r\n"); ck(fd, "INSERTED 2\r\n");
    snd(fd, "watch ka\r\n");              ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");        ck(fd, "WATCHING 1\r\n");
    snd(fd, "reserve\r\n");               cksub(fd, "RESERVED 1 1\r\n"); rd(fd);
    snd(fd, "bury 1 0\r\n");              ck(fd, "BURIED\r\n");

    // Tube B: a buried job that must not be touched from tube A.
    snd(fd, "use kb\r\n");                ck(fd, "USING kb\r\n");
    snd(fd, "put 0 0 3600 1\r\nc\r\n");  ck(fd, "INSERTED 3\r\n");
    snd(fd, "watch kb\r\n");              ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore ka\r\n");             ck(fd, "WATCHING 1\r\n");
    snd(fd, "reserve\r\n");               cksub(fd, "RESERVED 3 1\r\n"); rd(fd);
    snd(fd, "bury 3 0\r\n");              ck(fd, "BURIED\r\n");

    // Kick from A with a bound of 10: exactly the one buried job in A.
    // Not the delayed job beside it, and nothing from B.
    snd(fd, "use ka\r\n");                ck(fd, "USING ka\r\n");
    snd(fd, "kick 10\r\n");               ck(fd, "KICKED 1\r\n");

    int n = agy_okbody(fd, "stats-tube ka\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats-tube ka must answer");
    assertf(agy_statfield(body, "current-jobs-buried") == 0,
            "the buried job in the used tube must have been kicked, %lld left",
            agy_statfield(body, "current-jobs-buried"));
    assertf(agy_statfield(body, "current-jobs-delayed") == 1,
            "the delayed job must be untouched while a buried one existed, "
            "delayed is %lld", agy_statfield(body, "current-jobs-delayed"));

    n = agy_okbody(fd, "stats-tube kb\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-jobs-buried") == 1,
            "kick must not reach another tube: kb has %lld buried",
            agy_statfield(body, "current-jobs-buried"));

    // With the bury queue empty, the same command takes the delayed job.
    snd(fd, "kick 10\r\n");               ck(fd, "KICKED 1\r\n");
    n = agy_okbody(fd, "stats-tube ka\r\n", body, sizeof body, tail);
    assertf(n > 0 && agy_statfield(body, "current-jobs-delayed") == 0
            && agy_statfield(body, "current-jobs-ready") == 2,
            "with nothing buried, kick takes delayed jobs: delayed %lld, "
            "ready %lld", agy_statfield(body, "current-jobs-delayed"),
            agy_statfield(body, "current-jobs-ready"));

    // And an empty tube kicks nothing.
    snd(fd, "kick 10\r\n");               ck(fd, "KICKED 0\r\n");
}

/* protocol.txt (release): "<delay> is an integer number of seconds to
 * wait before putting the job in the ready queue. The job will be in
 * the 'delayed' state during this time." This is the retry-with-backoff
 * that every worker library builds on — release with a growing delay
 * after a transient failure. A release that ignored the delay would
 * hand the job straight back to the same worker that just failed it,
 * turning a backoff into a spin. Existing tests cover release with
 * delay 0 and the NOT_FOUND path; the delay itself was untested. */
void
cttest_release_with_a_delay_holds_the_job_back()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    snd(fd, "put 0 0 3600 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);

    snd(fd, "release 1 7 2\r\n");         // new priority 7, 2s delay
    ck(fd, "RELEASED\r\n");

    int n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats-job must answer");
    const char *state = strstr(body, "\nstate: ");
    assertf(state && strncmp(state, "\nstate: delayed\n", 16) == 0,
            "a released job with a delay must be delayed, not ready");
    assertf(agy_statfield(body, "pri") == 7,
            "release must apply the new priority, got %lld",
            agy_statfield(body, "pri"));
    long long left = agy_statfield(body, "time-left");
    assertf(left > 0 && left <= 2,
            "time-left must be the remaining delay, got %lld", left);

    // It is genuinely withheld...
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");

    // ...and it does arrive once the delay is up.
    snd(fd, "reserve-with-timeout 4\r\n");
    cksub(fd, "RESERVED 1 1\r\n");
    rd(fd);
}

/* protocol.txt (peek): all but `peek <id>` "operate only on the
 * currently used tube", and each names WHICH job it returns —
 * peek-ready the next ready one, peek-delayed the one with the shortest
 * delay left, peek-buried the next in the buried list. peek is how an
 * operator looks at a queue they cannot drain; a peek that answers from
 * the wrong tube, or shows the wrong job, is worse than one that says
 * NOT_FOUND, because it is believed. */
void
cttest_peek_variants_pick_the_right_job_from_the_used_tube()
{
    int port = startsrv();
    int fd = diallocal(port);

    // Another tube, holding a job in each state, that no peek below
    // may ever return.
    snd(fd, "use other\r\n");             ck(fd, "USING other\r\n");
    snd(fd, "put 0 0 3600 1\r\nO\r\n");  ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 1 3600 1\r\nP\r\n");  ck(fd, "INSERTED 2\r\n");
    snd(fd, "watch other\r\n");           ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");        ck(fd, "WATCHING 1\r\n");
    snd(fd, "reserve\r\n");               cksub(fd, "RESERVED 1 1\r\n"); rd(fd);
    snd(fd, "bury 1 0\r\n");              ck(fd, "BURIED\r\n");

    // The tube under test.
    snd(fd, "use mine\r\n");              ck(fd, "USING mine\r\n");
    snd(fd, "put 50 0 3600 1\r\nb\r\n");  ck(fd, "INSERTED 3\r\n"); // ready, pri 50
    snd(fd, "put 10 0 3600 1\r\na\r\n");  ck(fd, "INSERTED 4\r\n"); // ready, pri 10
    snd(fd, "put 0 60 3600 1\r\nl\r\n");  ck(fd, "INSERTED 5\r\n"); // delayed 60s
    snd(fd, "put 0 5 3600 1\r\ns\r\n");   ck(fd, "INSERTED 6\r\n"); // delayed 5s
    snd(fd, "watch mine\r\n");            ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore other\r\n");          ck(fd, "WATCHING 1\r\n");
    snd(fd, "reserve\r\n");               cksub(fd, "RESERVED 4 1\r\n"); rd(fd);
    snd(fd, "bury 4 0\r\n");              ck(fd, "BURIED\r\n");

    // peek-ready: the lowest priority among what is ready HERE (job 3;
    // job 4 was the lower priority but is now buried, and job 1 is in
    // the other tube).
    snd(fd, "peek-ready\r\n");
    ck(fd, "FOUND 3 1\r\n");
    ck(fd, "b\r\n");

    // peek-delayed: the shortest delay left, which is job 6 at 5s, not
    // job 5 at 60s and not job 2 in the other tube.
    snd(fd, "peek-delayed\r\n");
    ck(fd, "FOUND 6 1\r\n");
    ck(fd, "s\r\n");

    // peek-buried: this tube's buried job, not the other tube's.
    snd(fd, "peek-buried\r\n");
    ck(fd, "FOUND 4 1\r\n");
    ck(fd, "a\r\n");

    // peek <id> is the exception: it reaches any tube.
    snd(fd, "peek 1\r\n");
    ck(fd, "FOUND 1 1\r\n");
    ck(fd, "O\r\n");

    // A tube with nothing in a state says so rather than borrowing.
    snd(fd, "use empty\r\n");             ck(fd, "USING empty\r\n");
    snd(fd, "peek-ready\r\n");            ck(fd, "NOT_FOUND\r\n");
    snd(fd, "peek-delayed\r\n");          ck(fd, "NOT_FOUND\r\n");
    snd(fd, "peek-buried\r\n");           ck(fd, "NOT_FOUND\r\n");
}

/* Invariant #1 (doc/invariants.md): a job is on exactly one of the four
 * structures. `stats` reports each of them separately, so the four
 * counts must add up to the jobs that exist — no more (a job on two
 * lists is counted twice and will be delivered twice) and no fewer (a
 * job on none leaks, and nothing will ever hand it out). Drive a mixed
 * population through the transitions and check the sum after each. */
void
cttest_job_state_counts_always_add_up_to_the_jobs_that_exist()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];

    // 2 ready, 1 delayed, and one that will move around.
    snd(fd, "put 0 0 120 1\r\nr\r\n");   ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 120 1\r\ns\r\n");   ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 30 120 1\r\nd\r\n");  ck(fd, "INSERTED 3\r\n");
    snd(fd, "put 0 0 120 1\r\nm\r\n");   ck(fd, "INSERTED 4\r\n");

    const char *steps[][2] = {
        { "with everything just put", NULL },
        { "with one reserved",   "reserve\r\n" },
        { "with it buried",      "bury 1 0\r\n" },
        { "with it kicked back", "kick 1\r\n" },
        { "with another reserved", "reserve\r\n" },
    };
    int live = 4;

    for (size_t i = 0; i < sizeof steps / sizeof *steps; i++) {
        if (steps[i][1]) {
            snd(fd, (char *)steps[i][1]);
            if (steps[i][1][0] == 'r') { cksub(fd, "RESERVED "); rd(fd); }
            else if (steps[i][1][0] == 'b') ck(fd, "BURIED\r\n");
            else ck(fd, "KICKED 1\r\n");
        }

        int n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
        assertf(n > 0, "setup: stats must answer %s", steps[i][0]);
        long long ready    = agy_statfield(body, "current-jobs-ready");
        long long reserved = agy_statfield(body, "current-jobs-reserved");
        long long delayed  = agy_statfield(body, "current-jobs-delayed");
        long long buried   = agy_statfield(body, "current-jobs-buried");

        assertf(ready + reserved + delayed + buried == live,
                "%s: the four state counts must add up to the %d jobs that "
                "exist (ready=%lld reserved=%lld delayed=%lld buried=%lld)",
                steps[i][0], live, ready, reserved, delayed, buried);
    }

    // Deleting removes a job from exactly one place, so the total drops
    // by exactly one.
    snd(fd, "delete 3\r\n");
    ck(fd, "DELETED\r\n");
    live--;

    int n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats must answer after the delete");
    assertf(agy_statfield(body, "current-jobs-ready")
            + agy_statfield(body, "current-jobs-reserved")
            + agy_statfield(body, "current-jobs-delayed")
            + agy_statfield(body, "current-jobs-buried") == live,
            "deleting the delayed job must drop the total to %d", live);
}

/* enqueue_job puts the job on a heap and THEN writes the WAL record, so
 * a failed write has to undo the insert — otherwise the job is on the
 * ready heap while the caller believes the enqueue failed, and the
 * counters that follow it (current-jobs-ready, current-jobs-urgent)
 * describe a queue nobody can drain. The rollback is only reachable
 * when the WAL write fails, which is what the injection produces.
 *
 * FAULT_WRITEV is safe to arm before the fork: this process talks to
 * the server with write(), so only the server spends its copy — on the
 * binlog record for this very put. */
void
cttest_wal_write_failure_on_put_leaves_no_phantom_ready_job()
{
    srv.wal.dir = ctdir();
    fault_set(FAULT_WRITEV, 0, EIO);
    int port = startsrv_wal();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 3\r\nabc\r\n");
    char *reply = agy_line_or_silence(fd, 3000);
    assertf(reply[0] != 0,
            "a put whose WAL record could not be written must still be "
            "answered, got silence");

    // Whatever the verdict was, the queue must agree with it: a job the
    // server refused cannot be sitting in the ready count, and one it
    // accepted (buried) cannot be there either.
    char body[8192], tail[4];
    int n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    assertf(n > 0, "setup: stats must answer");
    long long ready = agy_statfield(body, "current-jobs-ready");
    assertf(ready == 0,
            "the put was answered [%s], so no job may be left on the ready "
            "heap; current-jobs-ready is %lld", reply, ready);

    // And the connection is still in step: the next command gets its
    // own reply, not leftovers from the stats body.
    snd(fd, "list-tube-used\r\n");
    ck(fd, "USING default\r\n");
    fault_clear_all();
}

/* Delivers s one byte per write() so the server sees the worst possible
 * TCP framing. Kept out of the test body: feeding bytes is mechanics,
 * not test logic. */
static void
agy_write_bytewise(int fd, const char *s)
{
    size_t i, n = strlen(s);

    for (i = 0; i < n; i++) {
        agy_write(fd, s + i, 1);
        usleep(1000);
    }
}

/* Fills buf with the 256 distinct byte values followed by CRLF. */
static void
agy_all_byte_values(char *buf)
{
    int i;

    for (i = 0; i < 256; i++)
        buf[i] = (char)i;
    buf[256] = '\r';
    buf[257] = '\n';
}

/* Builds a pipelined burst of n one-byte puts into cmds and the acks
 * they must produce into want; returns the expected byte count. */
static size_t
agy_put_burst(char *cmds, size_t cmdcap, char *want, size_t wantcap, int n)
{
    size_t c = 0, w = 0;
    int i;

    for (i = 1; i <= n; i++) {
        c += (size_t)snprintf(cmds + c, cmdcap - c, "put 0 0 60 1\r\nx\r\n");
        w += (size_t)snprintf(want + w, wantcap - w, "INSERTED %d\r\n", i);
    }
    return w;
}


/* ---------- which_cmd: strict literal prefix (README invariant #13) ---------- */

/* "stats-job " is a literal, so the byte at offset 9 must be a space.
 * which_cmd only checks cmd[5]=='-' && cmd[6]=='j', and read_u64 then
 * starts at cmd+10 — so a junk separator reaches straight into the
 * stats-job namespace and hands out the job's stats block. */
void
cttest_which_cmd_stats_job_junk_separator_serves_no_job_stats()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "stats-jobZ1\r\n");
    reply = rd(fd);

    assertf(strncmp(reply, "OK ", 3) != 0,
            "stats-jobZ1 is not the literal \"stats-job \" verb, so it must"
            " not be dispatched into stats-job's namespace; server answered"
            " with a stats body: [%s]", reply);
}

/* Same leak one branch over: "stats-tube " is a literal, the name starts
 * at cmd+11, and offset 10 is unchecked — "stats-tubeZdefault" returns
 * the default tube's stats block. */
void
cttest_which_cmd_stats_tube_junk_separator_serves_no_tube_stats()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "stats-tubeZdefault\r\n");
    reply = rd(fd);

    assertf(strncmp(reply, "OK ", 3) != 0,
            "stats-tubeZdefault is not the literal \"stats-tube \" verb and"
            " must not be dispatched into stats-tube's namespace; server"
            " answered with a stats body: [%s]", reply);
}

/* CMD_PAUSE_TUBE is the only command literal without a trailing
 * separator, so the 'a' arm is a bare 10-byte strncmp and
 * "pause-tubeNAME <n>" pauses NAME. Asserted through the effect a
 * client depends on (protocol.txt: a paused tube delays reserves), so
 * the test does not depend on which error string the rejection uses. */
void
cttest_which_cmd_pause_tube_junk_separator_leaves_tube_unpaused()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use pausejunk\r\n");
    ck(fd, "USING pausejunk\r\n");
    snd(fd, "watch pausejunk\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "pause-tubepausejunk 30\r\n");
    rd(fd);

    snd(fd, "reserve-with-timeout 1\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "RESERVED 1 1\r\n") == 0,
            "\"pause-tubepausejunk 30\" is not the literal \"pause-tube\""
            " verb followed by a name, so tube pausejunk must still be"
            " servable; reserve answered [%s]", reply);
}

/* README.md:27 documents this exact case: a well-known command line
 * with a trailing space is UNKNOWN_COMMAND in this fork. */
void
cttest_which_cmd_stats_with_trailing_space_is_unknown_command()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "stats \r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "UNKNOWN_COMMAND\r\n") == 0,
            "\"stats \\r\\n\" is documented as UNKNOWN_COMMAND"
            " (README.md, invariant #13), got [%s]", reply);
}

/* The second literal README.md:27 names by example. */
void
cttest_which_cmd_list_tubes_with_trailing_space_is_unknown_command()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "list-tubes \r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "UNKNOWN_COMMAND\r\n") == 0,
            "\"list-tubes \\r\\n\" is documented as UNKNOWN_COMMAND"
            " (README.md, invariant #13), got [%s]", reply);
}

/* The 'r'/'d'/'b'/'u' arms read bytes past the NUL terminator, so their
 * verdict could drift with whatever a previous long command left in
 * c->cmd. Pipelined behind a 22-byte predecessor, four one-letter lines
 * must still all be unknown verbs. */
void
cttest_which_cmd_one_letter_lines_unknown_after_long_predecessor()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *stream;

    snd(fd, "pause-tube default 0\r\nr\r\nd\r\nb\r\nu\r\n");
    stream = agy_lines(fd, 5);

    assertf(strcmp(stream,
                   "PAUSED\r\n"
                   "UNKNOWN_COMMAND\r\n"
                   "UNKNOWN_COMMAND\r\n"
                   "UNKNOWN_COMMAND\r\n"
                   "UNKNOWN_COMMAND\r\n") == 0,
            "one-letter lines are not documented verbs and their verdict"
            " must not depend on command-buffer residue, got [%s]", stream);
}


/* ---------- dispatch_cmd ---------- */

/* protocol.txt:44 — BAD_FORMAT is the answer for a command line that is
 * not well-formed ("the wrong number of arguments are present").
 * dispatch_cmd runs the JOB_TOO_BIG branch first, so a malformed line
 * arms a multi-gigabyte bit-bucket instead. */
void
cttest_dispatch_put_oversize_with_trailing_garbage_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 0 4294967295 trailing-junk\r\n");
    reply = agy_line_or_silence(fd, 3000);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "a put line carrying a 6th argument is not well-formed and"
            " protocol.txt:44 answers BAD_FORMAT; got [%s] (an empty reply"
            " means the connection was parked discarding a 4GB body)",
            reply);
}

/* The JOB_TOO_BIG discard must consume exactly body_size+2 bytes, so
 * the very next byte after the body's CRLF starts a command again. One
 * byte of slip either way executes job data as a command or eats the
 * following command. */
void
cttest_dispatch_job_too_big_discard_ends_after_body_crlf()
{
    enum { over = 65536 };
    static char body[over + 2];
    char first[64], second[64], stream[160];
    int port, fd;

    job_data_size_limit = 65535;
    port = startsrv();
    fd = diallocal(port);
    memset(body, 'z', over);
    memcpy(body + over - 12, "\r\nquit\r\nzzzz", 12);
    body[over] = '\r';
    body[over + 1] = '\n';

    snd(fd, "put 0 0 60 65536\r\n");
    agy_write(fd, body, over + 2);
    snd(fd, "list-tube-used\r\n");
    snprintf(first, sizeof first, "%s", agy_line_or_silence(fd, 3000));
    snprintf(second, sizeof second, "%s", agy_line_or_silence(fd, 3000));
    snprintf(stream, sizeof stream, "%s%s", first, second);

    assertf(strcmp(stream, "JOB_TOO_BIG\r\nUSING default\r\n") == 0,
            "the discard must end exactly at body+CRLF so the next"
            " command is answered and nothing inside the body executes;"
            " got [%s] (a missing second line means one byte of the"
            " command was eaten by the discard)", stream);
}


/* ---------- do_stats / fmt_stats ---------- */

/* protocol.txt: current-jobs-ready "is the number of jobs in the ready
 * queue". The 500ms global-stats cache replays the previous body, so a
 * client that reads stats, changes the queue and reads again is served
 * a number that was already false when it was sent. */
void
cttest_stats_reports_a_job_put_since_the_previous_stats()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    assertf(agy_okbody(fd, "stats\r\n", body, sizeof body, tail) > 0,
            "setup: stats must answer");

    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);

    assertf(n > 0 && agy_statfield(body, "current-jobs-ready") == 1,
            "stats must report the ready queue as it is when the command"
            " is processed; got current-jobs-ready=%lld after one put",
            agy_statfield(body, "current-jobs-ready"));
}

/* protocol.txt: "OK <bytes>\r\n" is followed by exactly <bytes> of data
 * and then \r\n. The second stats reply is built by the cache branch,
 * which computes the length differently from the formatting branch. */
void
cttest_stats_cached_reply_length_matches_its_payload()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    assertf(agy_okbody(fd, "stats\r\n", body, sizeof body, tail) > 0,
            "setup: stats must answer");
    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);

    assertf(n > 0 && (int)strlen(body) == n && strcmp(tail, "\r\n") == 0,
            "the second (cached) stats reply must announce exactly its own"
            " payload length and be followed by CRLF; announced %d, read"
            " %d bytes, trailer [%s]", n, (int)strlen(body), tail);
}

/* protocol.txt lists one cmd-* counter per command. Driving exactly one
 * verb pins the positional mapping between op_ct[OP_*] and the STATS_FMT
 * argument list: a swapped pair moves the count onto a neighbour. */
void
cttest_stats_cmd_counters_do_not_bleed_onto_neighbour_verbs()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "list-tube-used\r\nlist-tube-used\r\nlist-tube-used\r\n");
    agy_lines(fd, 3);

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "cmd-list-tube-used") == 3
            && agy_statfield(body, "cmd-list-tubes") == 0
            && agy_statfield(body, "cmd-list-tubes-watched") == 0,
            "three list-tube-used commands must move only cmd-list-tube-used;"
            " got used=%lld tubes=%lld watched=%lld",
            agy_statfield(body, "cmd-list-tube-used"),
            agy_statfield(body, "cmd-list-tubes"),
            agy_statfield(body, "cmd-list-tubes-watched"));
}

/* protocol.txt: current-jobs-urgent counts jobs with priority < 1024.
 * Both sides of the boundary in one reading pins the strict '<' that
 * enqueue_job and remove_ready_job must agree on. */
void
cttest_stats_urgent_gauge_counts_priority_1023_but_not_1024()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 1023 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 1024 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "current-jobs-urgent") == 1
            && agy_statfield(body, "current-jobs-ready") == 2,
            "priority 1023 is urgent and 1024 is not (URGENT_THRESHOLD is"
            " exclusive); got urgent=%lld ready=%lld",
            agy_statfield(body, "current-jobs-urgent"),
            agy_statfield(body, "current-jobs-ready"));
}

/* Conservation: every gauge protocol.txt defines as a count of live
 * jobs must return to zero once every job is gone, whatever states the
 * jobs passed through. A leaked increment on any transition survives
 * here and nowhere else. */
void
cttest_stats_job_gauges_return_to_zero_after_every_job_is_deleted()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 30 60 1\r\nc\r\n");
    ck(fd, "INSERTED 3\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
    snd(fd, "delete 2\r\n");
    ck(fd, "DELETED\r\n");
    snd(fd, "delete 3\r\n");
    ck(fd, "DELETED\r\n");

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "current-jobs-urgent") == 0
            && agy_statfield(body, "current-jobs-ready") == 0
            && agy_statfield(body, "current-jobs-reserved") == 0
            && agy_statfield(body, "current-jobs-delayed") == 0
            && agy_statfield(body, "current-jobs-buried") == 0
            && agy_statfield(body, "total-jobs") == 3,
            "all five job gauges must be 0 once every job is deleted and"
            " total-jobs must still be 3; urgent=%lld ready=%lld"
            " reserved=%lld delayed=%lld buried=%lld total=%lld",
            agy_statfield(body, "current-jobs-urgent"),
            agy_statfield(body, "current-jobs-ready"),
            agy_statfield(body, "current-jobs-reserved"),
            agy_statfield(body, "current-jobs-delayed"),
            agy_statfield(body, "current-jobs-buried"),
            agy_statfield(body, "total-jobs"));
}

/* uptime is seconds since prot_init. Bracketed by the test's own clock,
 * so a started_at that is zero or unset prints a value near the machine
 * uptime and lands outside the bracket. */
void
cttest_stats_uptime_never_exceeds_the_elapsed_wall_time()
{
    int64 t0 = nanoseconds();
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;
    long long elapsed, up;

    n = agy_okbody(fd, "stats\r\n", body, sizeof body, tail);
    elapsed = (long long)((nanoseconds() - t0) / 1000000000) + 1;
    up = agy_statfield(body, "uptime");

    assertf(n > 0 && up >= 0 && up <= elapsed,
            "uptime is seconds since prot_init, which started at most"
            " %lld seconds ago; got %lld", elapsed, up);
}

/* prot_init renders 8 random bytes as 16 lowercase hex characters into
 * the stats 'id' key. */
void
cttest_stats_instance_id_is_sixteen_lowercase_hex_digits()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    const char *p;
    size_t hex;

    assertf(agy_okbody(fd, "stats\r\n", body, sizeof body, tail) > 0,
            "setup: stats must answer");
    p = strstr(body, "\nid: ");
    hex = p ? strspn(p + 5, "0123456789abcdef") : 0;

    assertf(p && hex == 16 && p[5 + 16] == '\n',
            "stats id must be exactly 16 lowercase hex characters,"
            " got %d hex digits in [%.40s]", (int)hex, p ? p : "(absent)");
}


/* ---------- fmt_stats_tube ---------- */

/* protocol.txt: pause-time-left is "the number of seconds until the tube
 * is un-paused". Once the pause has elapsed the only legal answer is 0 —
 * a subtraction that is allowed to go negative prints an astronomically
 * large unsigned value here (the named wraparound regression). */
void
cttest_stats_tube_pause_time_left_is_zero_once_the_pause_elapsed()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "pause-tube default 1\r\n");
    ck(fd, "PAUSED\r\n");
    usleep(1400000);

    n = agy_okbody(fd, "stats-tube default\r\n", body, sizeof body, tail);

    assertf(n > 0 && agy_statfield(body, "pause-time-left") == 0,
            "1.4s after a 1-second pause the tube is un-paused, so"
            " pause-time-left must be 0, got %lld",
            agy_statfield(body, "pause-time-left"));
}

/* protocol.txt: pause is the number of seconds the tube has been paused
 * for and pause-time-left counts down to the un-pause, so the remainder
 * can never exceed the pause the client asked for nor go below zero. */
void
cttest_stats_tube_pause_time_left_never_exceeds_the_requested_pause()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;
    long long left;

    snd(fd, "pause-tube default 5\r\n");
    ck(fd, "PAUSED\r\n");

    n = agy_okbody(fd, "stats-tube default\r\n", body, sizeof body, tail);
    left = agy_statfield(body, "pause-time-left");

    assertf(n > 0 && agy_statfield(body, "pause") == 5
            && left >= 0 && left <= 5,
            "a 5-second pause must report pause: 5 with 0 <= "
            "pause-time-left <= 5, got pause=%lld left=%lld",
            agy_statfield(body, "pause"), left);
}

/* protocol.txt: pause-tube takes a delay in seconds, so a 0-second
 * pause must be reported as 0. The implementation promotes it to one
 * nanosecond internally; that promotion must not surface on the wire. */
void
cttest_stats_tube_zero_pause_reports_zero_seconds()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "pause-tube default 0\r\n");
    ck(fd, "PAUSED\r\n");

    n = agy_okbody(fd, "stats-tube default\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "pause") == 0
            && agy_statfield(body, "pause-time-left") == 0,
            "a 0-second pause must report pause: 0 and pause-time-left: 0,"
            " got pause=%lld left=%lld",
            agy_statfield(body, "pause"),
            agy_statfield(body, "pause-time-left"));
}

/* protocol.txt: a paused tube delays reserves until the pause elapses.
 * The internal 1ns promotion exists so waiters still wake — assert the
 * wake, not the promotion. */
void
cttest_zero_pause_still_serves_a_waiting_reserve()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "pause-tube default 0\r\n");
    ck(fd, "PAUSED\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve-with-timeout 2\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "RESERVED 1 1\r\n") == 0,
            "a tube paused for 0 seconds must not withhold its ready job"
            " from a waiting client, got [%s]", reply);
}


/* ---------- fmt_job_stats / read_duration ---------- */

/* read_duration multiplies whole seconds by 1e9 into an int64 deadline.
 * The maximum delay and ttr the protocol accepts must survive that
 * conversion without truncation or a negative time-left. */
void
cttest_stats_job_maximum_delay_and_ttr_survive_nanosecond_conversion()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 0 4294967295 4294967295 0\r\n\r\n");
    ck(fd, "INSERTED 1\r\n");

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "delay") == 4294967295LL
            && agy_statfield(body, "ttr") == 4294967295LL
            && agy_statfield(body, "time-left") > 0,
            "the maximum delay/ttr must round-trip as whole seconds and"
            " leave a positive time-left; delay=%lld ttr=%lld left=%lld",
            agy_statfield(body, "delay"),
            agy_statfield(body, "ttr"),
            agy_statfield(body, "time-left"));
}

/* protocol.txt: time-left is the number of seconds left, and it is only
 * meaningful while the job is reserved or delayed. It can never exceed
 * the ttr the client asked for, and can never be negative. */
void
cttest_stats_job_time_left_stays_within_the_requested_ttr()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;
    long long left;

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);
    left = agy_statfield(body, "time-left");

    assertf(n > 0 && left > 0 && left <= 60,
            "a job reserved with ttr 60 must report 0 < time-left <= 60,"
            " got %lld", left);
}

/* protocol.txt defines reserves/releases/buries/kicks as counts of the
 * corresponding events on that job. One pass through each transition
 * must move each counter exactly once and never move a foreign one. */
void
cttest_stats_job_lifecycle_counters_move_once_per_event()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "release 1 0 0\r\n");
    ck(fd, "RELEASED\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "kick-job 1\r\n");
    ck(fd, "KICKED\r\n");

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "reserves") == 2
            && agy_statfield(body, "releases") == 1
            && agy_statfield(body, "buries") == 1
            && agy_statfield(body, "kicks") == 1
            && agy_statfield(body, "timeouts") == 0,
            "one put/reserve/release/reserve/bury/kick pass must give"
            " reserves=2 releases=1 buries=1 kicks=1 timeouts=0 (the"
            " timeout side is covered by"
            " cttest_a_lapsed_ttr_requeues_the_job_and_counts_the_timeout);"
            " got"
            " %lld/%lld/%lld/%lld/%lld",
            agy_statfield(body, "reserves"),
            agy_statfield(body, "releases"),
            agy_statfield(body, "buries"),
            agy_statfield(body, "kicks"),
            agy_statfield(body, "timeouts"));
}


/* ---------- do_list_tubes ---------- */

/* The announced size is "---\n" plus "- <name>\n" per tube, and the
 * final CRLF is excluded. With one 200-byte tube in the watch list the
 * whole relation is pinned by a single number. */
void
cttest_list_tubes_watched_size_equals_the_yaml_it_sends()
{
    int port = startsrv();
    int fd = diallocal(port);
    char name[201], cmd[256], body[8192], tail[4];
    int n;

    memset(name, 'w', 200);
    name[200] = '\0';
    snprintf(cmd, sizeof cmd, "watch %s\r\n", name);
    snd(fd, cmd);
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    n = agy_okbody(fd, "list-tubes-watched\r\n", body, sizeof body, tail);

    assertf(n == 4 + 2 + 200 + 1
            && (int)strlen(body) == n
            && strstr(body, name) != NULL
            && strcmp(tail, "\r\n") == 0,
            "one 200-byte watched tube must announce %d bytes of YAML"
            " carrying the whole name and end with CRLF; announced %d,"
            " read %d", 4 + 2 + 200 + 1, n, (int)strlen(body));
}

/* Set semantics: list-tubes-watched must name exactly the tubes the
 * connection watches — no omission, no duplicate, no leftover. Order is
 * the Ms insertion order and is not part of the contract, so the check
 * is by membership. */
void
cttest_list_tubes_watched_names_exactly_the_watched_set()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "watch alpha\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "watch beta\r\n");
    ck(fd, "WATCHING 3\r\n");
    snd(fd, "watch alpha\r\n");
    ck(fd, "WATCHING 3\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 2\r\n");

    n = agy_okbody(fd, "list-tubes-watched\r\n", body, sizeof body, tail);

    assertf(n == 4 + (2 + 5 + 1) + (2 + 4 + 1)
            && strstr(body, "- alpha\n") != NULL
            && strstr(body, "- beta\n") != NULL
            && strstr(body, "default") == NULL,
            "watch alpha/beta with default ignored must list exactly those"
            " two tubes once each; announced %d for [%s]", n, body);
}


/* ---------- read_uint / read_u64 / read_u32 ---------- */

/* UINT64_MAX is a legal job id: read_u64's ceiling is UINT64_MAX, so the
 * value must reach job_find and come back NOT_FOUND, never BAD_FORMAT
 * and never a wrapped lookup. */
void
cttest_delete_uint64_max_id_is_in_range()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "delete 18446744073709551615\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "NOT_FOUND\r\n") == 0,
            "18446744073709551615 is inside read_u64's range, so delete"
            " must report NOT_FOUND for the missing job, got [%s]", reply);
}

/* One past UINT64_MAX overflows the accumulator and must be refused as
 * a malformed integer (protocol.txt:44), not truncated into a live id. */
void
cttest_delete_uint64_max_plus_one_id_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "delete 18446744073709551616\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "18446744073709551616 exceeds read_u64's range and must be"
            " BAD_FORMAT rather than a wrapped id, got [%s]", reply);
}

/* A 25-digit number is far past the accumulator's range; it must not
 * silently wrap into a small id that names a live job. */
void
cttest_put_twenty_five_digit_body_size_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 60 1234567890123456789012345\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "a 25-digit body size overflows the parser and must be"
            " BAD_FORMAT, got [%s]", reply);
}

/* read_uint documents that it skips leading spaces (plural). Two spaces
 * before the id are still a well-formed argument. */
void
cttest_delete_with_two_spaces_before_the_id_is_accepted()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "delete  7\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "NOT_FOUND\r\n") == 0,
            "read_uint skips leading spaces, so \"delete  7\" names job 7"
            " and must answer NOT_FOUND, got [%s]", reply);
}

/* A sign is not a decimal digit; protocol.txt:44 makes non-numeric
 * characters where an integer is expected a BAD_FORMAT. */
void
cttest_delete_with_plus_signed_id_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "delete +1\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "'+' is not a decimal digit, so \"delete +1\" is BAD_FORMAT,"
            " got [%s]", reply);
}

/* Only the space is a separator: a tab before the id is a non-numeric
 * character where an integer is expected. */
void
cttest_delete_with_tab_before_the_id_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "delete \t1\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "a tab is not the documented space separator, so"
            " \"delete \\t1\" is BAD_FORMAT, got [%s]", reply);
}

/* The id argument is decimal; a hex literal must be rejected whole
 * rather than partially accepted as 0. */
void
cttest_stats_job_hex_literal_id_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "stats-job 0x10\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "job ids are decimal, so \"0x10\" must not be partially"
            " accepted as 0, got [%s]", reply);
}

/* reserve-with-timeout carries an extra INT_MAX ceiling above read_u32's
 * range, so a value read_u32 accepts must still be refused here. */
void
cttest_reserve_with_timeout_above_int_max_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "reserve-with-timeout 2147483648\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "reserve-with-timeout caps its argument at INT_MAX, so"
            " 2147483648 must be BAD_FORMAT, got [%s]", reply);
}


/* ---------- read_tube_name ---------- */

/* pause-tube takes a name AND a delay; with the delay missing the line
 * has the wrong number of arguments (protocol.txt:44). */
void
cttest_pause_tube_without_a_delay_argument_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "pause-tube default\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "pause-tube needs both a name and a delay; the one-argument"
            " form is BAD_FORMAT, got [%s]", reply);
}

/* read_tube_name and read_uint both skip runs of leading spaces, so
 * generous whitespace around the name is still a well-formed line. */
void
cttest_pause_tube_tolerates_runs_of_spaces_around_the_name()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "pause-tube    default    5\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "PAUSED\r\n") == 0,
            "leading spaces are skipped in runs, so this names tube"
            " default with delay 5, got [%s]", reply);
}

/* protocol.txt: a tube name may not begin with a hyphen. '-' is a valid
 * name character elsewhere, so read_tube_name happily consumes "-foo"
 * and only is_valid_tube can reject it. */
void
cttest_pause_tube_name_starting_with_a_hyphen_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "pause-tube -foo 5\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "protocol.txt forbids a tube name beginning with a hyphen,"
            " got [%s]", reply);
}


/* ---------- is_valid_tube ---------- */

/* protocol.txt names exactly nine punctuation characters as legal in a
 * tube name. All nine in one name, echoed back verbatim, pins the whole
 * accepted half of the character table. */
void
cttest_use_tube_accepts_every_documented_punctuation_character()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use a-b+c/d;e.f$g_h(i)j\r\n");
    rd(fd);
    snd(fd, "list-tube-used\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "USING a-b+c/d;e.f$g_h(i)j\r\n") == 0,
            "the nine documented punctuation characters must all be legal"
            " in a tube name and echoed unchanged, got [%s]", reply);
}

/* ':' is outside protocol.txt's character set; is_valid_tube must
 * reject the whole name rather than stop at the offending byte. */
void
cttest_use_tube_with_a_colon_in_the_name_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use a:b\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "':' is not in protocol.txt's tube-name character set, so the"
            " name must be rejected whole, got [%s]", reply);
}

/* The name must consume the rest of the line: a second word means the
 * wrong number of arguments, not a silently truncated tube name. */
void
cttest_use_tube_with_trailing_garbage_after_the_name_is_bad_format()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use foo bar\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "BAD_FORMAT\r\n") == 0,
            "\"use foo bar\" carries trailing garbage and must not be"
            " accepted as tube \"foo\", got [%s]", reply);
}


/* ---------- reply_using ---------- */

/* The longest legal USING line is "USING " + 200 bytes + CRLF = 208
 * bytes, which must still fit LINE_BUF_SIZE and arrive whole. */
void
cttest_list_tube_used_echoes_a_two_hundred_byte_name_whole()
{
    int port = startsrv();
    int fd = diallocal(port);
    char name[201], cmd[256], want[256], *reply;

    memset(name, 'k', 200);
    name[200] = '\0';
    snprintf(cmd, sizeof cmd, "use %s\r\n", name);
    snprintf(want, sizeof want, "USING %s\r\n", name);
    snd(fd, cmd);
    rd(fd);
    snd(fd, "list-tube-used\r\n");
    reply = rd(fd);

    assertf(strlen(reply) == 208 && strcmp(reply, want) == 0,
            "the maximum-length USING line is 208 bytes and must arrive"
            " intact, got %d bytes", (int)strlen(reply));
}


/* ---------- reply_job_n ---------- */

/* protocol.txt: the server never inspects or modifies a job body and
 * always sends it back in its original form. A body holding every one
 * of the 256 byte values proves it end to end. */
// The same promise across a restart. Between the put and the reserve
// the body goes through the WAL: framed with a length, checksummed,
// written with writev, read back by readrec. Every one of those steps
// is a place a byte could be dropped, doubled or reinterpreted — a
// zero taken for a terminator, a CRLF taken for a record boundary —
// and the body-for-body test above never leaves memory.
void
cttest_a_body_of_every_byte_value_survives_a_restart(void)
{
    srv.wal.dir = ctdir();
    int port = startsrv_wal();
    int fd = diallocal(port);
    char sent[258], got[258];

    agy_all_byte_values(sent);

    snd(fd, "put 0 0 3600 256\r\n");
    agy_write(fd, sent, 258);
    ck(fd, "INSERTED 1\r\n");
    close(fd);

    port = restartsrv_wal();
    fd = diallocal(port);

    snd(fd, "reserve\r\n");
    char *header = rd(fd);
    int n = agy_readn(fd, got, 258);

    assertf(strcmp(header, "RESERVED 1 256\r\n") == 0,
            "the recovered job must announce its original size, got [%s]",
            header);
    assertf(n == 258, "the whole body plus CRLF must come back, read %d", n);

    // Report the first byte that differs rather than "not equal": with
    // 256 distinct values the index names the culprit.
    int bad = -1;
    for (int i = 0; i < 258; i++) {
        if (got[i] != sent[i]) { bad = i; break; }
    }
    assertf(bad < 0,
            "byte %d changed across the WAL round trip: wrote 0x%02x, "
            "read back 0x%02x", bad,
            bad < 0 ? 0 : (unsigned char)sent[bad],
            bad < 0 ? 0 : (unsigned char)got[bad]);
}


void
cttest_reserved_body_returns_every_byte_value_unmodified()
{
    int port = startsrv();
    int fd = diallocal(port);
    char sent[258], got[258];
    int n;
    char *header;

    agy_all_byte_values(sent);

    snd(fd, "put 0 0 60 256\r\n");
    agy_write(fd, sent, 258);
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve\r\n");
    header = rd(fd);
    n = agy_readn(fd, got, 258);

    assertf(strcmp(header, "RESERVED 1 256\r\n") == 0
            && n == 258 && memcmp(got, sent, 258) == 0,
            "a 256-byte body holding every byte value must come back"
            " byte-for-byte behind a header announcing 256; header [%s],"
            " read %d bytes", header, n);
}

/* An empty body is a legal job: the announced size is 0 and only the
 * trailing CRLF follows the header. */
void
cttest_reserved_empty_body_announces_zero_bytes()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *stream;

    snd(fd, "put 0 0 60 0\r\n\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve\r\n");
    stream = agy_lines(fd, 2);

    assertf(strcmp(stream, "RESERVED 1 0\r\n\r\n") == 0,
            "an empty body must be announced as 0 bytes followed by the"
            " trailing CRLF alone, got [%s]", stream);
}

/* protocol.txt separates the body from the command stream: a body that
 * looks like a delete command must never be executed. */
void
cttest_job_body_that_looks_like_a_command_is_not_executed()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 0 0 60 12\r\n\r\ndelete 1\r\n\r\n");
    ck(fd, "INSERTED 1\r\n");

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);

    assertf(n > 0 && strstr(body, "\nstate: ready\n") != NULL,
            "a job body containing \"delete 1\" must stay data; job 1 must"
            " still be ready, stats-job answered [%s]", body);
}


/* ---------- kick_jobs / reply_kicked ---------- */

/* protocol.txt: "If there are any buried jobs, it will only kick buried
 * jobs." With 3 buried and 2 delayed in the used tube, a bound of 100
 * must move exactly the 3 buried ones. */
void
cttest_kick_moves_only_buried_jobs_while_any_are_buried()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use kickpref\r\n");
    ck(fd, "USING kickpref\r\n");
    snd(fd, "watch kickpref\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 0 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 0 60 1\r\nc\r\n");
    ck(fd, "INSERTED 3\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 2 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 3 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "put 0 60 60 1\r\nd\r\n");
    ck(fd, "INSERTED 4\r\n");
    snd(fd, "put 0 60 60 1\r\ne\r\n");
    ck(fd, "INSERTED 5\r\n");

    snd(fd, "kick 100\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "KICKED 3\r\n") == 0,
            "with 3 buried and 2 delayed jobs present, kick must move the"
            " buried ones only, got [%s]", reply);
}

/* The delayed jobs become eligible only once the buried list is empty,
 * on a later kick command. */
void
cttest_kick_takes_the_delayed_jobs_only_after_the_buried_ones()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use kicknext\r\n");
    ck(fd, "USING kicknext\r\n");
    snd(fd, "watch kicknext\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "ignore default\r\n");
    ck(fd, "WATCHING 1\r\n");

    snd(fd, "put 0 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "bury 1 0\r\n");
    ck(fd, "BURIED\r\n");
    snd(fd, "put 0 60 60 1\r\nd\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 0 60 60 1\r\ne\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "kick 100\r\n");
    ck(fd, "KICKED 1\r\n");
    snd(fd, "kick 100\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "KICKED 2\r\n") == 0,
            "once nothing is buried, kick must move the 2 delayed jobs,"
            " got [%s]", reply);
}


/* ---------- reply_watching ---------- */

/* protocol.txt: a client cannot ignore its last watched tube. */
void
cttest_ignoring_the_last_watched_tube_is_refused()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "ignore default\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "NOT_IGNORED\r\n") == 0,
            "the last watched tube cannot be ignored, got [%s]", reply);
}


/* ---------- reply_peeked_copy ---------- */

/* protocol.txt: peek-ready operates on the currently USED tube, not on
 * the watch list. */
void
cttest_peek_ready_inspects_the_used_tube_not_the_watched_ones()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");
    snd(fd, "put 0 0 60 1\r\nx\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "watch alpha\r\n");
    ck(fd, "WATCHING 2\r\n");
    snd(fd, "use beta\r\n");
    ck(fd, "USING beta\r\n");

    snd(fd, "peek-ready\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "NOT_FOUND\r\n") == 0,
            "peek-ready must look at the used tube (beta, empty), not at"
            " the watched tube alpha, got [%s]", reply);
}

/* protocol.txt: the ready queue is ordered by priority, ties broken by
 * insertion order, so peek-ready must name job 2. */
void
cttest_peek_ready_returns_highest_priority_then_lowest_id()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 5 0 60 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "put 1 0 60 1\r\nb\r\n");
    ck(fd, "INSERTED 2\r\n");
    snd(fd, "put 1 0 60 1\r\nc\r\n");
    ck(fd, "INSERTED 3\r\n");

    snd(fd, "peek-ready\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "FOUND 2 1\r\n") == 0,
            "the ready head is the most urgent job, ties broken by the"
            " lower id, got [%s]", reply);
}


/* ---------- enqueue_incoming_job ---------- */

/* A body whose last two bytes are not CRLF is EXPECTED_CRLF, and the
 * bytes that followed the body in the same segment must still be parsed
 * as the next command — exactly one reply per command, no desync. */
void
cttest_put_body_with_a_wrong_trailer_replies_expected_crlf_and_resyncs()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *stream;

    snd(fd, "put 0 0 60 4\r\nabcdXXlist-tube-used\r\n");
    stream = agy_lines(fd, 2);

    assertf(strcmp(stream, "EXPECTED_CRLF\r\nUSING default\r\n") == 0,
            "a bad body trailer must give exactly one EXPECTED_CRLF and"
            " leave the following command intact, got [%s]", stream);
}

/* protocol.txt: in drain mode the server no longer accepts new jobs.
 * The check must still be effective when the drain arrives between the
 * command line and the body. */
void
cttest_put_drained_between_header_and_body_replies_draining()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *reply;

    snd(fd, "put 0 0 60 4\r\n");
    kill(srvpid2, SIGUSR1);
    usleep(200000);
    snd(fd, "abcd\r\n");
    reply = rd(fd);

    assertf(strcmp(reply, "DRAINING\r\n") == 0,
            "a server that entered drain mode before the body arrived must"
            " refuse the job with DRAINING, got [%s]", reply);
}


/* ---------- fill_extra_data / conn_process_io framing ---------- */

/* The protocol is a byte stream: a put command, its body and the next
 * command delivered one byte per segment must produce exactly the same
 * replies as a single write. */
void
cttest_put_delivered_one_byte_per_segment_matches_a_single_write()
{
    int port = startsrv();
    int fd = diallocal(port);
    char *stream;

    agy_write_bytewise(fd, "put 0 0 60 5\r\nhello\r\nlist-tube-used\r\n");
    stream = agy_lines(fd, 2);

    assertf(strcmp(stream, "INSERTED 1\r\nUSING default\r\n") == 0,
            "the FSM must not depend on TCP framing: byte-per-segment"
            " delivery must answer exactly as one write, got [%s]", stream);
}


/* ---------- conn_timeout ---------- */

/* README.md's -I row: connections idle for SEC seconds are closed, and
 * the only documented exemption is a client waiting on reserve. After a
 * DEADLINE_SOON the client is no longer waiting, and after deleting its
 * job it holds nothing — so it is idle and must be reaped.
 * conn_timeout clears pending_timeout in the TIMED_OUT branch but not in
 * the DEADLINE_SOON one, and conntickat's idle gate requires
 * pending_timeout < 0. */
void
cttest_deadline_soon_does_not_exempt_a_conn_from_idle_reaping()
{
    int port;
    int fd;

    srv.idle_timeout = 1000000000LL;
    port = startsrv();
    fd = diallocal(port);

    snd(fd, "put 0 0 2 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    snd(fd, "reserve-with-timeout 20\r\n");
    ck(fd, "DEADLINE_SOON\r\n");
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");

    assertf(agy_saw_eof(fd, 5000),
            "after DEADLINE_SOON and a delete the conn holds no job and is"
            " not waiting, so -I 1s must close it");
}

/* The complementary green path: the TIMED_OUT branch does clear the
 * pending reserve timeout, so the connection stays eligible for -I. */
void
cttest_reserve_timeout_leaves_a_conn_eligible_for_idle_reaping()
{
    int port;
    int fd;

    srv.idle_timeout = 1000000000LL;
    port = startsrv();
    fd = diallocal(port);

    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");
    snd(fd, "reserve-with-timeout 0\r\n");
    ck(fd, "TIMED_OUT\r\n");

    assertf(agy_saw_eof(fd, 5000),
            "a conn whose reserve timed out is neither waiting nor holding"
            " a job, so -I 1s must close it");
}

/* protocol.txt: when a job's TTR elapses the server returns it to the
 * ready queue. The per-job 'timeouts' count and the global job-timeouts
 * counter must both record exactly that one event. */
void
cttest_ttr_expiry_records_exactly_one_job_timeout()
{
    int port = startsrv();
    int fd = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(fd, "put 0 0 1 1\r\na\r\n");
    ck(fd, "INSERTED 1\r\n");
    snd(fd, "reserve\r\n");
    agy_lines(fd, 2);
    usleep(1600000);

    n = agy_okbody(fd, "stats-job 1\r\n", body, sizeof body, tail);

    assertf(n > 0
            && agy_statfield(body, "timeouts") == 1
            && strstr(body, "\nstate: ready\n") != NULL,
            "one elapsed TTR must count exactly one timeout and put the"
            " job back in the ready queue, got timeouts=%lld in [%s]",
            agy_statfield(body, "timeouts"), body);
}


/* ---------- enqueue_waiting_conn / remove_waiting_conn ---------- */

/* A waiting connection is registered in EVERY tube it watches. Once it
 * is served from one of them, the registration must be gone from all of
 * them: a residual entry permanently inflates that tube's
 * current-waiting and makes process_tube hand a job to a conn that is
 * no longer waiting. */
void
cttest_a_served_reserve_clears_waiting_in_every_watched_tube()
{
    int port = startsrv();
    int worker = diallocal(port);
    int producer = diallocal(port);
    char body[8192], tail[4];
    int n;

    snd(worker, "watch alpha\r\n");
    ck(worker, "WATCHING 2\r\n");
    snd(worker, "watch beta\r\n");
    ck(worker, "WATCHING 3\r\n");
    snd(worker, "ignore default\r\n");
    ck(worker, "WATCHING 2\r\n");
    snd(worker, "reserve-with-timeout 5\r\n");
    usleep(200000);

    snd(producer, "use alpha\r\n");
    ck(producer, "USING alpha\r\n");
    snd(producer, "put 0 0 60 1\r\nx\r\n");
    ck(producer, "INSERTED 1\r\n");
    ck(worker, "RESERVED 1 1\r\n");
    ck(worker, "x\r\n");
    usleep(200000);

    n = agy_okbody(producer, "stats-tube beta\r\n", body, sizeof body, tail);

    assertf(n > 0 && agy_statfield(body, "current-waiting") == 0,
            "serving the reserve from tube alpha must also deregister the"
            " conn from tube beta; beta still reports current-waiting=%lld",
            agy_statfield(body, "current-waiting"));
}


/* ---------- http_health_reply ---------- */

/* The declared Content-Length must equal the number of body bytes that
 * actually follow the blank line, whichever status the probe gets. */
void
cttest_http_health_content_length_matches_the_body_it_sends()
{
    int port;
    int fd;
    char buf[4096];
    int total, hdrlen, bodylen;
    char *sep, *cl;
    long declared;

    srv.http_health = 1;
    port = startsrv();
    fd = diallocal(port);

    snd(fd, "GET /health HTTP/1.1\r\n");
    total = read_until_close(fd, buf, (int)sizeof buf - 1, 2000);
    sep = strstr(buf, "\r\n\r\n");
    cl = strstr(buf, "Content-Length: ");
    declared = cl ? strtol(cl + 16, NULL, 10) : -1;
    hdrlen = sep ? (int)(sep - buf) + 4 : total;
    bodylen = total - hdrlen;

    assertf(sep && cl && declared == bodylen,
            "the health reply must declare exactly the body it sends;"
            " Content-Length %ld vs %d bytes after the blank line",
            declared, bodylen);
}


/* ---------- h_conn pipelining ---------- */

/* protocol.txt: commands are processed in order and each gets one
 * reply. "quit" closes the connection, so a command pipelined behind it
 * in the same segment must never be answered. */
void
cttest_a_command_pipelined_after_quit_is_never_answered()
{
    int port = startsrv();
    int fd = diallocal(port);
    char first[64];
    int eof;

    snd(fd, "list-tube-used\r\nquit\r\nlist-tube-used\r\n");
    snprintf(first, sizeof first, "%s", rd(fd));
    eof = agy_saw_eof(fd, 2000);

    assertf(strcmp(first, "USING default\r\n") == 0 && eof,
            "the command before quit must be answered and the one"
            " pipelined behind it must produce no reply before the close;"
            " first reply [%s], eof=%d", first, eof);
}


/* ---------- reply() under -D: soft-cap overflow ordering ---------- */

/* Invariant #16's overflow window: once the staged acks pass
 * DUR_REPLY_SOFT_MAX the WHOLE buffer is pushed, in order. A long
 * USING line (208 bytes, the maximum) crossing that boundary must land
 * behind every ack that preceded it, not ahead of them and not instead
 * of them. */
void
cttest_dur_overflow_keeps_a_long_using_line_behind_the_staged_acks()
{
    enum { nput = 400 };
    int port;
    int fd;
    char name[201], cmd[256];
    static char burst[nput * 20 + 64];
    static char want[nput * 20 + 512];
    static char got[nput * 20 + 512];
    size_t o;
    int n;

    memset(name, 'd', 200);
    name[200] = '\0';
    srv.wal.dir = ctdir();
    port = startsrv_durable();
    fd = diallocal(port);

    snprintf(cmd, sizeof cmd, "use %s\r\n", name);
    snd(fd, cmd);
    rd(fd);

    o = agy_put_burst(burst, sizeof burst, want, sizeof want, nput);
    o += (size_t)snprintf(want + o, sizeof want - o, "USING %s\r\n", name);
    strcat(burst, "list-tube-used\r\n");
    snd(fd, burst);

    n = agy_readn(fd, got, (int)o);
    got[n < 0 ? 0 : n] = '\0';

    assertf(n == (int)o && memcmp(got, want, o) == 0,
            "every buffered ack must reach the client in command order"
            " across the soft-cap flush; expected %d bytes, read %d",
            (int)o, n);
}

/* ============================================================
 * PIPELINE COALESCING AND DEFERRED DISPATCH
 * ============================================================ */

/* agy_wait_until_waiting polls the global stats until exactly n clients
 * are parked on a reserve. Tests that need a reserve to actually BLOCK
 * cannot just sleep: an early put would satisfy it instantly and the
 * test would pass without ever reaching the path it is about. */
static void
agy_wait_until_waiting(int port, int n)
{
    char body[4096], want[64];
    int tries;

    snprintf(want, sizeof want, "current-waiting: %d\n", n);
    for (tries = 0; tries < 200; tries++) {
        int fd = diallocal(port);
        snd(fd, "stats\r\n");
        readstats(fd, body, sizeof body);
        close(fd);
        if (strstr(body, want))
            return;
        usleep(10000);
    }
    assertf(0, "no conn reached 'current-waiting: %d' in 2s", n);
}

/* A command pipelined BEHIND a blocking reserve must still be answered.
 *
 * The server reads "reserve\r\nlist-tubes\r\n" in one read(): the
 * reserve blocks, and the second command sits in the conn's command
 * buffer. Nothing on the socket will ever announce it again — the client
 * is blocked reading, so it sends nothing more, and the bytes are
 * already out of the kernel buffer, so level-triggered EPOLLIN has
 * nothing to report. Answering the reserve therefore has to hand the
 * conn to the run queue, or the server keeps bytes it owes a reply for
 * and the client hangs forever (upstream #647). */
void
cttest_command_pipelined_behind_blocking_reserve_is_answered()
{
    int port = startsrv();
    int worker = diallocal(port);

    /* ONE write: both commands land in a single read on the server. */
    snd(worker, "reserve\r\nlist-tubes\r\n");
    agy_wait_until_waiting(port, 1);   /* the reserve really is blocked */

    int producer = diallocal(port);
    snd(producer, "put 0 0 60 3\r\nabc\r\n");
    ck(producer, "INSERTED 1\r\n");

    ck(worker, "RESERVED 1 3\r\n");
    ck(worker, "abc\r\n");
    cksub(worker, "OK ");
}

/* Same hole, reached through the timeout side: nothing arrives at all
 * and the reserve is answered by prottick, not by another conn's put.
 * The queued command must come back with it. */
void
cttest_command_pipelined_behind_a_timing_out_reserve_is_answered()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "reserve-with-timeout 0\r\nlist-tubes\r\n");
    ck(fd, "TIMED_OUT\r\n");
    cksub(fd, "OK ");
}

/* A burst must reach the wire in command order even though its replies
 * are now staged in one buffer and pushed with a single write — and a
 * job reply carried inside that buffer must keep header and body
 * together and in place. */
void
cttest_pipelined_burst_keeps_replies_in_command_order()
{
    static const char want[] =
        "INSERTED 1\r\n"
        "INSERTED 2\r\n"
        "RESERVED 1 3\r\naaa\r\n"
        "DELETED\r\n"
        "RESERVED 2 3\r\nbbb\r\n"
        "DELETED\r\n";
    char got[sizeof want + 64];
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd,
        "put 0 0 60 3\r\naaa\r\n"
        "put 0 0 60 3\r\nbbb\r\n"
        "reserve\r\n"
        "delete 1\r\n"
        "reserve\r\n"
        "delete 2\r\n");

    int n = agy_readn(fd, got, (int)sizeof want - 1);
    got[n < 0 ? 0 : n] = '\0';
    assertf(n == (int)sizeof want - 1 && memcmp(got, want, n) == 0,
            "expected\n%s\ngot\n%s", want, got);
}

/* A job too big to join the burst buffer takes the writev road, where
 * the staged acks are merged in front of the header and the body is not
 * copied at all. The burst must therefore START with something that is
 * not a put — a put carries its body behind the command line, so it
 * ends the dispatch loop and nothing gets staged at all. */
void
cttest_pipelined_job_too_big_for_the_burst_buffer_keeps_order()
{
    enum { big = 5000 };            /* > DUR_REPLY_SOFT_MAX */
    static char body[big + 1];
    static char put[big + 64];
    static char want[big + 128];
    static char got[big + 256];
    int port = startsrv();
    int fd = diallocal(port);
    int i, n, w, o;

    for (i = 0; i < big; i++)
        body[i] = (char)('a' + i % 26);

    o = sprintf(put, "put 0 0 60 %d\r\n", big);
    memcpy(put + o, body, big);
    o += big;
    o += sprintf(put + o, "\r\n");
    agy_write(fd, put, o);
    ck(fd, "INSERTED 1\r\n");

    /* ONE write, and NOT starting with a put: the delete's ack is
     * staged in the burst buffer, then the reserve produces a body far
     * too large to join it. */
    snd(fd, "delete 999\r\nreserve\r\n");

    w = sprintf(want, "NOT_FOUND\r\nRESERVED 1 %d\r\n", big);
    memcpy(want + w, body, big);
    w += big;
    w += sprintf(want + w, "\r\n");

    n = agy_readn(fd, got, w);
    assertf(n == w && memcmp(got, want, (size_t)w) == 0,
            "a %d-byte job reply must not overtake or swallow the ack"
            " staged in front of it; expected %d bytes, read %d",
            big, w, n);
}

/* An ack staged in the burst buffer must reach the client even when the
 * command that follows it in the same burst closes the connection. */
void
cttest_pipelined_quit_still_delivers_the_acks_before_it()
{
    static const char want[] = "INSERTED 1\r\nDELETED\r\n";
    char got[sizeof want + 32];
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 3\r\nabc\r\ndelete 1\r\nquit\r\n");

    int n = agy_readn(fd, got, (int)sizeof want - 1);
    got[n < 0 ? 0 : n] = '\0';
    assertf(n == (int)sizeof want - 1 && memcmp(got, want, n) == 0,
            "quit must not swallow the replies queued in front of it;"
            " expected \"%s\", got \"%s\"", want, got);
}

/* An ack staged in the burst buffer, a short write, and a conn that is
 * MID-COMMAND when the flush happens. The SEND_WORD retry FSM cannot
 * take the remainder here — c->reply and c->state belong to the
 * unfinished put — so the bytes are carried to the next tick instead.
 * Carrying them wrong loses a reply the client is owed; carrying them
 * out of order puts the put's own ack in front of it. */
void
cttest_partial_burst_flush_while_mid_put_keeps_both_acks_in_order()
{
    fault_set_short(FAULT_WRITE, 0, 4);   /* the burst flush lands 4 bytes */
    int port = startsrv();
    int fd = diallocal(port);

    /* One write: a command that acks, then a put whose body is
     * deliberately short, so the burst ends in STATE_WANT_DATA. */
    snd(fd, "delete 1\r\nput 0 0 60 11\r\nhello");
    snd(fd, " world\r\n");                 /* finish the body */

    ck(fd, "NOT_FOUND\r\n");
    ck(fd, "INSERTED 1\r\n");
    fault_clear_all();
}

/* An over-long command line used to be impossible to receive whole: the
 * input buffer was exactly one line long, so a longer line could only
 * ever arrive as an unterminated fragment and fell into the discard
 * path. The buffer now holds a whole pipelined burst, so the line CAN
 * arrive complete — with valid commands queued behind it. The protocol
 * limit must still be enforced, and only that one line skipped. */
void
cttest_over_long_line_inside_a_burst_is_rejected_alone()
{
    char burst[LINE_BUF_SIZE * 2 + 64];
    int port = startsrv();
    int fd = diallocal(port);
    int o = 0, i;

    o += sprintf(burst + o, "use alpha\r\n");
    for (i = 0; i < LINE_BUF_SIZE; i++)   /* + CRLF puts it over the limit */
        burst[o + i] = 'z';
    o += LINE_BUF_SIZE;
    o += sprintf(burst + o, "\r\nlist-tube-used\r\n");
    assertf(o < CMD_BUF_SIZE, "the burst must fit one read to be the case"
            " this test is about");
    agy_write(fd, burst, o);

    ck(fd, "USING alpha\r\n");
    ck(fd, "BAD_FORMAT\r\n");
    ck(fd, "USING alpha\r\n");
}

/* The longest LEGAL line is exactly LINE_BUF_SIZE bytes including the
 * CRLF, and it must still be accepted — the check above is ">", not
 * ">=", and an off-by-one there silently caps every tube name. */
void
cttest_longest_legal_line_is_still_accepted()
{
    char name[MAX_TUBE_NAME_LEN], cmd[LINE_BUF_SIZE + 32];
    int port = startsrv();
    int fd = diallocal(port);
    int n;

    memset(name, 'a', MAX_TUBE_NAME_LEN - 1);
    name[MAX_TUBE_NAME_LEN - 1] = '\0';
    n = sprintf(cmd, "pause-tube %s 4294967295\r\n", name);
    assertf(n == LINE_BUF_SIZE, "the documented longest command must be"
            " exactly LINE_BUF_SIZE bytes; it is %d", n);

    snd(fd, cmd);
    ck(fd, "NOT_FOUND\r\n");   /* no such tube, but the LINE parsed */
}

/* protocol.txt gives delete exactly two answers, DELETED and NOT_FOUND,
 * and says the DEADLINE_SOON window exists precisely to "give the
 * client a chance to delete or release its reserved job". A client
 * inside that window is therefore the one that most needs both commands
 * to work; upstream #609 reports getting DEADLINE_SOON out of delete
 * instead. Pinned here so the deadline check can never spread from
 * reserve to the commands the margin was invented to allow. */
void
cttest_delete_and_release_inside_the_deadline_margin_still_work()
{
    int port = startsrv();
    int fd = diallocal(port);

    /* ttr 1 == the whole run time is the one-second safety margin, so
     * the window is open the moment the job is reserved. */
    snd(fd, "put 0 0 1 3\r\nabc\r\n");
    ck(fd, "INSERTED 1\r\n");

    snd(fd, "reserve\r\n");
    ck(fd, "RESERVED 1 3\r\n");
    ck(fd, "abc\r\n");

    /* Prove the window really is open: with nothing else ready, reserve
     * must say so rather than block (a ready job would rightly be handed
     * over instead — see protocol.txt). */
    snd(fd, "reserve\r\n");
    ck(fd, "DEADLINE_SOON\r\n");

    /* The two commands the margin exists to make room for. */
    snd(fd, "release 1 0 0\r\n");
    ck(fd, "RELEASED\r\n");

    snd(fd, "reserve\r\n");
    ck(fd, "RESERVED 1 3\r\n");
    ck(fd, "abc\r\n");
    snd(fd, "delete 1\r\n");
    ck(fd, "DELETED\r\n");
}

/* Invariant #14 under a pipelined burst: an ack may not leave before
 * the tick's fdatasync, however many syscalls coalescing saves. The
 * pipeline hold on the reply buffer therefore has to yield to the
 * durability hold, never the other way round.
 *
 * A SIGKILL bench cannot see this — killing the process does not lose
 * the page cache, so records acked one moment too early still come
 * back. Failing the commit does see it: acks already on the wire mean
 * the client holds three INSERTEDs for records that never reached the
 * disk. Held correctly, the whole batch turns into INTERNAL_ERROR plus
 * a FIN instead (invariants #14/#16).
 *
 * The skip of 1 is the single fdatasync the WAL start-up spends on its
 * directory; the next one is this burst's commit. Should start-up ever
 * change shape, this test says so — by seeing INSERTED here. */
void
cttest_dur_pipelined_acks_never_outrun_the_commit()
{
    srv.wal.dir = ctdir();
    fault_set(FAULT_FDATASYNC, 1, EIO);
    int port = startsrv_durable();
    int fd = diallocal(port);

    snd(fd, "put 0 0 60 1\r\na\r\nput 0 0 60 1\r\nb\r\nput 0 0 60 1\r\nc\r\n");

    char *got = rd(fd);
    assertf(strstr(got, "INTERNAL_ERROR"),
            "a commit that failed must not leave a pipelined burst acked;"
            " first reply line was \"%s\"", got);
    fault_clear_all();
}

/* A job handed over INSIDE a coalesced burst must still get its TTR
 * deadline into the connection's tick heap. That reply skips
 * conn_want_command — the header and body go straight into the burst
 * buffer — so it has to reschedule the conn itself. An idle conn is not
 * in the tick heap at all, so missing that step does not make the
 * timeout late, it removes it: the job stays reserved for as long as
 * the client keeps the connection open, and no other worker can have
 * it.
 *
 * A second worker asking for the job is the honest way to check that:
 * nothing but the TTR lapsing can hand it over, and its own timeout
 * turns "never" into TIMED_OUT rather than a hung read. */
void
cttest_job_reserved_inside_a_burst_still_times_out()
{
    int64 saved_timeout = timeout2;
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "put 0 0 1 3\r\nabc\r\n");     /* ttr 1 second */
    ck(fd, "INSERTED 1\r\n");

    /* ONE write, not starting with a put: the delete's ack is staged in
     * the burst buffer, so the reserve's reply is produced inside the
     * burst rather than on its own. */
    snd(fd, "delete 999\r\nreserve\r\n");
    ck(fd, "NOT_FOUND\r\n");
    ck(fd, "RESERVED 1 3\r\n");
    ck(fd, "abc\r\n");

    /* Never touch fd again: an event on it would reschedule the conn
     * and hide exactly the bug under test. The read has to outlast the
     * server-side timeout, or a lost TTR shows up as a hung test
     * instead of a failed one. */
    timeout2 = 20000000000LL;
    int other = diallocal(port);
    snd(other, "reserve-with-timeout 8\r\n");
    ck(other, "RESERVED 1 3\r\n");
    ck(other, "abc\r\n");
    timeout2 = saved_timeout;
}

/* The length gates in which_cmd separate "known verb, missing
 * argument" from "not a command at all", and both answers are part of
 * the contract: a name must be at least one character (BAD_FORMAT),
 * while a verb without its separator is a different string entirely and
 * must not be served out of this one's namespace (UNKNOWN_COMMAND,
 * invariant #13). Mutation found the boundary untested — off by one and
 * `stats-tube ` starts answering UNKNOWN_COMMAND, which tells a client
 * the command does not exist when in fact it forgot the tube. */
void
cttest_stats_verbs_separate_missing_argument_from_unknown_verb()
{
    int port = startsrv();
    int fd = diallocal(port);

    snd(fd, "stats-tube \r\n");        /* separator, no name */
    ck(fd, "BAD_FORMAT\r\n");
    snd(fd, "stats-job \r\n");
    ck(fd, "BAD_FORMAT\r\n");

    snd(fd, "stats-tube\r\n");         /* no separator: another word */
    ck(fd, "UNKNOWN_COMMAND\r\n");
    snd(fd, "stats-job\r\n");
    ck(fd, "UNKNOWN_COMMAND\r\n");

    /* ...and the shortest legal forms still work. */
    snd(fd, "stats-tube a\r\n");
    ck(fd, "NOT_FOUND\r\n");
    snd(fd, "stats-job 1\r\n");
    ck(fd, "NOT_FOUND\r\n");
}

/* A write error the socket will never recover from, on the ordinary
 * reply path this time (dur_flush_one has its own test). EAGAIN means
 * "ask again"; EPIPE means the peer is gone. Treating the second as the
 * first leaves a dead conn registered for write, woken by epoll for as
 * long as it lives. Mutation found this one too: inverting the EAGAIN
 * test here left the whole suite green. */
void
cttest_reply_closes_the_conn_on_a_write_error_that_cannot_recover()
{
    /* The fault table is inherited across the fork and each side then
     * spends its own copy, so the parent's has to go somewhere harmless
     * before snd() needs a working write. The server's first wrapped
     * write is the reply this test is about — the readiness handshake
     * deliberately avoids write() (see testserv.c), so it does not
     * count here. */
    int sink = open("/dev/null", O_WRONLY);
    assertf(sink > 2, "setup: /dev/null must land above the wrapped fds");
    fault_set(FAULT_WRITE, 0, EPIPE);
    int port = startsrv();
    ssize_t spent = write(sink, "x", 1);
    assertf(spent == -1 && errno == EPIPE,
            "setup: the parent's copy of the injection must be spent here");
    close(sink);

    int fd = diallocal(port);
    snd(fd, "list-tube-used\r\n");     /* its reply write fails with EPIPE */

    /* The server must give up on this connection, not keep it. */
    char c;
    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    fd_set rfd;
    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    assertf(select(fd + 1, &rfd, NULL, NULL, &tv) > 0,
            "a conn whose reply hit EPIPE must be closed, not left open");
    assertf(read(fd, &c, 1) == 0,
            "expected EOF on the connection the server gave up on");
    fault_clear_all();
}

/* The burst buffer's overflow path, with the socket refusing to take it
 * all at once. Past DUR_REPLY_SOFT_MAX the whole buffer is pushed
 * immediately so nothing is dropped and order is kept; a short write
 * there has to park the unsent tail on the SEND_WORD FSM and release
 * BOTH holds on the buffer, or the next reply is memcpy'd over bytes
 * the client has not seen.
 *
 * Reaching it at all takes some care, and the reason is worth knowing:
 * one dispatch loop can only consume what one read() brought in, so
 * CMD_BUF_SIZE bounds a burst at about fifty short commands — roughly
 * 700 bytes of "INSERTED n", nowhere near the 4KB cap. The overflow is
 * therefore only reachable when the REPLIES are much larger than the
 * commands that produce them. "list-tube-used" is 16 bytes in and, on a
 * tube with the longest legal name, 208 bytes out: thirteen to one. */
void
cttest_burst_overflow_survives_a_short_write()
{
    enum { nask = 60 };                /* 60 x 208B replies: over 12KB */
    char name[MAX_TUBE_NAME_LEN];
    static char burst[nask * 20 + 512];
    static char want[nask * 256 + 512];
    static char got[nask * 256 + 512];
    int port, fd, i, o = 0, w = 0, n;

    memset(name, 'q', MAX_TUBE_NAME_LEN - 1);
    name[MAX_TUBE_NAME_LEN - 1] = '\0';

    /* Skip 1: the server's first write is the USING reply to the lone
     * `use` below, which is not part of a burst and goes straight to
     * the socket; the second is the overflow flush this test wants to
     * come up short. (The readiness handshake deliberately does not
     * use write() — see the note in testserv.c — or it would count
     * here and, worse, silently shift every other test's arming.) */
    fault_set_short(FAULT_WRITE, 1, 3);
    port = startsrv();
    fd = diallocal(port);

    snd(fd, "use ");                   /* set the long name up first */
    snd(fd, name);
    snd(fd, "\r\n");
    cksub(fd, "USING ");

    for (i = 0; i < nask; i++) {
        o += sprintf(burst + o, "list-tube-used\r\n");
        w += sprintf(want + w, "USING %s\r\n", name);
    }
    assertf(w > DUR_REPLY_SOFT_MAX,
            "the burst must overflow the soft cap to be the case this test"
            " is about; it is only %d bytes", w);
    agy_write(fd, burst, o);

    n = agy_readn(fd, got, w);
    got[n < 0 ? 0 : n] = '\0';
    assertf(n == w && memcmp(got, want, (size_t)w) == 0,
            "every reply must arrive, in order, across an overflow flush the"
            " socket only partly accepted; expected %d bytes, read %d", w, n);
    fault_clear_all();
}
