#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <errno.h>


static int srvpid, size;
static int srvport;   // last port SERVER() handed out; see nudge_srv
// Read end of a pipe the forked server's SIGTERM handler writes one byte
// to. It answers the only question /proc cannot: the kernel says the
// signal is caught, unblocked and not pending, which means it was
// delivered — but "delivered to the process" and "the handler ran" are
// different things in an instrumented build, and only one of them means
// the server was told to stop.
static int srvsigfd = -1;
static int srvsigwfd = -1;

// Global timeout set for reading response in tests; 5sec.
static int64 timeout = 5000000000LL;

// Allocation pattern for wrapfalloc that replaces falloc in tests.
// Zero value at N-th element means that N-th call to the falloc
// should fail with ENOSPC result.
static byte fallocpat[3];


static int
exist(char *path)
{
    struct stat s;

    int r = stat(path, &s);
    return r != -1;
}

static int
wrapfalloc(int fd, int len)
{
    static size_t c = 0;

    printf("\nwrapfalloc: fd=%d size=%d\n", fd, len);
    if (c >= sizeof(fallocpat) || !fallocpat[c++]) {
        return ENOSPC;
    }
    return rawfalloc(fd, len);
}

static int
mustdiallocal(int port)
{
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
    };

    int r = inet_aton("127.0.0.1", &addr.sin_addr);
    if (!r) {
        errno = EINVAL;
        twarn("inet_aton");
        exit(1);
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        twarn("socket");
        exit(1);
    }

    // Fix of the benchmarking issue on Linux. See issue #430.
    int flags = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(int))) {
        twarn("setting TCP_NODELAY on fd %d", fd);
        exit(1);
    }

    r = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    if (r == -1) {
        twarn("connect");
        exit(1);
    }

    return fd;
}

static int
mustdialunix(char *socket_file)
{
    struct sockaddr_un addr;
    const size_t maxlen = sizeof(addr.sun_path);
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, maxlen, "%s", socket_file);

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        twarn("socket");
        exit(1);
    }

    int r = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    if (r == -1) {
        twarn("connect");
        exit(1);
    }

    return fd;
}

// SIGTERM in the forked test server. It used to call exit() straight
// from the handler, which is not async-signal-safe: exit() runs atexit
// handlers and flushes stdio, and under a sanitizer it also enters the
// runtime's own teardown — while the interrupted thread may be holding
// the very locks that needs. Under TSan that deadlocked about one run
// in five, and the parent's waitpid then waited forever, so the whole
// suite hung with no output rather than failing.
//
// Do what the real server does instead: set the flag, wake the loop
// through the async-signal-safe eventfd, and let srvserve return so the
// child exits from ordinary context. That also keeps the gcov flush at
// exit that the old comment was protecting (issue #443) — it is exit()
// that gcov needs, not exit() from a handler.
static void
exit_process(int signum)
{
    UNUSED_PARAMETER(signum);
    // First thing, before anything that could be deferred or blocked:
    // leave proof that this handler ran at all. write() is
    // async-signal-safe and the pipe has room for one byte.
    if (srvsigwfd >= 0) {
        ssize_t w = write(srvsigwfd, "1", 1);
        (void)w;
    }
    atomic_store_explicit(&shutdown_requested, 1, memory_order_relaxed);
    srv_wake();
}

static void
set_sig_handler()
{
    struct sigaction sa;

    sa.sa_flags = 0;
    int r = sigemptyset(&sa.sa_mask);
    if (r == -1) {
        twarn("sigemptyset()");
        exit(111);
    }

    // This is required to trigger gcov on exit. See issue #443.
    sa.sa_handler = exit_process;
    r = sigaction(SIGTERM, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGTERM)");
        exit(111);
    }
}

// Kill the srvpid (child process) with SIGTERM to give it a chance
// to write gcov data to the filesystem before ct kills it with SIGKILL.
// Do nothing in case of srvpid==0; child was already killed.
// nudge_srv makes the server's epoll return, by connecting to it and
// hanging up. Needed only under ThreadSanitizer, and the reason is
// worth writing down: TSan defers a signal that arrives while the
// thread sits inside an intercepted blocking call, and runs the handler
// at the next safe point instead. An idle server's next safe point is
// whenever its epoll park ends — up to 60s away, since the malloc_trim
// cadence is what caps it. So SIGTERM alone can leave the process
// asleep in do_epoll_wait for a minute. That is what /proc/<pid>/wchan
// said on the timeout path below, after two wrong guesses (the signal
// handler, then exit() in a forked sanitizer child) had already been
// tried and disproved.
//
// Measured, over fifty runs: this takes the failure rate from about one
// run in five to about one in twenty-five. Most of the window, not all
// of it — a signal can also land somewhere the park ending does not
// resolve — so the timeout path below still reports the signal masks
// and whether the handler left its byte on the pipe. The nudge stays
// because it costs nothing on the normal path and cannot weaken a
// check (a server that truly ignored SIGTERM would still never exit),
// but the timeout path below is where the answer will come from: it
// reports the signal masks, which tell "blocked", "no handler yet" and
// "pending" apart. The pure "SIGTERM while parked in epoll" property is
// measured against the real binary, with no sanitizer in the way, by
// bench/shutdown/run.sh.
static void
nudge_srv(void)
{
    if (srvport <= 0)
        return;
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(srvport),
    };
    inet_aton("127.0.0.1", &addr.sin_addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1)
        return;
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) == 0) {
        ssize_t w = write(fd, "quit\r\n", 6);
        (void)w;
    }
    close(fd);
}

static void
kill_srvpid(void)
{
    if (!srvpid)
        return;
    kill(srvpid, SIGTERM);
    // Bounded wait. A plain waitpid here turns any server that fails to
    // stop into a suite that hangs with no output and no failing test
    // name — the worst possible way to learn about it. Give it ten
    // seconds, then SIGKILL and say so: a red test beats a wedged run.
    for (int i = 0; i < 1000; i++) {
        // Cheap for a server that is already on its way out (the first
        // poll usually wins); decisive for one parked in epoll.
        if (i == 2 || i == 20 || i == 200)
            nudge_srv();
        int st;
        pid_t r = waitpid(srvpid, &st, WNOHANG);
        if (r == srvpid) {
            srvpid = 0;
            srvport = 0;
            if (srvsigfd >= 0) { close(srvsigfd); srvsigfd = -1; }
            return;
        }
        if (r == -1 && errno != EINTR)
            break;
        usleep(10000);
    }
    // Say WHERE it is stuck, not just that it is. A server that ignores
    // SIGTERM is worth a real diagnosis, and by the time the run is
    // over the process is gone — so read it here, before the kill.
    char state[64] = "?", wchan[128] = "?", path[64];
    snprintf(path, sizeof path, "/proc/%d/wchan", (int)srvpid);
    int df = open(path, O_RDONLY);
    if (df >= 0) {
        ssize_t n = read(df, wchan, sizeof wchan - 1);
        wchan[n > 0 ? n : 0] = '\0';
        close(df);
    }
    // Signal masks too: "stuck in epoll" and "never got the signal" look
    // identical from wchan alone. SigBlk says whether SIGTERM is blocked,
    // SigCgt whether a handler is installed at all, and SigPnd/ShdPnd
    // whether it is sitting undelivered. SIGTERM is bit 14 (0x4000).
    char sigs[256] = "";
    snprintf(path, sizeof path, "/proc/%d/status", (int)srvpid);
    df = open(path, O_RDONLY);
    if (df >= 0) {
        char buf[4096];
        ssize_t n = read(df, buf, sizeof buf - 1);
        buf[n > 0 ? n : 0] = '\0';
        close(df);
        size_t used = 0;
        for (char *line = buf; line && *line; ) {
            char *eol = strchr(line, '\n');
            if (eol) *eol = '\0';
            if (!strncmp(line, "Sig", 3) || !strncmp(line, "ShdPnd", 6)) {
                int k = snprintf(sigs + used, sizeof sigs - used, "%s%s",
                                 used ? " " : "", line);
                if (k > 0 && used + (size_t)k < sizeof sigs)
                    used += (size_t)k;
                else
                    break;
            }
            line = eol ? eol + 1 : NULL;
        }
    }
    snprintf(path, sizeof path, "/proc/%d/stat", (int)srvpid);
    df = open(path, O_RDONLY);
    if (df >= 0) {
        char buf[512];
        ssize_t n = read(df, buf, sizeof buf - 1);
        buf[n > 0 ? n : 0] = '\0';
        char *close_paren = strrchr(buf, ')');
        if (close_paren && close_paren[1] && close_paren[2])
            snprintf(state, sizeof state, "%c", close_paren[2]);
        close(df);
    }
    // Did the handler run at all? One byte on the pipe says yes.
    const char *ran = "unknown";
    if (srvsigfd >= 0) {
        char b;
        int fl = fcntl(srvsigfd, F_GETFL);
        fcntl(srvsigfd, F_SETFL, fl | O_NONBLOCK);
        ssize_t n = read(srvsigfd, &b, 1);
        ran = n == 1 ? "handler RAN" : "handler NEVER ran";
    }
    kill(srvpid, SIGKILL);
    waitpid(srvpid, 0, 0);
    pid_t stuck = srvpid;
    srvpid = 0;
    srvport = 0;
    assertf(0, "the test server (pid %d) did not exit on SIGTERM within 10s;"
               " it had to be killed (%s, proc state '%s', wchan '%s', %s)",
            (int)stuck, ran, state, wchan, sigs);
}

#define SERVER() (progname=__func__, mustforksrv())
#define SERVER_UNIX() (progname=__func__, mustforksrv_unix())

// Forks the server storing the pid in srvpid.
// The parent process returns port assigned.
// The child process serves until the SIGTERM is received by it.
static int
mustforksrv(void)
{
    struct sockaddr_in addr;

    srv.sock.fd = make_server_socket("127.0.0.1", "0");
    if (srv.sock.fd == -1) {
        puts("mustforksrv failed");
        exit(1);
    }

    socklen_t len = sizeof(addr);
    int r = getsockname(srv.sock.fd, (struct sockaddr *)&addr, &len);
    if (r == -1 || len > sizeof(addr)) {
        puts("mustforksrv failed");
        exit(1);
    }

    int port = ntohs(addr.sin_port);

    // Ready pipe: the listening socket exists before the fork, so a
    // connect() succeeds whether or not the child is serving yet.
    // Without this the test races the child's startup — a SIGTERM sent
    // "after" the fork could land before walinit had made a binlog.
    // socketpair + send/recv, not pipe + write/read, and the reason is
    // load-bearing: the fault injector wraps write(), so a ready byte
    // sent with write() becomes the server's FIRST wrapped write and
    // quietly eats any fault armed at skip 0. Three tests that arm
    // "the first reply lands short" stopped exercising a short reply
    // at all when this handshake was introduced, and nothing failed —
    // coverage is what noticed, years of green runs later. send() is
    // not wrapped, so skip counts mean what their comments say.
    int ready[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, ready) != 0) {
        twarn("socketpair");
        exit(1);
    }
    int sigpipe[2];
    if (pipe(sigpipe) != 0) {
        twarn("pipe");
        exit(1);
    }

    srvpid = fork();
    if (srvpid < 0) {
        twarn("fork");
        exit(1);
    }

    if (srvpid > 0) {
        close(ready[1]);
        close(sigpipe[1]);
        if (srvsigfd >= 0)
            close(srvsigfd);
        srvsigfd = sigpipe[0];
        // On exit the parent (test) sends SIGTERM to the child.
        atexit(kill_srvpid);
        char go;
        ssize_t n = recv(ready[0], &go, 1, 0);
        close(ready[0]);
        assertf(n == 1, "setup: the server did not come up");
        srvport = port;
        printf("start server port=%d pid=%d\n", port, srvpid);
        return port;
    }

    /* now in child */

    close(ready[0]);
    close(sigpipe[0]);
    srvsigwfd = sigpipe[1];
    // Before the handler: srv_wake() is a no-op while the eventfd does
    // not exist, and then a SIGTERM arriving while srvserve is parked in
    // epoll would sit on the flag until the next natural wake-up — up to
    // an hour on an idle server.
    if (srv_wake_init() == -1) {
        twarn("srv_wake_init");
        exit(111);
    }
    set_sig_handler();
    prot_init();

    srv_acquire_wal(&srv);

    for (;;) {
        ssize_t w = send(ready[1], "1", 1, 0);
        if (w == 1)
            break;
        if (w == -1 && (errno == EINTR || errno == EAGAIN
                        || errno == EWOULDBLOCK))
            continue;
        break;
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
mustforksrv_unix(void)
{
    static char path[90];
    char name[95];
    snprintf(path, sizeof(path), "%s/socket", ctdir());
    snprintf(name, sizeof(name), "unix:%s", path);
    srv.sock.fd = make_server_socket(name, NULL);
    if (srv.sock.fd == -1) {
        puts("mustforksrv_unix failed");
        exit(1);
    }

    srvpid = fork();
    if (srvpid < 0) {
        twarn("fork");
        exit(1);
    }

    if (srvpid > 0) {
        // On exit the parent (test) sends SIGTERM to the child.
        atexit(kill_srvpid);
        printf("start server socket=%s\n", path);
        assert(exist(path));
        return path;
    }

    /* now in child */

    set_sig_handler();
    prot_init();

    srv_acquire_wal(&srv);

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
readline(int fd)
{
    char c = 0, p = 0;
    // Must exceed STATS_BUF_SIZE (4096): the global `stats` YAML block
    // arrives as ONE line (it terminates with \r\n) and is well over
    // 1KB, so the old 1024-byte buffer made bare `stats` untestable
    // from this file at all.
    static char buf[8192];
    fd_set rfd;
    struct timeval tv;

    printf("<%d ", fd);
    fflush(stdout);

    size_t i = 0;
    for (;;) {
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        tv.tv_sec = timeout / 1000000000;
        tv.tv_usec = (timeout/1000) % 1000000;
        int r = select(fd+1, &rfd, NULL, NULL, &tv);
        switch (r) {
        case 1:
            break;
        case 0:
            fputs("timeout", stderr);
            exit(8);
        case -1:
            perror("select");
            exit(1);
        default:
            fputs("unknown error", stderr);
            exit(3);
        }

        // TODO: try reading into a buffer to improve performance.
        // See related issue #430.
        r = read(fd, &c, 1);
        if (r == -1) {
            perror("write");
            exit(1);
        }
        if (i >= sizeof(buf)-1) {
            fputs("response too big", stderr);
            exit(4);
        }
        putc(c, stdout);
        fflush(stdout);
        buf[i++] = c;
        if (p == '\r' && c == '\n') {
            break;
        }
        p = c;
    }
    buf[i] = '\0';
    return buf;
}

static void
ckresp(int fd, char *exp)
{
    char *line = readline(fd);
    assertf(strcmp(exp, line) == 0, "\"%s\" != \"%s\"", exp, line);
}

static void
ckrespsub(int fd, char *sub)
{
    char *line = readline(fd);
    assertf(strstr(line, sub), "\"%s\" not in \"%s\"", sub, line);
}

// Asserts that `sub` is absent from the next reply line. Used where the
// promise is that a field must NOT carry a particular value (e.g.
// time-left on a job where the number is meaningful).
static void
cknotsub(int fd, char *sub)
{
    char *line = readline(fd);
    assertf(!strstr(line, sub), "\"%s\" must NOT appear in \"%s\"", sub, line);
}

// Asserts the server closed the connection: readable, and the read
// returns 0 bytes. A server that merely stops answering fails on the
// select, one that keeps the socket open fails on the read.
static void
ckeof(int fd)
{
    char c;
    fd_set rfd;
    struct timeval tv;

    FD_ZERO(&rfd);
    FD_SET(fd, &rfd);
    tv.tv_sec = timeout / 1000000000;
    tv.tv_usec = (timeout / 1000) % 1000000;
    int r = select(fd + 1, &rfd, NULL, NULL, &tv);
    assertf(r == 1,
            "server must close the connection, select returned %d", r);
    r = (int)read(fd, &c, 1);
    assertf(r == 0,
            "server must send EOF, read returned %d (byte 0x%02x)",
            r, (unsigned char)c);
}

// Sleeps until the absolute deadline (nanoseconds), so a probe lands at
// a known offset from an event instead of drifting with the round trips
// spent in between.
static void
sleep_until(int64 deadline)
{
    int64 d = deadline - nanoseconds();
    if (d > 0)
        usleep((useconds_t)(d / 1000));
}

static void
writefull(int fd, char *s, int n)
{
    int c;
    for (; n; n -= c) {
        c = write(fd, s, n);
        if (c == -1) {
            perror("write");
            exit(1);
        }
        s += c;
    }
}

static void
mustsend(int fd, char *s)
{
    writefull(fd, s, strlen(s));
    printf(">%d %s", fd, s);
    fflush(stdout);
}

static int
filesize(char *path)
{
    struct stat s;

    int r = stat(path, &s);
    if (r == -1) {
        twarn("stat");
        exit(1);
    }
    return s.st_size;
}

void
cttest_unknown_command()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "nont10knowncommand\r\n");
    ckresp(fd, "UNKNOWN_COMMAND\r\n");

    // 'n' is not even a case label in which_cmd, so the line above only
    // ever reaches the final `return OP_UNKNOWN` — it cannot see any
    // loosening of the dispatcher. The contract that can regress is the
    // strict-prefix one (#723/#P1/#P2): a NEAR MISS of a real verb must
    // not dispatch as that verb. Each impostor below enters a live case
    // label and is stopped only by the guard named beside it.
    mustsend(fd, "sleep\r\n");          // memcmp(cmd, "stats", 5) guard
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
    mustsend(fd, "pexk 1\r\n");         // peek's cmd[2..4]=="ek " peek
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
    mustsend(fd, "kxck 1\r\n");         // kick's cmd[1..4]=="ick " peek
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
    mustsend(fd, "quitNOW\r\n");        // quit's exact cmd_len gate
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
    mustsend(fd, "reserve-xyz 1\r\n");  // reserve-* full strncmp
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
    mustsend(fd, "purge 1\r\n");        // 'p' branch, cmd[1] not u/e/a
    ckresp(fd, "UNKNOWN_COMMAND\r\n");

    // Every impostor left the connection usable: a `quitNOW` that had
    // reached OP_QUIT would have closed it, and the next command would
    // die on the read instead of answering.
    mustsend(fd, "use survivor\r\n");
    ckresp(fd, "USING survivor\r\n");
}

void
cttest_too_long_commandline()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    int i;
    for (i = 0; i < 10; i++)
        mustsend(fd, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); // 50 bytes
    mustsend(fd, "\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    // Issue another command and check that reponse is not "UNKNOWN_COMMAND"
    // as described in issue #337
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    // 502 bytes says nothing about WHERE the cliff is. protocol.txt:45
    // promises it at "224 bytes including \r\n", so pin both sides with
    // lines that differ only in length. An off-by-one or a raised limit
    // moves exactly one of these two replies.
    char line[512];
    memset(line, 'z', 222);
    line[222] = '\r';
    line[223] = '\n';
    line[224] = '\0';
    mustsend(fd, line);                 // 224 bytes: inside the limit
    ckresp(fd, "UNKNOWN_COMMAND\r\n");  // ...so it is PARSED, then rejected

    memset(line, 'z', 224);
    line[224] = '\r';
    line[225] = '\n';
    line[226] = '\0';
    mustsend(fd, line);                 // 226 bytes: over the limit
    ckresp(fd, "BAD_FORMAT\r\n");

    // ...and the stream is still in sync after the boundary case too.
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");
}

void
cttest_put_in_drain()
{
    enter_drain_mode(SIGUSR1);
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "x\r\n");
    ckresp(fd, "DRAINING\r\n");
}

void
cttest_peek_ok()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 1\r\n");
    ckresp(fd, "a\r\n");
}

void
cttest_peek_not_found()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "peek 2\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "peek 18446744073709551615\r\n");  // UINT64_MAX
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_peek_ok_unix()
{
    char *name = SERVER_UNIX();
    int fd = mustdialunix(name);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 1\r\n");
    ckresp(fd, "a\r\n");

    unlink(name);
}

void
cttest_unix_auto_removal()
{
    // Twice, to trigger autoremoval
    char *path = SERVER_UNIX();
    struct stat before;
    assertf(stat(path, &before) == 0,
            "first server must bind a socket at %s", path);
    assertf(S_ISSOCK(before.st_mode),
            "%s must be a socket, got mode 0%o",
            path, (unsigned)before.st_mode);
    kill_srvpid();

    // The socket file outlives the dead server; the next start must
    // REMOVE it and bind a fresh one.
    struct stat stale;
    assertf(stat(path, &stale) == 0,
            "the stale socket file must survive the killed server");

    path = SERVER_UNIX();
    struct stat after;
    assertf(stat(path, &after) == 0,
            "second server must leave a socket at %s", path);
    assertf(after.st_ino != stale.st_ino || after.st_dev != stale.st_dev,
            "stale socket was bound on top of, not removed: inode %llu "
            "unchanged", (unsigned long long)after.st_ino);

    // The body of this test used to be three calls and no assertion at
    // all: a second server that accepted nothing would have passed.
    // Dial the new socket and make it answer.
    int fd = mustdialunix(path);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 1\r\n");
    ckresp(fd, "a\r\n");

    unlink(path);
}

void
cttest_peek_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "peek 18446744073709551616\r\n"); // UINT64_MAX+1
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "peek 184467440737095516160000000000000000000000000000\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "peek foo111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "peek 111foo\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_peek_delayed()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "peek-delayed\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 99 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "put 0 1 1 1\r\n");
    mustsend(fd, "C\r\n");
    ckresp(fd, "INSERTED 3\r\n");

    mustsend(fd, "peek-delayed\r\n");
    ckresp(fd, "FOUND 3 1\r\n");
    ckresp(fd, "C\r\n");

    mustsend(fd, "delete 3\r\n");
    ckresp(fd, "DELETED\r\n");

    mustsend(fd, "peek-delayed\r\n");
    ckresp(fd, "FOUND 2 1\r\n");
    ckresp(fd, "B\r\n");

    mustsend(fd, "delete 2\r\n");
    ckresp(fd, "DELETED\r\n");

    mustsend(fd, "peek-delayed\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_peek_buried_kick()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    // cannot bury unreserved job
    mustsend(fd, "bury 1 0\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "peek-buried\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    mustsend(fd, "reserve-with-timeout 0\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "A\r\n");

    // now we can bury
    mustsend(fd, "bury 1 0\r\n");
    ckresp(fd, "BURIED\r\n");
    mustsend(fd, "peek-buried\r\n");
    ckresp(fd, "FOUND 1 1\r\n");
    ckresp(fd, "A\r\n");

    // kick and verify the job is ready
    mustsend(fd, "kick 1\r\n");
    ckresp(fd, "KICKED 1\r\n");
    mustsend(fd, "peek-buried\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "peek-ready\r\n");
    ckresp(fd, "FOUND 1 1\r\n");
    ckresp(fd, "A\r\n");

    // nothing is left to kick
    mustsend(fd, "kick 1\r\n");
    ckresp(fd, "KICKED 0\r\n");
}

void
cttest_touch_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "touch a111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "touch 111a\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "touch !@#!@#\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_touch_not_found()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "touch 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "touch 100000000000000\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_bury_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "bury 111abc 2\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "bury 111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "bury 111 222abc\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_kickjob_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "kick-job a111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "kick-job 111a\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "kick-job !@#!@#\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_kickjob_buried()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "A\r\n");
    mustsend(fd, "bury 1 0\r\n");
    ckresp(fd, "BURIED\r\n");

    mustsend(fd, "kick-job 100\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "kick-job 1\r\n");
    ckresp(fd, "KICKED\r\n");
    mustsend(fd, "kick-job 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_kickjob_delayed()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    // jid=1 - no delay, jid=2 - delay
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 10 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");

    mustsend(fd, "kick-job 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
    mustsend(fd, "kick-job 2\r\n");
    ckresp(fd, "KICKED\r\n");
    mustsend(fd, "kick-job 2\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_pause()
{
    int64 s;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 1\r\n");
    mustsend(fd, "x\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    s = nanoseconds();
    mustsend(fd, "pause-tube default 1\r\n");
    ckresp(fd, "PAUSED\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "x\r\n");
    assert(nanoseconds() - s >= 1000000000); // 1s
}

void
cttest_underscore()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "use x_y\r\n");
    ckresp(fd, "USING x_y\r\n");

    // The 'u' branch is a single TEST_CMD; replaced by an unconditional
    // `return OP_USE` the line above still passes. A near miss must not.
    mustsend(fd, "uXX x_y\r\n");
    ckresp(fd, "UNKNOWN_COMMAND\r\n");

    // The other half of the contract is the tube-name charset: one legal
    // character proves nothing about the rejected ones. A name may not
    // begin with '-', may not be empty, and may hold only the documented
    // characters — trailing garbage after a valid run is not a name.
    mustsend(fd, "use -lead\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "use a*b\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "use \r\n");
    ckresp(fd, "BAD_FORMAT\r\n");

    // ...while every documented punctuation character must be accepted,
    // not just the underscore.
    mustsend(fd, "use a+b/c;d.e$f_g()-2\r\n");
    ckresp(fd, "USING a+b/c;d.e$f_g()-2\r\n");
}

void
cttest_2cmdpacket()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "use a\r\nuse b\r\n");
    ckresp(fd, "USING a\r\n");
    ckresp(fd, "USING b\r\n");

    // The hostile half of the same code path: a command SPLIT across
    // segments. scan_line_end resumes from prev_read-1 precisely so a
    // boundary-straddling line still terminates; a single well-formed
    // packet can never exercise that.
    mustsend(fd, "use spl");
    mustsend(fd, "it\r\n");
    ckresp(fd, "USING split\r\n");

    // Worst split of all: \r and \n in different segments.
    mustsend(fd, "use halved\r");
    mustsend(fd, "\n");
    ckresp(fd, "USING halved\r\n");

    // One segment, three DIFFERENT verbs: with only 'use' in the packet
    // a which_cmd regression on any other branch stays invisible here.
    mustsend(fd, "watch w1\r\nlist-tube-used\r\nuse w2\r\n");
    ckresp(fd, "WATCHING 2\r\n");
    ckresp(fd, "USING halved\r\n");
    ckresp(fd, "USING w2\r\n");
}

void
cttest_too_big()
{
    job_data_size_limit = 10;
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 11\r\n");
    mustsend(fd, "delete 9999\r\n");
    mustsend(fd, "put 0 0 0 1\r\n");
    mustsend(fd, "x\r\n");
    ckresp(fd, "JOB_TOO_BIG\r\n");
    ckresp(fd, "INSERTED 1\r\n");
}

void
cttest_job_size_invalid()
{
    job_data_size_limit = JOB_DATA_SIZE_LIMIT_MAX;
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 4294967296\r\n");
    mustsend(fd, "put 0 0 0 10b\r\n");
    mustsend(fd, "put 0 0 0 --!@#$%^&&**()0b\r\n");
    mustsend(fd, "put 0 0 0 1\r\n");
    mustsend(fd, "x\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    ckresp(fd, "INSERTED 1\r\n");
}

void
cttest_job_size_max_plus_1()
{
    /* verify that server reject the job larger than maximum allowed. */
    job_data_size_limit = JOB_DATA_SIZE_LIMIT_MAX;
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 1073741825\r\n");

    enum { len = 1024*1024 };
    char body[len+1];
    memset(body, 'a', len);
    body[len] = 0;

    int i;
    for (i=0; i<JOB_DATA_SIZE_LIMIT_MAX; i+=len) {
        mustsend(fd, body);
    }
    mustsend(fd, "x");
    mustsend(fd, "\r\n");
    ckresp(fd, "JOB_TOO_BIG\r\n");
}

void
cttest_delete_ready()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
}

void
cttest_delete_reserved_by_other()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    int o = mustdiallocal(port);
    mustsend(o, "reserve\r\n");
    ckresp(o, "RESERVED 1 1\r\n");
    ckresp(o, "a\r\n");

    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_delete_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "delete 18446744073709551616\r\n"); // UINT64_MAX+1
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "delete 184467440737095516160000000000000000000000000000\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "delete foo111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "delete 111foo\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_multi_tube()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "use abc\r\n");
    ckresp(fd, "USING abc\r\n");
    mustsend(fd, "put 999999 0 0 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "use def\r\n");
    ckresp(fd, "USING def\r\n");
    mustsend(fd, "put 99 0 0 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "watch abc\r\n");
    ckresp(fd, "WATCHING 2\r\n");
    mustsend(fd, "watch def\r\n");
    ckresp(fd, "WATCHING 3\r\n");
    // With multi-tube watch, reserve scans tubes in order.
    // First ready tube is abc (added first), so job 1 is reserved.
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 0\r\n");
    // The job body follows the RESERVED line — job 1 was put with a
    // zero-length body, so a bare CRLF. Leaving it in the socket
    // desynchronises every later exchange on this connection.
    ckresp(fd, "\r\n");

    // Two tubes holding one job each cannot exercise ordering WITHIN a
    // tube, so a ready heap rebuilt as pure FIFO (priority ignored
    // outright) leaves everything above green. protocol.txt:218-220:
    // "beanstalkd will choose the one with the smallest priority value.
    // Within each priority, it will choose the one that was received
    // first." Put four jobs in one tube, out of priority order, and
    // walk the whole promise: priority first, FIFO inside a priority.
    mustsend(fd, "use ghi\r\n");
    ckresp(fd, "USING ghi\r\n");
    mustsend(fd, "put 700 0 100 1\r\n"); // id 3: lowest priority
    mustsend(fd, "d\r\n");
    ckresp(fd, "INSERTED 3\r\n");
    mustsend(fd, "put 10 0 100 1\r\n");  // id 4: most urgent
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 4\r\n");
    mustsend(fd, "put 300 0 100 1\r\n"); // id 5: middle, arrived first
    mustsend(fd, "b\r\n");
    ckresp(fd, "INSERTED 5\r\n");
    mustsend(fd, "put 300 0 100 1\r\n"); // id 6: same priority, later
    mustsend(fd, "c\r\n");
    ckresp(fd, "INSERTED 6\r\n");

    mustsend(fd, "watch ghi\r\n");
    ckresp(fd, "WATCHING 4\r\n");
    mustsend(fd, "ignore abc\r\n");
    ckresp(fd, "WATCHING 3\r\n");
    mustsend(fd, "ignore def\r\n");
    ckresp(fd, "WATCHING 2\r\n");
    mustsend(fd, "ignore default\r\n");
    ckresp(fd, "WATCHING 1\r\n");

    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 4 1\r\n");      // pri 10 beats every later put
    ckresp(fd, "a\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 5 1\r\n");      // pri 300, the earlier of the pair
    ckresp(fd, "b\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 6 1\r\n");      // pri 300, arrived second
    ckresp(fd, "c\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 3 1\r\n");      // pri 700 last, though put first
    ckresp(fd, "d\r\n");
}

void
cttest_negative_delay()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 512 -1 100 0\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

/* TODO: add more edge cases tests for delay and ttr */

void
cttest_garbage_priority()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put -1kkdj9djjkd9 0 100 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_negative_priority()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put -1 0 100 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_max_priority()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 4294967295 0 100 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");
}

void
cttest_too_big_priority()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 4294967296 0 100 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_omit_time_left()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 5 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ntime-left: 0\n");

    // protocol.txt:483-485 makes time-left meaningful for exactly two
    // states — reserved and delayed — and this test reaches neither.
    // Reporting 0 for a job that really is counting down is the
    // regression the field can suffer; a ready job's 0 cannot see it.
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "a\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: reserved\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    cknotsub(fd, "\ntime-left: 0\n");

    mustsend(fd, "put 0 9 5 1\r\n");
    mustsend(fd, "b\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "stats-job 2\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: delayed\n");
    mustsend(fd, "stats-job 2\r\n");
    ckrespsub(fd, "OK ");
    cknotsub(fd, "\ntime-left: 0\n");
}

void
cttest_small_delay()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 1 1 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");
}

void
cttest_delayed_to_ready()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    int64 put_at = nanoseconds();
    mustsend(fd, "put 0 1 1 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-ready: 0\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 1\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ntotal-jobs: 1\n");

    // Lower bound. "delay 1" promises the job is NOT ready before the
    // second is up; the checks above run at t≈0 and the ones below at
    // t≈1.01s, so promoting the job 100ms early is invisible to both.
    // Probe at 0.93s measured from the put, not from here.
    sleep_until(put_at + 930000000LL);
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 1\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-ready: 0\n");

    usleep(1010000); // 1.01 sec

    // check that after 1 sec the delayed job is ready again

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-ready: 1\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 0\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ntotal-jobs: 1\n");
}

void
cttest_statsjob_ck_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "stats-job 111ABC\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "stats-job 111 222\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "stats-job 111\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_stats_tube()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "use tubea\r\n");
    ckresp(fd, "USING tubea\r\n");
    mustsend(fd, "put 0 0 0 1\r\n");
    mustsend(fd, "x\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");

    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nname: \"tubea\"\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-urgent: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-ready: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-reserved: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-buried: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ntotal-jobs: 1\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-using: 1\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-watching: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-waiting: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncmd-delete: 1\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncmd-pause-tube: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\npause: 0\n");
    mustsend(fd, "stats-tube tubea\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\npause-time-left: 0\n");

    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nname: \"default\"\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-urgent: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-ready: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-reserved: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-buried: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ntotal-jobs: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-using: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-watching: 1\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-waiting: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncmd-delete: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncmd-pause-tube: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\npause: 0\n");
    mustsend(fd, "stats-tube default\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\npause-time-left: 0\n");

    // Bare "stats" is sent nowhere else in this file, so the exact
    // cmd_len==7 gate that separates OP_STATS from "stats-job" /
    // "stats-tube" has no guard here at all. These three fields exist
    // ONLY in the global block: current-jobs-delayed is prot.c's
    // delayed_ct, current-tubes is tubes.len.
    mustsend(fd, "stats\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-jobs-delayed: 0\n");
    mustsend(fd, "stats\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncmd-delete: 1\n");
    mustsend(fd, "stats\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\ncurrent-tubes: 2\n");

    // Near-miss verbs on the same 's' branch. protocol.txt (fork note,
    // "Wire-observable differences") promises that "command dispatch
    // matches literal prefixes strictly"; the #P1 fix tightened only
    // the leading "stats". A verb that is not the literal "stats-tube "
    // must not reach the stats-tube handler and hand back a tube's
    // stats block.
    mustsend(fd, "stats-tuba tubea\r\n");
    ckresp(fd, "UNKNOWN_COMMAND\r\n");
}

void
cttest_ttrlarge()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 120 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 0 4294 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "put 0 0 4295 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 3\r\n");
    mustsend(fd, "put 0 0 4296 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 4\r\n");
    mustsend(fd, "put 0 0 4297 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 5\r\n");
    mustsend(fd, "put 0 0 5000 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 6\r\n");
    mustsend(fd, "put 0 0 21600 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 7\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 120\n");
    mustsend(fd, "stats-job 2\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 4294\n");
    mustsend(fd, "stats-job 3\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 4295\n");
    mustsend(fd, "stats-job 4\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 4296\n");
    mustsend(fd, "stats-job 5\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 4297\n");
    mustsend(fd, "stats-job 6\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 5000\n");
    mustsend(fd, "stats-job 7\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 21600\n");
}

void
cttest_ttr_small()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nttr: 1\n");
}

void
cttest_zero_delay()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 1 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");
}

void
cttest_reserve_with_timeout_2conns()
{
    int fd0, fd1;

    job_data_size_limit = 10;

    int port = SERVER();
    fd0 = mustdiallocal(port);
    fd1 = mustdiallocal(port);
    mustsend(fd0, "watch foo\r\n");
    ckresp(fd0, "WATCHING 2\r\n");
    int64 began = nanoseconds();
    mustsend(fd0, "reserve-with-timeout 1\r\n");
    mustsend(fd1, "watch foo\r\n");
    ckresp(fd1, "WATCHING 2\r\n");
    timeout = 1100000000; // 1.1s
    ckresp(fd0, "TIMED_OUT\r\n");
    // The read timeout above is only an UPPER bound: firing the reserve
    // timeout after 100ms also produces TIMED_OUT. protocol.txt:220-224
    // promises the timeout LIMITS how long the client blocks, so a
    // requested 1s must not expire before 1s has actually passed.
    int64 waited = nanoseconds() - began;
    assertf(waited >= 1000000000LL,
            "reserve-with-timeout 1 fired after %lld ns, before the "
            "requested 1s", (long long)waited);
}

void
cttest_reserve_ttr_deadline_soon()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "reserve-with-timeout 1\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "a\r\n");

    // After 0.2s the job should be still reserved.
    usleep(200000);
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: reserved\n");

    mustsend(fd, "reserve-with-timeout 1\r\n");
    ckresp(fd, "DEADLINE_SOON\r\n");

    // Job should be reserved; last "reserve" took less than 1s.
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: reserved\n");

    // We don't want to process the job, so release it and check that it's ready.
    mustsend(fd, "release 1 0 0\r\n");
    ckresp(fd, "RELEASED\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: ready\n");
}

void
cttest_reserve_job_ttr_deadline_soon()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    mustsend(fd, "put 0 5 1 1\r\n");
    mustsend(fd, "a\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: delayed\n");

    mustsend(fd, "reserve-job 1\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "a\r\n");

    // After 0.1s the job should be still reserved.
    usleep(100000);
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: reserved\n");

    // Reservation made with reserve-job should behave the same way as other
    // reserve commands, e.g. produce "deadline soon" message, and get released
    // when ttr ends.
    mustsend(fd, "reserve-with-timeout 1\r\n");
    ckresp(fd, "DEADLINE_SOON\r\n");

    // Job should be reserved; last "reserve" took less than 1s.
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: reserved\n");

    // We are not able to process the job in time. Check that it gets released.
    // The job was in delayed state. It becomes ready when it gets auto-released.
    usleep(1000000); // 1.0s
    // put a dummy job
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    // check that ID=1 gets released
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: ready\n");
}

void
cttest_reserve_job_already_reserved()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    mustsend(fd, "reserve-job 1\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "A\r\n");

    // Job should not be reserved twice.
    mustsend(fd, "reserve-job 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_reserve_job_ready()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");

    mustsend(fd, "reserve-job 2\r\n");
    ckresp(fd, "RESERVED 2 1\r\n");
    ckresp(fd, "B\r\n");

    // Non-existing job.
    mustsend(fd, "reserve-job 3\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    // id=1 was not reserved.
    mustsend(fd, "release 1 1 0\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    mustsend(fd, "release 2 1 0\r\n");
    ckresp(fd, "RELEASED\r\n");
}

void
cttest_reserve_job_delayed()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    mustsend(fd, "put 0 100 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 100 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "put 0 100 1 1\r\n");
    mustsend(fd, "C\r\n");
    ckresp(fd, "INSERTED 3\r\n");

    mustsend(fd, "reserve-job 2\r\n");
    ckresp(fd, "RESERVED 2 1\r\n");
    ckresp(fd, "B\r\n");

    mustsend(fd, "release 2 1 0\r\n");
    ckresp(fd, "RELEASED\r\n");

    // verify that job was released in ready state.
    mustsend(fd, "stats-job 2\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: ready\n");
}

void
cttest_reserve_job_buried()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    // put, reserve and bury
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "A\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "reserve-job 1\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "A\r\n");
    mustsend(fd, "bury 1 1\r\n");
    ckresp(fd, "BURIED\r\n");

    // put, reserve and bury
    mustsend(fd, "put 0 0 1 1\r\n");
    mustsend(fd, "B\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "reserve-job 2\r\n");
    ckresp(fd, "RESERVED 2 1\r\n");
    ckresp(fd, "B\r\n");
    mustsend(fd, "bury 2 1\r\n");
    ckresp(fd, "BURIED\r\n");

    // reserve by ids
    mustsend(fd, "reserve-job 2\r\n");
    ckresp(fd, "RESERVED 2 1\r\n");
    ckresp(fd, "B\r\n");
    mustsend(fd, "reserve-job 1\r\n");
    ckresp(fd, "RESERVED 1 1\r\n");
    ckresp(fd, "A\r\n");

    // release back and check if jobs are ready.
    mustsend(fd, "release 1 1 0\r\n");
    ckresp(fd, "RELEASED\r\n");
    mustsend(fd, "release 2 1 0\r\n");
    ckresp(fd, "RELEASED\r\n");
    mustsend(fd, "stats-job 1\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: ready\n");
    mustsend(fd, "stats-job 2\r\n");
    ckrespsub(fd, "OK ");
    ckrespsub(fd, "\nstate: ready\n");

}

void
cttest_release_bad_format()
{
    int port = SERVER();
    int fd = mustdiallocal(port);

    // bad id
    mustsend(fd, "release 18446744073709551616 1 1\r\n"); // UINT64_MAX+1
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "release 184467440737095516160000000000000000000000000000 1 1\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "release foo111\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
    mustsend(fd, "release 111foo\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");

    // bad priority
    mustsend(fd, "release 18446744073709551615 abc 1\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");

    // bad duration
    mustsend(fd, "release 18446744073709551615 1 abc\r\n");
    ckresp(fd, "BAD_FORMAT\r\n");
}

void
cttest_release_not_found()
{
    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "release 1 1 1\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_close_releases_job()
{
    int port = SERVER();
    int cons = mustdiallocal(port);
    int prod = mustdiallocal(port);
    mustsend(cons, "reserve-with-timeout 1\r\n");

    mustsend(prod, "put 0 0 100 1\r\n");
    mustsend(prod, "a\r\n");
    ckresp(prod, "INSERTED 1\r\n");

    ckresp(cons, "RESERVED 1 1\r\n");
    ckresp(cons, "a\r\n");

    mustsend(prod, "stats-job 1\r\n");
    ckrespsub(prod, "OK ");
    ckrespsub(prod, "\nstate: reserved\n");

    // Closed consumer connection should make the job ready sooner than ttr=100.
    close(cons);

    // Job should be released in less than 1s. It is not instantly;
    // we do not make guarantees about how soon jobs should be released.
    mustsend(prod, "reserve-with-timeout 1\r\n");
    ckresp(prod, "RESERVED 1 1\r\n");
    ckresp(prod, "a\r\n");
}

void
cttest_quit_releases_job()
{
    // This test is similar to the close_releases_job test, except that
    // connection is not closed, but command quit is sent.
    int port = SERVER();
    int cons = mustdiallocal(port);
    int prod = mustdiallocal(port);
    mustsend(cons, "reserve-with-timeout 1\r\n");

    mustsend(prod, "put 0 0 100 1\r\n");
    mustsend(prod, "a\r\n");
    ckresp(prod, "INSERTED 1\r\n");

    ckresp(cons, "RESERVED 1 1\r\n");
    ckresp(cons, "a\r\n");

    mustsend(prod, "stats-job 1\r\n");
    ckrespsub(prod, "OK ");
    ckrespsub(prod, "\nstate: reserved\n");

    // Everything up to here is what cttest_close_releases_job already
    // gets from closing the socket. The part specific to `quit` is its
    // exact-length gate: without it "quitXYZ\r\n" silently closes a live
    // connection (#723). The impostor must be rejected AND leave the
    // reservation standing.
    mustsend(cons, "quitTHIS\r\n");
    ckresp(cons, "UNKNOWN_COMMAND\r\n");
    mustsend(prod, "stats-job 1\r\n");
    ckrespsub(prod, "OK ");
    ckrespsub(prod, "\nstate: reserved\n");

    // Quitting consumer should make the job ready sooner than ttr=100.
    mustsend(cons, "quit\r\n");

    // Job should be released in less than 1s. It is not instantly;
    // we do not make guarantees about how soon jobs should be released.
    mustsend(prod, "reserve-with-timeout 1\r\n");
    ckresp(prod, "RESERVED 1 1\r\n");
    ckresp(prod, "a\r\n");

    // ...and `quit` must actually CLOSE the connection, which nothing in
    // this test observed: a quit that merely released the job and kept
    // the socket open passed everything above.
    ckeof(cons);
}

void
cttest_unpause_tube()
{
    int fd0, fd1;

    int port = SERVER();
    fd0 = mustdiallocal(port);
    fd1 = mustdiallocal(port);

    mustsend(fd0, "put 0 0 0 0\r\n");
    mustsend(fd0, "\r\n");
    ckresp(fd0, "INSERTED 1\r\n");

    mustsend(fd0, "pause-tube default 86400\r\n");
    ckresp(fd0, "PAUSED\r\n");

    mustsend(fd1, "reserve\r\n");

    mustsend(fd0, "pause-tube default 0\r\n");
    ckresp(fd0, "PAUSED\r\n");

    // ckresp will time out if this takes too long, so the
    // test will not pass.
    ckresp(fd1, "RESERVED 1 0\r\n");
    ckresp(fd1, "\r\n");
}

void
cttest_list_tube()
{
    int port = SERVER();
    int fd0 = mustdiallocal(port);

    mustsend(fd0, "watch w\r\n");
    ckresp(fd0, "WATCHING 2\r\n");

    mustsend(fd0, "use u\r\n");
    ckresp(fd0, "USING u\r\n");

    mustsend(fd0, "list-tubes\r\n");
    ckrespsub(fd0, "OK ");
    ckresp(fd0,
           "---\n"
           "- default\n"
           "- w\n"
           "- u\n\r\n");

    mustsend(fd0, "list-tube-used\r\n");
    ckresp(fd0, "USING u\r\n");

    mustsend(fd0, "list-tubes-watched\r\n");
    ckrespsub(fd0, "OK ");
    ckresp(fd0,
           "---\n"
           "- default\n"
           "- w\n\r\n");

    mustsend(fd0, "ignore default\r\n");
    ckresp(fd0, "WATCHING 1\r\n");

    mustsend(fd0, "list-tubes-watched\r\n");
    ckrespsub(fd0, "OK ");
    ckresp(fd0,
           "---\n"
           "- w\n\r\n");

    mustsend(fd0, "ignore w\r\n");
    ckresp(fd0, "NOT_IGNORED\r\n");

    // Every command above is a literal verb, so the 'l' branch's strict
    // memcmp guard (#P2) is untouched: dropping it lets any 12-byte
    // command with cmd[9]=='s' dispatch as list-tubes and dump the tube
    // namespace this test has just built up.
    mustsend(fd0, "lisT-tubes\r\n");    // 12 bytes, cmd[9]=='s'
    ckresp(fd0, "UNKNOWN_COMMAND\r\n");
    mustsend(fd0, "l-ist-ubes\r\n");    // 12 bytes, cmd[9]=='s'
    ckresp(fd0, "UNKNOWN_COMMAND\r\n");
    mustsend(fd0, "list-tubesX\r\n");   // right prefix, wrong length
    ckresp(fd0, "UNKNOWN_COMMAND\r\n");

    // Positive control on the same branch: the real verb still answers,
    // so the guard rejects impostors without shadowing the command.
    mustsend(fd0, "list-tube-used\r\n");
    ckresp(fd0, "USING u\r\n");
}

#define STRING_LEN_200  \
    "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" \
    "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"

void
cttest_use_tube_long()
{
    int port = SERVER();
    int fd0 = mustdiallocal(port);
    // 200 chars is okay
    mustsend(fd0, "use " STRING_LEN_200 "\r\n");
    ckresp(fd0, "USING " STRING_LEN_200 "\r\n");
    // 201 chars is too much
    mustsend(fd0, "use " STRING_LEN_200 "Z\r\n");
    ckresp(fd0, "BAD_FORMAT\r\n");
}

void
cttest_longest_command()
{
    int port = SERVER();
    int fd0 = mustdiallocal(port);
    mustsend(fd0, "use " STRING_LEN_200 "\r\n");
    ckresp(fd0, "USING " STRING_LEN_200 "\r\n");
    mustsend(fd0, "pause-tube " STRING_LEN_200 " 4294967295\r\n");
    ckresp(fd0, "PAUSED\r\n");
}

void
cttest_binlog_empty_exit()
{
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    job_data_size_limit = 10;

    int port = SERVER();
    // SERVER() now returns only once the child is serving (mustforksrv's
    // ready pipe), so the kill below cannot land before walinit has made
    // the binlog this test is about.
    kill_srvpid();

    // The name promises something about the binlog after a clean exit,
    // and nothing about the binlog was ever looked at. A shutdown that
    // unlinks or truncates binlog.1 still lets the next server accept a
    // put — only a replay notices.
    char *b1 = fmtalloc("%s/binlog.1", ctdir());
    assertf(exist(b1),
            "a clean exit with an empty queue must still leave %s", b1);
    int hdr = filesize(b1);
    assertf(hdr >= (int)sizeof(int),
            "%s must carry at least the version header, got %d bytes",
            b1, hdr);

    port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 0 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    // The acked put must survive the SECOND clean exit: this is the
    // whole reason the WAL file has to be there and intact.
    kill_srvpid();
    assertf(exist(b1), "%s must survive the second clean exit", b1);

    port = SERVER();
    fd = mustdiallocal(port);
    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 0\r\n");
    ckresp(fd, "\r\n");
    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
    free(b1);
}

void
cttest_binlog_bury()
{
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    job_data_size_limit = 10;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 100 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 0\r\n");
    ckresp(fd, "\r\n");
    mustsend(fd, "bury 1 0\r\n");
    ckresp(fd, "BURIED\r\n");
}

void
cttest_binlog_basic()
{
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    job_data_size_limit = 10;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 100 0\r\n");
    mustsend(fd, "\r\n");
    ckresp(fd, "INSERTED 1\r\n");

    kill_srvpid();

    port = SERVER();
    fd = mustdiallocal(port);
    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
}

void
cttest_binlog_size_limit()
{
    int i = 0;
    int gotsize;

    size = 4096;
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.filesize = size;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;

    int port = SERVER();
    int fd = mustdiallocal(port);
    char *b2 = fmtalloc("%s/binlog.2", ctdir());
    while (!exist(b2)) {
        char *exp = fmtalloc("INSERTED %d\r\n", ++i);
        mustsend(fd, "put 0 0 100 50\r\n");
        mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
        ckresp(fd, exp);
        free(exp);
    }

    char *b1 = fmtalloc("%s/binlog.1", ctdir());
    gotsize = filesize(b1);
    assertf(gotsize == size, "binlog.1 %d != %d", gotsize, size);
    gotsize = filesize(b2);
    assertf(gotsize == size, "binlog.2 %d != %d", gotsize, size);
    free(b1);
    free(b2);
}

void
cttest_binlog_allocation()
{
    int i = 0;

    size = 601;
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.filesize = size;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;

    int port = SERVER();
    int fd = mustdiallocal(port);
    for (i = 1; i <= 96; i++) {
        char *exp = fmtalloc("INSERTED %d\r\n", i);
        mustsend(fd, "put 0 0 120 22\r\n");
        mustsend(fd, "job payload xxxxxxxxxx\r\n");
        ckresp(fd, exp);
        free(exp);
    }
    for (i = 1; i <= 96; i++) {
        char *exp = fmtalloc("delete %d\r\n", i);
        mustsend(fd, exp);
        ckresp(fd, "DELETED\r\n");
        free(exp);
    }
}

void
cttest_binlog_read()
{
    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "use test\r\n");
    ckresp(fd, "USING test\r\n");
    mustsend(fd, "put 0 0 120 4\r\n");
    mustsend(fd, "test\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 0 120 4\r\n");
    mustsend(fd, "tes1\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "watch test\r\n");
    ckresp(fd, "WATCHING 2\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 4\r\n");
    ckresp(fd, "test\r\n");
    mustsend(fd, "release 1 1 1\r\n");
    ckresp(fd, "RELEASED\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 2 4\r\n");
    ckresp(fd, "tes1\r\n");
    mustsend(fd, "delete 2\r\n");
    ckresp(fd, "DELETED\r\n");

    kill_srvpid();

    port = SERVER();
    fd = mustdiallocal(port);
    mustsend(fd, "watch test\r\n");
    ckresp(fd, "WATCHING 2\r\n");
    mustsend(fd, "reserve\r\n");
    ckresp(fd, "RESERVED 1 4\r\n");
    ckresp(fd, "test\r\n");
    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 2\r\n");
    ckresp(fd, "NOT_FOUND\r\n");
}

void
cttest_binlog_disk_full()
{
    // v8 binlog records carry a 4-byte CRC32C trailer, so per-put
    // reservation grew by 8 bytes (full + delete). Bumping filesize
    // keeps the expected "4 puts per file" allocation pattern.
    size = 1080;
    falloc = wrapfalloc;
    fallocpat[0] = 1;
    fallocpat[2] = 1;

    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.filesize = size;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 3\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 4\r\n");

    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "OUT_OF_MEMORY\r\n");

    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 6\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 7\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 8\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 9\r\n");

    // Everything so far is in-memory: a failed falloc that left a
    // half-written record behind in binlog.1 replies exactly the same.
    // Restart and replay — that is the only thing that reads the bytes
    // the ENOSPC episode wrote. rawfalloc is restored first so the
    // fresh server can open its own files.
    falloc = rawfalloc;
    // Replay reserves a delete record per surviving job; give the fresh
    // server room to allocate for them instead of re-entering ENOSPC.
    srv.wal.filesize = 32768;
    kill_srvpid();
    port = SERVER();
    fd = mustdiallocal(port);

    // Every acked job survived...
    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 50\r\n");
    ckresp(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    mustsend(fd, "peek 9\r\n");
    ckresp(fd, "FOUND 9 50\r\n");
    ckresp(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    // ...and the put that answered OUT_OF_MEMORY left no ghost job.
    mustsend(fd, "peek 5\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 2\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 3\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 4\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 6\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 7\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 8\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 9\r\n");
    ckresp(fd, "DELETED\r\n");
}

void
cttest_binlog_disk_full_delete()
{
    // v8 binlog records carry a 4-byte CRC32C trailer; see note in
    // cttest_binlog_disk_full above.
    size = 1080;
    falloc = wrapfalloc;
    fallocpat[0] = 1;
    fallocpat[1] = 1;

    srv.wal.dir = ctdir();
    srv.wal.use = 1;
    srv.wal.filesize = size;
    srv.wal.syncrate = 0;
    srv.wal.wantsync = 1;

    int port = SERVER();
    int fd = mustdiallocal(port);
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 1\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 2\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 3\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 4\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 5\r\n");

    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 6\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 7\r\n");
    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "INSERTED 8\r\n");

    mustsend(fd, "put 0 0 100 50\r\n");
    mustsend(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    ckresp(fd, "OUT_OF_MEMORY\r\n");

    char *b1 = fmtalloc("%s/binlog.1", ctdir());
    assert(exist(b1));
    free(b1);

    // "the file exists" is the weakest possible check on a WAL. Restart
    // and replay it: a half-written record left by the failed falloc,
    // or a ghost record for the put that answered OUT_OF_MEMORY, only
    // shows on the way back in.
    falloc = rawfalloc;
    // Replay reserves a delete record per surviving job; give the fresh
    // server room to allocate for them instead of re-entering ENOSPC.
    srv.wal.filesize = 32768;
    kill_srvpid();
    port = SERVER();
    fd = mustdiallocal(port);

    mustsend(fd, "peek 1\r\n");
    ckresp(fd, "FOUND 1 50\r\n");
    ckresp(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    mustsend(fd, "peek 8\r\n");
    ckresp(fd, "FOUND 8 50\r\n");
    ckresp(fd, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n");
    mustsend(fd, "peek 9\r\n");
    ckresp(fd, "NOT_FOUND\r\n");

    mustsend(fd, "delete 1\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 2\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 3\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 4\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 5\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 6\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 7\r\n");
    ckresp(fd, "DELETED\r\n");
    mustsend(fd, "delete 8\r\n");
    ckresp(fd, "DELETED\r\n");
}

static void
bench_put_delete_size(int n, int bodylen, int walsize, int sync, int64 syncrate_ms)
{
    if (walsize > 0) {
        srv.wal.dir = ctdir();
        srv.wal.use = 1;
        srv.wal.filesize = walsize;
        srv.wal.syncrate = syncrate_ms * 1000000;
        srv.wal.wantsync = sync;
    }

    job_data_size_limit = JOB_DATA_SIZE_LIMIT_MAX;
    int port = SERVER();
    int fd = mustdiallocal(port);
    char buf[50], put[50];
    char body[bodylen+1];
    memset(body, 'a', bodylen);
    body[bodylen] = 0;
    ctsetbytes(bodylen);
    sprintf(put, "put 0 0 0 %d\r\n", bodylen);
    ctresettimer();
    int i;
    for (i = 0; i < n; i++) {
        mustsend(fd, put);
        mustsend(fd, body);
        mustsend(fd, "\r\n");
        ckrespsub(fd, "INSERTED ");
        sprintf(buf, "delete %d\r\n", i + 1);
        mustsend(fd, buf);
        ckresp(fd, "DELETED\r\n");
    }
    ctstoptimer();
}

void
ctbench_put_delete_0008(int n)
{
    bench_put_delete_size(n, 8, 0, 0, 0);
}

void
ctbench_put_delete_1024(int n)
{
    bench_put_delete_size(n, 1024, 0, 0, 0);
}

void
ctbench_put_delete_8192(int n)
{
    bench_put_delete_size(n, 8192, 0, 0, 0);
}

void
ctbench_put_delete_81920(int n)
{
    bench_put_delete_size(n, 81920, 0, 0, 0);
}

void
ctbench_put_delete_wal_1024_fsync_000ms(int n)
{
    bench_put_delete_size(n, 1024, 512000, 1, 0);
}

void
ctbench_put_delete_wal_1024_fsync_050ms(int n)
{
    bench_put_delete_size(n, 1024, 512000, 1, 50);
}

void
ctbench_put_delete_wal_1024_fsync_200ms(int n)
{
    bench_put_delete_size(n, 1024, 512000, 1, 200);
}

void
ctbench_put_delete_wal_1024_no_fsync(int n)
{
    bench_put_delete_size(n, 1024, 512000, 0, 0);
}

void
ctbench_put_delete_wal_8192_fsync_000ms(int n)
{
    bench_put_delete_size(n, 8192, 512000, 1, 0);
}

void
ctbench_put_delete_wal_8192_fsync_050ms(int n)
{
    bench_put_delete_size(n, 8192, 512000, 1, 50);
}

void
ctbench_put_delete_wal_8192_fsync_200ms(int n)
{
    bench_put_delete_size(n, 8192, 512000, 1, 200);
}

void
ctbench_put_delete_wal_8192_no_fsync(int n)
{
    bench_put_delete_size(n, 8192, 512000, 0, 0);
}
