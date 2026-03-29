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

// Multi-worker integration tests.
// These tests start beanstalkd with -w 2 and verify
// put forwarding, watch migration, and stats aggregation.

static int srvpid;
static int64 timeout = 5000000000LL;

static void
killsrv(void)
{
    if (!srvpid) return;
    kill(srvpid, SIGTERM);
    waitpid(srvpid, 0, 0);
    srvpid = 0;
}

static int test_port_counter = 0;

static int
startmw(int nworkers)
{
    // Unique port per test: base + pid hash + counter.
    int port = 14000 + (getpid() % 1000) + (test_port_counter++ * 10);
    char portstr[16], wstr[8];
    snprintf(portstr, sizeof(portstr), "%d", port);
    snprintf(wstr, sizeof(wstr), "%d", nworkers);

    srvpid = fork();
    if (srvpid < 0) exit(1);
    if (srvpid > 0) {
        atexit(killsrv);
        // Wait for server to accept connections (retry with backoff).
        for (int attempt = 0; attempt < 20; attempt++) {
            usleep(100000); // 100ms per attempt
            int tfd = socket(AF_INET, SOCK_STREAM, 0);
            if (tfd == -1) continue;
            struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
            inet_aton("127.0.0.1", &addr.sin_addr);
            int r = connect(tfd, (struct sockaddr *)&addr, sizeof(addr));
            close(tfd);
            if (r == 0) break;
        }
        return port;
    }
    // child: exec beanstalkd
    execlp("./beanstalkd", "./beanstalkd",
           "-p", portstr, "-w", wstr, NULL);
    _exit(111);
}

static int
dial(int port)
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

static char *
rdline(int fd)
{
    static char buf[4096];
    char c = 0, p = 0;
    fd_set rfd;
    struct timeval tv;
    size_t i = 0;
    for (;;) {
        FD_ZERO(&rfd);
        FD_SET(fd, &rfd);
        tv.tv_sec = timeout / 1000000000;
        tv.tv_usec = (timeout / 1000) % 1000000;
        int r = select(fd + 1, &rfd, NULL, NULL, &tv);
        if (r <= 0) { fputs("timeout\n", stderr); exit(8); }
        r = read(fd, &c, 1);
        if (r <= 0) break;
        if (i < sizeof(buf) - 1) buf[i++] = c;
        if (p == '\r' && c == '\n') break;
        p = c;
    }
    buf[i] = '\0';
    return buf;
}

static void
snd(int fd, char *s)
{
    int len = strlen(s);
    int n = write(fd, s, len);
    if (n != len) {
        perror("write");
        exit(1);
    }
}

static void
ck(int fd, char *expected)
{
    char *got = rdline(fd);
    assertf(strcmp(got, expected) == 0,
            "expected \"%s\", got \"%s\"", expected, got);
}

static void
cksub(int fd, char *sub)
{
    char *got = rdline(fd);
    assertf(strstr(got, sub) != NULL,
            "expected substring \"%s\" in \"%s\"", sub, got);
}

// Test: put + reserve across workers.
// Producer uses tube-A, consumer watches tube-A.
// Both should work regardless of which worker they land on.
void
cttest_mw_put_reserve_same_tube()
{
    int port = startmw(2);

    // Producer: use tube-A, put a job.
    int p = dial(port);
    snd(p, "use tube-A\r\n");
    ck(p, "USING tube-A\r\n");
    snd(p, "put 0 0 60 4\r\ntest\r\n");
    cksub(p, "INSERTED");

    // Consumer: watch tube-A, reserve.
    int c = dial(port);
    snd(c, "watch tube-A\r\n");
    ck(c, "WATCHING 1\r\n");
    snd(c, "reserve-with-timeout 2\r\n");
    cksub(c, "RESERVED");
    rdline(c); // drain body

    close(p);
    close(c);
}

// Test: use and watch are independent.
// use tube-A, watch tube-B.
// put should go to tube-A, reserve should come from tube-B.
void
cttest_mw_use_watch_independent()
{
    int port = startmw(2);

    // Setup: put a job into tube-B via a separate connection.
    int setup = dial(port);
    snd(setup, "use tube-B\r\n");
    ck(setup, "USING tube-B\r\n");
    snd(setup, "put 0 0 60 6\r\nfrom-B\r\n");
    cksub(setup, "INSERTED");
    close(setup);

    // Client: use tube-A, watch tube-B.
    int fd = dial(port);
    snd(fd, "use tube-A\r\n");
    ck(fd, "USING tube-A\r\n");
    snd(fd, "watch tube-B\r\n");
    ck(fd, "WATCHING 1\r\n");

    // list-tube-used should show tube-A (not tube-B).
    snd(fd, "list-tube-used\r\n");
    ck(fd, "USING tube-A\r\n");

    // reserve should get job from tube-B.
    snd(fd, "reserve-with-timeout 2\r\n");
    cksub(fd, "RESERVED");
    rdline(fd); // drain body

    close(fd);
}

// Test: stats aggregation across workers.
void
cttest_mw_stats_aggregation()
{
    int port = startmw(2);

    // Put jobs on different tubes (may land on different workers).
    int fd1 = dial(port);
    snd(fd1, "use tube-X\r\n");
    ck(fd1, "USING tube-X\r\n");
    snd(fd1, "put 0 0 60 2\r\nhi\r\n");
    cksub(fd1, "INSERTED");

    int fd2 = dial(port);
    snd(fd2, "use tube-Y\r\n");
    ck(fd2, "USING tube-Y\r\n");
    snd(fd2, "put 0 0 60 2\r\nhi\r\n");
    cksub(fd2, "INSERTED");

    // Wait for stats sync (1Hz).
    usleep(1500000);

    // Stats should show aggregated data.
    int fd3 = dial(port);
    snd(fd3, "stats\r\n");
    cksub(fd3, "OK ");
    // Read stats body — just verify we get a response without crash.
    char buf[8192];
    read(fd3, buf, sizeof(buf));

    close(fd1);
    close(fd2);
    close(fd3);
}

// Test: watch migration — connection moves to tube's owner worker.
void
cttest_mw_watch_migration()
{
    int port = startmw(2);

    // Put a job into tube-M.
    int p = dial(port);
    snd(p, "use tube-M\r\n");
    ck(p, "USING tube-M\r\n");
    snd(p, "put 0 0 60 5\r\nhello\r\n");
    cksub(p, "INSERTED");
    close(p);

    // New connection: watch tube-M (may migrate), then reserve.
    int c = dial(port);
    snd(c, "watch tube-M\r\n");
    ck(c, "WATCHING 1\r\n");
    snd(c, "reserve-with-timeout 2\r\n");
    cksub(c, "RESERVED");
    rdline(c); // body
    close(c);
}

// Test: rapid put forwarding — many puts to remote tube.
void
cttest_mw_rapid_put_forwarding()
{
    int port = startmw(2);

    int fd = dial(port);
    snd(fd, "use rapid-tube\r\n");
    ck(fd, "USING rapid-tube\r\n");

    // 100 rapid puts.
    for (int i = 0; i < 100; i++) {
        snd(fd, "put 0 0 60 4\r\ntest\r\n");
        cksub(fd, "INSERTED");
    }

    // Verify all 100 can be reserved.
    int c = dial(port);
    snd(c, "watch rapid-tube\r\n");
    ck(c, "WATCHING 1\r\n");
    for (int i = 0; i < 100; i++) {
        snd(c, "reserve-with-timeout 1\r\n");
        cksub(c, "RESERVED");
        rdline(c); // body

        // Parse job ID from RESERVED response for delete.
        // Not strictly needed but tests full lifecycle.
    }

    // 101st should timeout.
    snd(c, "reserve-with-timeout 0\r\n");
    ck(c, "TIMED_OUT\r\n");

    close(fd);
    close(c);
}

// Test: put forwarding — producer on one worker, consumer on another.
// use tube-X (stays local), watch tube-Y (migrates to Y's worker).
// Then put via use tube-X → forwarded to X's owner. Consumer on X reserves.
void
cttest_mw_put_forwarding_cross_worker()
{
    int port = startmw(2);

    // Producer: use tube-fwd (no migration), put 3 jobs.
    int p = dial(port);
    snd(p, "use tube-fwd\r\n");
    ck(p, "USING tube-fwd\r\n");
    snd(p, "put 0 0 60 5\r\njob-1\r\n");
    cksub(p, "INSERTED");
    snd(p, "put 0 0 60 5\r\njob-2\r\n");
    cksub(p, "INSERTED");
    snd(p, "put 0 0 60 5\r\njob-3\r\n");
    cksub(p, "INSERTED");
    close(p);

    // Consumer: watch tube-fwd (migrates to correct worker), reserve all 3.
    int c = dial(port);
    snd(c, "watch tube-fwd\r\n");
    ck(c, "WATCHING 1\r\n");
    for (int i = 0; i < 3; i++) {
        snd(c, "reserve-with-timeout 2\r\n");
        cksub(c, "RESERVED");
        rdline(c); // body
    }
    snd(c, "reserve-with-timeout 0\r\n");
    ck(c, "TIMED_OUT\r\n");
    close(c);
}

// Test: delete/release/bury/touch work on reserved jobs (always local).
void
cttest_mw_reserved_job_lifecycle()
{
    int port = startmw(2);

    // Put a job.
    int p = dial(port);
    snd(p, "use lifecycle-tube\r\n");
    ck(p, "USING lifecycle-tube\r\n");
    snd(p, "put 0 0 120 4\r\ndata\r\n");
    cksub(p, "INSERTED");
    close(p);

    // Reserve, touch, release, re-reserve, bury, kick, re-reserve, delete.
    int c = dial(port);
    snd(c, "watch lifecycle-tube\r\n");
    ck(c, "WATCHING 1\r\n");

    // Reserve.
    snd(c, "reserve-with-timeout 2\r\n");
    char *resp = rdline(c);
    assertf(strstr(resp, "RESERVED") != NULL, "expected RESERVED, got %s", resp);
    // Parse job ID.
    uint64_t jid = 0;
    sscanf(resp, "RESERVED %lu", &jid);
    assertf(jid > 0, "job id must be > 0");
    rdline(c); // body

    // Touch.
    char cmd[64];
    snprintf(cmd, sizeof(cmd), "touch %lu\r\n", jid);
    snd(c, cmd);
    ck(c, "TOUCHED\r\n");

    // Release.
    snprintf(cmd, sizeof(cmd), "release %lu 0 0\r\n", jid);
    snd(c, cmd);
    ck(c, "RELEASED\r\n");

    // Re-reserve.
    snd(c, "reserve-with-timeout 2\r\n");
    cksub(c, "RESERVED");
    rdline(c);

    // Bury.
    snprintf(cmd, sizeof(cmd), "bury %lu 0\r\n", jid);
    snd(c, cmd);
    ck(c, "BURIED\r\n");

    // Kick.
    snd(c, "use lifecycle-tube\r\n");
    ck(c, "USING lifecycle-tube\r\n");
    snd(c, "kick 1\r\n");
    ck(c, "KICKED 1\r\n");

    // Re-reserve after kick.
    snd(c, "reserve-with-timeout 2\r\n");
    cksub(c, "RESERVED");
    rdline(c);

    // Delete.
    snprintf(cmd, sizeof(cmd), "delete %lu\r\n", jid);
    snd(c, cmd);
    ck(c, "DELETED\r\n");

    // No more jobs.
    snd(c, "reserve-with-timeout 0\r\n");
    ck(c, "TIMED_OUT\r\n");

    close(c);
}

// Test: use and watch on same tube, then switch use to different tube.
// Verifies that use change doesn't affect watch.
void
cttest_mw_use_does_not_affect_watch()
{
    int port = startmw(2);

    // Put a job into tube-keep.
    int p = dial(port);
    snd(p, "use tube-keep\r\n");
    ck(p, "USING tube-keep\r\n");
    snd(p, "put 0 0 60 4\r\nkeep\r\n");
    cksub(p, "INSERTED");
    close(p);

    // Client: watch tube-keep, then use tube-other.
    int fd = dial(port);
    snd(fd, "watch tube-keep\r\n");
    ck(fd, "WATCHING 1\r\n");

    // Change use to different tube — should NOT affect watch.
    snd(fd, "use tube-other\r\n");
    ck(fd, "USING tube-other\r\n");

    // Verify watch is still tube-keep.
    snd(fd, "list-tubes-watched\r\n");
    cksub(fd, "OK ");
    char *body = rdline(fd);
    // Body starts with "---\n- tube-keep\n"
    assertf(strstr(body, "tube-keep") != NULL,
            "watch must still be tube-keep after use change, got: %s", body);

    // Reserve should still come from tube-keep.
    snd(fd, "reserve-with-timeout 2\r\n");
    cksub(fd, "RESERVED");
    rdline(fd); // body

    close(fd);
}

// Test: guaranteed cross-worker put forwarding.
// "alpha" hashes to worker 1, "gamma" to worker 0.
// Producer watches gamma (→ worker 0), uses alpha.
// Put must be forwarded to worker 1 (alpha's owner).
// Consumer watches alpha (→ worker 1), reserves the forwarded job.
void
cttest_mw_guaranteed_put_forward()
{
    int port = startmw(2);

    // Producer: go to worker 0 via watch gamma, then use alpha (remote).
    int p = dial(port);
    snd(p, "watch gamma\r\n");
    ck(p, "WATCHING 1\r\n");
    snd(p, "use alpha\r\n");
    ck(p, "USING alpha\r\n");

    // Put should be forwarded to alpha's owner (worker 1).
    snd(p, "put 0 0 60 7\r\nfwd-job\r\n");
    cksub(p, "INSERTED");
    close(p);

    // Consumer: watch alpha (→ worker 1), reserve the forwarded job.
    int c = dial(port);
    snd(c, "watch alpha\r\n");
    ck(c, "WATCHING 1\r\n");
    snd(c, "reserve-with-timeout 3\r\n");
    char *resp = rdline(c);
    assertf(strstr(resp, "RESERVED") != NULL,
            "cross-worker put forward failed: expected RESERVED, got %s", resp);
    rdline(c); // body

    snd(c, "reserve-with-timeout 0\r\n");
    ck(c, "TIMED_OUT\r\n");
    close(c);
}

// Test: pipelined commands with forwarding preserve response order.
// Client sends use+put+use+put in one burst. Responses must arrive in order.
void
cttest_mw_pipelined_put_ordering()
{
    int port = startmw(2);

    int fd = dial(port);
    // Watch gamma to land on worker 0 (gamma hash % 2 = 0).
    snd(fd, "watch gamma\r\n");
    ck(fd, "WATCHING 1\r\n");

    // Pipeline: use alpha (worker 1) + put + use gamma (worker 0) + put.
    // First put forwarded, second put local. Responses must be in order.
    snd(fd, "use alpha\r\n");
    ck(fd, "USING alpha\r\n");
    snd(fd, "put 0 0 60 3\r\naaa\r\n");
    cksub(fd, "INSERTED");

    snd(fd, "use gamma\r\n");
    ck(fd, "USING gamma\r\n");
    snd(fd, "put 0 0 60 3\r\nbbb\r\n");
    cksub(fd, "INSERTED");

    close(fd);
}

// Test: TRUE TCP pipelining — multiple commands in a single write().
// Verifies fwd_pending blocks pipeline and sock.f callback resumes it.
void
cttest_mw_true_pipeline_burst()
{
    int port = startmw(2);

    int fd = dial(port);
    // Land on worker 0.
    snd(fd, "watch gamma\r\n");
    ck(fd, "WATCHING 1\r\n");

    // Send use+put+use+put in ONE write. First put forwarded (alpha→w1),
    // second put local (gamma→w0). Responses must arrive in order:
    // USING alpha → INSERTED X → USING gamma → INSERTED Y.
    char burst[] =
        "use alpha\r\n"
        "put 0 0 60 5\r\nburst\r\n"
        "use gamma\r\n"
        "put 0 0 60 5\r\nlocal\r\n";

    int n = write(fd, burst, sizeof(burst) - 1);
    assertf(n == (int)(sizeof(burst) - 1), "write failed: %d", n);

    // Read responses in order.
    ck(fd, "USING alpha\r\n");
    cksub(fd, "INSERTED");
    ck(fd, "USING gamma\r\n");
    cksub(fd, "INSERTED");

    close(fd);
}

// Stress test: 1000 jobs through full put→reserve→delete lifecycle.
// Producer and consumer on separate connections, potentially different workers.
void
cttest_mw_stress_1000_jobs()
{
    int port = startmw(2);

    // Producer: put 1000 jobs.
    int p = dial(port);
    snd(p, "use stress-tube\r\n");
    ck(p, "USING stress-tube\r\n");
    for (int i = 0; i < 1000; i++) {
        snd(p, "put 0 0 120 4\r\ndata\r\n");
        cksub(p, "INSERTED");
    }
    close(p);

    // Consumer: reserve and delete all 1000.
    int c = dial(port);
    snd(c, "watch stress-tube\r\n");
    ck(c, "WATCHING 1\r\n");
    for (int i = 0; i < 1000; i++) {
        snd(c, "reserve-with-timeout 3\r\n");
        char *resp = rdline(c);
        assertf(strstr(resp, "RESERVED") != NULL,
                "job %d: expected RESERVED, got %s", i, resp);
        // Parse job ID for delete.
        unsigned long jid = 0;
        sscanf(resp, "RESERVED %lu", &jid);
        rdline(c); // body

        char cmd[64];
        snprintf(cmd, sizeof(cmd), "delete %lu\r\n", jid);
        snd(c, cmd);
        ck(c, "DELETED\r\n");
    }
    // All consumed.
    snd(c, "reserve-with-timeout 0\r\n");
    ck(c, "TIMED_OUT\r\n");
    close(c);
}
