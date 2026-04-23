#define _GNU_SOURCE
#include "dat.h"
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <pwd.h>
#include <sched.h>

static void
su(const char *user)
{
    errno = 0;
    struct passwd *pwent = getpwnam(user);
    if (errno) {
        twarn("getpwnam(\"%s\")", user);
        exit(32);
    }
    if (!pwent) {
        twarnx("getpwnam(\"%s\"): no such user", user);
        exit(33);
    }

    int r = setgid(pwent->pw_gid);
    if (r == -1) {
        twarn("setgid(%d \"%s\")", pwent->pw_gid, user);
        exit(34);
    }

    r = setuid(pwent->pw_uid);
    if (r == -1) {
        twarn("setuid(%d \"%s\")", pwent->pw_uid, user);
        exit(34);
    }
}

static void
handle_sigterm(int _unused)
{
    UNUSED_PARAMETER(_unused);
    shutdown_requested = 1;
}

static void
set_sig_handlers()
{
    struct sigaction sa;

    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    int r = sigemptyset(&sa.sa_mask);
    if (r == -1) {
        twarn("sigemptyset()");
        exit(111);
    }

    r = sigaction(SIGPIPE, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGPIPE)");
        exit(111);
    }

    sa.sa_handler = enter_drain_mode;
    r = sigaction(SIGUSR1, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGUSR1)");
        exit(111);
    }

    sa.sa_handler = handle_sigterm;
    r = sigaction(SIGTERM, &sa, 0);
    if (r == -1) {
        twarn("sigaction(SIGTERM)");
        exit(111);
    }
}

// Pin process to a specific CPU core.
static void
pin_to_cpu(int cpu)
{
    // Reject cpu indices that can't be represented in a cpu_set_t.
    // CPU_SET() with an out-of-range index is undefined (glibc docs) —
    // on some builds it silently writes past the fd_set-shaped bitmap.
    if (cpu < 0 || cpu >= CPU_SETSIZE) {
        twarnx("pin_to_cpu: cpu %d out of range [0, %d); skipping",
               cpu, CPU_SETSIZE);
        srv.cpu = -1;
        return;
    }

    // Reject cpu indices beyond the kernel's online CPU count. Prevents
    // silent sched_setaffinity EINVAL and, more importantly, a bogus
    // SO_INCOMING_CPU value leaking into make_server_socket.
    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc > 0 && cpu >= nproc) {
        twarnx("pin_to_cpu: cpu %d exceeds online cpu count %ld; skipping",
               cpu, nproc);
        srv.cpu = -1;
        return;
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == -1) {
        twarn("sched_setaffinity(%d)", cpu);
        srv.cpu = -1;  // do not propagate a bogus cpu to SO_INCOMING_CPU
    } else {
        srv.cpu = cpu; // SO_INCOMING_CPU in make_server_socket uses this
        if (verbose)
            printf("pinned to CPU %d\n", cpu);
    }
}

int
main(int argc, char **argv)
{
    UNUSED_PARAMETER(argc);

    progname = argv[0];
    setlinebuf(stdout);

    // Single arena; single-threaded server.
    setenv("MALLOC_ARENA_MAX", "1", 0);

    optparse(&srv, argv+1);

    // -D without a WAL makes every persistent command fail. Since #C1
    // (2026-04-23), walwrite/wal_write_truncate/reserve explicitly
    // refuse under `durable_sync && !w->use` and propagate 0 upward,
    // so the dispatcher replies with BURIED/INTERNAL_ERROR/OUT_OF_MEMORY
    // rather than silently ack'ing a ghost write. That is strictly
    // better than the pre-#C1 "ack ⇒ durable" regression, but an
    // operator who asked for durability without providing storage is
    // getting a server that can accept a connection yet answer no
    // requests. Warn loudly at startup rather than a night of BURIED.
    if (srv.wal.durable_sync && !srv.wal.use)
        warnx("-D without -b: every persistent command will fail; "
              "durable mode needs a WAL");

    if (srv.user)
        su(srv.user);

    if (verbose)
        printf("pid %d\n", getpid());

    if (srv.cpu >= 0)
        pin_to_cpu(srv.cpu);

    int r = make_server_socket(srv.addr, srv.port);
    if (r == -1) {
        twarnx("make_server_socket()");
        exit(111);
    }
    srv.sock.fd = r;

    prot_init();

    srv_acquire_wal(&srv);

    if (srv.wal.use && srv.wal.wantsync)
        walsyncstart(&srv.wal);

    set_sig_handlers();
    srvserve(&srv);

    if (srv.wal.use && srv.wal.wantsync)
        walsyncstop(&srv.wal);

    exit(0);
}
