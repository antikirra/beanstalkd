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
#include <grp.h>
#include <sched.h>
#include <malloc.h>

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

    // Reset supplementary groups before setgid, while CAP_SETGID is
    // still held: setuid/setgid alone leave the caller's (root's)
    // supplementary groups attached for the process lifetime (CWE-271).
    // Gated on euid==0 — an unprivileged `-u <self>` invocation has no
    // CAP_SETGID (initgroups would fail EPERM) and nothing to shed.
    if (geteuid() == 0) {
        if (initgroups(user, pwent->pw_gid) == -1) {
            twarn("initgroups(\"%s\")", user);
            exit(34);
        }
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
    atomic_store_explicit(&shutdown_requested, 1, memory_order_relaxed);
    // Wake the epoll loop via the eventfd (self-pipe trick) so a
    // SIGTERM landing between srvserve's shutdown_requested check and
    // the epoll syscall cannot strand the flag until the next wake-up
    // (up to 1h idle prottick period).
    srv_wake();
}

static void
set_sig_handlers(void)
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

    // Single arena; single-threaded server (plus the optional WAL fsync
    // thread, which must share the main arena). mallopt takes effect at
    // runtime; setenv("MALLOC_ARENA_MAX", ...) here would be a no-op —
    // glibc reads the env alias during allocator init, before main().
    if (mallopt(M_ARENA_MAX, 1) == 0)
        twarnx("mallopt(M_ARENA_MAX, 1) failed; keeping glibc default");

    optparse(&srv, argv+1);

    // -D without a WAL makes every persistent command fail. Since #C1
    // (2026-04-23), walwrite/reserve explicitly
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

    // Close the check-then-block shutdown race: handle_sigterm /
    // enter_drain_mode set their flag AND write the wake eventfd
    // (srv_wake, self-pipe trick); srvserve registers that fd in the
    // epoll set, so a signal delivered in the user-space window between
    // the flag check and the epoll syscall turns into fd readiness and
    // wakes the wait immediately. fd readiness was chosen over the
    // epoll_pwait+blocked-sigmask variant deliberately: the sigmask
    // dance is correct on a real kernel but qemu/Rosetta user-mode
    // emulation (CI gate) never delivers the pending signal into the
    // mask-swap window, leaving the server parked forever. The eventfd
    // must exist before the handlers that write to it are installed.
    if (srv_wake_init() == -1) {
        twarn("eventfd");
        exit(111);
    }

    // Handlers installed before WAL replay: a SIGTERM arriving during
    // replay sets the flag (plus a wake-fd write), replay completes,
    // and srvserve exits gracefully at its first shutdown check instead
    // of the signal killing the process mid-replay. The handlers only
    // set flags and write the eventfd, so installing them early is safe.
    set_sig_handlers();

    // -z and -s are set independently, and a job larger than a whole
    // binlog can never be stored: needfree refuses the reservation and
    // the client gets OUT_OF_MEMORY. That is the right answer, but
    // discovering it on the first large put — in production, at 3am —
    // is not. Say so at startup instead, where the numbers came from.
    //
    // Largest record a put can produce: namelen + tube name + Jobrec +
    // body + CRC trailer, against what a binlog has after its header.
    if (srv.wal.use) {
        int64 worst = (int64)sizeof(int) + (MAX_TUBE_NAME_LEN - 1)
                    + (int64)sizeof(Jobrec) + (int64)job_data_size_limit + 2
                    + (int64)sizeof(uint32);
        int64 room = (int64)srv.wal.filesize - (int64)sizeof(int);
        if (worst > room) {
            warnx("-z %zu needs a binlog of at least %lld bytes, but -s is "
                  "%d: jobs near the size limit will be refused with "
                  "OUT_OF_MEMORY",
                  job_data_size_limit, (long long)(worst + sizeof(int)),
                  srv.wal.filesize);
        }
    }

    srv_acquire_wal(&srv);

    if (srv.wal.use && srv.wal.wantsync)
        walsyncstart(&srv.wal);

    srvserve(&srv);

    // A clean exit should leave the WAL durable and the binlog no longer
    // than what it holds. Without this the records written since the
    // last periodic fsync live only in the page cache — a shutdown
    // followed by a power loss drops jobs the client was acked for —
    // and the file keeps its full preallocated size, so the next start
    // reads a tail of zeroes it did not need to write.
    //
    // Under -D every commit already synced, and filewclose skips the
    // second fdatasync in that case (uncommitted_bytes == 0).
    if (srv.wal.use && srv.wal.cur)
        filewclose(srv.wal.cur);

    if (srv.wal.use && srv.wal.wantsync) {
        walsyncstop(&srv.wal);
        // walsyncstop joins the thread; an error it recorded on its last
        // round has still not been reported to anyone. Say so before
        // exiting rather than exiting 0 on a wal that failed to sync.
        if (atomic_load_explicit(&srv.wal.sync_err, memory_order_relaxed)) {
            errno = atomic_load_explicit(&srv.wal.sync_err,
                                         memory_order_relaxed);
            twarn("async fsync failed before shutdown");
        }
    }

    exit(0);
}
