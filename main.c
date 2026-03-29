#define _GNU_SOURCE
#include "dat.h"
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>
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
handle_sigterm_pid1(int _unused)
{
    _exit(143);
}

// Master: forward SIGTERM to all workers.
static volatile sig_atomic_t got_sigterm = 0;
static volatile sig_atomic_t got_sigusr1 = 0;

static void
master_sigterm(int _unused)
{
    UNUSED_PARAMETER(_unused);
    got_sigterm = 1;
}

static void
master_sigusr1(int _unused)
{
    UNUSED_PARAMETER(_unused);
    got_sigusr1 = 1;
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

    // Workaround for running the server with pid=1 in Docker.
    // Handle SIGTERM so the server is killed immediately and
    // not after 10 seconds timeout. See issue #527.
    if (getpid() == 1) {
        sa.sa_handler = handle_sigterm_pid1;
        r = sigaction(SIGTERM, &sa, 0);
        if (r == -1) {
            twarn("sigaction(SIGTERM)");
            exit(111);
        }
    }
}

// Set master-specific signal handlers.
static void
set_master_sig_handlers()
{
    struct sigaction sa;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, 0);

    sa.sa_handler = master_sigterm;
    sigaction(SIGTERM, &sa, 0);

    sa.sa_handler = master_sigusr1;
    sigaction(SIGUSR1, &sa, 0);
}

// Pin process to a specific CPU core.
static void
pin_to_cpu(int cpu)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) == -1) {
        twarn("sched_setaffinity(%d)", cpu);
    } else {
        srv.cpu = cpu; // SO_INCOMING_CPU in make_server_socket uses this
        if (verbose)
            printf("worker %d pinned to CPU %d\n", srv.worker_id, cpu);
    }
}

// Start a single worker process. Returns child PID to master,
// does not return in child (calls srvserve then _exit).
static pid_t
spawn_worker(int id, int nworkers, int mesh[MAX_WORKERS][MAX_WORKERS])
{
    pid_t pid = fork();
    if (pid == -1) {
        twarn("fork worker %d", id);
        return -1;
    }
    if (pid > 0)
        return pid; // master

    // === CHILD: become worker ===
    srv.worker_id = id;

    // Keep only our row of the mesh; close everything else.
    for (int i = 0; i < nworkers; i++) {
        srv.peer_fd[i] = mesh[id][i]; // our fd to talk to worker i (-1 for self)
        for (int j = 0; j < nworkers; j++) {
            if (i == id) continue; // keep our row
            if (mesh[i][j] >= 0)
                close(mesh[i][j]);
        }
    }

    // Pin to CPU core (round-robin if more workers than cores).
    int ncpu = detect_ncpu();
    pin_to_cpu(id % ncpu);

    // Fresh per-process protocol state (tubes, heaps, counters).
    prot_init();
    job_init_id(id, srv.nworkers);

    // Worker-specific WAL: each worker owns one shard directory.
    // Replay WAL BEFORE binding socket so clients don't connect
    // to a worker with incomplete state.
    if (srv.wal.use) {
        char *wdir = fmtalloc("%s/w%d", srv.wal.dir, id);
        if (!wdir) {
            twarnx("worker %d: OOM wal dir", id);
            _exit(111);
        }
        mkdir(wdir, 0700);
        srv.wal.dir = wdir;
        srv.nshards = 0;
        srv_acquire_wal(&srv);
        if (srv.wal.wantsync)
            walsyncstart(&srv.wal);
    }

    // Bind socket AFTER WAL recovery — no connections accepted before state is ready.
    int r = make_server_socket(srv.addr, srv.port);
    if (r == -1) {
        twarnx("worker %d: make_server_socket failed", id);
        _exit(111);
    }
    srv.sock.fd = r;

    set_sig_handlers();
    srvserve(&srv);

    if (srv.wal.use && srv.wal.wantsync)
        walsyncstop(&srv.wal);
    _exit(0);
}

// Master process: monitor workers, restart on crash, forward signals.
static void
master_loop(int nworkers)
{
    set_master_sig_handlers();

    for (;;) {
        // Block on waitpid — woken by SIGCHLD, SIGTERM, or SIGUSR1.
        // No busy-wait, zero CPU usage in steady state.
        int status;
        pid_t pid = waitpid(-1, &status, 0);

        // Forward signals to workers.
        if (got_sigterm) {
            for (int i = 0; i < nworkers; i++) {
                if (srv.worker_pids[i] > 0)
                    kill(srv.worker_pids[i], SIGTERM);
            }
            for (int i = 0; i < nworkers; i++) {
                if (srv.worker_pids[i] > 0)
                    waitpid(srv.worker_pids[i], NULL, 0);
            }
            exit(0);
        }
        if (got_sigusr1) {
            got_sigusr1 = 0;
            for (int i = 0; i < nworkers; i++) {
                if (srv.worker_pids[i] > 0)
                    kill(srv.worker_pids[i], SIGUSR1);
            }
        }

        if (pid > 0) {
            // Reap all terminated children (multiple may die simultaneously).
            do {
                for (int i = 0; i < nworkers; i++) {
                    if (srv.worker_pids[i] == pid) {
                        twarnx("worker %d (pid %d) exited status %d, restarting",
                               i, (int)pid, status);
                        int empty_mesh[MAX_WORKERS][MAX_WORKERS];
                        memset(empty_mesh, -1, sizeof(empty_mesh));
                        pid_t newpid = spawn_worker(i, nworkers, empty_mesh);
                        if (newpid > 0)
                            srv.worker_pids[i] = newpid;
                        break;
                    }
                }
            } while ((pid = waitpid(-1, &status, WNOHANG)) > 0);
        }
    }
}

// Legacy single-process mode (nworkers == 0).
static void
run_single_process()
{
    if (srv.cpu >= 0)
        pin_to_cpu(srv.cpu);

    int r = make_server_socket(srv.addr, srv.port);
    if (r == -1) {
        twarnx("make_server_socket()");
        exit(111);
    }
    srv.sock.fd = r;

    prot_init();

    if (srv.wal.use)
        srv.nshards = detect_ncpu();

    srv_acquire_wal(&srv);

    if (srv.wal.use && srv.wal.wantsync) {
        if (srv.nshards > 0) {
            for (int i = 0; i < srv.nshards; i++)
                walsyncstart(&srv.shards[i]);
        } else {
            walsyncstart(&srv.wal);
        }
    }

    set_sig_handlers();
    srvserve(&srv);

    if (srv.nshards > 0) {
        for (int i = 0; i < srv.nshards; i++)
            walsyncstop(&srv.shards[i]);
    } else {
        walsyncstop(&srv.wal);
    }
}

int
main(int argc, char **argv)
{
    UNUSED_PARAMETER(argc);

    progname = argv[0];
    setlinebuf(stdout);

    // Single arena; workers are single-threaded.
    setenv("MALLOC_ARENA_MAX", "1", 0);

    optparse(&srv, argv+1);

    if (srv.user)
        su(srv.user);

    if (verbose)
        printf("pid %d\n", getpid());

    int nworkers;

    // -w flag overrides auto-detection. -w 0 or -w 1 = single-process.
    if (srv.nworkers > 0) {
        nworkers = srv.nworkers;
    } else {
        nworkers = detect_ncpu();
    }

    // Legacy single-process mode when pinned to specific CPU (-t flag),
    // explicitly set -w 0 or -w 1, or when only 1 CPU is available.
    if (srv.cpu >= 0 || nworkers <= 1) {
        srv.nworkers = 0;
        srv.worker_id = 0;
        run_single_process();
        exit(0);
    }

    // Multi-process mode: master forks N workers.
    srv.nworkers = nworkers;
    srv.worker_id = -1;

    if (verbose)
        printf("starting %d workers\n", nworkers);

    // Allocate shared memory for cross-worker stats aggregation.
    size_t shm_size = sizeof(struct SharedStats) * nworkers;
    shared_stats = mmap(NULL, shm_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shared_stats == MAP_FAILED) {
        twarn("mmap shared stats");
        shared_stats = NULL;
    } else {
        memset(shared_stats, 0, shm_size);
    }

    // Create socketpair mesh for direct worker-to-worker fd migration.
    // mesh[i][j] = fd that worker i uses to communicate with worker j.
    int mesh[MAX_WORKERS][MAX_WORKERS];
    memset(mesh, -1, sizeof(mesh));
    for (int i = 0; i < nworkers; i++) {
        for (int j = i + 1; j < nworkers; j++) {
            int sv[2];
            // SOCK_SEQPACKET: message boundaries + SCM_RIGHTS support.
            // Prevents partial reads that corrupt message framing on SOCK_STREAM.
            if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) == -1) {
                twarn("socketpair for workers %d↔%d", i, j);
                exit(111);
            }
            mesh[i][j] = sv[0]; // worker i's end
            mesh[j][i] = sv[1]; // worker j's end
        }
    }

    // Copy mesh row into srv.peer_fd before forking.
    // Each worker inherits the full mesh; spawn_worker closes non-own fds.
    for (int i = 0; i < nworkers; i++) {
        pid_t pid = spawn_worker(i, nworkers, mesh);
        if (pid == -1) {
            twarnx("failed to spawn worker %d", i);
            exit(111);
        }
        srv.worker_pids[i] = pid;
    }

    // Master keeps mesh fds open — workers inherited copies via fork.
    // Closing master's copies would NOT affect workers (fd is per-process),
    // but we keep them to avoid confusing fd accounting.

    master_loop(nworkers);
    exit(0);
}
