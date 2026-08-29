#include "dat.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/eventfd.h>

volatile sig_atomic_t shutdown_requested = 0;

// Self-pipe trick (eventfd flavor): closes the check-then-block
// shutdown race. srvserve tests shutdown_requested/drain_mode once per
// tick, then parks in epoll for up to 1h (idle prottick period); a
// signal handler that only sets a flag in the user-space window between
// that check and the epoll syscall used to strand the flag until the
// next wake-up. Handlers therefore also call srv_wake(), which writes
// to this eventfd; the fd is registered in the epoll set, so the signal
// turns into fd readiness and the loop wakes immediately and re-checks
// the flags. fd readiness (unlike an epoll_pwait sigmask) survives
// qemu/Rosetta user-mode emulation, where a pending blocked signal is
// never delivered into the mask-swap window — the CI gate runs there.
static int wake_fd = -1;
static Socket wake_sock;

// srv_wake_init creates the wake eventfd. Must run before
// set_sig_handlers so a handler never races fd creation.
int
srv_wake_init(void)
{
    wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd == -1)
        return -1;
    return 0;
}

// srv_wake is async-signal-safe (write + errno save/restore) and
// thread-agnostic: with no signal blocking, the kernel may deliver
// SIGTERM/SIGUSR1 to the WAL fsync thread; writing the eventfd from
// there still wakes the main loop's epoll. If the counter is already
// nonzero (EAGAIN on overflow), epoll is already awake — losing the
// write is fine.
void
srv_wake(void)
{
    if (wake_fd == -1)
        return;
    int saved_errno = errno;
    uint64_t one = 1;
    ssize_t r = write(wake_fd, &one, sizeof one);
    (void)r;
    errno = saved_errno;
}

// srvwakedrain resets the eventfd counter; the decision is made by the
// shutdown_requested/drain_mode checks in srvserve/prottick, not here.
static void
srvwakedrain(void *x, int rw)
{
    UNUSED_PARAMETER(x);
    UNUSED_PARAMETER(rw);
    uint64_t v;
    ssize_t r = read(wake_fd, &v, sizeof v);
    (void)r; // EAGAIN after a racing read is fine; level-trigger re-arms
}

struct Server srv = {
    .port = Portdef,
    .cpu = -1,
    .wal = {
        .filesize = Filesizedef,
        .wantsync = 1,
        .syncrate = DEFAULT_FSYNC_MS * 1000000,
    },
};

// srv_acquire_wal locks the WAL directory, replays existing entries.
void
srv_acquire_wal(Server *s)
{
    if (!s->wal.use)
        return;

    if (!waldirlock(&s->wal)) {
        twarnx("failed to lock wal dir %s", s->wal.dir);
        exit(10);
    }

    Job list = {.prev=NULL, .next=NULL};
    list.prev = list.next = &list;
    walinit(&s->wal, &list);
    int ok = prot_replay(s, &list);
    if (!ok) {
        twarnx("failed to replay log");
        exit(1);
    }
}

void
srvserve(Server *s)
{
    Socket *sock;

    if (sockinit() == -1) {
        twarnx("sockinit");
        exit(1);
    }

    s->sock.x = s;
    s->sock.f = (Handle)srvaccept;
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;

    if (sockwant(&s->sock, 'r') == -1) {
        twarn("sockwant");
        exit(2);
    }

    // Register the shutdown wake eventfd (see srv_wake above). A signal
    // that arrived between srv_wake_init and this point left the
    // counter nonzero; level-triggered EPOLLIN reports it on the first
    // wait, so nothing is lost. Absent (-1) only in unit-test servers
    // that never call srv_wake_init.
    if (wake_fd != -1) {
        wake_sock.fd = wake_fd;
        wake_sock.f = srvwakedrain;
        wake_sock.x = &wake_sock;
        if (sockwant(&wake_sock, 'r') == -1) {
            twarn("sockwant wake_fd");
            exit(2);
        }
    }

    for (;;) {
        if (unlikely(shutdown_requested))
            break;

        int64 period = prottick(s);

        // A durable batch can carry an undelivered ack remainder across
        // ticks (partial socket write while the conn was mid-PUT or
        // waiting). Such a conn generates no epoll event by itself, so
        // cap the park to retry the flush promptly instead of sleeping
        // until the next natural wake-up (up to 1h when idle).
        if (unlikely(dur_batch_pending()))
            period = min(period, 10000000LL); // 10ms retry cadence

        // Drain all ready events before next prottick.
        // Update `now` once per batch — events within a batch are
        // effectively simultaneous, saves ~5ns vDSO call per event.
        // conn_defer_free_begin/end brackets the drain: connclose during
        // dispatch (e.g. a sockwant failure in epollq_apply) must not let
        // make_conn recycle the struct while later events in the same
        // epoll batch still point at it.
        conn_defer_free_begin();
        int rw;
        while ((rw = socknext(&sock, period)) > 0) {
            if (period) {
                now = nanoseconds();
                period = 0; // subsequent calls: non-blocking drain, reuse `now`
            }
            sock->f(sock->x, rw);
        }
        conn_defer_free_end();
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }

        // Group commit: one fdatasync covers every walwrite staged in
        // this tick's prottick + event drain; dur_flush_all then sends
        // the acks buffered while waiting (INTERNAL_ERROR on commit
        // failure). Upholds invariants #14 and #16. No-op in non-durable
        // mode — async fsync runs via walsync() inside walmaint.
        int commit_ok = walcommit(&s->wal);
        dur_flush_all(commit_ok);
    }
}


void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
