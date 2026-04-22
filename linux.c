#define _GNU_SOURCE

#include "dat.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>
#include <limits.h>

// Batch size for epoll_wait. Amortizes syscall overhead: with 64 events
// per call, a server handling 100K events/sec makes ~1562 syscalls instead
// of 100K. Timer processing (prottick) still runs between each event,
// so deadline accuracy is unaffected.
#define EPOLL_BATCH 64

static int epfd;
static struct epoll_event ep_buf[EPOLL_BATCH];
static int ep_nready;
static int ep_pos;


int
sockinit(void)
{
    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd == -1) {
        twarn("epoll_create1");
        return -1;
    }
    ep_nready = 0;
    ep_pos = 0;
    return 0;
}


int
sockwant(Socket *s, int rw)
{
    int op;

    // Hot path: already added, same mode → skip syscall.
    if (likely(s->added)) {
        if (likely(rw)) {
            if (likely(s->rw_cached == rw))
                return 0;
            op = EPOLL_CTL_MOD;
        } else {
            op = EPOLL_CTL_DEL;
        }
    } else {
        if (!rw) return 0;
        op = EPOLL_CTL_ADD;
    }

    struct epoll_event ev = {.events=0};
    switch (rw) {
    case 'r':
        ev.events = EPOLLIN;
        break;
    case 'w':
        ev.events = EPOLLOUT;
        break;
    }
    ev.events |= EPOLLRDHUP;
    ev.data.ptr = s;

    int r = epoll_ctl(epfd, op, s->fd, &ev);
    if (r == 0) {
        s->added = !!rw;
        s->rw_cached = rw;
    }
    return r;
}


int
socknext(Socket **s, int64 timeout)
{
    if (ep_pos >= ep_nready) {
        int ms = 0;
        if (timeout > 0) {
            int64 ms64 = timeout / 1000000;
            ms = ms64 > INT_MAX ? INT_MAX : (int)ms64;
        }
        ep_nready = epoll_wait(epfd, ep_buf, EPOLL_BATCH, ms);
        ep_pos = 0;
        if (ep_nready == -1) {
            ep_nready = 0;
            if (errno != EINTR) {
                twarn("epoll_wait");
                exit(1);
            }
            return 0;
        }
    }

    if (ep_pos < ep_nready) {
        struct epoll_event *ev = &ep_buf[ep_pos++];
        *s = ev->data.ptr;

        // EPOLLIN takes priority over half/full close flags so we never
        // drop a trailing command sitting in the receive buffer when the
        // peer has already shut down its write side (bundled EPOLLIN|
        // EPOLLRDHUP on the same wake-up). Level-triggered epoll re-arms
        // EPOLLRDHUP/EPOLLHUP after the buffer is drained, so the close
        // path is guaranteed to fire on a subsequent iteration.
        if (likely(ev->events & EPOLLIN)) {
            return 'r';
        }
        if (unlikely(ev->events & (EPOLLERR|EPOLLHUP|EPOLLRDHUP))) {
            return 'h';
        }
        if (ev->events & EPOLLOUT) {
            return 'w';
        }
    }
    return 0;
}
