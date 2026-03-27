#define _XOPEN_SOURCE 600

#include "dat.h"
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0x2000
#endif

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
    epfd = epoll_create(1);
    if (epfd == -1) {
        twarn("epoll_create");
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

    if (!s->added && !rw) {
        return 0;
    } else if (!s->added && rw) {
        s->added = 1;
        op = EPOLL_CTL_ADD;
    } else if (!rw) {
        s->added = 0;
        op = EPOLL_CTL_DEL;
    } else {
        op = EPOLL_CTL_MOD;
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
    ev.events |= EPOLLRDHUP | EPOLLPRI;
    ev.data.ptr = s;

    return epoll_ctl(epfd, op, s->fd, &ev);
}


int
socknext(Socket **s, int64 timeout)
{
    if (ep_pos >= ep_nready) {
        int ms = (int)(timeout / 1000000);
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
        if (ev->events & (EPOLLERR|EPOLLHUP|EPOLLRDHUP)) {
            return 'h';
        } else if (ev->events & EPOLLIN) {
            return 'r';
        } else if (ev->events & EPOLLOUT) {
            return 'w';
        }
    }
    return 0;
}
