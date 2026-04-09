#include "dat.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

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

    for (;;) {
        int64 period = prottick(s);

        // Drain all ready events before next prottick.
        // Update `now` once per batch — events within a batch are
        // effectively simultaneous, saves ~5ns vDSO call per event.
        int rw;
        while ((rw = socknext(&sock, period)) > 0) {
            if (period) {
                now = nanoseconds();
                period = 0; // subsequent calls: non-blocking drain, reuse `now`
            }
            sock->f(sock->x, rw);
        }
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }
    }
}


void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
