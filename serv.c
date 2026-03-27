#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

struct Server srv = {
    .port = Portdef,
    .wal = {
        .filesize = Filesizedef,
        .wantsync = 1,
        .syncrate = DEFAULT_FSYNC_MS * 1000000,
    },
};

// Detect available CPU cores for WAL sharding.
int
detect_ncpu(void)
{
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 1;
    if (n > 64) n = 64;
    return (int)n;
}

// Initialize a single WAL shard: create dir, lock, init file chain.
static void
init_wal_shard(Wal *w, Wal *tmpl, char *dir, Job *list)
{
    w->filesize = tmpl->filesize;
    w->wantsync = tmpl->wantsync;
    w->syncrate = tmpl->syncrate;
    w->use = 1;
    w->dir = dir;

    mkdir(dir, 0700); // ignore error if exists

    if (!waldirlock(w)) {
        twarnx("failed to lock wal shard dir %s", dir);
        exit(10);
    }
    walinit(w, list);
}

// srv_acquire_wal locks the WAL directory, replays existing entries,
// and creates per-shard WAL instances (one per CPU core) for parallel
// disk I/O. Legacy binlog.* in the root directory are read for
// backward-compatible migration; new writes go to shard subdirectories.
void
srv_acquire_wal(Server *s)
{
    if (!s->wal.use)
        return;

    // Phase 1: Lock root dir and replay legacy WAL (backward compat).
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

    // Phase 2: Create per-shard WAL instances (if requested).
    // nshards is set by main before calling srv_acquire_wal.
    // 0 = single legacy WAL (default, used by tests).
    if (!s->nshards)
        return;

    // Persist shard count to <wal_dir>/shards on first run.
    // On subsequent runs, read from file to keep routing stable
    // even if CPU count changes (server upgrade/downgrade).
    char *shards_path = fmtalloc("%s/shards", s->wal.dir);
    if (shards_path) {
        FILE *f = fopen(shards_path, "r");
        if (f) {
            int saved = 0;
            if (fscanf(f, "%d", &saved) == 1 && saved > 0 && saved <= 64) {
                if (saved != s->nshards) {
                    twarnx("WAL has %d shards (from %s), ignoring current CPU count %d",
                           saved, shards_path, s->nshards);
                }
                s->nshards = saved;
            }
            fclose(f);
        } else {
            f = fopen(shards_path, "w");
            if (f) {
                fprintf(f, "%d\n", s->nshards);
                fclose(f);
            }
        }
        free(shards_path);
    }

    s->shards = zalloc(s->nshards * sizeof(Wal));
    if (!s->shards) {
        twarnx("OOM allocating WAL shards");
        s->nshards = 0;
        return; // fall back to single legacy WAL
    }

    for (int i = 0; i < s->nshards; i++) {
        char *dir = fmtalloc("%s/s%d", s->wal.dir, i);
        if (!dir) {
            twarnx("OOM allocating shard dir");
            s->nshards = i;
            break;
        }

        Job slist = {.prev=NULL, .next=NULL};
        slist.prev = slist.next = &slist;
        init_wal_shard(&s->shards[i], &s->wal, dir, &slist);

        // Replay any jobs already in this shard (from previous sharded run).
        if (!prot_replay(s, &slist)) {
            twarnx("failed to replay shard %d", i);
        }
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

        int rw = socknext(&sock, period);
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }

        if (rw) {
            now = nanoseconds();
            sock->f(sock->x, rw);
        }
    }
}


void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
