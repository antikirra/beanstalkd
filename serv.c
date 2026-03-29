#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

struct SharedStats *shared_stats = NULL;

struct Server srv = {
    .port = Portdef,
    .cpu = -1,
    .wal = {
        .filesize = Filesizedef,
        .wantsync = 1,
        .syncrate = DEFAULT_FSYNC_MS * 1000000,
    },
    .nworkers = 0,
    .worker_id = 0,
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

// Execute a forwarded command locally and send reply back.
// Called when a peer worker sends a CmdFwdMsg for stats-tube/pause-tube.
void prot_handle_forwarded_cmd(Server *s, struct CmdFwdMsg *fwd);

// Per-peer context passed through Socket.x to avoid linear scan.
struct PeerCtx {
    Server *srv;
    int     peer_idx;
};
static struct PeerCtx peer_ctx[MAX_WORKERS];

// Handler for incoming data on a peer worker socket.
// Dispatches migration (MigMsg), command forwarding (CmdFwdMsg),
// and command replies (CmdReplyMsg) based on message magic.
static void
handle_peer(struct PeerCtx *ctx, int ev)
{
    UNUSED_PARAMETER(ev);
    Server *s = ctx->srv;
    int fd = s->peer_fd[ctx->peer_idx];

    // Peek at the magic to determine message type.
    uint32 magic = 0;
    ssize_t r = recv(fd, &magic, sizeof(magic), MSG_PEEK);
    if (r < (ssize_t)sizeof(magic))
        return;

    if (magic == MIG_MSG_MAGIC) {
        struct MigMsg mm;
        int cfd = recv_fd(fd, &mm, sizeof(mm));
        if (cfd < 0)
            return;
        h_accept_migrated(cfd, s, &mm);
    } else if (magic == CMD_FWD_MAGIC) {
        struct CmdFwdMsg fwd;
        r = read(fd, &fwd, sizeof(fwd));
        if (r < (ssize_t)sizeof(fwd))
            return;
        prot_handle_forwarded_cmd(s, &fwd);
    } else if (magic == CMD_REPLY_MAGIC) {
        struct CmdReplyMsg rpl;
        r = read(fd, &rpl, sizeof(rpl));
        if (r < (ssize_t)sizeof(rpl))
            return;
        // Write reply directly to the waiting connection's socket.
        if (s->pending_fwd_conn && rpl.data_len > 0) {
            write(s->pending_fwd_conn->sock.fd, rpl.data, rpl.data_len);
            s->pending_fwd_conn = NULL;
        }
    } else {
        // Unknown magic — drain the data.
        char drain[4096];
        read(fd, drain, sizeof(drain));
    }
}

// Per-peer Socket structs for epoll registration.
static Socket peer_socks[MAX_WORKERS];

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

    // Register peer worker sockets for migration receive.
    for (int i = 0; i < s->nworkers; i++) {
        if (s->peer_fd[i] < 0 || i == s->worker_id)
            continue;
        peer_ctx[i].srv = s;
        peer_ctx[i].peer_idx = i;
        peer_socks[i].fd = s->peer_fd[i];
        peer_socks[i].x = &peer_ctx[i];
        peer_socks[i].f = (Handle)handle_peer;
        if (sockwant(&peer_socks[i], 'r') == -1) {
            twarn("sockwant peer %d", i);
        }
    }

    for (;;) {
        int64 period = prottick(s);

        // Drain all ready events before next prottick.
        int rw;
        while ((rw = socknext(&sock, period)) > 0) {
            now = nanoseconds();
            sock->f(sock->x, rw);
            period = 0; // subsequent calls use zero timeout (non-blocking drain)
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
