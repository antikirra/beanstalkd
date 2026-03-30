#include "dat.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

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
    .ctl_fd = -1,
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
void prot_handle_forwarded_put(Server *s, struct PutFwdMsg *pm);

// Per-peer context passed through Socket.x to avoid linear scan.
struct PeerCtx {
    Server *srv;
    int     peer_idx;
};
static struct PeerCtx peer_ctx[MAX_WORKERS];
static Socket peer_socks[MAX_WORKERS];

// Handler for incoming data on a peer worker socket.
// Dispatches migration (MigMsg), command forwarding (CmdFwdMsg),
// and command replies (CmdReplyMsg) based on message magic.
// Uses a single recvmsg() instead of recv(MSG_PEEK) + read() to halve syscalls.
static void
handle_peer(struct PeerCtx *ctx, int ev)
{
    UNUSED_PARAMETER(ev);
    Server *s = ctx->srv;
    int fd = s->peer_fd[ctx->peer_idx];

    // Unified read: single recvmsg for all message types.
    // Heap-allocated on first use to avoid 66KB BSS pollution
    // in single-process mode (handle_peer is never called).
    static union {
        uint32              magic;
        struct MigMsg       mig;
        struct CmdFwdMsg    cmd;
        struct PutFwdMsg    put;
        struct CmdReplyMsg  rpl;
    } *pbuf;
    if (!pbuf) {
        pbuf = malloc(sizeof(*pbuf));
        if (!pbuf) return;
    }
    #define buf (*pbuf)
    char cmsgbuf[CMSG_SPACE(sizeof(int))];

    struct iovec iov = { .iov_base = &buf, .iov_len = sizeof(buf) };
    struct msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);

    ssize_t r;
    do {
        r = recvmsg(fd, &msg, 0);
    } while (r == -1 && errno == EINTR);

    if (r == 0 || (r == -1 && errno != EAGAIN)) {
        // Peer closed or broken — deregister to prevent busy loop.
        // Release all clients waiting for forwarded replies.
        int remaining = s->pending_fwd_used;
        for (int i = 0; i < PENDING_FWD_SLOTS && remaining > 0; i++) {
            if (s->pending_fwd[i].conn) {
                remaining--;
                Conn *pc = s->pending_fwd[i].conn;
                if (pc->gen == s->pending_fwd[i].gen && pc->sock.fd >= 0) {
                    pc->fwd_pending = 0;
                    if (write(pc->sock.fd, "NOT_FOUND\r\n", 11) <= 0) {
                        sockwant(&pc->sock, 0);
                    } else {
                        sockwant(&pc->sock, 'r');
                        if (pc->cmd_read > 0)
                            pc->sock.f(pc->sock.x, 'r');
                    }
                }
                s->pending_fwd[i].conn = NULL;
                s->pending_fwd[i].seq = 0;
                s->pending_fwd_used--;
            }
        }
        sockwant(&peer_socks[ctx->peer_idx], 0);
        close(fd);
        s->peer_fd[ctx->peer_idx] = -1;
        twarnx("peer %d disconnected", ctx->peer_idx);
        return;
    }
    if (r < (ssize_t)sizeof(uint32))
        return;

    // Check for SCM_RIGHTS (migration message carries a file descriptor).
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    if (cmsg && cmsg->cmsg_type == SCM_RIGHTS
        && cmsg->cmsg_len >= CMSG_LEN(sizeof(int))) {
        int cfd;
        memcpy(&cfd, CMSG_DATA(cmsg), sizeof(int));
        if (buf.magic == MIG_MSG_MAGIC && (size_t)r >= sizeof(struct MigMsg)) {
            h_accept_migrated(cfd, s, &buf.mig);
        } else {
            close(cfd);
        }
        return;
    }

    if (buf.magic == CMD_FWD_MAGIC) {
        // Variable-length: header + cmd_len bytes.
        if (r < (ssize_t)offsetof(struct CmdFwdMsg, cmd)) {
            twarnx("handle_peer: short CmdFwdMsg read %zd from peer %d",
                   r, ctx->peer_idx);
            return;
        }
        if (buf.cmd.cmd_len > LINE_BUF_SIZE ||
            r < (ssize_t)(offsetof(struct CmdFwdMsg, cmd) + buf.cmd.cmd_len)) {
            twarnx("handle_peer: truncated CmdFwdMsg cmd from peer %d", ctx->peer_idx);
            return;
        }
        prot_handle_forwarded_cmd(s, &buf.cmd);
    } else if (buf.magic == PUT_FWD_MAGIC) {
        if (r < (ssize_t)offsetof(struct PutFwdMsg, body)) {
            twarnx("handle_peer: short PutFwdMsg read %zd from peer %d",
                   r, ctx->peer_idx);
            return;
        }
        if (buf.put.body_size < 0 || buf.put.body_size > PUT_FWD_MAX_BODY + 2) {
            twarnx("handle_peer: invalid body_size %d from peer %d",
                   buf.put.body_size, ctx->peer_idx);
            return;
        }
        if (r < (ssize_t)(offsetof(struct PutFwdMsg, body) + buf.put.body_size)) {
            twarnx("handle_peer: truncated PutFwdMsg body from peer %d (%zd < %zu)",
                   ctx->peer_idx, r,
                   offsetof(struct PutFwdMsg, body) + buf.put.body_size);
            return;
        }
        prot_handle_forwarded_put(s, &buf.put);
    } else if (buf.magic == CMD_REPLY_MAGIC) {
        // Variable-length: header + data_len bytes.
        if (r < (ssize_t)offsetof(struct CmdReplyMsg, data)) {
            twarnx("handle_peer: short CmdReplyMsg read %zd from peer %d",
                   r, ctx->peer_idx);
            return;
        }
        if (buf.rpl.data_len < 0 || buf.rpl.data_len > CMD_FWD_REPLY_SIZE ||
            r < (ssize_t)(offsetof(struct CmdReplyMsg, data) + buf.rpl.data_len)) {
            twarnx("handle_peer: truncated CmdReplyMsg from peer %d", ctx->peer_idx);
            return;
        }
        // Match reply to pending slot by sequence number (hash lookup).
        if (buf.rpl.data_len > 0 && buf.rpl.seq) {
            int i = pending_fwd_find(s, buf.rpl.seq);
            if (i >= 0) {
                Conn *pc = s->pending_fwd[i].conn;
                if (pc->gen == s->pending_fwd[i].gen && pc->sock.fd >= 0) {
                    pc->fwd_pending = 0;
                    if (write(pc->sock.fd, buf.rpl.data, buf.rpl.data_len) <= 0) {
                        sockwant(&pc->sock, 0);
                    } else {
                        sockwant(&pc->sock, 'r');
                        if (pc->cmd_read > 0)
                            pc->sock.f(pc->sock.x, 'r');
                    }
                }
                s->pending_fwd[i].conn = NULL;
                s->pending_fwd[i].seq = 0;
                s->pending_fwd_used--;
            }
        }
    }
    // Unknown magic — already consumed by recvmsg, nothing to drain.
    #undef buf
}

// Register (or re-register) a peer socket in epoll.
static void
register_peer(Server *s, int idx)
{
    if (s->peer_fd[idx] < 0 || idx == s->worker_id)
        return;
    peer_ctx[idx].srv = s;
    peer_ctx[idx].peer_idx = idx;
    peer_socks[idx].fd = s->peer_fd[idx];
    peer_socks[idx].x = &peer_ctx[idx];
    peer_socks[idx].f = (Handle)handle_peer;
    peer_socks[idx].added = 0;
    peer_socks[idx].rw_cached = 0;
    if (sockwant(&peer_socks[idx], 'r') == -1)
        twarn("sockwant peer %d", idx);
}

// Handler for control pipe from master (peer fd updates).
static Socket ctl_sock;
static void
handle_ctl(Server *s, int ev)
{
    UNUSED_PARAMETER(ev);
    struct CtlPeerUpdate msg;
    int newfd = recv_fd(s->ctl_fd, &msg, sizeof(msg));
    if (newfd < 0) return;
    if (msg.magic != CTL_PEER_UPDATE_MAGIC) {
        close(newfd);
        return;
    }
    int idx = msg.peer_idx;
    if (idx < 0 || idx >= MAX_WORKERS || idx >= s->nworkers || idx == s->worker_id) {
        close(newfd);
        return;
    }
    // Close old broken peer socket if still registered.
    if (s->peer_fd[idx] >= 0) {
        sockwant(&peer_socks[idx], 0);
        close(s->peer_fd[idx]);
    }
    s->peer_fd[idx] = newfd;
    register_peer(s, idx);
    if (verbose)
        printf("worker %d: peer %d reconnected (fd %d)\n",
               s->worker_id, idx, newfd);
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

    // Register peer worker sockets for migration receive.
    for (int i = 0; i < s->nworkers; i++)
        register_peer(s, i);

    // Register control pipe from master for peer updates.
    if (s->ctl_fd >= 0) {
        ctl_sock.fd = s->ctl_fd;
        ctl_sock.x = s;
        ctl_sock.f = (Handle)handle_ctl;
        if (sockwant(&ctl_sock, 'r') == -1)
            twarn("sockwant ctl");
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
