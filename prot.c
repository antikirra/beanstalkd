#define _GNU_SOURCE
#include "dat.h"
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <inttypes.h>
#include <stdarg.h>
#include <signal.h>
#include <limits.h>
#include <malloc.h>

/* job body cannot be greater than this many bytes long */
size_t job_data_size_limit = JOB_DATA_SIZE_LIMIT_DEFAULT;

int64 mem_trim_rate = 60000000000LL; /* 60 seconds in nanoseconds */

// shard_wal returns the WAL instance for a job based on its tube name.
// With sharding enabled, jobs are distributed across per-CPU WAL instances
// by tube name hash, parallelizing disk I/O (writes + fsync).
// Falls back to the single legacy WAL when sharding is not active.
static inline Wal *
shard_wal(Server *s, Job *j)
{
    if (s->nshards > 0 && s->shards && j->tube) {
        return &s->shards[j->tube->shard];
    }
    return &s->wal;
}

static void epollq_add(Conn *c, char rw);

// Allocate a pending forward slot and return its sequence number.
// Uses hash-based indexing: seq & PENDING_FWD_MASK with linear probing.
// Returns 0 if no slot available (all busy).
static uint32
pending_fwd_alloc(Server *s, Conn *c)
{
    uint32 seq = ++s->pending_fwd_seq;
    if (!seq) seq = ++s->pending_fwd_seq; // skip 0
    int base = seq & PENDING_FWD_MASK;
    for (int probe = 0; probe < PENDING_FWD_SLOTS; probe++) {
        int i = (base + probe) & PENDING_FWD_MASK;
        if (!s->pending_fwd[i].conn) {
            s->pending_fwd[i].conn = c;
            s->pending_fwd[i].gen = c->gen;
            s->pending_fwd[i].seq = seq;
            s->pending_fwd[i].at = now;
            s->pending_fwd_used++;
            return seq;
        }
    }
    return 0;
}

// Look up a pending forward slot by sequence number.
// Returns the slot index, or -1 if not found.
int
pending_fwd_find(Server *s, uint32 seq)
{
    if (!seq) return -1;
    int base = seq & PENDING_FWD_MASK;
    for (int probe = 0; probe < PENDING_FWD_SLOTS; probe++) {
        int i = (base + probe) & PENDING_FWD_MASK;
        if (s->pending_fwd[i].seq == seq && s->pending_fwd[i].conn)
            return i;
        if (!s->pending_fwd[i].conn && !s->pending_fwd[i].seq)
            return -1; // empty slot — not found
    }
    return -1;
}

// Forward the current command to the worker that owns job_id.
// Returns 1 if forwarded (caller should return), 0 if local or forwarding failed.
static int
try_forward_job_cmd(Conn *c, uint64 job_id)
{
    Server *s = c->srv;
    if (s->nworkers <= 1)
        return 0;
    int target = (int)((job_id - 1) % s->nworkers);
    if (target == s->worker_id || s->peer_fd[target] < 0)
        return 0;
    uint32 seq = pending_fwd_alloc(s, c);
    if (!seq) return 0;
    struct CmdFwdMsg fwd;
    fwd.magic = CMD_FWD_MAGIC;
    fwd.from_worker = s->worker_id;
    fwd.seq = seq;
    fwd.cmd_len = c->cmd_len;
    memcpy(fwd.cmd, c->cmd, c->cmd_len);
    // Variable-length send: header + actual cmd bytes only.
    size_t msg_len = offsetof(struct CmdFwdMsg, cmd) + c->cmd_len;
    ssize_t wr = write(s->peer_fd[target], &fwd, msg_len);
    if (wr != (ssize_t)msg_len) {
        int idx = pending_fwd_find(s, seq);
        if (idx >= 0) {
            s->pending_fwd[idx].conn = NULL;
            s->pending_fwd[idx].seq = 0;
            s->pending_fwd_used--;
        }
        return 0;
    }
    c->fwd_pending = 1;
    epollq_add(c, 0); // suspend until reply
    return 1;
}

// Forward the current command to the worker owning the given tube hash.
// Returns 1 if forwarded, 0 if local or failed.
static int
try_forward_tube_cmd(Conn *c, uint32 name_hash)
{
    Server *s = c->srv;
    if (s->nworkers <= 1)
        return 0;
    int target = name_hash % s->nworkers;
    if (target == s->worker_id || s->peer_fd[target] < 0)
        return 0;
    uint32 seq = pending_fwd_alloc(s, c);
    if (!seq) return 0;
    struct CmdFwdMsg fwd;
    fwd.magic = CMD_FWD_MAGIC;
    fwd.from_worker = s->worker_id;
    fwd.seq = seq;
    fwd.cmd_len = c->cmd_len;
    memcpy(fwd.cmd, c->cmd, c->cmd_len);
    size_t msg_len = offsetof(struct CmdFwdMsg, cmd) + c->cmd_len;
    ssize_t wr = write(s->peer_fd[target], &fwd, msg_len);
    if (wr != (ssize_t)msg_len) {
        int idx = pending_fwd_find(s, seq);
        if (idx >= 0) {
            s->pending_fwd[idx].conn = NULL;
            s->pending_fwd[idx].seq = 0;
            s->pending_fwd_used--;
        }
        return 0;
    }
    c->fwd_pending = 1;
    epollq_add(c, 0); // suspend until reply
    return 1;
}

// Valid tube name characters (lookup table replaces strspn).
static const char valid_name_char[256] __attribute__((aligned(64))) = {
    ['A']=1,['B']=1,['C']=1,['D']=1,['E']=1,['F']=1,['G']=1,['H']=1,
    ['I']=1,['J']=1,['K']=1,['L']=1,['M']=1,['N']=1,['O']=1,['P']=1,
    ['Q']=1,['R']=1,['S']=1,['T']=1,['U']=1,['V']=1,['W']=1,['X']=1,
    ['Y']=1,['Z']=1,
    ['a']=1,['b']=1,['c']=1,['d']=1,['e']=1,['f']=1,['g']=1,['h']=1,
    ['i']=1,['j']=1,['k']=1,['l']=1,['m']=1,['n']=1,['o']=1,['p']=1,
    ['q']=1,['r']=1,['s']=1,['t']=1,['u']=1,['v']=1,['w']=1,['x']=1,
    ['y']=1,['z']=1,
    ['0']=1,['1']=1,['2']=1,['3']=1,['4']=1,['5']=1,['6']=1,['7']=1,
    ['8']=1,['9']=1,
    ['-']=1,['+']=1,['/']=1,[';']=1,['.']=1,['$']=1,['_']=1,['(']=1,
    [')']=1,
};

#define CMD_PUT "put "
#define CMD_PEEKJOB "peek "
#define CMD_PEEK_READY "peek-ready"
#define CMD_PEEK_DELAYED "peek-delayed"
#define CMD_PEEK_BURIED "peek-buried"
#define CMD_RESERVE "reserve"
#define CMD_RESERVE_TIMEOUT "reserve-with-timeout "
#define CMD_RESERVE_JOB "reserve-job "
#define CMD_DELETE "delete "
#define CMD_RELEASE "release "
#define CMD_BURY "bury "
#define CMD_KICK "kick "
#define CMD_KICKJOB "kick-job "
#define CMD_TOUCH "touch "
#define CMD_STATS "stats"
#define CMD_STATSJOB "stats-job "
#define CMD_USE "use "
#define CMD_WATCH "watch "
#define CMD_IGNORE "ignore "
#define CMD_LIST_TUBES "list-tubes"
#define CMD_LIST_TUBE_USED "list-tube-used"
#define CMD_LIST_TUBES_WATCHED "list-tubes-watched"
#define CMD_STATS_TUBE "stats-tube "
#define CMD_QUIT "quit"
#define CMD_PAUSE_TUBE "pause-tube"

#define CONSTSTRLEN(m) (sizeof(m) - 1)

#define CMD_PEEK_READY_LEN CONSTSTRLEN(CMD_PEEK_READY)
#define CMD_PEEK_DELAYED_LEN CONSTSTRLEN(CMD_PEEK_DELAYED)
#define CMD_PEEK_BURIED_LEN CONSTSTRLEN(CMD_PEEK_BURIED)
#define CMD_PEEKJOB_LEN CONSTSTRLEN(CMD_PEEKJOB)
#define CMD_RESERVE_LEN CONSTSTRLEN(CMD_RESERVE)
#define CMD_RESERVE_TIMEOUT_LEN CONSTSTRLEN(CMD_RESERVE_TIMEOUT)
#define CMD_RESERVE_JOB_LEN CONSTSTRLEN(CMD_RESERVE_JOB)
#define CMD_DELETE_LEN CONSTSTRLEN(CMD_DELETE)
#define CMD_RELEASE_LEN CONSTSTRLEN(CMD_RELEASE)
#define CMD_BURY_LEN CONSTSTRLEN(CMD_BURY)
#define CMD_KICK_LEN CONSTSTRLEN(CMD_KICK)
#define CMD_KICKJOB_LEN CONSTSTRLEN(CMD_KICKJOB)
#define CMD_TOUCH_LEN CONSTSTRLEN(CMD_TOUCH)
#define CMD_STATS_LEN CONSTSTRLEN(CMD_STATS)
#define CMD_STATSJOB_LEN CONSTSTRLEN(CMD_STATSJOB)
#define CMD_USE_LEN CONSTSTRLEN(CMD_USE)
#define CMD_WATCH_LEN CONSTSTRLEN(CMD_WATCH)
#define CMD_IGNORE_LEN CONSTSTRLEN(CMD_IGNORE)
#define CMD_LIST_TUBES_LEN CONSTSTRLEN(CMD_LIST_TUBES)
#define CMD_LIST_TUBE_USED_LEN CONSTSTRLEN(CMD_LIST_TUBE_USED)
#define CMD_LIST_TUBES_WATCHED_LEN CONSTSTRLEN(CMD_LIST_TUBES_WATCHED)
#define CMD_STATS_TUBE_LEN CONSTSTRLEN(CMD_STATS_TUBE)
#define CMD_PAUSE_TUBE_LEN CONSTSTRLEN(CMD_PAUSE_TUBE)

#define MSG_FOUND "FOUND"
#define MSG_NOTFOUND "NOT_FOUND\r\n"
#define MSG_RESERVED "RESERVED"
#define MSG_DEADLINE_SOON "DEADLINE_SOON\r\n"
#define MSG_TIMED_OUT "TIMED_OUT\r\n"
#define MSG_DELETED "DELETED\r\n"
#define MSG_RELEASED "RELEASED\r\n"
#define MSG_BURIED "BURIED\r\n"
#define MSG_KICKED "KICKED\r\n"
#define MSG_TOUCHED "TOUCHED\r\n"
#define MSG_BURIED_FMT "BURIED %"PRIu64"\r\n"
#define MSG_INSERTED_FMT "INSERTED %"PRIu64"\r\n"
#define MSG_NOT_IGNORED "NOT_IGNORED\r\n"
#define MSG_WATCHING_1  "WATCHING 1\r\n"

#define MSG_OUT_OF_MEMORY "OUT_OF_MEMORY\r\n"
#define MSG_INTERNAL_ERROR "INTERNAL_ERROR\r\n"
#define MSG_DRAINING "DRAINING\r\n"
#define MSG_BAD_FORMAT "BAD_FORMAT\r\n"
#define MSG_UNKNOWN_COMMAND "UNKNOWN_COMMAND\r\n"
#define MSG_EXPECTED_CRLF "EXPECTED_CRLF\r\n"
#define MSG_JOB_TOO_BIG "JOB_TOO_BIG\r\n"

// Connection can be in one of these states:
#define STATE_WANT_COMMAND  0  // conn expects a command from the client
#define STATE_WANT_DATA     1  // conn expects a job data
#define STATE_SEND_JOB      2  // conn sends job to the client
#define STATE_SEND_WORD     3  // conn sends a line reply
#define STATE_WAIT          4  // client awaits for the job reservation
#define STATE_BITBUCKET     5  // conn discards content
#define STATE_CLOSE         6  // conn should be closed
#define STATE_WANT_ENDLINE  7  // skip until the end of a line

#define OP_UNKNOWN 0
#define OP_PUT 1
#define OP_PEEKJOB 2
#define OP_RESERVE 3
#define OP_DELETE 4
#define OP_RELEASE 5
#define OP_BURY 6
#define OP_KICK 7
#define OP_STATS 8
#define OP_STATSJOB 9
#define OP_PEEK_BURIED 10
#define OP_USE 11
#define OP_WATCH 12
#define OP_IGNORE 13
#define OP_LIST_TUBES 14
#define OP_LIST_TUBE_USED 15
#define OP_LIST_TUBES_WATCHED 16
#define OP_STATS_TUBE 17
#define OP_PEEK_READY 18
#define OP_PEEK_DELAYED 19
#define OP_RESERVE_TIMEOUT 20
#define OP_TOUCH 21
#define OP_QUIT 22
#define OP_PAUSE_TUBE 23
#define OP_KICKJOB 24
#define OP_RESERVE_JOB 25
#define TOTAL_OPS 26

#define STATS_FMT "---\n" \
    "current-jobs-urgent: %" PRIu64 "\n" \
    "current-jobs-ready: %" PRIu64 "\n" \
    "current-jobs-reserved: %" PRIu64 "\n" \
    "current-jobs-delayed: %" PRIu64 "\n" \
    "current-jobs-buried: %" PRIu64 "\n" \
    "cmd-put: %" PRIu64 "\n" \
    "cmd-peek: %" PRIu64 "\n" \
    "cmd-peek-ready: %" PRIu64 "\n" \
    "cmd-peek-delayed: %" PRIu64 "\n" \
    "cmd-peek-buried: %" PRIu64 "\n" \
    "cmd-reserve: %" PRIu64 "\n" \
    "cmd-reserve-with-timeout: %" PRIu64 "\n" \
    "cmd-delete: %" PRIu64 "\n" \
    "cmd-release: %" PRIu64 "\n" \
    "cmd-use: %" PRIu64 "\n" \
    "cmd-watch: %" PRIu64 "\n" \
    "cmd-ignore: %" PRIu64 "\n" \
    "cmd-bury: %" PRIu64 "\n" \
    "cmd-kick: %" PRIu64 "\n" \
    "cmd-touch: %" PRIu64 "\n" \
    "cmd-stats: %" PRIu64 "\n" \
    "cmd-stats-job: %" PRIu64 "\n" \
    "cmd-stats-tube: %" PRIu64 "\n" \
    "cmd-list-tubes: %" PRIu64 "\n" \
    "cmd-list-tube-used: %" PRIu64 "\n" \
    "cmd-list-tubes-watched: %" PRIu64 "\n" \
    "cmd-pause-tube: %" PRIu64 "\n" \
    "job-timeouts: %" PRIu64 "\n" \
    "total-jobs: %" PRIu64 "\n" \
    "max-job-size: %zu\n" \
    "current-tubes: %zu\n" \
    "current-connections: %u\n" \
    "current-producers: %u\n" \
    "current-workers: %u\n" \
    "current-waiting: %" PRIu64 "\n" \
    "total-connections: %u\n" \
    "pid: %ld\n" \
    "version: \"%s\"\n" \
    "rusage-utime: %d.%06d\n" \
    "rusage-stime: %d.%06d\n" \
    "uptime: %u\n" \
    "binlog-oldest-index: %d\n" \
    "binlog-current-index: %d\n" \
    "binlog-records-migrated: %" PRId64 "\n" \
    "binlog-records-written: %" PRId64 "\n" \
    "binlog-max-size: %d\n" \
    "draining: %s\n" \
    "id: %s\n" \
    "hostname: \"%s\"\n" \
    "os: \"%s\"\n" \
    "platform: \"%s\"\n" \
    "\r\n"

#define STATS_TUBE_FMT "---\n" \
    "name: \"%s\"\n" \
    "current-jobs-urgent: %" PRIu64 "\n" \
    "current-jobs-ready: %zu\n" \
    "current-jobs-reserved: %" PRIu64 "\n" \
    "current-jobs-delayed: %zu\n" \
    "current-jobs-buried: %" PRIu64 "\n" \
    "total-jobs: %" PRIu64 "\n" \
    "current-using: %u\n" \
    "current-watching: %u\n" \
    "current-waiting: %" PRIu64 "\n" \
    "cmd-delete: %" PRIu64 "\n" \
    "cmd-pause-tube: %" PRIu64 "\n" \
    "pause: %" PRId64 "\n" \
    "pause-time-left: %" PRId64 "\n" \
    "\r\n"

#define STATS_JOB_FMT "---\n" \
    "id: %" PRIu64 "\n" \
    "tube: \"%s\"\n" \
    "state: %s\n" \
    "pri: %u\n" \
    "age: %" PRId64 "\n" \
    "delay: %" PRId64 "\n" \
    "ttr: %" PRId64 "\n" \
    "time-left: %" PRId64 "\n" \
    "file: %d\n" \
    "reserves: %u\n" \
    "timeouts: %u\n" \
    "releases: %u\n" \
    "buries: %u\n" \
    "kicks: %u\n" \
    "\r\n"

// The size of the throw-away (BITBUCKET) buffer. Arbitrary.
#define BUCKET_BUF_SIZE 1024

static uint64 ready_ct = 0;
static uint64 delayed_ct = 0;
static uint64 paused_ct = 0;
static uint64 timeout_ct = 0;
static int tubes_dirty = 1; // set when tube list changes, cleared after stats sync

int64 now = 0;
static uint64 op_ct[TOTAL_OPS] = {0};
static struct stats global_stat = {0};

// Global delay tube heap: tubes ordered by their soonest delayed job's deadline.
// Provides O(1) lookup of the soonest delayed job across all tubes,
// replacing the O(tubes.len) scan in soonest_delayed_job().
static Heap delay_tube_heap;

static int
tube_delay_less(void *ta, void *tb)
{
    Tube *a = ta;
    Tube *b = tb;
    if (a->delay.len == 0) return 0;
    if (b->delay.len == 0) return 1;
    Job *ja = a->delay.data[0];
    Job *jb = b->delay.data[0];
    return ja->r.deadline_at < jb->r.deadline_at;
}

static void
tube_delay_setpos(void *t, size_t pos)
{
    ((Tube *)t)->delay_heap_index = pos;
}

// Update the global delay tube heap after a tube's delay heap changes.
// Call after inserting/removing a delayed job in a tube.
// On removal (delay.len == 0): always succeeds.
// On insert/re-sort: may fail under OOM. Returns 0 on failure, 1 on success.
// OOM failure is non-fatal: tube is temporarily invisible to
// soonest_delayed_job(). Next insert into this tube retries.
static int
delay_tube_update(Tube *t)
{
    if (t->delay.len == 0) {
        if (t->in_delay_heap) {
            heapremove(&delay_tube_heap, t->delay_heap_index);
            t->in_delay_heap = 0;
        }
        return 1;
    }
    if (t->in_delay_heap) {
        // Resift in-place: O(log n) single traversal vs remove+insert.
        heapresift(&delay_tube_heap, t->delay_heap_index);
        return 1;
    }
    t->in_delay_heap = heapinsert(&delay_tube_heap, t);
    if (!t->in_delay_heap) {
        twarnx("delay_tube_update: OOM inserting tube %s into delay heap", t->name);
    }
    return t->in_delay_heap;
}

// Pause tube heap: paused tubes ordered by unpause_at (soonest first).
// Provides O(1) lookup of the soonest tube to unpause,
// replacing the O(tubes.len) scan in prottick().
static Heap pause_tube_heap;

static int
tube_pause_less(void *ta, void *tb)
{
    Tube *a = ta;
    Tube *b = tb;
    return a->unpause_at < b->unpause_at;
}

static void
tube_pause_setpos(void *t, size_t pos)
{
    ((Tube *)t)->pause_heap_index = pos;
}

// Update tube's membership in the pause heap.
// Call after any change to tube->pause.
static void
pause_tube_update(Tube *t)
{
    if (!t->pause) {
        if (t->in_pause_heap) {
            heapremove(&pause_tube_heap, t->pause_heap_index);
            t->in_pause_heap = 0;
        }
        return;
    }
    if (t->in_pause_heap) {
        heapresift(&pause_tube_heap, t->pause_heap_index);
        return;
    }
    t->in_pause_heap = heapinsert(&pause_tube_heap, t);
    if (!t->in_pause_heap) {
        twarnx("pause_tube_update: OOM inserting tube %s into pause heap", t->name);
    }
}

// Clean up prot.c internal state when a tube is about to be freed.
// Called from tube_free() in tube.c.
void
prot_remove_tube(Tube *t)
{
    if (t->pause)
        paused_ct--;
    if (t->ready.len > 0)
        ready_ct -= t->ready.len;
    if (t->delay.len > 0)
        delayed_ct -= t->delay.len;
    if (t->in_delay_heap) {
        heapremove(&delay_tube_heap, t->delay_heap_index);
        t->in_delay_heap = 0;
    }
    if (t->in_pause_heap) {
        heapremove(&pause_tube_heap, t->pause_heap_index);
        t->in_pause_heap = 0;
    }
    tubes_dirty = 1;
}

static Tube *default_tube;

// If drain_mode is 1, then server does not accept new jobs.
// Variable is set by the SIGUSR1 handler.
static volatile sig_atomic_t drain_mode = 0;

static int64 started_at;

enum { instance_id_bytes = 8 };
static char instance_hex[instance_id_bytes * 2 + 1]; // hex-encoded len of instance_id_bytes

static struct utsname node_info;

// Single linked list with connections that require updates
// in the event notification mechanism.
static Conn *epollq;

static const char * op_names[] = {
    "<unknown>",
    CMD_PUT,
    CMD_PEEKJOB,
    CMD_RESERVE,
    CMD_DELETE,
    CMD_RELEASE,
    CMD_BURY,
    CMD_KICK,
    CMD_STATS,
    CMD_STATSJOB,
    CMD_PEEK_BURIED,
    CMD_USE,
    CMD_WATCH,
    CMD_IGNORE,
    CMD_LIST_TUBES,
    CMD_LIST_TUBE_USED,
    CMD_LIST_TUBES_WATCHED,
    CMD_STATS_TUBE,
    CMD_PEEK_READY,
    CMD_PEEK_DELAYED,
    CMD_RESERVE_TIMEOUT,
    CMD_TOUCH,
    CMD_QUIT,
    CMD_PAUSE_TUBE,
    CMD_KICKJOB,
    CMD_RESERVE_JOB,
};

static Job *remove_ready_job(Job *j);
static Job *remove_buried_job(Job *j);
static Job *remove_delayed_job(Job *j);

// epollq_add schedules connection c in the s->conns heap, adds c
// to the epollq list to change expected operation in event notifications.
// rw='w' means to notify when socket is writeable, 'r' - readable, 'h' - closed.
__attribute__((hot)) static inline void
epollq_add(Conn *c, char rw) {
    c->rw = rw;
    connsched(c);
    c->next = epollq;
    epollq = c;
}

// epollq_rmconn removes connection c from the epollq.
static void
epollq_rmconn(Conn *c)
{
    Conn **pp = &epollq;
    while (*pp) {
        if (*pp == c) {
            *pp = c->next;
            c->next = NULL;
            return;
        }
        pp = &(*pp)->next;
    }
}

static void conn_want_command(Conn *c);

// Propagate changes to event notification mechanism about expected operations
// in connections' sockets. Clear the epollq list.
static void
epollq_apply()
{
    Conn *c;

    while (epollq) {
        c = epollq;
        epollq = epollq->next;
        c->next = NULL;
        if (c->sock.fd >= 0) {
            int r = sockwant(&c->sock, c->rw);
            if (r == -1 && errno != EBADF) {
                twarn("sockwant");
                connclose(c);
            }
        }
    }
}

#define reply_msg(c, m) \
    reply((c), (m), CONSTSTRLEN(m), STATE_SEND_WORD)

#define reply_serr(c, e) \
    (twarnx("server error: %s", (e)), reply_msg((c), (e)))

__attribute__((hot)) static void
reply(Conn *c, char *line, int len, int state)
{
    if (!c)
        return;

    if (verbose >= 2) {
        printf(">%d reply %.*s\n", c->sock.fd, len-2, line);
    }

    // Try immediate write; fall through to epoll on EAGAIN/partial.
    if (likely(state == STATE_SEND_WORD)) {
        int r = write(c->sock.fd, line, len);
        if (likely(r == len)) {
            // Fast path: skip c->reply* stores (only needed for retry).
            c->state = STATE_WANT_COMMAND;
            if (unlikely(c->out_job)) {
                if (c->out_job->r.state == Copy)
                    job_free(c->out_job);
                c->out_job = NULL;
            }
            if (unlikely(c->in_conns || c->pending_timeout >= 0))
                connsched(c);
            return;
        }
        // Partial write: record progress for retry via epoll.
        if (unlikely(r > 0)) {
            c->reply = line;
            c->reply_len = len;
            c->reply_sent = r;
            c->state = state;
            epollq_add(c, 'w');
            return;
        }
        // r == -1: fatal errors close immediately, EAGAIN falls through.
        if (r == -1 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            c->state = STATE_CLOSE;
            return;
        }
    }

    // Immediate writev of header + body.
    if (state == STATE_SEND_JOB && c->out_job) {
        struct iovec iov[2];
        iov[0].iov_base = line;
        iov[0].iov_len = len;
        iov[1].iov_base = c->out_job->body;
        iov[1].iov_len = c->out_job->r.body_size;

        ssize_t total = len + c->out_job->r.body_size;
        ssize_t r = writev(c->sock.fd, iov, 2);
        if (r == total) {
            // Fast path: skip c->reply* stores (conn_want_command resets state).
            if (verbose >= 2) {
                printf(">%d job %"PRIu64"\n", c->sock.fd, c->out_job->r.id);
            }
            conn_want_command(c);
            return;
        }
        if (r > 0) {
            // Partial write: track progress, fall through to epoll.
            c->reply = line;
            c->reply_len = len;
            c->reply_sent = 0;
            c->out_job_sent = 0;
            if (r >= len) {
                c->reply_sent = len;
                c->out_job_sent = r - len;
            } else {
                c->reply_sent = r;
            }
            c->state = STATE_SEND_JOB;
            epollq_add(c, 'w');
            return;
        }
        if (r == -1 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            c->state = STATE_CLOSE;
            return;
        }
        // EAGAIN: fall through to epoll-based send.
    }

    epollq_add(c, 'w');

    c->reply = line;
    c->reply_len = len;
    c->reply_sent = 0;
    c->state = state;
}

static void
reply_line(Conn*, int, const char*, ...)
__attribute__((format(printf, 3, 4)));

// reply_line prints *fmt into c->reply_buffer and
// calls reply() for the string and state.
static void
reply_line(Conn *c, int state, const char *fmt, ...)
{
    int r;
    va_list ap;

    va_start(ap, fmt);
    r = vsnprintf(c->reply_buf, LINE_BUF_SIZE, fmt, ap);
    va_end(ap);

    /* Make sure the buffer was big enough. If not, we have a bug. */
    if (r < 0 || r >= LINE_BUF_SIZE) {
        reply_serr(c, MSG_INTERNAL_ERROR);
        return;
    }

    reply(c, c->reply_buf, r, state);
}

// Two-digit lookup table: "00", "01", ..., "99".
// Halves the number of divisions in decimal conversion.
static const char digits[200] __attribute__((aligned(64))) = {
    '0','0','0','1','0','2','0','3','0','4','0','5','0','6','0','7','0','8','0','9',
    '1','0','1','1','1','2','1','3','1','4','1','5','1','6','1','7','1','8','1','9',
    '2','0','2','1','2','2','2','3','2','4','2','5','2','6','2','7','2','8','2','9',
    '3','0','3','1','3','2','3','3','3','4','3','5','3','6','3','7','3','8','3','9',
    '4','0','4','1','4','2','4','3','4','4','4','5','4','6','4','7','4','8','4','9',
    '5','0','5','1','5','2','5','3','5','4','5','5','5','6','5','7','5','8','5','9',
    '6','0','6','1','6','2','6','3','6','4','6','5','6','6','6','7','6','8','6','9',
    '7','0','7','1','7','2','7','3','7','4','7','5','7','6','7','7','7','8','7','9',
    '8','0','8','1','8','2','8','3','8','4','8','5','8','6','8','7','8','8','8','9',
    '9','0','9','1','9','2','9','3','9','4','9','5','9','6','9','7','9','8','9','9',
};

// uint64 to decimal, right-to-left. buf must have 20 bytes.
// Uses 2-digit pairs to halve the number of div/mod operations.
__attribute__((hot)) static char *
u64toa(char *end, uint64 v)
{
    while (v >= 100) {
        int r = (int)(v % 100);
        v /= 100;
        *--end = digits[r * 2 + 1];
        *--end = digits[r * 2];
    }
    if (v >= 10) {
        *--end = digits[v * 2 + 1];
        *--end = digits[v * 2];
    } else {
        *--end = '0' + (char)v;
    }
    return end;
}

// reply "INSERTED <id>\r\n" without vsnprintf.
__attribute__((hot)) static void
reply_inserted(Conn *c, uint64 id)
{
    char *buf = c->reply_buf;
    memcpy(buf, "INSERTED ", 9);
    char numbuf[20];
    char *numend = numbuf + 20;
    char *numstart = u64toa(numend, id);
    int nlen = (int)(numend - numstart);
    memcpy(buf + 9, numstart, nlen);
    buf[9 + nlen] = '\r';
    buf[10 + nlen] = '\n';
    reply(c, buf, 11 + nlen, STATE_SEND_WORD);
}

// reply "USING <name>\r\n" without vsnprintf.
static void
reply_using(Conn *c, Tube *t)
{
    char *buf = c->reply_buf;
    memcpy(buf, "USING ", 6);
    memcpy(buf + 6, t->name, t->name_len);
    buf[6 + t->name_len] = '\r';
    buf[7 + t->name_len] = '\n';
    reply(c, buf, 8 + t->name_len, STATE_SEND_WORD);
}

// reply_job tells the connection c which job to send.
// Build "MSG ID SIZE\r\n" without vsnprintf.
// Use the reply_job macro to compute msg length at compile time.
__attribute__((hot)) static void
reply_job_n(Conn *c, Job *j, const char *msg, int msglen)
{
    c->out_job = j;
    c->out_job_sent = 0;

    char *buf = c->reply_buf;
    memcpy(buf, msg, msglen);
    buf[msglen] = ' ';
    buf += msglen + 1;

    char numbuf[20];
    char *end = numbuf + 20;
    char *start = u64toa(end, j->r.id);
    int nlen = (int)(end - start);
    memcpy(buf, start, nlen);
    buf += nlen;

    *buf++ = ' ';
    start = u64toa(end, j->r.body_size - 2);
    nlen = (int)(end - start);
    memcpy(buf, start, nlen);
    buf += nlen;

    *buf++ = '\r';
    *buf++ = '\n';
    reply(c, c->reply_buf, (int)(buf - c->reply_buf), STATE_SEND_JOB);
}
#define reply_job(c, j, msg) reply_job_n(c, j, msg, sizeof(msg) - 1)

// remove_waiting_conn unsets CONN_TYPE_WAITING for the connection,
// removes it from the waiting_conns set of every tube it's watching.
// Noop if connection is not waiting.
void
remove_waiting_conn(Conn *c)
{
    if (!conn_waiting(c))
        return;

    c->type &= ~CONN_TYPE_WAITING;
    global_stat.waiting_ct--;
    Tube *t = c->watch;
    if (!t) return;
    t->stat.waiting_ct--;
    ms_remove_at(&t->waiting_conns, c->watch_idx, c);
}

// enqueue_waiting_conn sets CONN_TYPE_WAITING for the connection,
// adds it to the waiting_conns of the single watched tube.
static int
enqueue_waiting_conn(Conn *c)
{
    Tube *t = c->watch;
    if (!ms_append(&t->waiting_conns, c))
        return 0;
    c->watch_idx = t->waiting_conns.len - 1;
    t->stat.waiting_ct++;
    c->type |= CONN_TYPE_WAITING;
    global_stat.waiting_ct++;
    return 1;
}

// process_tube matches ready jobs with waiting connections in a single tube.
__attribute__((hot)) static void
process_tube(Tube *t)
{
    if (t->pause) return;  // hoisted: pause can't change during the loop
    while (t->ready.len > 0 && t->waiting_conns.len > 0) {
        Job *j = remove_ready_job(t->ready.data[0]);
        if (!j) {
            twarnx("job not ready");
            break;
        }
        Conn *c = ms_take(&t->waiting_conns);
        if (!c) {
            // Re-enqueue the orphaned job.
            twarnx("waiting_conns is empty");
            heapinsert(&j->tube->ready, j);
            j->r.state = Ready;
            ready_ct++;
            if (j->r.pri < URGENT_THRESHOLD) {
                global_stat.urgent_ct++;
                j->tube->stat.urgent_ct++;
            }
            break;
        }
        global_stat.reserved_ct++;

        // Clear waiting state directly — ms_take already removed c
        // from this tube's waiting_conns. Calling remove_waiting_conn()
        // would try to remove c again (double-removal bug).
        c->type &= ~CONN_TYPE_WAITING;
        global_stat.waiting_ct--;
        t->stat.waiting_ct--;

        conn_reserve_job(c, j);
        reply_job(c, j, MSG_RESERVED);
    }
}

// soonest_delayed_job returns the delayed job
// with the smallest deadline_at among all tubes. O(1) via global heap.
static Job *
soonest_delayed_job()
{
    if (delay_tube_heap.len == 0)
        return NULL;
    Tube *t = delay_tube_heap.data[0];
    if (t->delay.len == 0)
        return NULL;
    return t->delay.data[0];
}

// enqueue_job inserts job j in the tube, returns 1 on success, otherwise 0.
// If update_store then it writes an entry to WAL.
// On success it processes the queue.
__attribute__((hot)) static int
enqueue_job(Server *s, Job *j, int64 delay, char update_store)
{
    int r;
    Wal *w = shard_wal(s, j);

    j->reserver = NULL;
    if (unlikely(delay)) {
        j->r.deadline_at = now + delay;
        r = heapinsert(&j->tube->delay, j);
        if (unlikely(!r))
            return 0;
        j->r.state = Delayed;
        delayed_ct++;
        delay_tube_update(j->tube);
    } else {
        r = heapinsert(&j->tube->ready, j);
        if (unlikely(!r))
            return 0;
        j->r.state = Ready;
        ready_ct++;
        if (j->r.pri < URGENT_THRESHOLD) {
            global_stat.urgent_ct++;
            j->tube->stat.urgent_ct++;
        }
        if (j->tube->waiting_conns.len)
            process_tube(j->tube);
    }

    if (update_store) {
        if (unlikely(!walwrite(w, j))) {
            // Rollback: remove from heap so callers don't face
            // a job present in two data structures.
            if (delay) {
                heapremove(&j->tube->delay, j->heap_index);
                delayed_ct--;
                delay_tube_update(j->tube);
            } else {
                heapremove(&j->tube->ready, j->heap_index);
                ready_ct--;
                if (j->r.pri < URGENT_THRESHOLD) {
                    global_stat.urgent_ct--;
                    j->tube->stat.urgent_ct--;
                }
            }
            return 0;
        }
        if (!walmaint(w)) {
            twarnx("walmaint failed after walwrite");
        }
    }

    return 1;
}

__attribute__((cold)) static int
bury_job(Server *s, Job *j, char update_store)
{
    Wal *w = shard_wal(s, j);
    if (update_store) {
        int z = walresvupdate(w);
        if (!z)
            return 0;
        j->walresv += z;
    }

    job_list_insert(&j->tube->buried, j);
    global_stat.buried_ct++;
    j->tube->stat.buried_ct++;
    j->r.state = Buried;
    j->reserver = NULL;
    j->r.bury_ct++;

    if (update_store) {
        if (!walwrite(w, j)) {
            return 0;
        }
        if (!walmaint(w)) {
            twarnx("walmaint failed after bury walwrite");
        }
    }

    return 1;
}

void
enqueue_reserved_jobs(Conn *c)
{
    c->soonest_job = NULL;
    while (!job_list_is_empty(&c->reserved_jobs)) {
        Job *j = job_list_remove(c->reserved_jobs.next);
        int r = enqueue_job(c->srv, j, 0, 0);
        if (r < 1)
            bury_job(c->srv, j, 0);
        global_stat.reserved_ct--;
        j->tube->stat.reserved_ct--;
    }
}

static int
kick_buried_job(Server *s, Job *j)
{
    int r;
    int z;
    Wal *w = shard_wal(s, j);

    z = walresvupdate(w);
    if (!z)
        return 0;
    j->walresv += z;

    remove_buried_job(j);

    j->r.kick_ct++;
    r = enqueue_job(s, j, 0, 1);
    if (r == 1)
        return 1;

    /* ready queue is full, so bury it */
    walresvreturn(w, z);
    j->walresv -= z;
    bury_job(s, j, 0);
    return 0;
}



static int
kick_delayed_job(Server *s, Job *j)
{
    int r;
    int z;
    Wal *w = shard_wal(s, j);

    z = walresvupdate(w);
    if (!z)
        return 0;
    j->walresv += z;

    remove_delayed_job(j);

    j->r.kick_ct++;
    r = enqueue_job(s, j, 0, 1);
    if (r == 1)
        return 1;

    /* ready queue is full, so delay it again */
    r = enqueue_job(s, j, j->r.delay, 0);
    if (r == 1) {
        walresvreturn(w, z);
        j->walresv -= z;
        return 0;
    }

    /* last resort */
    walresvreturn(w, z);
    j->walresv -= z;
    bury_job(s, j, 0);
    return 0;
}

/* return the number of jobs successfully kicked */
static uint
kick_buried_jobs(Server *s, Tube *t, uint n)
{
    uint i;
    for (i = 0; (i < n) && !job_list_is_empty(&t->buried); ++i) {
        kick_buried_job(s, t->buried.next);
    }
    return i;
}

/* return the number of jobs successfully kicked */
static uint
kick_delayed_jobs(Server *s, Tube *t, uint n)
{
    uint i;
    for (i = 0; (i < n) && (t->delay.len > 0); ++i) {
        kick_delayed_job(s, (Job *)t->delay.data[0]);
    }
    return i;
}

static inline uint
kick_jobs(Server *s, Tube *t, uint n)
{
    if (!job_list_is_empty(&t->buried))
        return kick_buried_jobs(s, t, n);
    return kick_delayed_jobs(s, t, n);
}

// remove_buried_job returns non-NULL value if job j was in the buried state.
// It excludes the job from the buried list and updates counters.
__attribute__((hot)) static Job *
remove_buried_job(Job *j)
{
    if (!j || j->r.state != Buried)
        return NULL;
    j = job_list_remove(j);
    if (j) {
        global_stat.buried_ct--;
        j->tube->stat.buried_ct--;
    }
    return j;
}

// remove_delayed_job returns non-NULL value if job j was in the delayed state.
// It removes the job from the tube delayed heap.
__attribute__((hot)) static Job *
remove_delayed_job(Job *j)
{
    if (!j || j->r.state != Delayed)
        return NULL;
    heapremove(&j->tube->delay, j->heap_index);
    delayed_ct--;
    delay_tube_update(j->tube);

    return j;
}

// remove_ready_job returns non-NULL value if job j was in the ready state.
// It removes the job from the tube ready heap and updates counters.
__attribute__((hot)) static Job *
remove_ready_job(Job *j)
{
    if (!j || j->r.state != Ready)
        return NULL;
    heapremove(&j->tube->ready, j->heap_index);
    ready_ct--;
    if (j->r.pri < URGENT_THRESHOLD) {
        global_stat.urgent_ct--;
        j->tube->stat.urgent_ct--;
    }
    return j;
}

static bool
is_job_reserved_by_conn(Conn *c, Job *j)
{
    return likely(j != NULL) && j->reserver == c && j->r.state == Reserved;
}

static bool
touch_job(Conn *c, Job *j)
{
    if (is_job_reserved_by_conn(c, j)) {
        j->r.deadline_at = now + j->r.ttr;
        c->soonest_job = NULL;
        return true;
    }
    return false;
}

static inline void
check_err(Conn *c, const char *s)
{
    if (likely(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
        return;

    twarn("%s", s);
    c->state = STATE_CLOSE;
}

/* Scan the given string for the sequence "\r\n" and return the line length.
 * Always returns at least 2 if a match is found. Returns 0 if no match. */
__attribute__((hot)) static size_t
scan_line_end(const char *s, size_t size)
{
    const char *p = s;
    size_t remaining = size > 0 ? size - 1 : 0;

    while (remaining > 0) {
        char *match = memchr(p, '\r', remaining);
        if (!match)
            return 0;

        /* safe: remaining guarantees match[1] is within the buffer */
        if (match[1] == '\n')
            return match - s + 2;

        size_t skip = match - p + 1;
        remaining -= skip;
        p = match + 1;
    }

    return 0;
}

/* parse the command line */
__attribute__((hot)) static int
which_cmd(Conn *c)
{
#define TEST_CMD(s,c,o) if (strncmp((s), (c), CONSTSTRLEN(c)) == 0) return (o);
    /* Dispatch by first byte reduces average strncmp calls from 25 to 1-6. */
    switch (c->cmd[0]) {
    case 'p':
        TEST_CMD(c->cmd, CMD_PUT, OP_PUT);
        TEST_CMD(c->cmd, CMD_PEEKJOB, OP_PEEKJOB);
        TEST_CMD(c->cmd, CMD_PEEK_READY, OP_PEEK_READY);
        TEST_CMD(c->cmd, CMD_PEEK_DELAYED, OP_PEEK_DELAYED);
        TEST_CMD(c->cmd, CMD_PEEK_BURIED, OP_PEEK_BURIED);
        TEST_CMD(c->cmd, CMD_PAUSE_TUBE, OP_PAUSE_TUBE);
        break;
    case 'r':
        // Disambiguate: "release " vs "reserve-..." vs bare "reserve\r"
        if (c->cmd[2] == 'l') { // reLease
            TEST_CMD(c->cmd, CMD_RELEASE, OP_RELEASE);
        } else if (c->cmd_len > 8 && c->cmd[7] == '-') { // reserve-...
            TEST_CMD(c->cmd, CMD_RESERVE_TIMEOUT, OP_RESERVE_TIMEOUT);
            TEST_CMD(c->cmd, CMD_RESERVE_JOB, OP_RESERVE_JOB);
        } else { // bare "reserve\r\n"
            TEST_CMD(c->cmd, CMD_RESERVE, OP_RESERVE);
        }
        break;
    case 'd':
        TEST_CMD(c->cmd, CMD_DELETE, OP_DELETE);
        break;
    case 'b':
        TEST_CMD(c->cmd, CMD_BURY, OP_BURY);
        break;
    case 'k':
        TEST_CMD(c->cmd, CMD_KICK, OP_KICK);
        TEST_CMD(c->cmd, CMD_KICKJOB, OP_KICKJOB);
        break;
    case 't':
        TEST_CMD(c->cmd, CMD_TOUCH, OP_TOUCH);
        break;
    case 's':
        TEST_CMD(c->cmd, CMD_STATSJOB, OP_STATSJOB);
        TEST_CMD(c->cmd, CMD_STATS_TUBE, OP_STATS_TUBE);
        TEST_CMD(c->cmd, CMD_STATS, OP_STATS);
        break;
    case 'u':
        TEST_CMD(c->cmd, CMD_USE, OP_USE);
        break;
    case 'w':
        TEST_CMD(c->cmd, CMD_WATCH, OP_WATCH);
        break;
    case 'i':
        TEST_CMD(c->cmd, CMD_IGNORE, OP_IGNORE);
        break;
    case 'l':
        TEST_CMD(c->cmd, CMD_LIST_TUBES_WATCHED, OP_LIST_TUBES_WATCHED);
        TEST_CMD(c->cmd, CMD_LIST_TUBE_USED, OP_LIST_TUBE_USED);
        TEST_CMD(c->cmd, CMD_LIST_TUBES, OP_LIST_TUBES);
        break;
    case 'q':
        TEST_CMD(c->cmd, CMD_QUIT, OP_QUIT);
        break;
    }
    return OP_UNKNOWN;
}

/* Copy up to body_size trailing bytes into the job, then the rest into the cmd
 * buffer. If c->in_job exists, this assumes that c->in_job->body is empty.
 * This function is idempotent(). */
__attribute__((hot)) static void
fill_extra_data(Conn *c)
{
    if (!c->cmd_len)
        return; /* we don't have a complete command */

    /* how many extra bytes did we read? */
    int64 extra_bytes = c->cmd_read - c->cmd_len;
    if (extra_bytes == 0 && !c->in_job && !c->in_job_read) {
        c->cmd_read = 0;
        c->cmd_len = 0;
        return;
    }

    int64 job_data_bytes = 0;
    /* how many bytes should we put into the job body? */
    if (c->in_job) {
        job_data_bytes = min(extra_bytes, c->in_job->r.body_size);
        memcpy(c->in_job->body, c->cmd + c->cmd_len, job_data_bytes);
        c->in_job_read = job_data_bytes;
    } else if (c->in_job_read) {
        /* we are in bit-bucket mode, throwing away data */
        job_data_bytes = min(extra_bytes, c->in_job_read);
        c->in_job_read -= job_data_bytes;
    }

    /* how many bytes are left to go into the future cmd? */
    int64 cmd_bytes = extra_bytes - job_data_bytes;
    if (cmd_bytes > 0)
        memmove(c->cmd, c->cmd + c->cmd_len + job_data_bytes, cmd_bytes);
    c->cmd_read = cmd_bytes;
    c->cmd_len = 0; /* we no longer know the length of the new command */
}

#define skip(conn,n,msg) (_skip(conn, n, msg, CONSTSTRLEN(msg)))

static void
_skip(Conn *c, int64 n, char *msg, int msglen)
{
    /* Invert the meaning of in_job_read while throwing away data -- it
     * counts the bytes that remain to be thrown away. */
    c->in_job = 0;
    c->in_job_read = n;
    fill_extra_data(c);

    if (c->in_job_read == 0) {
        reply(c, msg, msglen, STATE_SEND_WORD);
        return;
    }

    c->reply = msg;
    c->reply_len = msglen;
    c->reply_sent = 0;
    c->state = STATE_BITBUCKET;
}

__attribute__((hot)) static void
enqueue_incoming_job(Conn *c)
{
    int r;
    Job *j = c->in_job;

    c->in_job = NULL; /* the connection no longer owns this job */
    c->in_job_read = 0;

    /* check if the trailer is present and correct */
    if (memcmp(j->body + j->r.body_size - 2, "\r\n", 2)) {
        job_free(j);
        reply_msg(c, MSG_EXPECTED_CRLF);
        return;
    }

    if (verbose >= 2) {
        printf("<%d job %"PRIu64"\n", c->sock.fd, j->r.id);
    }

    if (drain_mode) {
        job_free(j);
        reply_serr(c, MSG_DRAINING);
        return;
    }

    if (j->walresv) {
        job_free(j);
        reply_serr(c, MSG_INTERNAL_ERROR);
        return;
    }
    j->walresv = walresvput(shard_wal(c->srv, j), j);
    if (!j->walresv) {
        job_free(j);
        reply_serr(c, MSG_OUT_OF_MEMORY);
        return;
    }

    // In multi-worker mode, forward put to the tube's owner worker.
    if (c->srv->nworkers > 1) {
        int target = j->tube->owner;
        if (target != c->srv->worker_id && c->srv->peer_fd[target] >= 0) {
          if (j->r.body_size > PUT_FWD_MAX_BODY + 2) {
            twarnx("put forwarding: body %d exceeds %d, enqueue local",
                   j->r.body_size, PUT_FWD_MAX_BODY);
          } else {
            uint32 seq = pending_fwd_alloc(c->srv, c);
            if (seq) {
                static struct PutFwdMsg pm;
                size_t hdr_size = offsetof(struct PutFwdMsg, body);
                pm.magic = PUT_FWD_MAGIC;
                pm.from_worker = c->srv->worker_id;
                pm.seq = seq;
                pm.pri = j->r.pri;
                pm.delay = j->r.delay;
                pm.ttr = j->r.ttr;
                memcpy(pm.tube, j->tube->name, j->tube->name_len + 1);
                pm.body_size = j->r.body_size;
                // sendmsg with iovec avoids memcpy of body (up to 64KB).
                struct iovec iov[2] = {
                    { .iov_base = &pm,     .iov_len = hdr_size },
                    { .iov_base = j->body, .iov_len = j->r.body_size },
                };
                struct msghdr msg = {0};
                msg.msg_iov = iov;
                msg.msg_iovlen = 2;
                ssize_t wr = sendmsg(c->srv->peer_fd[target], &msg, MSG_NOSIGNAL);
                size_t msg_size = hdr_size + j->r.body_size;
                if (wr == (ssize_t)msg_size) {
                    if (j->walresv) {
                        Wal *w = shard_wal(c->srv, j);
                        walresvreturn(w, j->walresv);
                        j->walresv = 0;
                    }
                    job_free(j);
                    c->fwd_pending = 1;
                    epollq_add(c, 0);
                    return;
                }
                // Undo slot on failure.
                {
                    int idx = pending_fwd_find(c->srv, seq);
                    if (idx >= 0) {
                        c->srv->pending_fwd[idx].conn = NULL;
                        c->srv->pending_fwd[idx].seq = 0;
                        c->srv->pending_fwd_used--;
                    }
                }
            }
          }
        }
    }

    /* we have a complete job, so let's stick it in the pqueue */
    global_stat.total_jobs_ct++;
    j->tube->stat.total_jobs_ct++;
    r = enqueue_job(c->srv, j, j->r.delay, 1);

    if (likely(r == 1)) {
        reply_inserted(c, j->r.id);
        return;
    }

    /* out of memory trying to grow the queue, so it gets buried */
    bury_job(c->srv, j, 0);
    reply_line(c, STATE_SEND_WORD, MSG_BURIED_FMT, j->r.id);
}

static uint
uptime()
{
    return (now - started_at) / 1000000000;
}

static int
fmt_stats(char *buf, size_t size, void *x)
{
    int whead = 0, wcur = 0;
    Server *s = x;
    struct rusage ru;
    int64 total_nmig = s->wal.nmig;
    int64 total_nrec = s->wal.nrec;

    if (s->wal.head) {
        whead = s->wal.head->seq;
    }

    if (s->wal.cur) {
        wcur = s->wal.cur->seq;
    }

    for (int i = 0; i < s->nshards; i++) {
        total_nmig += s->shards[i].nmig;
        total_nrec += s->shards[i].nrec;
    }

    // In multi-worker mode, aggregate stats from all workers via shared memory.
    uint64 agg_urgent = global_stat.urgent_ct;
    uint64 agg_ready = ready_ct;
    uint64 agg_reserved = global_stat.reserved_ct;
    uint64 agg_delayed = delayed_ct;
    uint64 agg_buried = global_stat.buried_ct;
    uint64 agg_waiting = global_stat.waiting_ct;
    uint64 agg_timeout = timeout_ct;
    uint64 agg_total_jobs = global_stat.total_jobs_ct;
    uint64 agg_op[TOTAL_OPS];
    for (int i = 0; i < TOTAL_OPS; i++)
        agg_op[i] = op_ct[i];
    uint agg_conns = count_cur_conns();
    uint agg_producers = count_cur_producers();
    uint agg_workers = count_cur_workers();
    uint agg_tot_conns = count_tot_conns();
    size_t agg_tubes = tubes.len;

    if (shared_stats && s->nworkers > 1) {
        // Sum stats from ALL workers (including self via shared_stats).
        agg_urgent = 0; agg_ready = 0; agg_reserved = 0;
        agg_delayed = 0; agg_buried = 0; agg_waiting = 0;
        agg_timeout = 0; agg_total_jobs = 0;
        agg_conns = 0; agg_producers = 0; agg_workers = 0;
        agg_tot_conns = 0; agg_tubes = 0;
        for (int i = 0; i < TOTAL_OPS; i++)
            agg_op[i] = 0;

        for (int w = 0; w < s->nworkers; w++) {
            struct SharedStats *ss = &shared_stats[w];
            // Seqlock read: snapshot counters, retry if writer was active.
            uint32 v1, v2;
            uint64 su, sr, sres, sd, sb, sw2, sto, stj;
            uint32 scc, scp, scw, stc;
            uint64 sop[SHARED_STATS_OPS];
            do {
                v1 = __atomic_load_n(&ss->version, __ATOMIC_ACQUIRE);
                su = ss->urgent_ct; sr = ss->ready_ct;
                sres = ss->reserved_ct; sd = ss->delayed_ct;
                sb = ss->buried_ct; sw2 = ss->waiting_ct;
                sto = ss->timeout_ct; stj = ss->total_jobs_ct;
                scc = ss->cur_conn_ct; scp = ss->cur_producer_ct;
                scw = ss->cur_worker_ct; stc = ss->tot_conn_ct;
                for (int i = 0; i < TOTAL_OPS && i < SHARED_STATS_OPS; i++)
                    sop[i] = ss->op_ct[i];
                v2 = __atomic_load_n(&ss->version, __ATOMIC_ACQUIRE);
            } while ((v1 & 1) || v1 != v2);
            agg_urgent += su; agg_ready += sr;
            agg_reserved += sres; agg_delayed += sd;
            agg_buried += sb; agg_waiting += sw2;
            agg_timeout += sto; agg_total_jobs += stj;
            agg_conns += scc; agg_producers += scp;
            agg_workers += scw; agg_tot_conns += stc;
            for (int i = 0; i < TOTAL_OPS && i < SHARED_STATS_OPS; i++)
                agg_op[i] += sop[i];
        }
        // Deduplicate tube count across workers using open-addressed hash set.
        // O(W*T) expected with O(1) per probe. Hash-first filter skips strcmp.
        #define SEEN_CAP 2048  // power of 2, > SHARED_MAX_TUBES
        #define SEEN_MASK (SEEN_CAP - 1)
        static const char *seen_name[SEEN_CAP];
        static uint32 seen_hash[SEEN_CAP];
        memset(seen_name, 0, sizeof(seen_name));
        int nseen = 0;
        for (int w = 0; w < s->nworkers; w++) {
            struct SharedStats *ss2 = &shared_stats[w];
            for (uint32 ti = 0; ti < ss2->tube_count; ti++) {
                const char *tname = ss2->tube_names[ti];
                uint32 h = tube_name_hash(tname);
                for (int probe = 0; probe < SEEN_CAP; probe++) {
                    int idx = (h + probe) & SEEN_MASK;
                    if (!seen_name[idx]) {
                        seen_name[idx] = tname;
                        seen_hash[idx] = h;
                        nseen++;
                        break;
                    }
                    if (seen_hash[idx] == h && strcmp(seen_name[idx], tname) == 0)
                        break;
                }
            }
        }
        #undef SEEN_CAP
        #undef SEEN_MASK
        agg_tubes = nseen;
    }

    // Cache rusage: refresh at most once per 100ms to avoid syscall per stats.
    static struct rusage cached_ru;
    static int64 last_ru_at;
    if (now - last_ru_at > 100000000LL) {
        getrusage(RUSAGE_SELF, &cached_ru);
        last_ru_at = now;
    }
    ru = cached_ru;
    return snprintf(buf, size, STATS_FMT,
                    agg_urgent,
                    agg_ready,
                    agg_reserved,
                    agg_delayed,
                    agg_buried,
                    agg_op[OP_PUT],
                    agg_op[OP_PEEKJOB],
                    agg_op[OP_PEEK_READY],
                    agg_op[OP_PEEK_DELAYED],
                    agg_op[OP_PEEK_BURIED],
                    agg_op[OP_RESERVE],
                    agg_op[OP_RESERVE_TIMEOUT],
                    agg_op[OP_DELETE],
                    agg_op[OP_RELEASE],
                    agg_op[OP_USE],
                    agg_op[OP_WATCH],
                    agg_op[OP_IGNORE],
                    agg_op[OP_BURY],
                    agg_op[OP_KICK],
                    agg_op[OP_TOUCH],
                    agg_op[OP_STATS],
                    agg_op[OP_STATSJOB],
                    agg_op[OP_STATS_TUBE],
                    agg_op[OP_LIST_TUBES],
                    agg_op[OP_LIST_TUBE_USED],
                    agg_op[OP_LIST_TUBES_WATCHED],
                    agg_op[OP_PAUSE_TUBE],
                    agg_timeout,
                    agg_total_jobs,
                    job_data_size_limit,
                    agg_tubes,
                    agg_conns,
                    agg_producers,
                    agg_workers,
                    agg_waiting,
                    agg_tot_conns,
                    (long) getpid(),
                    version,
                    (int) ru.ru_utime.tv_sec, (int) ru.ru_utime.tv_usec,
                    (int) ru.ru_stime.tv_sec, (int) ru.ru_stime.tv_usec,
                    uptime(),
                    whead,
                    wcur,
                    total_nmig,
                    total_nrec,
                    s->wal.filesize,
                    drain_mode ? "true" : "false",
                    instance_hex,
                    node_info.nodename,
                    node_info.version,
                    node_info.machine);
}

/* Read an unsigned integer from buf, validate it fits within max_val.
 * Skip leading spaces. If end is NULL, require the entire string to be consumed.
 * Return 0 on success, -1 on failure. On failure, out and end are unmodified. */
static int
read_uint(uintmax_t *out, uintmax_t max_val, const char *buf, char **end)
{
    while (buf[0] == ' ')
        buf++;
    if (buf[0] < '0' || '9' < buf[0])
        return -1;

    // Inline base-10 parse (no locale/errno).
    // Pre-computed overflow threshold: avoids per-digit division.
    static const uintmax_t cutoff = UINTMAX_MAX / 10;
    static const uintmax_t cutlim = UINTMAX_MAX % 10;
    uintmax_t tnum = 0;
    const char *p = buf;
    while (*p >= '0' && *p <= '9') {
        uintmax_t d = *p - '0';
        if (unlikely(tnum > cutoff || (tnum == cutoff && d > cutlim)))
            return -1;
        tnum = tnum * 10 + d;
        p++;
    }
    if (p == buf)
        return -1;
    if (!end && *p != '\0')
        return -1;
    if (tnum > max_val)
        return -1;

    if (out) *out = tnum;
    if (end) *end = (char *)p;
    return 0;
}

static int
read_u64(uint64 *num, const char *buf, char **end)
{
    uintmax_t v;
    int r = read_uint(&v, UINT64_MAX, buf, end);
    if (r == 0 && num) *num = (uint64)v;
    return r;
}

static int
read_u32(uint32 *num, const char *buf, char **end)
{
    uintmax_t v;
    int r = read_uint(&v, UINT32_MAX, buf, end);
    if (r == 0 && num) *num = (uint32)v;
    return r;
}

/* Read a delay value in seconds from the given buffer and
   place it in duration in nanoseconds.
   The interface and behavior are analogous to read_u32(). */
static int
read_duration(int64 *duration, const char *buf, char **end)
{
    int r;
    uint32 dur_sec;

    r = read_u32(&dur_sec, buf, end);
    if (r)
        return r;
    *duration = ((int64) dur_sec) * 1000000000;
    return 0;
}

/* Read a tube name from the given buffer moving the buffer to the name start */
static int
read_tube_name(char **tubename, char *buf, char **end)
{
    size_t len;

    while (buf[0] == ' ')
        buf++;
    len = 0;
    while (valid_name_char[(unsigned char)buf[len]])
        len++;
    if (len == 0)
        return -1;
    if (tubename)
        *tubename = buf;
    if (end)
        *end = buf + len;
    return 0;
}

static void
wait_for_job(Conn *c, int timeout)
{
    if (!enqueue_waiting_conn(c)) {
        /* OOM: cannot register for waiting. Reply TIMED_OUT because
         * the protocol only allows RESERVED, DEADLINE_SOON, or
         * TIMED_OUT as responses to reserve commands. */
        reply_msg(c, MSG_TIMED_OUT);
        return;
    }
    c->state = STATE_WAIT;

    /* Set the pending timeout to the requested timeout amount */
    c->pending_timeout = timeout;

    // only care if they hang up
    epollq_add(c, 'h');
}

typedef int(*fmt_fn)(char *, size_t, void *);

// Stats buffer size. Enough for any stats response (typical ~1.5KB).
#define STATS_BUF_SIZE 4096

static void
do_stats(Conn *c, fmt_fn fmt, void *data)
{
    /* Allocate once, format directly into the job body.
     * Avoids the old two-pass approach (measure + format)
     * and also avoids stack buffer + memcpy. */
    c->out_job = allocate_job(STATS_BUF_SIZE);
    if (!c->out_job) {
        reply_serr(c, MSG_OUT_OF_MEMORY);
        return;
    }

    c->out_job->r.state = Copy;
    int r = fmt(c->out_job->body, STATS_BUF_SIZE, data);
    if (r < 0 || r >= STATS_BUF_SIZE) {
        job_free(c->out_job);
        c->out_job = NULL;
        reply_serr(c, MSG_INTERNAL_ERROR);
        return;
    }
    c->out_job->r.body_size = r;

    c->out_job_sent = 0;
    reply_line(c, STATE_SEND_JOB, "OK %d\r\n", r - 2);
}

static void
do_list_tubes(Conn *c, Ms *l)
{
    Tube *t;
    size_t i;

    /* Exact size: "---\n" (4) + N × ("- " + name + "\n") + "\r\n" (2). */
    size_t maxsz = 6;
    for (i = 0; i < l->len; i++)
        maxsz += 3 + ((Tube *)l->items[i])->name_len;
    if (maxsz > INT_MAX) {
        reply_serr(c, MSG_OUT_OF_MEMORY);
        return;
    }
    c->out_job = allocate_job(maxsz);
    if (!c->out_job) {
        reply_serr(c, MSG_OUT_OF_MEMORY);
        return;
    }
    c->out_job->r.state = Copy;

    char *buf = c->out_job->body;
    memcpy(buf, "---\n", 4);
    buf += 4;
    for (i = 0; i < l->len; i++) {
        t = l->items[i];
        *buf++ = '-';
        *buf++ = ' ';
        size_t nl = t->name_len;
        memcpy(buf, t->name, nl);
        buf += nl;
        *buf++ = '\n';
    }
    *buf++ = '\r';
    *buf++ = '\n';

    size_t resp_z = buf - c->out_job->body;
    c->out_job->r.body_size = resp_z;
    c->out_job_sent = 0;
    reply_line(c, STATE_SEND_JOB, "OK %zu\r\n", resp_z - 2);
}

static int
fmt_job_stats(char *buf, size_t size, void *x)
{
    Job *j = x;
    int64 t;
    int64 time_left;
    int file = 0;

    t = now;
    if (j->r.state == Reserved || j->r.state == Delayed) {
        time_left = (j->r.deadline_at - t) / 1000000000;
    } else {
        time_left = 0;
    }
    if (j->file) {
        file = j->file->seq;
    }
    return snprintf(buf, size, STATS_JOB_FMT,
            j->r.id,
            j->tube->name,
            job_state(j),
            j->r.pri,
            (t - j->r.created_at) / 1000000000,
            j->r.delay / 1000000000,
            j->r.ttr / 1000000000,
            time_left,
            file,
            j->r.reserve_ct,
            j->r.timeout_ct,
            j->r.release_ct,
            j->r.bury_ct,
            j->r.kick_ct);
}

static int
fmt_stats_tube(char *buf, size_t size, void *x)
{
    Tube *t = x;
    int64 time_left;

    if (t->pause > 0) {
        int64 d = t->unpause_at - now;
        time_left = d > 0 ? d / 1000000000 : 0;
    } else {
        time_left = 0;
    }
    return snprintf(buf, size, STATS_TUBE_FMT,
            t->name,
            t->stat.urgent_ct,
            t->ready.len,
            t->stat.reserved_ct,
            t->delay.len,
            t->stat.buried_ct,
            t->stat.total_jobs_ct,
            t->using_ct,
            t->watching_ct,
            t->stat.waiting_ct,
            t->stat.total_delete_ct,
            t->stat.pause_ct,
            t->pause / 1000000000,
            time_left);
}

__attribute__((hot)) static void
maybe_enqueue_incoming_job(Conn *c)
{
    Job *j = c->in_job;

    /* do we have a complete job? (common for small bodies) */
    if (likely(c->in_job_read == j->r.body_size)) {
        enqueue_incoming_job(c);
        return;
    }

    /* otherwise we have incomplete data, so just keep waiting */
    c->state = STATE_WANT_DATA;
}

/* j can be NULL */
__attribute__((hot)) static Job *
remove_this_reserved_job(Conn *c, Job *j)
{
    j = job_list_remove(j);
    if (j) {
        global_stat.reserved_ct--;
        j->tube->stat.reserved_ct--;
        j->reserver = NULL;
    }
    c->soonest_job = NULL;
    return j;
}

static Job *
remove_reserved_job(Conn *c, Job *j)
{
    if (!is_job_reserved_by_conn(c, j))
        return NULL;
    return remove_this_reserved_job(c, j);
}

// is_valid_tube validates a tube name.
// Returns the name length on success, 0 on failure.
// Uses valid_name_char[] lookup table.
static size_t
is_valid_tube(const char *name, size_t max)
{
    if (name[0] == '\0' || name[0] == '-')
        return 0;
    size_t len = 0;
    while (valid_name_char[(unsigned char)name[len]])
        len++;
    if (len == 0 || len > max || name[len] != '\0')
        return 0;
    return len;
}

__attribute__((hot)) static void
dispatch_cmd(Conn *c)
{
    int r, timeout = -1;
    uint i;
    uint count;
    Job *j = 0;
    byte type;
    char *size_buf, *delay_buf, *ttr_buf, *pri_buf, *end_buf, *name;
    uint32 pri;
    uint32 body_size;
    int64 delay, ttr;
    uint64 id;
    Tube *t = NULL;

    /* NUL-terminate this string so we can use strtol and friends */
    c->cmd[c->cmd_len - 2] = '\0';

    /* Check for embedded NUL bytes (injection attack).
     * Use memchr instead of strlen to detect embedded NUL. */
    if (unlikely(memchr(c->cmd, '\0', c->cmd_len - 2) != NULL)) {
        reply_msg(c, MSG_BAD_FORMAT);
        return;
    }

    type = which_cmd(c);
    if (verbose >= 2) {
        printf("<%d command %s\n", c->sock.fd, op_names[type]);
    }

    switch (type) {
    case OP_PUT:
        if (read_u32(&pri, c->cmd + 4, &delay_buf) ||
            read_duration(&delay, delay_buf, &ttr_buf) ||
            read_duration(&ttr, ttr_buf, &size_buf) ||
            read_u32(&body_size, size_buf, &end_buf)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (body_size > job_data_size_limit) {
            /* throw away the job body and respond with JOB_TOO_BIG */
            skip(c, (int64)body_size + 2, MSG_JOB_TOO_BIG);
            return;
        }

        /* don't allow trailing garbage */
        if (end_buf[0] != '\0') {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }

        CONNSETPRODUCER(c);

        if (ttr < 1000000000) {
            ttr = 1000000000;
        }

        c->in_job = make_job(pri, delay, ttr, body_size + 2, c->use);

        /* OOM? */
        if (!c->in_job) {
            /* throw away the job body and respond with OUT_OF_MEMORY */
            twarnx("server error: " MSG_OUT_OF_MEMORY);
            skip(c, body_size + 2, MSG_OUT_OF_MEMORY);
            return;
        }

        fill_extra_data(c);

        /* it's possible we already have a complete job */
        maybe_enqueue_incoming_job(c);
        return;

    case OP_PEEK_READY:
        if (c->cmd_len != CMD_PEEK_READY_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (c->use->ready.len) {
            j = job_copy(c->use->ready.data[0]);
        }

        if (!j) {
            if (try_forward_tube_cmd(c, c->use->name_hash)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_PEEK_DELAYED:
        if (c->cmd_len != CMD_PEEK_DELAYED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (c->use->delay.len) {
            j = job_copy(c->use->delay.data[0]);
        }

        if (!j) {
            if (try_forward_tube_cmd(c, c->use->name_hash)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_PEEK_BURIED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_BURIED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (!job_list_is_empty(&c->use->buried))
            j = job_copy(c->use->buried.next);
        else
            j = NULL;

        if (!j) {
            if (try_forward_tube_cmd(c, c->use->name_hash)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_PEEKJOB:
        if (read_u64(&id, c->cmd + CMD_PEEKJOB_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        /* So, peek is annoying, because some other connection might free the
         * job while we are still trying to write it out. So we copy it and
         * free the copy when it's done sending, in the "conn_want_command" function. */
        j = job_copy(job_find(id));

        if (!j) {
            if (try_forward_job_cmd(c, id)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_RESERVE_TIMEOUT: {
        uint32 utimeout = 0;
        if (read_u32(&utimeout, c->cmd + CMD_RESERVE_TIMEOUT_LEN, NULL) != 0 || utimeout > INT_MAX) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        timeout = (int)utimeout;
        }
        /* Falls through */

    case OP_RESERVE:
        /* don't allow trailing garbage */
        if (type == OP_RESERVE && c->cmd_len != CMD_RESERVE_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        CONNSETWORKER(c);

        if (conndeadlinesoon(c) && !conn_ready(c)) {
            reply_msg(c, MSG_DEADLINE_SOON);
            return;
        }

        /* try to get a new job for this guy */
        wait_for_job(c, timeout);
        process_tube(c->watch);
        return;

    case OP_RESERVE_JOB:
        if (read_u64(&id, c->cmd + CMD_RESERVE_JOB_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        // This command could produce "deadline soon" if
        // the connection has a reservation about to expire.
        // We choose not to do it, because this command does not block
        // for an arbitrary amount of time as reserve and reserve-with-timeout.

        j = job_find(id);
        if (!j) {
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        // Check if this job is already reserved.
        if (j->r.state == Reserved || j->r.state == Invalid) {
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        // Job can be in ready, buried or delayed states.
        if (j->r.state == Ready) {
            j = remove_ready_job(j);
        } else if (j->r.state == Buried) {
            j = remove_buried_job(j);
        } else if (j->r.state == Delayed) {
            j = remove_delayed_job(j);
        } else {
            reply_serr(c, MSG_INTERNAL_ERROR);
            return;
        }

        CONNSETWORKER(c);
        global_stat.reserved_ct++;

        conn_reserve_job(c, j);
        reply_job(c, j, MSG_RESERVED);
        return;

    case OP_DELETE:
        if (read_u64(&id, c->cmd + CMD_DELETE_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        {
            Job *jf = job_find(id);
            if (jf) switch (jf->r.state) {
            case Reserved: j = remove_reserved_job(c, jf); break;
            case Ready:    j = remove_ready_job(jf); break;
            case Buried:   j = remove_buried_job(jf); break;
            case Delayed:  j = remove_delayed_job(jf); break;
            }
        }

        if (!j) {
            if (try_forward_job_cmd(c, id)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        j->tube->stat.total_delete_ct++;

        j->r.state = Invalid;
        {
            Wal *w = shard_wal(c->srv, j);
            r = walwrite(w, j);
            if (r && !walmaint(w)) {
                twarnx("walmaint failed after delete walwrite");
            }
        }
        job_free(j);

        if (!r) {
            reply_serr(c, MSG_INTERNAL_ERROR);
            return;
        }
        reply_msg(c, MSG_DELETED);
        return;

    case OP_RELEASE:
        if (read_u64(&id, c->cmd + CMD_RELEASE_LEN, &pri_buf) ||
            read_u32(&pri, pri_buf, &delay_buf) ||
            read_duration(&delay, delay_buf, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        j = remove_reserved_job(c, job_find(id));

        if (!j) {
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        /* We want to update the delay deadline on disk, so reserve space for
         * that. */
        int z = 0;
        Wal *release_wal = NULL;
        if (delay) {
            release_wal = shard_wal(c->srv, j);
            z = walresvupdate(release_wal);
            if (!z) {
                /* Undo remove_reserved_job: restore counters and re-link. */
                global_stat.reserved_ct++;
                j->tube->stat.reserved_ct++;
                job_list_insert(&c->reserved_jobs, j);
                j->reserver = c;
                reply_serr(c, MSG_OUT_OF_MEMORY);
                return;
            }
            j->walresv += z;
        }

        j->r.pri = pri;
        j->r.delay = delay;
        j->r.release_ct++;

        r = enqueue_job(c->srv, j, delay, !!delay);
        if (r == 1) {
            reply_msg(c, MSG_RELEASED);
            return;
        }

        /* out of memory trying to grow the queue, so it gets buried */
        if (z) {
            walresvreturn(release_wal, z);
            j->walresv -= z;
        }
        bury_job(c->srv, j, 0);
        reply_msg(c, MSG_BURIED);
        return;

    case OP_BURY:
        if (read_u64(&id, c->cmd + CMD_BURY_LEN, &pri_buf) ||
            read_u32(&pri, pri_buf, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }

        op_ct[type]++;

        j = remove_reserved_job(c, job_find(id));

        if (!j) {
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        j->r.pri = pri;
        r = bury_job(c->srv, j, 1);
        if (!r) {
            if (j->r.state == Buried) {
                // Job was buried in memory but WAL write failed.
                // WAL is now disabled; in-memory state is correct.
                reply_msg(c, MSG_BURIED);
            } else {
                // WAL reservation failed; undo remove_reserved_job.
                global_stat.reserved_ct++;
                j->tube->stat.reserved_ct++;
                job_list_insert(&c->reserved_jobs, j);
                j->reserver = c;
                reply_serr(c, MSG_INTERNAL_ERROR);
            }
            return;
        }
        reply_msg(c, MSG_BURIED);
        return;

    case OP_KICK:
        {
            uint32 kick_count;
            if (read_u32(&kick_count, c->cmd + CMD_KICK_LEN, NULL)) {
                reply_msg(c, MSG_BAD_FORMAT);
                return;
            }
            count = kick_count;
        }

        op_ct[type]++;

        i = kick_jobs(c->srv, c->use, count);
        if (i == 0 && try_forward_tube_cmd(c, c->use->name_hash)) return;
        reply_line(c, STATE_SEND_WORD, "KICKED %u\r\n", i);
        return;

    case OP_KICKJOB:
        if (read_u64(&id, c->cmd + CMD_KICKJOB_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }

        op_ct[type]++;

        j = job_find(id);
        if (!j) {
            if (try_forward_job_cmd(c, id)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        if ((j->r.state == Buried && kick_buried_job(c->srv, j)) ||
            (j->r.state == Delayed && kick_delayed_job(c->srv, j))) {
            reply_msg(c, MSG_KICKED);
        } else {
            reply_msg(c, MSG_NOTFOUND);
        }
        return;

    case OP_TOUCH:
        if (read_u64(&id, c->cmd + CMD_TOUCH_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (touch_job(c, job_find(id))) {
            reply_msg(c, MSG_TOUCHED);
        } else {
            reply_msg(c, MSG_NOTFOUND);
        }
        return;

    case OP_STATS:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_STATS_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        do_stats(c, fmt_stats, c->srv);
        return;

    case OP_STATSJOB:
        if (read_u64(&id, c->cmd + CMD_STATSJOB_LEN, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        j = job_find(id);
        if (!j) {
            if (try_forward_job_cmd(c, id)) return;
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        if (!j->tube) {
            reply_serr(c, MSG_INTERNAL_ERROR);
            return;
        }
        do_stats(c, fmt_job_stats, j);
        return;

    case OP_STATS_TUBE: {
        size_t namelen;
        name = c->cmd + CMD_STATS_TUBE_LEN;
        namelen = is_valid_tube(name, MAX_TUBE_NAME_LEN - 1);
        if (!namelen) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        t = tube_find_name(name, namelen);
        if (!t) {
            // In multi-worker mode, forward to the worker that owns this tube.
            if (c->srv->nworkers > 1) {
                uint32 h = tube_name_hash(name);
                int target = h % c->srv->nworkers;
                if (target != c->srv->worker_id && c->srv->peer_fd[target] >= 0) {
                    if (try_forward_tube_cmd(c, h)) return;
                }
            }
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        do_stats(c, fmt_stats_tube, t);
        t = NULL;
        return;
    }

    case OP_LIST_TUBES:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBES_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        // In multi-worker mode, build YAML from all workers' tube names
        // via shared memory. Avoids creating phantom tube objects.
        if (shared_stats && c->srv->nworkers > 1) {
            // Collect unique tube names using open-addressed hash dedup.
            #define LT_CAP 2048
            #define LT_MASK (LT_CAP - 1)
            static const char *lt_set[LT_CAP];
            memset(lt_set, 0, sizeof(lt_set));
            const char *names[SHARED_MAX_TUBES];
            int count = 0;
            for (int w = 0; w < c->srv->nworkers; w++) {
                struct SharedStats *ss = &shared_stats[w];
                for (uint32 ti = 0; ti < ss->tube_count && count < SHARED_MAX_TUBES; ti++) {
                    const char *name = ss->tube_names[ti];
                    uint32 h = tube_name_hash(name);
                    int dup = 0;
                    for (int probe = 0; probe < LT_CAP; probe++) {
                        int idx = (h + probe) & LT_MASK;
                        if (!lt_set[idx]) {
                            lt_set[idx] = name;
                            names[count++] = name;
                            break;
                        }
                        if (strcmp(lt_set[idx], name) == 0) {
                            dup = 1;
                            break;
                        }
                    }
                    (void)dup;
                }
            }
            #undef LT_CAP
            #undef LT_MASK
            // Format YAML manually.
            size_t maxsz = 6 + count * (3 + MAX_TUBE_NAME_LEN);
            c->out_job = allocate_job(maxsz);
            if (!c->out_job) {
                reply_serr(c, MSG_OUT_OF_MEMORY);
                return;
            }
            c->out_job->r.state = Copy;
            char *buf = c->out_job->body;
            memcpy(buf, "---\n", 4); buf += 4;
            for (int i = 0; i < count; i++) {
                *buf++ = '-'; *buf++ = ' ';
                size_t nl = strlen(names[i]);
                memcpy(buf, names[i], nl); buf += nl;
                *buf++ = '\n';
            }
            size_t body_z = buf - c->out_job->body;
            memcpy(buf, "\r\n", 2);
            c->out_job->r.body_size = body_z + 2;
            c->out_job_sent = 0;
            reply_line(c, STATE_SEND_JOB, "OK %zu\r\n", body_z);
        } else {
            do_list_tubes(c, &tubes);
        }
        return;

    case OP_LIST_TUBE_USED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBE_USED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        reply_using(c, c->use);
        return;

    case OP_LIST_TUBES_WATCHED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBES_WATCHED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        {
            // Format: "---\n- <name>\n\r\n"
            size_t nl = c->watch->name_len;
            size_t body_z = 4 + 2 + nl + 1; // "---\n" + "- " + name + "\n"
            c->out_job = allocate_job(body_z + 2); // +2 for "\r\n"
            if (!c->out_job) {
                reply_serr(c, MSG_OUT_OF_MEMORY);
                return;
            }
            c->out_job->r.state = Copy;
            char *buf = c->out_job->body;
            memcpy(buf, "---\n- ", 6); buf += 6;
            memcpy(buf, c->watch->name, nl); buf += nl;
            *buf++ = '\n';
            memcpy(buf, "\r\n", 2);
            c->out_job_sent = 0;
            reply_line(c, STATE_SEND_JOB, "OK %zu\r\n", body_z);
        }
        return;

    case OP_USE: {
        name = c->cmd + CMD_USE_LEN;
        size_t use_namelen = is_valid_tube(name, MAX_TUBE_NAME_LEN - 1);
        if (!use_namelen) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        // use is always local — no migration. Put forwarding handles remote tubes.
        TUBE_ASSIGN(t, tube_find_or_make_n(name, use_namelen));
        if (!t) {
            reply_serr(c, MSG_OUT_OF_MEMORY);
            return;
        }

        c->use->using_ct--;
        TUBE_ASSIGN(c->use, t);
        TUBE_ASSIGN(t, NULL);
        c->use->using_ct++;


        reply_using(c, c->use);
        return;
    }

    case OP_WATCH: {
        name = c->cmd + CMD_WATCH_LEN;
        size_t watch_namelen = is_valid_tube(name, MAX_TUBE_NAME_LEN - 1);
        if (!watch_namelen) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        // In multi-worker mode, check if this tube belongs to another worker.
        if (c->srv->nworkers > 1) {
            int target = tube_name_hash(name) % c->srv->nworkers;
            if (target != c->srv->worker_id && c->srv->peer_fd[target] >= 0) {
                // Cannot migrate if connection has reserved jobs.
                if (!job_list_is_empty(&c->reserved_jobs))
                    goto watch_local;

                // Prepare migration message with pending reply.
                struct MigMsg mm = {0};
                mm.magic = MIG_MSG_MAGIC;
                memcpy(mm.watch_tube, name, watch_namelen);
                memcpy(mm.use_tube, c->use->name, c->use->name_len);
                mm.type = c->type;

                // Include pending reply so client sees WATCHING 1.
                memcpy(mm.pending_reply, "WATCHING 1\r\n", 12);
                mm.pending_reply_len = 12;

                // Copy any pipelined data remaining in cmd buffer.
                size_t consumed = c->cmd_len;
                size_t leftover = c->cmd_read > consumed ? c->cmd_read - consumed : 0;
                if (leftover > 0 && leftover <= sizeof(mm.cmd)) {
                    memcpy(mm.cmd, c->cmd + consumed, leftover);
                    mm.cmd_len = leftover;
                }

                if (send_fd(c->srv->peer_fd[target], c->sock.fd, &mm, sizeof(mm)) == 0) {
                    sockwant(&c->sock, 0); // deregister from epoll before giving away fd
                    c->sock.fd = -1;
                    remove_waiting_conn(c);
                    c->state = STATE_CLOSE;
                    return;
                }
                // send_fd failed — fall through to local handling.
            }
        }

watch_local:
        TUBE_ASSIGN(t, tube_find_or_make_n(name, watch_namelen));
        if (!t) {
            reply_serr(c, MSG_OUT_OF_MEMORY);
            return;
        }

        if (c->watch != t) {
            if (verbose && strcmp(c->watch->name, "default") != 0)
                printf("watch %d: %s → %s (single-tube mode)\n",
                       c->sock.fd, c->watch->name, t->name);
            remove_waiting_conn(c);
            c->watch->watching_ct--;
            TUBE_ASSIGN(c->watch, t);
            c->watch->watching_ct++;
        }

        TUBE_ASSIGN(t, NULL);
        reply_msg(c, MSG_WATCHING_1);
        return;
    }

    case OP_IGNORE:
        name = c->cmd + CMD_IGNORE_LEN;
        if (!is_valid_tube(name, MAX_TUBE_NAME_LEN - 1)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        // Cannot ignore the only watched tube.
        if (c->watch && strncmp(c->watch->name, name, MAX_TUBE_NAME_LEN) == 0) {
            reply_msg(c, MSG_NOT_IGNORED);
            return;
        }
        // Tube not watched — no-op.
        reply_msg(c, MSG_WATCHING_1);
        return;

    case OP_QUIT:
        c->state = STATE_CLOSE;
        return;

    case OP_PAUSE_TUBE: {
        size_t namelen;
        if (read_tube_name(&name, c->cmd + CMD_PAUSE_TUBE_LEN, &delay_buf) ||
            read_duration(&delay, delay_buf, NULL)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        *delay_buf = '\0';
        namelen = is_valid_tube(name, MAX_TUBE_NAME_LEN - 1);
        if (!namelen) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        t = tube_find_name(name, namelen);
        if (!t) {
            // In multi-worker mode, forward to the owner worker.
            if (c->srv->nworkers > 1) {
                uint32 h = tube_name_hash(name);
                int target = h % c->srv->nworkers;
                if (target != c->srv->worker_id && c->srv->peer_fd[target] >= 0) {
                    if (try_forward_tube_cmd(c, h)) return;
                }
            }
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        // Always pause for a positive amount of time, to make sure
        // that waiting clients wake up when the deadline arrives.
        if (delay == 0) {
            delay = 1;
        }

        t->unpause_at = now + delay;
        if (!t->pause)
            paused_ct++;
        t->pause = delay;
        t->stat.pause_ct++;
        pause_tube_update(t);

        reply_msg(c, "PAUSED\r\n");
        return;
    }

    default:
        reply_msg(c, MSG_UNKNOWN_COMMAND);
    }
}

/* There are three reasons this function may be called. We need to check for
 * all of them.
 *
 *  1. A reserved job has run out of time.
 *  2. A waiting client's reserved job has entered the safety margin.
 *  3. A waiting client's requested timeout has occurred.
 *
 * If any of these happen, we must do the appropriate thing. */
static void
conn_timeout(Conn *c)
{
    int should_timeout = 0;
    Job *j;

    /* Check if the client was trying to reserve a job. */
    if (conn_waiting(c) && conndeadlinesoon(c))
        should_timeout = 1;

    /* Check if any reserved jobs have run out of time. We should do this
     * whether or not the client is waiting for a new reservation. */
    while ((j = connsoonestjob(c))) {
        if (j->r.deadline_at >= now)
            break;

        /* This job is in the middle of being written out. If we return it to
         * the ready queue, someone might free it before we finish writing it
         * out to the socket. So we'll copy it here and free the copy when it's
         * done sending. */
        if (j == c->out_job) {
            c->out_job = job_copy(c->out_job);
            if (!c->out_job) {
                c->state = STATE_CLOSE;
                return;
            }
        }

        timeout_ct++; /* stats */
        j->r.timeout_ct++;
        int r = enqueue_job(c->srv, remove_this_reserved_job(c, j), 0, 0);
        if (r < 1)
            bury_job(c->srv, j, 0); /* out of memory, so bury it */
    }
    connsched(c);

    if (should_timeout) {
        remove_waiting_conn(c);
        reply_msg(c, MSG_DEADLINE_SOON);
    } else if (conn_waiting(c) && c->pending_timeout >= 0) {
        c->pending_timeout = -1;
        remove_waiting_conn(c);
        reply_msg(c, MSG_TIMED_OUT);
    }
}

__attribute__((cold)) void
enter_drain_mode(int sig)
{
    UNUSED_PARAMETER(sig);
    drain_mode = 1;
}

__attribute__((hot)) static void
conn_want_command(Conn *c)
{
    epollq_add(c, 'r');

    /* was this a peek or stats command? */
    if (unlikely(c->out_job)) {
        if (c->out_job->r.state == Copy)
            job_free(c->out_job);
        c->out_job = NULL;
    }

    c->reply_sent = 0; /* now that we're done, reset this */
    c->state = STATE_WANT_COMMAND;
}

__attribute__((hot)) static void
conn_process_io(Conn *c)
{
    int r;
    int64 to_read;
    Job *j;
    struct iovec iov[2];

    switch (c->state) {
    case STATE_WANT_COMMAND: {
        // Don't read more data while waiting for forwarded reply.
        if (unlikely(c->fwd_pending))
            return;
        r = read(c->sock.fd, c->cmd + c->cmd_read, LINE_BUF_SIZE - c->cmd_read);
        if (unlikely(r == -1)) {
            check_err(c, "read()");
            return;
        }
        if (unlikely(r == 0)) {
            c->state = STATE_CLOSE;
            return;
        }

        c->cmd_read += r;
        c->cmd_len = scan_line_end(c->cmd, c->cmd_read);
        if (c->cmd_len) {
            // We found complete command line. Bail out to h_conn.
            return;
        }

        // c->cmd_read > LINE_BUF_SIZE can't happen

        if (c->cmd_read == LINE_BUF_SIZE) {
            // Command line too long.
            // Put connection into special state that discards
            // the command line until the end line is found.
            c->cmd_read = 0; // discard the input so far
            c->state = STATE_WANT_ENDLINE;
        }
        // We have an incomplete line, so just keep waiting.
        return;
    }
    case STATE_WANT_ENDLINE:
        r = read(c->sock.fd, c->cmd + c->cmd_read, LINE_BUF_SIZE - c->cmd_read);
        if (r == -1) {
            check_err(c, "read()");
            return;
        }
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        c->cmd_read += r;
        c->cmd_len = scan_line_end(c->cmd, c->cmd_read);
        if (c->cmd_len) {
            // Found the EOL. Reply and reuse whatever was read afer the EOL.
            reply_msg(c, MSG_BAD_FORMAT);
            fill_extra_data(c);
            return;
        }

        // c->cmd_read > LINE_BUF_SIZE can't happen

        if (c->cmd_read == LINE_BUF_SIZE) {
            // Keep discarding the input since no EOL was found.
            c->cmd_read = 0;
        }
        return;

    case STATE_BITBUCKET: {
        /* Invert the meaning of in_job_read while throwing away data -- it
         * counts the bytes that remain to be thrown away. */
        static char bucket[BUCKET_BUF_SIZE];
        to_read = min(c->in_job_read, BUCKET_BUF_SIZE);
        r = read(c->sock.fd, bucket, to_read);
        if (r == -1) {
            check_err(c, "read()");
            return;
        }
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        c->in_job_read -= r; /* we got some bytes */

        /* (c->in_job_read < 0) can't happen */

        if (c->in_job_read == 0) {
            reply(c, c->reply, c->reply_len, STATE_SEND_WORD);
        }
        return;
    }
    case STATE_WANT_DATA: {
        j = c->in_job;

        r = read(c->sock.fd, j->body + c->in_job_read, j->r.body_size -c->in_job_read);
        if (unlikely(r == -1)) {
            check_err(c, "read()");
            return;
        }
        if (unlikely(r == 0)) {
            c->state = STATE_CLOSE;
            return;
        }

        c->in_job_read += r; /* we got some bytes */

        /* (j->in_job_read > j->r.body_size) can't happen */

        maybe_enqueue_incoming_job(c);
        return;
    }
    case STATE_SEND_WORD:
        r= write(c->sock.fd, c->reply + c->reply_sent, c->reply_len - c->reply_sent);
        if (unlikely(r == -1)) {
            check_err(c, "write()");
            return;
        }
        if (unlikely(r == 0)) {
            c->state = STATE_CLOSE;
            return;
        }

        c->reply_sent += r; /* we got some bytes */

        /* (c->reply_sent > c->reply_len) can't happen */

        if (c->reply_sent == c->reply_len) {
            conn_want_command(c);
            return;
        }

        /* otherwise we sent an incomplete reply, so just keep waiting */
        break;
    case STATE_SEND_JOB:
        j = c->out_job;

        iov[0].iov_base = (void *)(c->reply + c->reply_sent);
        iov[0].iov_len = c->reply_len - c->reply_sent; /* maybe 0 */
        iov[1].iov_base = j->body + c->out_job_sent;
        iov[1].iov_len = j->r.body_size - c->out_job_sent;

        r = writev(c->sock.fd, iov, 2);
        if (unlikely(r == -1)) {
            check_err(c, "writev()");
            return;
        }
        if (unlikely(r == 0)) {
            c->state = STATE_CLOSE;
            return;
        }

        /* update the sent values */
        c->reply_sent += r;
        if (c->reply_sent >= c->reply_len) {
            c->out_job_sent += c->reply_sent - c->reply_len;
            c->reply_sent = c->reply_len;
        }

        /* (c->out_job_sent > j->r.body_size) can't happen */

        /* are we done? */
        if (c->out_job_sent == j->r.body_size) {
            if (verbose >= 2) {
                printf(">%d job %"PRIu64"\n", c->sock.fd, j->r.id);
            }
            conn_want_command(c);
            return;
        }

        /* otherwise we sent incomplete data, so just keep waiting */
        break;
    case STATE_WAIT:
        if (c->halfclosed) {
            c->pending_timeout = -1;
            remove_waiting_conn(c);
            reply_msg(c, MSG_TIMED_OUT);
            return;
        }
        break;
    }
}

#define want_command(c) ((c)->sock.fd && ((c)->state == STATE_WANT_COMMAND))
#define cmd_data_ready(c) (want_command(c) && (c)->cmd_read)

__attribute__((hot)) static void
h_conn(const int fd, const short which, Conn *c)
{
    if (unlikely(fd != c->sock.fd)) {
        twarnx("Argh! event fd doesn't match conn fd.");
        close(fd);
        connclose(c);
        epollq_apply();
        return;
    }

    if (which == 'h') {
        c->halfclosed = 1;
    }

    // TCP_QUICKACK removed: with TCP_NODELAY enabled, replies carry
    // piggybacked ACKs. The extra setsockopt syscall per read event
    // (~400ns) provides no benefit — delayed ACK only matters when
    // no reply is sent, which doesn't affect hot-path throughput.

    conn_process_io(c);

    // Dispatch commands. Cork only if pipelining detected (multiple
    // commands buffered), saving 2 setsockopt syscalls (~400ns) for the
    // common non-pipelined case.
    int corked = 0;
    while (cmd_data_ready(c) && !c->fwd_pending
           && (c->cmd_len = scan_line_end(c->cmd, c->cmd_read))) {
        if (!corked && c->cmd_read > c->cmd_len) {
            int cork = 1;
            setsockopt(c->sock.fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork);
            corked = 1;
        }
        dispatch_cmd(c);
        if (c->sock.fd < 0)
            break;
        fill_extra_data(c);
    }
    if (corked && c->sock.fd >= 0) {
        int cork = 0;
        setsockopt(c->sock.fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork);
    }

    if (c->state == STATE_CLOSE) {
        epollq_rmconn(c);
        connclose(c);
    }
    epollq_apply();
}

static void
prothandle(Conn *c, int ev)
{
    h_conn(c->sock.fd, ev, c);
}

// prottick returns nanoseconds till the next work.
int64
prottick(Server *s)
{
    Job *j;
    Tube *t;
    int64 period = 0x34630B8A000LL; /* 1 hour in nanoseconds */
    int64 d;

    now = nanoseconds();

    // Enqueue all jobs that are no longer delayed.
    // Capture the smallest period from the soonest delayed job.
    // Loop bound: each iteration removes one job via remove_delayed_job.
    while (delayed_ct > 0 && (j = soonest_delayed_job())) {
        d = j->r.deadline_at - now;
        if (d > 0) {
            period = min(period, d);
            break;
        }
        remove_delayed_job(j);
        int r = enqueue_job(s, j, 0, 0);
        if (r < 1)
            bury_job(s, j, 0);  /* out of memory */
    }

    // Unpause tubes whose deadline has arrived. O(k log n) for k expired tubes.
    // Uses pause_tube_heap for O(1) lookup of soonest unpause deadline.
    while (pause_tube_heap.len) {
        t = pause_tube_heap.data[0];
        d = t->unpause_at - now;
        if (d > 0) {
            period = min(period, d);
            break;
        }
        heapremove(&pause_tube_heap, 0);
        t->in_pause_heap = 0;
        t->pause = 0;
        paused_ct--;
        process_tube(t);
    }

    // Process connections with pending timeouts. Release jobs with expired ttr.
    // Capture the smallest period from the soonest connection.
    while (s->conns.len) {
        Conn *c = s->conns.data[0];
        d = c->tickat - now;
        if (d > 0) {
            period = min(period, d);
            break;
        }
        heapremove(&s->conns, 0);
        c->in_conns = 0;
        conn_timeout(c);
    }

    // Periodically return unused heap pages to the OS.
    // Addresses glibc not releasing memory after mass job deletion.
    // Controlled by -m flag; 0 disables.
    if (mem_trim_rate > 0) {
        static int64 last_trim;
        if (now - last_trim >= mem_trim_rate) {
            malloc_trim(0);
            last_trim = now;
        }
    }

    // Timeout forwarded commands (5s). Prevents client hanging if peer is dead.
    if (s->pending_fwd_used > 0) {
        int remaining = s->pending_fwd_used;
        for (int i = 0; i < PENDING_FWD_SLOTS && remaining > 0; i++) {
            if (!s->pending_fwd[i].conn)
                continue;
            remaining--;
            if (now - s->pending_fwd[i].at > 5000000000LL) {
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
    }

    // Publish stats to shared memory (1Hz) with seqlock for torn-read protection.
    if (shared_stats && s->worker_id >= 0) {
        static int64 last_stats_sync;
        if (now - last_stats_sync >= 1000000000LL) { // 1 second
            last_stats_sync = now;
            struct SharedStats *ss = &shared_stats[s->worker_id];
            __atomic_add_fetch(&ss->version, 1, __ATOMIC_RELEASE); // odd = writing
            ss->ready_ct = ready_ct;
            ss->delayed_ct = delayed_ct;
            ss->buried_ct = global_stat.buried_ct;
            ss->reserved_ct = global_stat.reserved_ct;
            ss->urgent_ct = global_stat.urgent_ct;
            ss->waiting_ct = global_stat.waiting_ct;
            ss->timeout_ct = timeout_ct;
            ss->total_jobs_ct = global_stat.total_jobs_ct;
            ss->cur_conn_ct = count_cur_conns();
            ss->cur_producer_ct = count_cur_producers();
            ss->cur_worker_ct = count_cur_workers();
            ss->tot_conn_ct = count_tot_conns();
            if (tubes_dirty || ss->tube_count != (tubes.len < SHARED_MAX_TUBES ? tubes.len : SHARED_MAX_TUBES)) {
                ss->tube_count = tubes.len < SHARED_MAX_TUBES ? tubes.len : SHARED_MAX_TUBES;
                for (size_t ti = 0; ti < ss->tube_count; ti++) {
                    Tube *tt = tubes.items[ti];
                    memcpy(ss->tube_names[ti], tt->name, tt->name_len + 1);
                }
                tubes_dirty = 0;
            }
            for (int i = 0; i < TOTAL_OPS && i < SHARED_STATS_OPS; i++)
                ss->op_ct[i] = op_ct[i];
            __atomic_add_fetch(&ss->version, 1, __ATOMIC_RELEASE); // even = done
        }
    }

    epollq_apply();

    return period;
}

// Try to extract tube name from a command buffer.
// Returns pointer to tube name within buf (NUL-terminated in place), or NULL.
static char *
parse_tube_from_first_cmd(char *buf, size_t len)
{
    // Find \r\n to ensure we have a complete command.
    size_t i;
    for (i = 1; i < len; i++) {
        if (buf[i-1] == '\r' && buf[i] == '\n')
            break;
    }
    if (i >= len)
        return NULL; // incomplete command

    buf[i-1] = '\0'; // NUL-terminate command (overwrite \r)

    // Only watch triggers accept-time migration.
    // use is local — put forwarding handles remote tubes.
    if (strncmp(buf, CMD_WATCH, CMD_WATCH_LEN) == 0)
        return buf + CMD_WATCH_LEN;
    return NULL;
}

void
h_accept(const int fd, const short which, Server *s)
{
    UNUSED_PARAMETER(which);
    struct sockaddr_storage addr;

    // Drain all pending connections from the accept queue.
    // With level-triggered epoll each pending conn would otherwise
    // cost a full epoll_wait + prottick round-trip.
    for (;;) {
        socklen_t addrlen = sizeof addr;
        int cfd = accept4(fd, (struct sockaddr *)&addr, &addrlen, SOCK_NONBLOCK|SOCK_CLOEXEC);
        if (cfd == -1) {
            if (errno == EMFILE || errno == ENFILE) {
                twarnx("accept: too many open files");
                sockwant(&s->sock, 0);
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                twarn("accept()");
            }
            break;
        }
        if (verbose) {
            printf("accept %d\n", cfd);
        }

        int flags = 1;
        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof flags);
        setsockopt(cfd, IPPROTO_TCP, TCP_QUICKACK, &flags, sizeof flags);

        // In multi-worker mode, speculatively read the first command
        // to determine which worker should own this connection.
        char first_buf[LINE_BUF_SIZE];
        ssize_t nr = 0;
        char *tube_name = NULL;

        if (s->nworkers > 1) {
            nr = read(cfd, first_buf, sizeof(first_buf) - 1);
            if (nr > 0) {
                first_buf[nr] = '\0';
                // Make a copy for parsing (parse_tube_from_first_cmd modifies buf).
                char parse_buf[LINE_BUF_SIZE];
                memcpy(parse_buf, first_buf, nr + 1);
                tube_name = parse_tube_from_first_cmd(parse_buf, nr);

                if (tube_name && is_valid_tube(tube_name, MAX_TUBE_NAME_LEN - 1)) {
                    size_t tnlen = strlen(tube_name);
                    int target = tube_name_hash(tube_name) % s->nworkers;
                    if (target != s->worker_id && s->peer_fd[target] >= 0) {
                        // Migrate fd to the correct worker.
                        struct MigMsg mm = {0};
                        mm.magic = MIG_MSG_MAGIC;
                        memcpy(mm.watch_tube, tube_name, tnlen);
                        memcpy(mm.use_tube, "default", 7);
                        memcpy(mm.cmd, first_buf, nr);
                        mm.cmd_len = nr;
                        if (send_fd(s->peer_fd[target], cfd, &mm, sizeof(mm)) == 0) {
                            close(cfd);
                            continue;
                        }
                        // send_fd failed — fall through to local handling.
                    }
                }
            }
            // nr <= 0 or tube not parsed or target == self: handle locally.
        }

        // Route "default" tube connections deterministically to one worker.
        if (s->nworkers > 1 && nr <= 0) {
            int dtarget = tube_name_hash("default") % s->nworkers;
            if (dtarget != s->worker_id && s->peer_fd[dtarget] >= 0) {
                struct MigMsg mm = {0};
                mm.magic = MIG_MSG_MAGIC;
                memcpy(mm.watch_tube, "default", 7);
                memcpy(mm.use_tube, "default", 7);
                if (send_fd(s->peer_fd[dtarget], cfd, &mm, sizeof(mm)) == 0) {
                    close(cfd);
                    continue;
                }
            }
        }

        Tube *use_tube = default_tube;
        Tube *watch_tube = default_tube;

        Conn *c = make_conn(cfd, STATE_WANT_COMMAND, use_tube, watch_tube);
        if (!c) {
            twarnx("make_conn() failed");
            close(cfd);
            if (verbose) {
                printf("close %d\n", cfd);
            }
            continue;
        }
        c->srv = s;
        c->sock.x = c;
        c->sock.f = (Handle)prothandle;
        c->sock.fd = cfd;

        // If we already read data, copy it into the connection's cmd buffer
        // Replay pre-read data — kernel buffer is already drained.
        if (nr > 0) {
            size_t to_copy = (size_t)nr < sizeof(c->cmd) ? (size_t)nr : sizeof(c->cmd);
            memcpy(c->cmd, first_buf, to_copy);
            c->cmd_read = to_copy;

            // Dispatch loop matching h_conn's pipeline processing.
            while (c->state == STATE_WANT_COMMAND && c->cmd_read > 0
                   && !c->fwd_pending
                   && (c->cmd_len = scan_line_end(c->cmd, c->cmd_read))) {
                dispatch_cmd(c);
                if (c->sock.fd < 0) break;
                fill_extra_data(c);
            }
            if (c->sock.fd < 0 || c->state == STATE_CLOSE) {
                connclose(c);
                continue;
            }
        }

        if (sockwant(&c->sock, 'r') == -1) {
            twarn("sockwant");
            connclose(c);
            continue;
        }
    }
    epollq_apply();
}

// Handle a forwarded command from a peer worker.
// Executes stats-tube or pause-tube locally, sends reply back via peer socket.
void
prot_handle_forwarded_cmd(Server *s, struct CmdFwdMsg *fwd)
{
    struct CmdReplyMsg rpl;
    rpl.magic = CMD_REPLY_MAGIC;
    rpl.seq = fwd->seq;
    rpl.data_len = 0;

    // Parse the forwarded command.
    char *cmd = fwd->cmd;
    size_t len = fwd->cmd_len;

    // NUL-terminate for string ops.
    if (len >= LINE_BUF_SIZE) len = LINE_BUF_SIZE - 1;
    cmd[len] = '\0';

    if (strncmp(cmd, "stats-tube ", 11) == 0) {
        char *name = cmd + 11;
        // Strip \r\n
        size_t nl = strlen(name);
        if (nl >= 2 && name[nl-2] == '\r') { name[nl-2] = '\0'; nl -= 2; }

        Tube *t = tube_find_name(name, nl);
        if (t) {
            char buf[STATS_BUF_SIZE];
            int n = fmt_stats_tube(buf, sizeof(buf), t);
            if (n > 0 && n < CMD_FWD_REPLY_SIZE - 32) {
                // fmt_stats_tube output already ends with \r\n.
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                    "OK %d\r\n%s", n, buf);
            }
        } else {
            rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
        }
    } else if (strncmp(cmd, "pause-tube ", 11) == 0) {
        // Parse: pause-tube <name> <delay>\r\n
        char *name = cmd + 11;
        char *sp = strchr(name, ' ');
        if (sp) {
            *sp = '\0';
            char *delay_str = sp + 1;
            Tube *t = tube_find_name(name, sp - name);
            if (t) {
                // Parse delay and apply pause.
                int64 delay = 0;
                if (read_duration(&delay, delay_str, NULL) == 0) {
                    if (delay == 0) delay = 1;
                    t->unpause_at = nanoseconds() + delay;
                    if (!t->pause) paused_ct++;
                    t->pause = delay;
                    t->stat.pause_ct++;
                    pause_tube_update(t);
                }
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "PAUSED\r\n");
            } else {
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
            }
        } else {
            rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "BAD_FORMAT\r\n");
        }
    } else if (strncmp(cmd, "peek ", 5) == 0 ||
               strncmp(cmd, "stats-job ", 10) == 0 ||
               strncmp(cmd, "kick-job ", 9) == 0 ||
               strncmp(cmd, "delete ", 7) == 0) {
        // ID-based commands: parse ID and execute locally.
        char *idstr = strchr(cmd, ' ') + 1;
        // Strip \r\n
        size_t il = strlen(idstr);
        if (il >= 2 && idstr[il-2] == '\r') idstr[il-2] = '\0';
        uint64 jid = 0;
        if (read_uint((uintmax_t*)&jid, UINT64_MAX, idstr, NULL) == 0) {
            Job *j = job_find(jid);
            if (strncmp(cmd, "peek ", 5) == 0) {
                if (j) {
                    // Format: FOUND <id> <size>\r\n<body>\r\n
                    int n = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                        "FOUND %"PRIu64" %u\r\n", j->r.id, j->r.body_size - 2);
                    if (n > 0 && n + j->r.body_size < CMD_FWD_REPLY_SIZE) {
                        memcpy(rpl.data + n, j->body, j->r.body_size);
                        rpl.data_len = n + j->r.body_size;
                    } else {
                        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                    }
                } else {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                }
            } else if (strncmp(cmd, "stats-job ", 10) == 0) {
                if (j && j->tube) {
                    char buf[STATS_BUF_SIZE];
                    int n = fmt_job_stats(buf, sizeof(buf), j);
                    if (n > 0 && n < CMD_FWD_REPLY_SIZE - 32) {
                        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                            "OK %d\r\n%s", n, buf);
                    } else {
                        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                    }
                } else {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                }
            } else if (strncmp(cmd, "kick-job ", 9) == 0) {
                if (j && ((j->r.state == Buried && kick_buried_job(s, j)) ||
                          (j->r.state == Delayed && kick_delayed_job(s, j)))) {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "KICKED\r\n");
                } else {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                }
            } else { // delete
                if (!j) {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                } else {
                    // Only delete non-reserved jobs (reserved = local to reserver).
                    Job *dj = NULL;
                    if (j->r.state == Ready) dj = remove_ready_job(j);
                    else if (j->r.state == Buried) dj = remove_buried_job(j);
                    else if (j->r.state == Delayed) dj = remove_delayed_job(j);
                    if (dj) {
                        dj->tube->stat.total_delete_ct++;
                        dj->r.state = Invalid;
                        Wal *w = shard_wal(s, dj);
                        walwrite(w, dj);
                        walmaint(w);
                        job_free(dj);
                        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "DELETED\r\n");
                    } else {
                        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                    }
                }
            }
        } else {
            rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
        }
    } else if (strncmp(cmd, "peek-ready", 10) == 0 ||
               strncmp(cmd, "peek-delayed", 12) == 0 ||
               strncmp(cmd, "peek-buried", 11) == 0 ||
               strncmp(cmd, "kick ", 5) == 0) {
        // Tube-based commands forwarded from remote use tube.
        // Determine tube from the use tube of the originating client.
        // These commands arrive with the full command line; we need the tube
        // that the sending worker's client was using. Since we can't know it
        // from the command alone, we look for local tubes that match the hash.
        // Simpler: execute against all local tubes.
        if (strncmp(cmd, "kick ", 5) == 0) {
            char *kstr = cmd + 5;
            size_t kl = strlen(kstr);
            if (kl >= 2 && kstr[kl-2] == '\r') kstr[kl-2] = '\0';
            uint32 count = 0;
            if (read_uint((uintmax_t*)&count, UINT32_MAX, kstr, NULL) == 0) {
                // Kick from all local tubes (best effort).
                uint kicked = 0;
                for (size_t ti = 0; ti < tubes.len && kicked < count; ti++) {
                    Tube *t = tubes.items[ti];
                    kicked += kick_jobs(s, t, count - kicked);
                }
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                    "KICKED %u\r\n", kicked);
            } else {
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "BAD_FORMAT\r\n");
            }
        } else {
            // peek-ready/delayed/buried: determine variant once, then search.
            int peek_type = 0; // 1=ready, 2=delayed, 3=buried
            if (strncmp(cmd, "peek-ready", 10) == 0) peek_type = 1;
            else if (strncmp(cmd, "peek-delayed", 12) == 0) peek_type = 2;
            else if (strncmp(cmd, "peek-buried", 11) == 0) peek_type = 3;
            Job *found = NULL;
            for (size_t ti = 0; ti < tubes.len && !found; ti++) {
                Tube *t = tubes.items[ti];
                if (peek_type == 1 && t->ready.len)
                    found = job_copy(t->ready.data[0]);
                else if (peek_type == 2 && t->delay.len)
                    found = job_copy(t->delay.data[0]);
                else if (peek_type == 3 && !job_list_is_empty(&t->buried))
                    found = job_copy(t->buried.next);
            }
            if (found) {
                int n = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                    "FOUND %"PRIu64" %u\r\n", found->r.id, found->r.body_size - 2);
                if (n > 0 && n + found->r.body_size < CMD_FWD_REPLY_SIZE) {
                    memcpy(rpl.data + n, found->body, found->r.body_size);
                    rpl.data_len = n + found->r.body_size;
                } else {
                    rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
                }
                job_free(found);
            } else {
                rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "NOT_FOUND\r\n");
            }
        }
    } else {
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "UNKNOWN_COMMAND\r\n");
    }

    // Send reply back to originating worker (variable-length: header + data).
    if (fwd->from_worker >= 0 && fwd->from_worker < s->nworkers
        && s->peer_fd[fwd->from_worker] >= 0) {
        size_t rpl_len = offsetof(struct CmdReplyMsg, data) + rpl.data_len;
        if (write(s->peer_fd[fwd->from_worker], &rpl, rpl_len) <= 0) {
            twarnx("fwd_cmd reply write to peer %d failed", fwd->from_worker);
        }
    }
}

// Handle a forwarded put from a peer worker.
// Creates the job locally and sends reply back.
void
prot_handle_forwarded_put(Server *s, struct PutFwdMsg *pm)
{
    struct CmdReplyMsg rpl;
    rpl.magic = CMD_REPLY_MAGIC;
    rpl.seq = pm->seq;
    rpl.data_len = 0;

    if (drain_mode) {
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "DRAINING\r\n");
        goto reply;
    }

    // Validate forwarded put fields.
    if (pm->body_size < 2 || pm->body_size > PUT_FWD_MAX_BODY + 2) {
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, "BAD_FORMAT\r\n");
        goto reply;
    }
    pm->tube[MAX_TUBE_NAME_LEN - 1] = '\0'; // ensure NUL-terminated
    if (pm->delay < 0) pm->delay = 0;

    Tube *t = tube_find_or_make(pm->tube);
    if (!t) {
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, MSG_OUT_OF_MEMORY);
        goto reply;
    }

    int64 ttr = pm->ttr;
    if (ttr < 1000000000) ttr = 1000000000;

    Job *j = make_job(pm->pri, pm->delay, ttr, pm->body_size, t);
    if (!j) {
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, MSG_OUT_OF_MEMORY);
        goto reply;
    }
    memcpy(j->body, pm->body, pm->body_size);

    // Reserve WAL space before enqueue (same as normal put path).
    Wal *w = shard_wal(s, j);
    j->walresv = walresvput(w, j);
    if (!j->walresv) {
        job_free(j);
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE, MSG_OUT_OF_MEMORY);
        goto reply;
    }

    int r = enqueue_job(s, j, j->r.delay, 1);
    if (r == 1) {
        global_stat.total_jobs_ct++;
        t->stat.total_jobs_ct++;
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                                MSG_INSERTED_FMT, j->r.id);
    } else {
        global_stat.total_jobs_ct++;
        t->stat.total_jobs_ct++;
        bury_job(s, j, 0);
        rpl.data_len = snprintf(rpl.data, CMD_FWD_REPLY_SIZE,
                                MSG_BURIED_FMT, j->r.id);
    }

reply:
    if (pm->from_worker >= 0 && pm->from_worker < s->nworkers
        && s->peer_fd[pm->from_worker] >= 0) {
        size_t rpl_len = offsetof(struct CmdReplyMsg, data) + rpl.data_len;
        if (write(s->peer_fd[pm->from_worker], &rpl, rpl_len) <= 0) {
            twarnx("fwd_put reply write to peer %d failed", pm->from_worker);
        }
    }
}

// Accept a migrated connection from a peer worker.
// Creates a Conn, sets up tubes from MigMsg, replays buffered command.
void
h_accept_migrated(int cfd, Server *s, struct MigMsg *mm)
{
    mm->use_tube[MAX_TUBE_NAME_LEN - 1] = '\0';
    mm->watch_tube[MAX_TUBE_NAME_LEN - 1] = '\0';
    Tube *use = tube_find_or_make(mm->use_tube);
    if (!use) use = default_tube;
    Tube *watch = tube_find_or_make(mm->watch_tube);
    if (!watch) watch = default_tube;

    Conn *c = make_conn(cfd, STATE_WANT_COMMAND, use, watch);
    if (!c) {
        close(cfd);
        return;
    }
    c->srv = s;
    c->sock.x = c;
    c->sock.f = (Handle)prothandle;
    c->sock.fd = cfd;

    // Flush pending reply (e.g. "WATCHING 1\r\n").
    if (mm->pending_reply_len > 0) {
        ssize_t wr = write(cfd, mm->pending_reply, mm->pending_reply_len);
        if (wr == -1 && errno != EAGAIN) {
            connclose(c);
            return;
        }
    }

    // Replay ALL buffered commands (not just the first one).
    // Without a full dispatch loop, pipelined commands are stranded:
    // the kernel buffer is empty (data forwarded in MigMsg), epoll
    // won't fire, and remaining commands never get processed.
    if (mm->cmd_len > 0 && mm->cmd_len <= sizeof(c->cmd)) {
        memcpy(c->cmd, mm->cmd, mm->cmd_len);
        c->cmd_read = mm->cmd_len;
        while (c->state == STATE_WANT_COMMAND && c->cmd_read > 0
               && !c->fwd_pending
               && (c->cmd_len = scan_line_end(c->cmd, c->cmd_read))) {
            dispatch_cmd(c);
            if (c->sock.fd < 0) break;
            fill_extra_data(c);
        }
        if (c->sock.fd < 0 || c->state == STATE_CLOSE) {
            connclose(c);
            epollq_apply();
            return;
        }
    }

    if (c->state == STATE_CLOSE) {
        connclose(c);
        epollq_apply();
        return;
    }

    if (sockwant(&c->sock, 'r') == -1) {
        twarn("sockwant migrated fd");
        connclose(c);
    }
    epollq_apply();
}

__attribute__((cold)) void
prot_init()
{
    now = nanoseconds();
    started_at = now;

    int dev_random = open("/dev/urandom", O_RDONLY);
    if (dev_random < 0) {
        twarn("open /dev/urandom");
        exit(50);
    }

    int i, r;
    byte rand_data[instance_id_bytes];
    r = read(dev_random, &rand_data, instance_id_bytes);
    if (r != instance_id_bytes) {
        twarn("read /dev/urandom");
        exit(50);
    }
    for (i = 0; i < instance_id_bytes; i++) {
        snprintf(instance_hex + (i * 2), 3, "%02x", rand_data[i]);
    }
    close(dev_random);

    if (uname(&node_info) == -1) {
        warn("uname");
        exit(50);
    }

    ms_init(&tubes, NULL, NULL);

    delay_tube_heap.less = tube_delay_less;
    delay_tube_heap.setpos = tube_delay_setpos;
    pause_tube_heap.less = tube_pause_less;
    pause_tube_heap.setpos = tube_pause_setpos;

    TUBE_ASSIGN(default_tube, tube_find_or_make("default"));
    if (!default_tube) {
        twarnx("Out of memory during startup!");
        exit(1);
    }
}

// For each job in list, inserts the job into the appropriate data
// structures and adds it to the log.
//
// Returns 1 on success, 0 on failure.
__attribute__((cold)) int
prot_replay(Server *s, Job *list)
{
    Job *j, *nj;
    int r;

    now = nanoseconds();

    int ok = 1;
    for (j = list->next ; j != list ; j = nj) {
        nj = j->next;
        job_list_remove(j);
        Wal *w = shard_wal(s, j);
        int z = walresvupdate(w);
        if (!z) {
            twarnx("failed to reserve space for job %"PRIu64", burying", j->r.id);
            bury_job(s, j, 0);
            ok = 0;
            continue;
        }
        j->walresv += z;
        int64 delay = 0;
        switch (j->r.state) {
        case Buried: {
            bury_job(s, j, 0);
            break;
        }
        case Delayed:
            if (now < j->r.deadline_at) {
                delay = j->r.deadline_at - now;
            }
            /* Falls through */
        default:
            r = enqueue_job(s, j, delay, 0);
            if (r < 1) {
                twarnx("error recovering job %"PRIu64", burying", j->r.id);
                walresvreturn(w, z);
                j->walresv -= z;
                bury_job(s, j, 0);
                ok = 0;
            }
        }
    }
    return ok;
}
