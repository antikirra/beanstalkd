#define _GNU_SOURCE
#include "dat.h"
#include <stdbool.h>
#include <stdint.h>
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
#ifdef __GLIBC__
#include <malloc.h>
#endif

/* job body cannot be greater than this many bytes long */
size_t job_data_size_limit = JOB_DATA_SIZE_LIMIT_DEFAULT;

int64 mem_trim_rate = 60000000000LL; /* 60 seconds in nanoseconds */

// shard_wal returns the WAL instance for a job based on its tube name.
// With sharding enabled, jobs are distributed across per-CPU WAL instances
// by tube name hash, parallelizing disk I/O (writes + fsync).
// Falls back to the single legacy WAL when sharding is not active.
static Wal *
shard_wal(Server *s, Job *j)
{
    if (s->nshards > 0 && s->shards && j->tube) {
        uint h = tube_name_hash(j->tube->name);
        return &s->shards[h % s->nshards];
    }
    return &s->wal;
}

#define NAME_CHARS \
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
    "abcdefghijklmnopqrstuvwxyz" \
    "0123456789-+/;.$_()"

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
        heapremove(&delay_tube_heap, t->delay_heap_index);
    }
    t->in_delay_heap = heapinsert(&delay_tube_heap, t);
    if (!t->in_delay_heap) {
        twarnx("delay_tube_update: OOM inserting tube %s into delay heap", t->name);
    }
    return t->in_delay_heap;
}

// Matchable tube heap: tubes with ready jobs AND waiting connections.
// Provides O(1) lookup of the highest-priority matchable job,
// replacing the O(tubes.len) scan in next_awaited_job().
static Heap matchable_heap;

static int
tube_match_less(void *ta, void *tb)
{
    Tube *a = ta;
    Tube *b = tb;
    if (a->ready.len == 0) return 0;
    if (b->ready.len == 0) return 1;
    return job_pri_less(a->ready.data[0], b->ready.data[0]);
}

static void
tube_match_setpos(void *t, size_t pos)
{
    ((Tube *)t)->matchable_index = pos;
}

// Update tube's membership in the matchable heap.
// A tube is matchable when it has ready jobs, waiting connections,
// and is not paused. Call after any change to these conditions.
static void
matchable_update(Tube *t)
{
    int dominated = t->ready.len > 0
                 && t->waiting_conns.len > 0
                 && !t->pause;
    if (dominated) {
        if (t->in_matchable) {
            heapremove(&matchable_heap, t->matchable_index);
        }
        t->in_matchable = heapinsert(&matchable_heap, t);
        if (!t->in_matchable) {
            twarnx("matchable_update: OOM inserting tube %s into matchable heap", t->name);
        }
    } else if (t->in_matchable) {
        heapremove(&matchable_heap, t->matchable_index);
        t->in_matchable = 0;
    }
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
        heapremove(&pause_tube_heap, t->pause_heap_index);
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
    if (t->in_matchable) {
        heapremove(&matchable_heap, t->matchable_index);
        t->in_matchable = 0;
    }
    if (t->in_pause_heap) {
        heapremove(&pause_tube_heap, t->pause_heap_index);
        t->in_pause_heap = 0;
    }
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
static void
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
        int r = sockwant(&c->sock, c->rw);
        if (r == -1) {
            twarn("sockwant");
            connclose(c);
        }
    }
}

#define reply_msg(c, m) \
    reply((c), (m), CONSTSTRLEN(m), STATE_SEND_WORD)

#define reply_serr(c, e) \
    (twarnx("server error: %s", (e)), reply_msg((c), (e)))

static void
reply(Conn *c, char *line, int len, int state)
{
    if (!c)
        return;

    if (verbose >= 2) {
        printf(">%d reply %.*s\n", c->sock.fd, len-2, line);
    }

    // Fast path: try immediate write for short text replies.
    // Saves 2 syscalls per command (epoll_ctl MOD + epoll_wait)
    // by skipping the read→epoll→write→epoll round-trip.
    // Falls through to normal epoll-based send on EAGAIN or partial write.
    if (state == STATE_SEND_WORD) {
        int r = write(c->sock.fd, line, len);
        if (r == len) {
            c->reply = line;
            c->reply_len = len;
            c->reply_sent = 0;
            c->state = STATE_WANT_COMMAND;
            if (c->out_job && c->out_job->r.state == Copy)
                job_free(c->out_job);
            c->out_job = NULL;
            connsched(c);
            return;
        }
        // Partial write: record progress, fall through to epoll.
        if (r > 0) {
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

// reply_job tells the connection c which job to send,
// and replies with this line: <msg> <job_id> <job_size>.
static void
reply_job(Conn *c, Job *j, const char *msg)
{
    c->out_job = j;
    c->out_job_sent = 0;
    reply_line(c, STATE_SEND_JOB, "%s %"PRIu64" %u\r\n",
               msg, j->r.id, j->r.body_size - 2);
}

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
    size_t i;
    for (i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        t->stat.waiting_ct--;
        ms_remove_at(&t->waiting_conns, c->watch_idx[i], c);
        matchable_update(t);
    }
}

// enqueue_waiting_conn sets CONN_TYPE_WAITING for the connection,
// adds it to the waiting_conns set of every tube it's watching.
// Records each position in c->watch_idx for O(1) removal later.
static int
enqueue_waiting_conn(Conn *c)
{
    size_t i;

    // Ensure watch_idx has capacity for all watched tubes.
    if (c->watch.len > c->watch_idx_cap) {
        size_t ncap = c->watch_idx_cap ? c->watch_idx_cap * 2 : 4;
        if (ncap < c->watch.len) ncap = c->watch.len;
        size_t *p = realloc(c->watch_idx, ncap * sizeof(size_t));
        if (!p)
            return 0;
        c->watch_idx = p;
        c->watch_idx_cap = ncap;
    }

    for (i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        if (!ms_append(&t->waiting_conns, c)) {
            size_t j;
            for (j = 0; j < i; j++) {
                Tube *u = c->watch.items[j];
                ms_remove_at(&u->waiting_conns, c->watch_idx[j], c);
                u->stat.waiting_ct--;
                matchable_update(u);
            }
            return 0;
        }
        c->watch_idx[i] = t->waiting_conns.len - 1;
        t->stat.waiting_ct++;
        matchable_update(t);
    }
    c->type |= CONN_TYPE_WAITING;
    global_stat.waiting_ct++;
    return 1;
}

// next_awaited_job returns the highest-priority ready job among all tubes
// that have both ready jobs and waiting connections. O(1) via matchable heap.
static Job *
next_awaited_job()
{
    if (matchable_heap.len == 0)
        return NULL;
    Tube *t = matchable_heap.data[0];
    if (t->ready.len == 0 || t->waiting_conns.len == 0)
        return NULL;
    return t->ready.data[0];
}

// process_queue performs reservation for every jobs that is awaited for.
static void
process_queue()
{
    Job *j = NULL;

    while ((j = next_awaited_job())) {
        j = remove_ready_job(j);
        if (j == NULL) {
            // Impossible: next_awaited_job found a ready job but remove
            // failed. Break to avoid infinite loop (NASA rule 2).
            twarnx("job not ready");
            break;
        }
        Conn *c = ms_take(&j->tube->waiting_conns);
        if (c == NULL) {
            // Impossible: next_awaited_job checked waiting_conns.len > 0
            // but ms_take returned NULL. Re-enqueue the job we just removed
            // from the ready heap to avoid orphaning it, then break.
            twarnx("waiting_conns is empty");
            heapinsert(&j->tube->ready, j);
            j->r.state = Ready;
            ready_ct++;
            if (j->r.pri < URGENT_THRESHOLD) {
                global_stat.urgent_ct++;
                j->tube->stat.urgent_ct++;
            }
            matchable_update(j->tube);
            break;
        }
        global_stat.reserved_ct++;

        remove_waiting_conn(c);
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
static int
enqueue_job(Server *s, Job *j, int64 delay, char update_store)
{
    int r;
    Wal *w = shard_wal(s, j);

    j->reserver = NULL;
    if (delay) {
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
        matchable_update(j->tube);
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
                matchable_update(j->tube);
            }
            return 0;
        }
        if (!walmaint(w)) {
            twarnx("walmaint failed after walwrite");
        }
    }

    return 1;
}

static int
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
    while (!job_list_is_empty(&c->reserved_jobs)) {
        Job *j = job_list_remove(c->reserved_jobs.next);
        int r = enqueue_job(c->srv, j, 0, 0);
        if (r < 1)
            bury_job(c->srv, j, 0);
        global_stat.reserved_ct--;
        j->tube->stat.reserved_ct--;
        c->soonest_job = NULL;
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

static uint
kick_jobs(Server *s, Tube *t, uint n)
{
    if (!job_list_is_empty(&t->buried))
        return kick_buried_jobs(s, t, n);
    return kick_delayed_jobs(s, t, n);
}

// remove_buried_job returns non-NULL value if job j was in the buried state.
// It excludes the job from the buried list and updates counters.
static Job *
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
static Job *
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
static Job *
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
    matchable_update(j->tube);
    return j;
}

static bool
is_job_reserved_by_conn(Conn *c, Job *j)
{
    return j && j->reserver == c && j->r.state == Reserved;
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

static void
check_err(Conn *c, const char *s)
{
    if (errno == EAGAIN)
        return;
    if (errno == EINTR)
        return;
    if (errno == EWOULDBLOCK)
        return;

    twarn("%s", s);
    c->state = STATE_CLOSE;
}

/* Scan the given string for the sequence "\r\n" and return the line length.
 * Always returns at least 2 if a match is found. Returns 0 if no match. */
static size_t
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
static int
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
        TEST_CMD(c->cmd, CMD_RESERVE_TIMEOUT, OP_RESERVE_TIMEOUT);
        TEST_CMD(c->cmd, CMD_RESERVE_JOB, OP_RESERVE_JOB);
        TEST_CMD(c->cmd, CMD_RESERVE, OP_RESERVE);
        TEST_CMD(c->cmd, CMD_RELEASE, OP_RELEASE);
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
static void
fill_extra_data(Conn *c)
{
    if (!c->cmd_len)
        return; /* we don't have a complete command */

    /* how many extra bytes did we read? */
    int64 extra_bytes = c->cmd_read - c->cmd_len;

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

static void
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

    /* we have a complete job, so let's stick it in the pqueue */
    r = enqueue_job(c->srv, j, j->r.delay, 1);

    if (r == 1) {
        global_stat.total_jobs_ct++;
        j->tube->stat.total_jobs_ct++;
        process_queue();
        reply_line(c, STATE_SEND_WORD, MSG_INSERTED_FMT, j->r.id);
        return;
    }

    /* out of memory trying to grow the queue, so it gets buried */
    global_stat.total_jobs_ct++;
    j->tube->stat.total_jobs_ct++;
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

    getrusage(RUSAGE_SELF, &ru); /* don't care if it fails */
    return snprintf(buf, size, STATS_FMT,
                    global_stat.urgent_ct,
                    ready_ct,
                    global_stat.reserved_ct,
                    delayed_ct,
                    global_stat.buried_ct,
                    op_ct[OP_PUT],
                    op_ct[OP_PEEKJOB],
                    op_ct[OP_PEEK_READY],
                    op_ct[OP_PEEK_DELAYED],
                    op_ct[OP_PEEK_BURIED],
                    op_ct[OP_RESERVE],
                    op_ct[OP_RESERVE_TIMEOUT],
                    op_ct[OP_DELETE],
                    op_ct[OP_RELEASE],
                    op_ct[OP_USE],
                    op_ct[OP_WATCH],
                    op_ct[OP_IGNORE],
                    op_ct[OP_BURY],
                    op_ct[OP_KICK],
                    op_ct[OP_TOUCH],
                    op_ct[OP_STATS],
                    op_ct[OP_STATSJOB],
                    op_ct[OP_STATS_TUBE],
                    op_ct[OP_LIST_TUBES],
                    op_ct[OP_LIST_TUBE_USED],
                    op_ct[OP_LIST_TUBES_WATCHED],
                    op_ct[OP_PAUSE_TUBE],
                    timeout_ct,
                    global_stat.total_jobs_ct,
                    job_data_size_limit,
                    tubes.len,
                    count_cur_conns(),
                    count_cur_producers(),
                    count_cur_workers(),
                    global_stat.waiting_ct,
                    count_tot_conns(),
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
    uintmax_t tnum;
    char *tend;

    errno = 0;
    while (buf[0] == ' ')
        buf++;
    if (buf[0] < '0' || '9' < buf[0])
        return -1;
    tnum = strtoumax(buf, &tend, 10);
    if (tend == buf)
        return -1;
    if (errno)
        return -1;
    if (!end && tend[0] != '\0')
        return -1;
    if (tnum > max_val)
        return -1;

    if (out) *out = tnum;
    if (end) *end = tend;
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
    len = strspn(buf, NAME_CHARS);
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

    /* Upper bound: "---\n" (4) + N × ("- " + name + "\n") + "\r\n" (2).
     * Over-allocates by (MAX_TUBE_NAME_LEN - actual_len) per tube,
     * but avoids a measurement pass over the tube list. */
    size_t maxsz = 6 + l->len * (3 + MAX_TUBE_NAME_LEN);
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

static void
maybe_enqueue_incoming_job(Conn *c)
{
    Job *j = c->in_job;

    /* do we have a complete job? */
    if (c->in_job_read == j->r.body_size) {
        enqueue_incoming_job(c);
        return;
    }

    /* otherwise we have incomplete data, so just keep waiting */
    c->state = STATE_WANT_DATA;
}

/* j can be NULL */
static Job *
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
static size_t
is_valid_tube(const char *name, size_t max)
{
    if (name[0] == '\0' || name[0] == '-')
        return 0;
    size_t len = strspn(name, NAME_CHARS);
    if (len == 0 || len > max || name[len] != '\0')
        return 0;
    return len;
}

static void
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

    /* check for possible maliciousness */
    if (strlen(c->cmd) != c->cmd_len - 2) {
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

        connsetproducer(c);

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
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_READY_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (c->use->ready.len) {
            j = job_copy(c->use->ready.data[0]);
        }

        if (!j) {
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_PEEK_DELAYED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_DELAYED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        if (c->use->delay.len) {
            j = job_copy(c->use->delay.data[0]);
        }

        if (!j) {
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
            reply_msg(c, MSG_NOTFOUND);
            return;
        }
        reply_job(c, j, MSG_FOUND);
        return;

    case OP_RESERVE_TIMEOUT:
        errno = 0;
        uint32 utimeout = 0;
        if (read_u32(&utimeout, c->cmd + CMD_RESERVE_TIMEOUT_LEN, NULL) != 0 || utimeout > INT_MAX) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        timeout = (int)utimeout;
        /* Falls through */

    case OP_RESERVE:
        /* don't allow trailing garbage */
        if (type == OP_RESERVE && c->cmd_len != CMD_RESERVE_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        connsetworker(c);

        if (conndeadlinesoon(c) && !conn_ready(c)) {
            reply_msg(c, MSG_DEADLINE_SOON);
            return;
        }

        /* try to get a new job for this guy */
        wait_for_job(c, timeout);
        process_queue();
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

        connsetworker(c);
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
            j = remove_reserved_job(c, jf);
            if (!j)
                j = remove_ready_job(jf);
            if (!j)
                j = remove_buried_job(jf);
            if (!j)
                j = remove_delayed_job(jf);
        }

        if (!j) {
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
        if (delay) {
            Wal *w = shard_wal(c->srv, j);
            z = walresvupdate(w);
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
            process_queue();
            reply_msg(c, MSG_RELEASED);
            return;
        }

        /* out of memory trying to grow the queue, so it gets buried */
        if (z) {
            walresvreturn(shard_wal(c->srv, j), z);
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
        process_queue();
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
            reply_msg(c, MSG_NOTFOUND);
            return;
        }

        if ((j->r.state == Buried && kick_buried_job(c->srv, j)) ||
            (j->r.state == Delayed && kick_delayed_job(c->srv, j))) {
            process_queue();
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
        do_list_tubes(c, &tubes);
        return;

    case OP_LIST_TUBE_USED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBE_USED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        reply_line(c, STATE_SEND_WORD, "USING %s\r\n", c->use->name);
        return;

    case OP_LIST_TUBES_WATCHED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBES_WATCHED_LEN + 2) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;
        do_list_tubes(c, &c->watch);
        return;

    case OP_USE:
        name = c->cmd + CMD_USE_LEN;
        if (!is_valid_tube(name, MAX_TUBE_NAME_LEN - 1)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        TUBE_ASSIGN(t, tube_find_or_make(name));
        if (!t) {
            reply_serr(c, MSG_OUT_OF_MEMORY);
            return;
        }

        c->use->using_ct--;
        TUBE_ASSIGN(c->use, t);
        TUBE_ASSIGN(t, NULL);
        c->use->using_ct++;

        reply_line(c, STATE_SEND_WORD, "USING %s\r\n", c->use->name);
        return;

    case OP_WATCH:
        name = c->cmd + CMD_WATCH_LEN;
        if (!is_valid_tube(name, MAX_TUBE_NAME_LEN - 1)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        TUBE_ASSIGN(t, tube_find_or_make(name));
        if (!t) {
            reply_serr(c, MSG_OUT_OF_MEMORY);
            return;
        }

        r = 1;
        if (!ms_contains(&c->watch, t))
            r = ms_append(&c->watch, t);
        TUBE_ASSIGN(t, NULL);
        if (!r) {
            reply_serr(c, MSG_OUT_OF_MEMORY);
            return;
        }
        reply_line(c, STATE_SEND_WORD, "WATCHING %zu\r\n", c->watch.len);
        return;

    case OP_IGNORE:
        name = c->cmd + CMD_IGNORE_LEN;
        if (!is_valid_tube(name, MAX_TUBE_NAME_LEN - 1)) {
            reply_msg(c, MSG_BAD_FORMAT);
            return;
        }
        op_ct[type]++;

        t = tube_find(&c->watch, name);
        if (t && c->watch.len < 2) {
            reply_msg(c, MSG_NOT_IGNORED);
            return;
        }

        if (t)
            ms_remove(&c->watch, t); /* may free t if refcount => 0 */
        t = NULL;
        reply_line(c, STATE_SEND_WORD, "WATCHING %zu\r\n", c->watch.len);
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
        matchable_update(t);
        pause_tube_update(t);

        reply_line(c, STATE_SEND_WORD, "PAUSED\r\n");
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
        connsched(c);
    }

    if (should_timeout) {
        remove_waiting_conn(c);
        reply_msg(c, MSG_DEADLINE_SOON);
    } else if (conn_waiting(c) && c->pending_timeout >= 0) {
        c->pending_timeout = -1;
        remove_waiting_conn(c);
        reply_msg(c, MSG_TIMED_OUT);
    }
}

void
enter_drain_mode(int sig)
{
    UNUSED_PARAMETER(sig);
    drain_mode = 1;
}

static void
conn_want_command(Conn *c)
{
    epollq_add(c, 'r');

    /* was this a peek or stats command? */
    if (c->out_job && c->out_job->r.state == Copy)
        job_free(c->out_job);
    c->out_job = NULL;

    c->reply_sent = 0; /* now that we're done, reset this */
    c->state = STATE_WANT_COMMAND;
}

static void
conn_process_io(Conn *c)
{
    int r;
    int64 to_read;
    Job *j;
    struct iovec iov[2];

    // Disable delayed ACK for this connection's current exchange.
    // Kernel resets this after each recv, so we set it every time.
    // Eliminates up to 40ms delayed ACK penalty in request-response patterns.
    int quickack = 1;
    setsockopt(c->sock.fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof quickack);

    switch (c->state) {
    case STATE_WANT_COMMAND:
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
    case STATE_WANT_DATA:
        j = c->in_job;

        r = read(c->sock.fd, j->body + c->in_job_read, j->r.body_size -c->in_job_read);
        if (r == -1) {
            check_err(c, "read()");
            return;
        }
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        c->in_job_read += r; /* we got some bytes */

        /* (j->in_job_read > j->r.body_size) can't happen */

        maybe_enqueue_incoming_job(c);
        return;
    case STATE_SEND_WORD:
        r= write(c->sock.fd, c->reply + c->reply_sent, c->reply_len - c->reply_sent);
        if (r == -1) {
            check_err(c, "write()");
            return;
        }
        if (r == 0) {
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
        if (r == -1) {
            check_err(c, "writev()");
            return;
        }
        if (r == 0) {
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

static void
h_conn(const int fd, const short which, Conn *c)
{
    if (fd != c->sock.fd) {
        twarnx("Argh! event fd doesn't match conn fd.");
        close(fd);
        connclose(c);
        epollq_apply();
        return;
    }

    if (which == 'h') {
        c->halfclosed = 1;
    }

    conn_process_io(c);

    // Cork the socket before dispatching commands: coalesce multiple
    // replies into a single TCP segment when processing pipelined commands.
    int cork = 1;
    setsockopt(c->sock.fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork);

    while (cmd_data_ready(c) && (c->cmd_len = scan_line_end(c->cmd, c->cmd_read))) {
        dispatch_cmd(c);
        fill_extra_data(c);
    }

    // Uncork: flush all coalesced replies as one TCP segment.
    cork = 0;
    setsockopt(c->sock.fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof cork);

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
        matchable_update(t);
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

    // Match ready jobs with waiting connections only if something
    // changed above (delayed→ready, unpause, timeout). Skip when idle.
    if (matchable_heap.len > 0)
        process_queue();

    // Periodically return unused heap pages to the OS.
    // Addresses glibc not releasing memory after mass job deletion.
    // Controlled by -m flag; 0 disables.
#ifdef __GLIBC__
    if (mem_trim_rate > 0) {
        static int64 last_trim;
        if (now - last_trim >= mem_trim_rate) {
            malloc_trim(0);
            last_trim = now;
        }
    }
#endif

    epollq_apply();

    return period;
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

        Conn *c = make_conn(cfd, STATE_WANT_COMMAND, default_tube, default_tube);
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

        if (sockwant(&c->sock, 'r') == -1) {
            twarn("sockwant");
            connclose(c);
            continue;
        }
    }
    epollq_apply();
}

void
prot_init()
{
    now = nanoseconds();
    started_at = now;
    memset(op_ct, 0, sizeof(op_ct));

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
    matchable_heap.less = tube_match_less;
    matchable_heap.setpos = tube_match_setpos;
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
int
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
    process_queue();
    return ok;
}
