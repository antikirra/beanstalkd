#include "dat.h"
#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SAFETY_MARGIN (1000000000) /* 1 second */

static uint cur_conn_ct = 0, cur_worker_ct = 0, cur_producer_ct = 0;
static uint tot_conn_ct = 0;
int verbose = 0;

// Conn slab pool: reuse freed Conn structs to avoid malloc/free per connection.
// Uses ht_next-style linking via the 'next' pointer (safe: conn removed from epollq).
#define CONN_POOL_MAX 256
static Conn *conn_pool = NULL;
static int conn_pool_len = 0;

// Deferred pool return: while the epoll batch drain runs (serv.c wraps
// it in conn_defer_free_begin/end), ep_buf can still hold events whose
// data.ptr targets a struct being closed right now. Pooling it
// immediately would let make_conn recycle the memory before the stale
// event is dispatched — a use-after-reuse. Deferral keeps the struct
// frozen (sock.fd == -1, gen unchanged) until the batch is drained, so
// prothandle can detect the dead conn and skip the event.
static int conn_free_defer = 0;
static Conn *conn_deferred = NULL;

// Live-conns list: every Conn between make_conn and connclose is linked
// here (conn_live_next/prev in struct Conn). It exists for one reason:
// when heapinsert(&srv->conns) fails in connsched, the conn's TTR /
// reserve-deadline / idle timers become invisible to prottick, and
// srv->conns alone cannot enumerate the victims (they are exactly the
// conns NOT in the heap). conn_sched_recover walks this list to retry
// the dropped inserts. conns_heap_degraded is the dirty flag.
static Conn *conn_live = NULL;
static int conns_heap_degraded = 0;

// conn_pool_put returns c to the slab pool, or frees it when the pool
// is full. Sole owner of the pool-push invariant (its pair is the pool
// take at the top of make_conn). During a batch drain it parks c on the
// deferred list instead (see above).
static void
conn_pool_put(Conn *c)
{
    if (unlikely(conn_free_defer > 0)) {
        c->next = conn_deferred;
        conn_deferred = c;
        return;
    }
    if (conn_pool_len < CONN_POOL_MAX) {
        c->next = conn_pool;
        conn_pool = c;
        conn_pool_len++;
    } else {
        free(c);
    }
}

// conn_pool_drain frees every pooled Conn back to glibc and resets the
// free list. Pooled entries are live allocations the allocator holds for
// O(1) reuse; malloc_trim(0) cannot reclaim their pages while they sit
// on the free list (after a connection burst that is ~1.2MB of
// permanently-resident slack: CONN_POOL_MAX * sizeof(Conn)). prottick
// calls this right before the periodic malloc_trim(0) (-m cadence),
// mirroring job_pool_drain. Every entry here went through connclose, so
// it is fully detached (no epollq link, no dur batch, no watch refs);
// plain free() is the same sanctioned path conn_pool_put takes on
// overflow. Counter balance (#2): conn_pool_len is zeroed exactly as
// every entry is freed.
void
conn_pool_drain(void)
{
    Conn *c = conn_pool;
    while (c) {
        Conn *next = c->next;
        free(c);
        c = next;
    }
    conn_pool = NULL;
    conn_pool_len = 0;
}

void
conn_defer_free_begin(void)
{
    conn_free_defer++;
}

// conn_defer_free_end releases every conn parked during the drain.
// Nested begins are counted; only the outermost end flushes.
void
conn_defer_free_end(void)
{
    if (--conn_free_defer > 0)
        return;
    Conn *c = conn_deferred;
    conn_deferred = NULL;
    while (c) {
        Conn *next = c->next;
        conn_pool_put(c);
        c = next;
    }
}

/* for unit tests: number of Conns currently sitting in the slab pool */
void
get_conn_pool_stats(int *count)
{
    if (count)
        *count = conn_pool_len;
}

// Callbacks for c->watch Ms: manage tube refcount and watching_ct.
static void
on_watch_insert(Ms *a, void *item, size_t i)
{
    UNUSED_PARAMETER(a);
    UNUSED_PARAMETER(i);
    Tube *t = item;
    tube_iref(t);
    t->watching_ct++;
}

static void
on_watch_remove(Ms *a, void *item, size_t i)
{
    Tube *t = item;
    t->watching_ct--;
    tube_dref(t);
    // ms_delete swap-moved the last watch entry into slot i; mirror the
    // swap in the parallel waitpos hint array so a WAITING conn's hints
    // keep tracking their tubes (OP_IGNORE shrinks watch while the conn
    // stays waiting on the remaining tubes). Bounds-gated: a conn that
    // never waited may have a smaller (or absent) hint array, and its
    // hints are meaningless anyway.
    Conn *c = (Conn *)((char *)a - offsetof(Conn, watch));
    if (i < a->len && a->len < c->waitpos_cap)
        c->waitpos[i] = c->waitpos[a->len];
}

// conn_waitpos_reserve — see dat.h. Doubling growth mirrors Ms.
int
conn_waitpos_reserve(Conn *c, size_t n)
{
    if (n <= c->waitpos_cap)
        return 1;
    size_t ncap = c->waitpos_cap ? c->waitpos_cap << 1 : 8;
    if (ncap < n)
        ncap = n;
    size_t *p = realloc(c->waitpos, ncap * sizeof(*p));
    if (!p)
        return 0;
    c->waitpos = p;
    c->waitpos_cap = ncap;
    return 1;
}

// on_waiting_conn_remove keeps waitpos hints fresh on the other side:
// removal from a tube's waiting_conns is swap-remove, so the conn that
// got MOVED into slot i must update its cached position for this tube.
// O(moved->watch.len), typically 1-2 entries. A miss (corrupt watch
// set) just leaves a stale hint — ms_remove_at degrades to the old
// linear scan, never to corruption.
void
on_waiting_conn_remove(Ms *a, void *item, size_t i)
{
    UNUSED_PARAMETER(item);
    if (i >= a->len)
        return; // removed the tail; nothing was moved
    Tube *t = (Tube *)((char *)a - offsetof(Tube, waiting_conns));
    Conn *moved = a->items[i];
    for (size_t k = 0; k < moved->watch.len; k++) {
        if (moved->watch.items[k] == t) {
            if (k < moved->waitpos_cap)
                moved->waitpos[k] = i;
            return;
        }
    }
}

// The pool-reuse memset below zeroes only [0, offsetof(Conn, cmd)).
// Keep every large buffer (cmd, reply_buf, dur_reply_buf) at the end of
// struct Conn so the hot accept path never wastes cycles zeroing them;
// their contents are gated by cmd_len/reply_len/dur_reply_len instead.
_Static_assert(offsetof(Conn, cmd) < 1024,
               "memset hot path must exclude large buffers; "
               "do not insert big arrays before cmd[] in struct Conn");

Conn *
make_conn(int fd, char start_state, Tube *use, Tube *watch)
{
    Conn *c;
    if (likely(conn_pool)) {
        c = conn_pool;
        conn_pool = c->next;
        conn_pool_len--;
        // Preserve gen (generation counter), zero the rest of the hot fields.
        uint64 gen = c->gen + 1;
        memset(c, 0, offsetof(Conn, cmd));  // zero up to large buffers
        c->gen = gen;
    } else {
        c = new(Conn);
    }
    if (!c) {
        twarn("OOM");
        return NULL;
    }

    c->sock.fd = fd;

    ms_init(&c->watch, on_watch_insert, on_watch_remove);
    if (!ms_append(&c->watch, watch)) { // callback: iref + watching_ct++
        twarn("OOM");
        // Don't close fd — caller is responsible for cleanup.
        c->sock.fd = -1;
        conn_pool_put(c);
        return NULL;
    }

    TUBE_ASSIGN(c->use, use);
    use->using_ct++;

    c->state = start_state;
    c->pending_timeout = -1;
    c->tickpos = 0; // Does not mean anything if in_conns is set to 0.
    c->in_conns = 0;
    c->last_activity_at = now; // baseline for -I; refreshed at every cmd

    // The list is empty.
    job_list_reset(&c->reserved_jobs);

    /* stats */
    cur_conn_ct++;
    tot_conn_ct++;

    // Link into the live-conns list (connsched OOM recovery — see top).
    // Done last so the early-failure path above never links a conn that
    // goes straight back to the pool. c->srv is still NULL here;
    // conn_sched_recover skips such conns.
    c->live_prev = NULL;
    c->live_next = conn_live;
    if (conn_live)
        conn_live->live_prev = c;
    conn_live = c;

    return c;
}

void
connsetproducer(Conn *c)
{
    if (c->type & CONN_TYPE_PRODUCER) return;
    c->type |= CONN_TYPE_PRODUCER;
    cur_producer_ct++; /* stats */
}

void
connsetworker(Conn *c)
{
    if (c->type & CONN_TYPE_WORKER) return;
    c->type |= CONN_TYPE_WORKER;
    cur_worker_ct++; /* stats */
}

uint
count_cur_conns(void)
{
    return cur_conn_ct;
}

uint
count_tot_conns(void)
{
    return tot_conn_ct;
}

uint
count_cur_producers(void)
{
    return cur_producer_ct;
}

uint
count_cur_workers(void)
{
    return cur_worker_ct;
}

static int
has_reserved_job(Conn *c)
{
    return !job_list_is_empty(&c->reserved_jobs);
}


// Returns positive nanoseconds when c should tick, 0 otherwise.
//
// The idle (-I) clause MUST mirror conn_timeout's idle close gate:
// same state, same job/reserve/wait conditions. If conntickat were
// looser than conn_timeout, the prottick loop would re-add the conn
// with an overdue tickat that conn_timeout refuses to act on, then
// connsched at conn_timeout's tail re-schedules the same overdue
// time — an unbounded busy loop. STATE_WANT_COMMAND is the load-
// bearing constraint here.
static inline int64
conntickat(Conn *c)
{
    int has_reserved = has_reserved_job(c);
    int idle_eligible = srv.idle_timeout > 0
                        && c->state == STATE_WANT_COMMAND
                        && !has_reserved
                        && c->pending_timeout < 0
                        && !conn_waiting(c);
    // Fast path: no pending timeout, no reserved jobs, no idle to track.
    if (likely(c->pending_timeout < 0 && !has_reserved && !idle_eligible))
        return 0;

    int margin = conn_waiting(c) ? SAFETY_MARGIN : 0;
    int64 t = INT64_MAX;

    if (has_reserved) {
        t = connsoonestjob(c)->r.deadline_at - now - margin;
    }
    if (c->pending_timeout >= 0) {
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
    }
    if (idle_eligible) {
        int64 idle_d = c->last_activity_at + srv.idle_timeout - now;
        t = min(t, idle_d);
    }
    return now + t;
}


// Remove c from the c->srv heap and reschedule it using the value
// returned by conntickat if there is an outstanding timeout in the c.
// Uses heapresift when conn stays in the heap — O(log n) vs 2*O(log n).
__attribute__((hot)) void
connsched(Conn *c)
{
    int64 newtickat = conntickat(c);
    if (c->in_conns) {
        if (newtickat) {
            if (newtickat != c->tickat) {
                c->tickat = newtickat;
                heapresift(&c->srv->conns, c->tickpos);
            }
        } else {
            c->tickat = 0;
            heapremove(&c->srv->conns, c->tickpos);
            c->in_conns = 0;
        }
    } else if (newtickat) {
        c->tickat = newtickat;
        c->in_conns = heapinsert(&c->srv->conns, c);
        if (unlikely(!c->in_conns)) {
            // OOM: c's timeouts are now invisible to prottick. Flag it;
            // conn_sched_recover retries the insert from the live list.
            conns_heap_degraded = 1;
        }
    }
}

// conn_sched_recover retries the srv->conns inserts that heapinsert OOM
// dropped in connsched. Walks the live-conns list (the heap itself
// cannot enumerate its missing members). Called from prottick; returns
// nonzero while still degraded so the caller can shorten its wake-up
// period and retry instead of parking for an hour.
int
conn_sched_recover(void)
{
    if (likely(!conns_heap_degraded))
        return 0;
    conns_heap_degraded = 0;
    for (Conn *c = conn_live; c; c = c->live_next) {
        if (c->in_conns || !c->srv)
            continue;
        int64 t = conntickat(c);
        if (!t)
            continue;
        c->tickat = t;
        c->in_conns = heapinsert(&c->srv->conns, c);
        if (!c->in_conns)
            conns_heap_degraded = 1;
    }
    return conns_heap_degraded;
}

// conn_set_soonestjob updates c->soonest_job with j
// if j should be handled sooner than c->soonest_job.
static inline void
conn_set_soonestjob(Conn *c, Job *j)
{
    if (likely(!c->soonest_job) || j->r.deadline_at < c->soonest_job->r.deadline_at) {
        c->soonest_job = j;
    }
}

// Return the reserved job with the earliest deadline,
// or NULL if there's no reserved job.
Job *
connsoonestjob(Conn *c)
{
    // use cached value and bail out.
    if (c->soonest_job != NULL)
        return c->soonest_job;

    Job *j = NULL;
    for (j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next) {
        conn_set_soonestjob(c, j);
    }
    return c->soonest_job;
}

__attribute__((hot)) void
conn_reserve_job(Conn *c, Job *j)
{
    j->tube->stat.reserved_ct++;
    j->r.reserve_ct++;

    j->r.deadline_at = now + j->r.ttr;
    j->r.state = Reserved;
    job_list_insert(&c->reserved_jobs, j);
    j->reserver = c;
    c->pending_timeout = -1;
    conn_set_soonestjob(c, j);
}

// Return true if c has a reserved job with less than one second until its
// deadline.
inline int
conndeadlinesoon(Conn *c)
{
    Job *j = connsoonestjob(c);
    return j && now >= j->r.deadline_at - SAFETY_MARGIN;
}

inline int
conn_ready(Conn *c)
{
    for (size_t i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        if (t->ready.len > 0 && !t->pause)
            return 1;
    }
    return 0;
}


__attribute__((hot)) int
conn_less(void *ca, void *cb)
{
    Conn *a = (Conn *)ca;
    Conn *b = (Conn *)cb;
    return a->tickat < b->tickat;
}


__attribute__((hot)) void
conn_setpos(void *c, size_t i)
{
    ((Conn *)c)->tickpos = i;
}


__attribute__((cold)) void
connclose(Conn *c)
{
    if (c->sock.fd >= 0) {
        sockwant(&c->sock, 0);
        if (verbose) {
            printf("close %d\n", c->sock.fd);
        }
        close(c->sock.fd);
        c->sock.fd = -1;
    } else {
        // Already closed — guard against double-close/double-pool.
        // Sufficient on its own: a closed conn keeps sock.fd == -1 while
        // pooled, and conn_defer_free_begin/end guarantees the struct is
        // never reused while an event batch that could re-enter us is
        // still draining.
        return;
    }

    // Detach from durable-commit batch if pending; must happen before
    // the pool slot could be reused, otherwise dur_flush_all() would
    // write buffered acks to a recycled conn's fd.
    dur_remove(c);

    job_free(c->in_job);

    /* was this a peek or stats command? */
    if (c->out_job && c->out_job->r.state == Copy)
        job_free(c->out_job);

    c->in_job = c->out_job = NULL;
    c->in_job_read = 0;

    if (c->type & CONN_TYPE_PRODUCER) cur_producer_ct--; /* stats */
    if (c->type & CONN_TYPE_WORKER) cur_worker_ct--; /* stats */

    cur_conn_ct--; /* stats */

    // Unlink from the live-conns list before the struct can be pooled
    // or freed — conn_sched_recover must never walk a recycled conn.
    if (c->live_prev)
        c->live_prev->live_next = c->live_next;
    else
        conn_live = c->live_next;
    if (c->live_next)
        c->live_next->live_prev = c->live_prev;
    c->live_next = c->live_prev = NULL;

    remove_waiting_conn(c);
    if (has_reserved_job(c))
        enqueue_reserved_jobs(c);

    ms_clear(&c->watch); // on_watch_remove callback: watching_ct-- + tube_dref

    if (c->use) {
        c->use->using_ct--;
        TUBE_ASSIGN(c->use, NULL);
    }

    if (c->in_conns) {
        heapremove(&c->srv->conns, c->tickpos);
        c->in_conns = 0;
    }

    if (c->srv && !c->srv->sock.added) {
        sockwant(&c->srv->sock, 'r');
    }

    // The waitpos hint array dies with the watch set: the pool-reuse
    // memset in make_conn would zero the pointer and leak the block.
    free(c->waitpos);
    c->waitpos = NULL;
    c->waitpos_cap = 0;

    // Return to pool for reuse, or free if pool is full.
    conn_pool_put(c);
}
