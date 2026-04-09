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
    UNUSED_PARAMETER(a);
    UNUSED_PARAMETER(i);
    Tube *t = item;
    t->watching_ct--;
    tube_dref(t);
}

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
    ms_append(&c->watch, watch); // callback: iref + watching_ct++

    TUBE_ASSIGN(c->use, use);
    use->using_ct++;

    c->state = start_state;
    c->pending_timeout = -1;
    c->tickpos = 0; // Does not mean anything if in_conns is set to 0.
    c->in_conns = 0;

    // The list is empty.
    job_list_reset(&c->reserved_jobs);

    /* stats */
    cur_conn_ct++;
    tot_conn_ct++;

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
count_cur_conns()
{
    return cur_conn_ct;
}

uint
count_tot_conns()
{
    return tot_conn_ct;
}

uint
count_cur_producers()
{
    return cur_producer_ct;
}

uint
count_cur_workers()
{
    return cur_worker_ct;
}

static int
has_reserved_job(Conn *c)
{
    return !job_list_is_empty(&c->reserved_jobs);
}


// Returns positive nanoseconds when c should tick, 0 otherwise.
static inline int64
conntickat(Conn *c)
{
    // Fast path: no pending timeout and no reserved jobs → no tick needed.
    // Covers the common case after a simple put/delete/stats reply.
    if (likely(c->pending_timeout < 0 && job_list_is_empty(&c->reserved_jobs)))
        return 0;

    int margin = conn_waiting(c) ? SAFETY_MARGIN : 0;
    int64 t = INT64_MAX;

    if (has_reserved_job(c)) {
        t = connsoonestjob(c)->r.deadline_at - now - margin;
    }
    if (c->pending_timeout >= 0) {
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
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
    }
}

// conn_set_soonestjob updates c->soonest_job with j
// if j should be handled sooner than c->soonest_job.
static inline void
conn_set_soonestjob(Conn *c, Job *j) {
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
conn_reserve_job(Conn *c, Job *j) {
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
        return; // already closed — guard against double-close/double-pool
    }

    job_free(c->in_job);

    /* was this a peek or stats command? */
    if (c->out_job && c->out_job->r.state == Copy)
        job_free(c->out_job);

    c->in_job = c->out_job = NULL;
    c->in_job_read = 0;

    if (c->type & CONN_TYPE_PRODUCER) cur_producer_ct--; /* stats */
    if (c->type & CONN_TYPE_WORKER) cur_worker_ct--; /* stats */

    cur_conn_ct--; /* stats */

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

    // Return to pool for reuse, or free if pool is full.
    if (conn_pool_len < CONN_POOL_MAX) {
        c->next = conn_pool;
        conn_pool = c;
        conn_pool_len++;
    } else {
        free(c);
    }
}
