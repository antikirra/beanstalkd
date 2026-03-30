#include "dat.h"
#include <errno.h>
#include <limits.h>
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

Conn *
make_conn(int fd, char start_state, Tube *use, Tube *watch)
{
    Conn *c;
    if (conn_pool) {
        c = conn_pool;
        conn_pool = c->next;
        conn_pool_len--;
        c->gen++;
        c->srv = NULL;
        c->type = 0;
        c->next = NULL;
        c->tickat = 0;
        c->fwd_pending = 0;
        c->soonest_job = NULL;
        c->rw = 0;
        c->halfclosed = 0;
        c->cmd_len = 0;
        c->cmd_read = 0;
        c->reply = NULL;
        c->reply_len = 0;
        c->reply_sent = 0;
        c->in_job_read = 0;
        c->in_job = NULL;
        c->out_job = NULL;
        c->out_job_sent = 0;
        c->watch_idx = 0;
    } else {
        c = new(Conn);
    }
    if (!c) {
        twarn("OOM");
        return NULL;
    }

    c->sock.fd = fd;

    TUBE_ASSIGN(c->watch, watch);
    c->watch->watching_ct++;

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
    int margin = 0, should_timeout = 0;
    int64 t = INT64_MAX;

    if (conn_waiting(c)) {
        margin = SAFETY_MARGIN;
    }

    if (has_reserved_job(c)) {
        t = connsoonestjob(c)->r.deadline_at - now - margin;
        should_timeout = 1;
    }
    if (c->pending_timeout >= 0) {
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
        should_timeout = 1;
    }

    if (should_timeout) {
        return now + t;
    }
    return 0;
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
int
conndeadlinesoon(Conn *c)
{
    int64 t = now;
    Job *j = connsoonestjob(c);

    return j && t >= j->r.deadline_at - SAFETY_MARGIN;
}

int
conn_ready(Conn *c)
{
    return c->watch && c->watch->ready.len && !c->watch->pause;
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
        close(c->sock.fd);
        if (verbose) {
            printf("close %d\n", c->sock.fd);
        }
    }

    // Clear all pending forward slots referencing this conn.
    if (c->srv && c->srv->pending_fwd_used > 0) {
        int remaining = c->srv->pending_fwd_used;
        for (int i = 0; i < PENDING_FWD_SLOTS && remaining > 0; i++) {
            if (!c->srv->pending_fwd[i].conn)
                continue;
            remaining--;
            if (c->srv->pending_fwd[i].conn == c) {
                c->srv->pending_fwd[i].conn = NULL;
                c->srv->pending_fwd[i].seq = 0;
                c->srv->pending_fwd_used--;
            }
        }
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

    if (c->watch) {
        c->watch->watching_ct--;
        TUBE_ASSIGN(c->watch, NULL);
    }
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
