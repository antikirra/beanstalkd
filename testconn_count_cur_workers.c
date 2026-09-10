// Angry tests for count_cur_workers (conn.c).
//
// Kept separate from the producer counter on purpose: they are two
// promises and one shared test would let a bit-swap in connclose pass
// while both counters moved together.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
ccw_setup(void)
{
    fault_clear_all();
    progname = "testconn_count_cur_workers";
    prot_init();
    conn_pool_drain();
}

static Tube *
ccw_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// The baseline must be restored by every cycle, not only by the last.
void
cttest_count_cur_workers_comes_home_after_every_reserve_and_close(void)
{
    ccw_setup();
    Tube *t = ccw_tube("ccw-cycle");
    uint before = count_cur_workers();
    uint stray = before;

    for (int i = 0; i < 5; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        connsetworker(c);
        connclose(c);
        if (count_cur_workers() != before)
            stray = count_cur_workers();
    }

    assertf(stray == before,
            "every reserve/close cycle must be counter-neutral: baseline "
            "%u, a cycle left %u", before, stray);

    tube_dref(t);
}


// A pure producer connection closes without ever having reserved. An
// unguarded decrement here wraps an unsigned zero to UINT_MAX.
void
cttest_count_cur_workers_ignores_the_close_of_a_conn_that_never_reserved(void)
{
    ccw_setup();
    Tube *t = ccw_tube("ccw-never");
    uint before = count_cur_workers();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    connsetproducer(c);
    assertf(!(c->type & CONN_TYPE_WORKER), "setup: the conn must not be a worker");

    connclose(c);

    assertf(count_cur_workers() == before,
            "closing a conn that never reserved must leave the counter at "
            "%u, got %u", before, count_cur_workers());

    tube_dref(t);
}


// Marked, closed, then the same struct comes back and is closed again
// without ever being marked: exactly one decrement across the two closes.
void
cttest_count_cur_workers_decrements_once_across_a_marked_and_a_recycled_close(void)
{
    ccw_setup();
    Tube *t = ccw_tube("ccw-recycle");
    uint before = count_cur_workers();
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connsetworker(first);
    assertf(count_cur_workers() == before + 1, "setup: the worker must count");
    connclose(first);
    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");

    connclose(second);

    assertf(count_cur_workers() == before,
            "one mark and two closes are one decrement: baseline %u, got %u",
            before, count_cur_workers());

    tube_dref(t);
}


// The close of a worker that still holds reservations takes the long
// route through enqueue_reserved_jobs. The counter must decrement once
// whichever route the teardown takes.
void
cttest_count_cur_workers_decrements_once_for_a_worker_still_holding_jobs(void)
{
    ccw_setup();
    Tube *t = ccw_tube("ccw-holding");
    Server s;
    memset(&s, 0, sizeof s);
    s.conns.less = conn_less;
    s.conns.setpos = conn_setpos;
    s.sock.fd = -1;
    s.sock.added = 1;
    uint before = count_cur_workers();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    connsetworker(c);
    now = 3000000000LL;
    Job js[2];
    for (int i = 0; i < 2; i++) {
        memset(&js[i], 0, sizeof js[i]);
        job_list_reset(&js[i]);
        js[i].tube = t;
        js[i].r.ttr = 5000000000LL;
        js[i].r.pri = 10 + (uint32)i;
        conn_reserve_job(c, &js[i]);
    }
    assertf(count_cur_workers() == before + 1, "setup: the worker must count");

    connclose(c);

    assertf(count_cur_workers() == before,
            "a worker closing with reservations still gives back exactly "
            "one: baseline %u, got %u", before, count_cur_workers());

    free(s.conns.data);
    tube_dref(t);
}
