// Angry tests for conn_pool_drain (conn.c).
//
// Its whole reason to exist is that a pooled Conn is a live allocation
// malloc_trim(0) cannot reclaim — after a connection burst that is over a
// megabyte of permanently resident slack. So a drain that only zeroes the
// counter looks perfect on every counter check and reclaims nothing; the
// tests below prove the structs were really released by watching whether
// make_conn hands back recycled memory afterwards.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Documented ceiling of the slab pool (conn.c CONN_POOL_MAX). */
#define CPD_POOL_CEILING 256
#define CPD_UNOPENED_FD_BASE 910000

static void
cpd_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_pool_drain";
    prot_init();
    conn_pool_drain();
}

static Tube *
cpd_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
cpd_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}


// The counter must be zeroed exactly as the entries are released. A
// drain that empties the list without the counter leaves make_conn
// decrementing a length for a list that is already NULL.
void
cttest_conn_pool_drain_empties_a_pool_holding_closed_conns(void)
{
    cpd_setup();
    Tube *t = cpd_tube("cpd-empty");
    Conn *cs[4];
    for (int i = 0; i < 4; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    for (int i = 0; i < 4; i++)
        connclose(cs[i]);
    assertf(cpd_pooled() == 4, "setup: four closed conns must be pooled");

    conn_pool_drain();

    assertf(cpd_pooled() == 0,
            "a drained pool must report nothing left, reports %d",
            cpd_pooled());

    tube_dref(t);
}


// The counter alone cannot tell "released the structs" from "zeroed the
// count and kept them". A pool take advances the generation counter; a
// fresh allocation starts it at zero, so three fresh takes in a row are
// the proof that the memory really went back to the allocator.
void
cttest_conn_pool_drain_really_releases_the_structs_it_reports_as_gone(void)
{
    cpd_setup();
    Tube *t = cpd_tube("cpd-release");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    for (int i = 0; i < 3; i++)
        connclose(cs[i]);
    conn_pool_drain();

    int recycled = 0;
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: the post-drain make_conn %d must succeed", i);
        if (cs[i]->gen > 0)
            recycled++;
    }

    assertf(recycled == 0,
            "after a drain every conn must be a fresh allocation: %d of 3 "
            "came back recycled", recycled);

    for (int i = 0; i < 3; i++)
        connclose(cs[i]);
    conn_pool_drain();
    tube_dref(t);
}


// Conns parked by an open defer batch are not in the pool: they are
// frozen precisely because a stale epoll event may still point at them.
// A drain that walked the deferred list would free memory the batch is
// about to dispatch into.
void
cttest_conn_pool_drain_leaves_conns_parked_by_an_open_batch_alone(void)
{
    cpd_setup();
    Tube *t = cpd_tube("cpd-parked");
    Conn *a = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *b = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(a && b, "setup: two conns must allocate");
    conn_defer_free_begin();
    connclose(a);
    connclose(b);
    assertf(cpd_pooled() == 0, "setup: parked conns are not pooled");

    conn_pool_drain();
    conn_defer_free_end();

    assertf(cpd_pooled() == 2,
            "the parked conns must survive the drain and pool at the flush, "
            "the pool reports %d", cpd_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// Drain, refill, drain again. The pool has to keep working after being
// released — a drain that poisons the list head shows up only on the
// second cycle.
void
cttest_conn_pool_drain_keeps_the_pool_working_across_repeated_cycles(void)
{
    cpd_setup();
    Tube *t = cpd_tube("cpd-cycles");
    int stray = 0;

    for (int cycle = 0; cycle < 3; cycle++) {
        Conn *cs[2];
        for (int i = 0; i < 2; i++) {
            cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
            assertf(cs[i], "setup: cycle %d conn %d must allocate", cycle, i);
        }
        for (int i = 0; i < 2; i++)
            connclose(cs[i]);
        if (cpd_pooled() != 2)
            stray = 100 + cycle;
        conn_pool_drain();
        if (cpd_pooled() != 0)
            stray = 200 + cycle;
    }

    assertf(stray == 0,
            "every drain/refill cycle must fill to 2 and drain to 0, cycle "
            "code %d disagreed (pool reports %d)", stray, cpd_pooled());

    tube_dref(t);
}


// A burst that overflowed the pool ceiling freed the surplus at close
// time. The drain must release exactly what is on the list and no more —
// a second release of the already-freed surplus is a double free.
void
cttest_conn_pool_drain_empties_a_pool_that_overflowed_its_ceiling(void)
{
    cpd_setup();
    Tube *t = cpd_tube("cpd-overflow");
    enum { N = CPD_POOL_CEILING + 2 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CPD_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    assertf(cpd_pooled() == CPD_POOL_CEILING,
            "setup: the pool must be full at its ceiling");

    conn_pool_drain();

    assertf(cpd_pooled() == 0,
            "a drain after an overflow must leave the pool empty, reports %d",
            cpd_pooled());

    Conn *fresh = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(fresh && fresh->gen == 0,
            "make_conn must still work after the drain and allocate fresh");
    connclose(fresh);
    conn_pool_drain();
    tube_dref(t);
}
