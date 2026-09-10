// Angry tests for conn_pool_put (conn.c, static).
//
// The only writer of the Conn slab free list, reached through connclose.
// Its invariant — conn_pool_len equals the number of structs actually on
// the list — is unobservable directly, so these tests prove it the only
// way a caller can: by draining the pool through make_conn and counting
// how many recycled structs really come back.
//
// The pool-ceiling tests need CONN_POOL_MAX+2 conns alive at once. They
// use descriptor numbers nothing has opened rather than 258 real
// descriptors: connclose's close() then fails harmlessly with EBADF and
// the pool bookkeeping under attack never touches the socket. Depending
// on the host's descriptor limit would test the host, not the code.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Documented ceiling of the slab pool (conn.c CONN_POOL_MAX). */
#define CPP_POOL_CEILING 256
#define CPP_UNOPENED_FD_BASE 900000

static void
cpp_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_pool_put";
    prot_init();
    conn_pool_drain();
}

static Tube *
cpp_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
cpp_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}


// Two more closes than the pool can hold. The pool must stop growing at
// its ceiling — a pool that keeps linking is unbounded memory the -m
// trim can never reclaim.
void
cttest_conn_pool_put_stops_linking_at_the_pool_ceiling(void)
{
    cpp_setup();
    Tube *t = cpp_tube("cpp-ceiling");
    enum { N = CPP_POOL_CEILING + 2 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CPP_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    assertf(cpp_pooled() == 0, "setup: the pool must start empty");

    for (int i = 0; i < N; i++)
        connclose(cs[i]);

    assertf(cpp_pooled() == CPP_POOL_CEILING,
            "the pool must stop at its ceiling of %d after %d closes, "
            "reports %d", CPP_POOL_CEILING, N, cpp_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// The counter and the list must describe the same thing. Draining the
// pool through make_conn is the only way to count what is really linked:
// a recycled struct arrives with an advanced generation, a fresh one
// starts at zero.
void
cttest_conn_pool_put_hands_back_exactly_as_many_structs_as_it_reports(void)
{
    cpp_setup();
    Tube *t = cpp_tube("cpp-agree");
    enum { N = CPP_POOL_CEILING + 2 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CPP_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    int reported = cpp_pooled();
    assertf(reported == CPP_POOL_CEILING, "setup: the pool must be full");

    int recycled = 0;
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CPP_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: the retake %d must succeed", i);
        if (cs[i]->gen > 0)
            recycled++;
    }

    assertf(recycled == reported,
            "the pool reported %d structs and handed back %d recycled ones "
            "out of %d takes", reported, recycled, N);

    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    conn_pool_drain();
    tube_dref(t);
}


// While an event batch is draining, a closed struct must stay frozen:
// pooling it lets make_conn hand the same memory to a new client before
// a stale epoll event pointing at it has been dispatched.
void
cttest_conn_pool_put_parks_a_close_that_lands_inside_a_defer_batch(void)
{
    cpp_setup();
    Tube *t = cpp_tube("cpp-defer");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    assertf(cpp_pooled() == 0, "setup: the pool must start empty");
    conn_defer_free_begin();

    for (int i = 0; i < 3; i++)
        connclose(cs[i]);

    assertf(cpp_pooled() == 0,
            "a conn closed during a batch drain must not reach the pool, "
            "the pool reports %d", cpp_pooled());

    conn_defer_free_end();
    conn_pool_drain();
    tube_dref(t);
}


// The flush at the end of the batch goes through the same push, so the
// ceiling still applies and the parked surplus must be freed rather than
// linked past it.
void
cttest_conn_pool_put_still_honours_the_ceiling_when_a_batch_flushes(void)
{
    cpp_setup();
    Tube *t = cpp_tube("cpp-defer-ceiling");
    enum { N = CPP_POOL_CEILING + 2 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CPP_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    conn_defer_free_begin();
    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    assertf(cpp_pooled() == 0, "setup: nothing may pool during the batch");

    conn_defer_free_end();

    assertf(cpp_pooled() == CPP_POOL_CEILING,
            "flushing %d parked conns must fill the pool to its ceiling of "
            "%d and no further, reports %d",
            N, CPP_POOL_CEILING, cpp_pooled());

    conn_pool_drain();
    tube_dref(t);
}
