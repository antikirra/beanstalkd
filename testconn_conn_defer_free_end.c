// Angry tests for conn_defer_free_end (conn.c).
//
// The flush. It detaches the parked list and pushes every struct through
// the pool path, so it inherits the pool ceiling and it must leave the
// list genuinely empty — a list that is walked but not detached hands the
// same struct to the pool twice, which is a free list with a cycle in it.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Documented ceiling of the slab pool (conn.c CONN_POOL_MAX). */
#define CDE_POOL_CEILING 256
#define CDE_UNOPENED_FD_BASE 930000

static void
cde_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_defer_free_end";
    prot_init();
    conn_pool_drain();
}

static Tube *
cde_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
cde_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}


// Every parked struct is released exactly once, so the pool grows by
// exactly the number that were parked.
void
cttest_conn_defer_free_end_releases_every_parked_conn_exactly_once(void)
{
    cde_setup();
    Tube *t = cde_tube("cde-count");
    enum { K = 5 };
    Conn *cs[K];
    for (int i = 0; i < K; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    int before = cde_pooled();
    conn_defer_free_begin();
    for (int i = 0; i < K; i++)
        connclose(cs[i]);

    conn_defer_free_end();

    assertf(cde_pooled() == before + K,
            "%d parked conns must add %d to the pool: %d became %d",
            K, K, before, cde_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// The parked list must actually be detached. A flush that walks it
// without clearing the head releases the same structs again on the next
// flush, linking each one into the free list twice.
void
cttest_conn_defer_free_end_empties_the_parked_list_so_a_later_flush_finds_nothing(void)
{
    cde_setup();
    Tube *t = cde_tube("cde-detach");
    Conn *cs[2];
    for (int i = 0; i < 2; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    conn_defer_free_begin();
    for (int i = 0; i < 2; i++)
        connclose(cs[i]);
    conn_defer_free_end();
    int after_first = cde_pooled();
    assertf(after_first == 2, "setup: both conns must pool at the first flush");

    conn_defer_free_begin();
    conn_defer_free_end();

    assertf(cde_pooled() == after_first,
            "a flush with nothing parked must release nothing: pool went "
            "from %d to %d", after_first, cde_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// An inner end is not a flush. Releasing at the wrong depth reopens the
// use-after-reuse window the outer begin was holding shut.
void
cttest_conn_defer_free_end_releases_nothing_while_an_outer_window_is_open(void)
{
    cde_setup();
    Tube *t = cde_tube("cde-inner");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    conn_defer_free_begin();
    conn_defer_free_begin();
    connclose(c);

    conn_defer_free_end();

    assertf(cde_pooled() == 0,
            "an inner end must release nothing, the pool reports %d",
            cde_pooled());

    conn_defer_free_end();
    conn_pool_drain();
    tube_dref(t);
}


// The flush pushes through the same pool path, so the ceiling still
// applies: parking more than the pool can hold must free the surplus
// rather than growing the free list past its bound.
void
cttest_conn_defer_free_end_stops_at_the_pool_ceiling_when_it_flushes(void)
{
    cde_setup();
    Tube *t = cde_tube("cde-ceiling");
    enum { N = CDE_POOL_CEILING + 3 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(CDE_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    conn_defer_free_begin();
    for (int i = 0; i < N; i++)
        connclose(cs[i]);

    conn_defer_free_end();

    assertf(cde_pooled() == CDE_POOL_CEILING,
            "flushing %d parked conns must leave exactly %d pooled, reports "
            "%d", N, CDE_POOL_CEILING, cde_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// A formerly parked struct must come back as a fully working conn: the
// descriptor it is given, no stale timeout, and an empty watch set of its
// own — anything less and the client on that socket is served by leftovers.
void
cttest_conn_defer_free_end_hands_back_a_formerly_parked_struct_ready_to_serve(void)
{
    cde_setup();
    Tube *t = cde_tube("cde-reuse");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    conn_defer_free_begin();
    connclose(c);
    conn_defer_free_end();
    int fd = dup(2);
    assertf(fd >= 0, "setup: the descriptor must be duplicated");

    Conn *again = make_conn(fd, STATE_WANT_DATA, t, t);

    assertf(again && again->sock.fd == fd && again->pending_timeout == -1
            && again->watch.len == 1 && again->watch.items[0] == t,
            "a released struct must serve its new descriptor: fd %d, "
            "pending_timeout %d, watch holds %zu",
            again ? again->sock.fd : -2, again ? again->pending_timeout : 0,
            again ? again->watch.len : (size_t)0);

    connclose(again);
    conn_pool_drain();
    tube_dref(t);
}
