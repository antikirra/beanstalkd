// Angry tests for count_cur_conns (conn.c).
//
// h_accept refuses every new client once this reaches -c maxconn, so an
// over-count is a server that stops accepting connections and never
// explains why. The interesting inputs are therefore the paths that must
// NOT count: a make_conn that failed, and a second connclose.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
ccc_setup(void)
{
    fault_clear_all();
    progname = "testconn_count_cur_conns";
    prot_init();
    conn_pool_drain();
}

static Tube *
ccc_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// The base contract: the count follows the live population up and back
// down again, with several conns alive at once so a stack-discipline
// mistake cannot hide.
void
cttest_count_cur_conns_returns_to_its_baseline_once_every_conn_is_closed(void)
{
    ccc_setup();
    Tube *t = ccc_tube("ccc-cycle");
    uint before = count_cur_conns();
    Conn *cs[4];
    for (int i = 0; i < 4; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    assertf(count_cur_conns() == before + 4, "setup: four conns must count");
    for (int i = 0; i < 4; i++)
        connclose(cs[i]);

    assertf(count_cur_conns() == before,
            "closing every conn must return the live count to %u, got %u",
            before, count_cur_conns());

    tube_dref(t);
}


// A refused allocation is not a connection. Counting it burns a slot of
// the -c budget that nothing will ever release.
void
cttest_count_cur_conns_ignores_a_conn_whose_allocation_was_refused(void)
{
    ccc_setup();
    Tube *t = ccc_tube("ccc-alloc-oom");
    uint before = count_cur_conns();
    fault_set(FAULT_CALLOC, 0, ENOMEM);

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(c == NULL && count_cur_conns() == before,
            "a make_conn that could not allocate must leave the live count "
            "at %u, got %u (make_conn returned %p)",
            before, count_cur_conns(), (void *)c);

    tube_dref(t);
}


// The second failure path: the struct exists but the watch set cannot
// grow, so make_conn hands the struct back to the pool. The accounting
// happens after that point and must not have run.
void
cttest_count_cur_conns_ignores_a_conn_whose_watch_set_could_not_grow(void)
{
    ccc_setup();
    Tube *t = ccc_tube("ccc-watch-oom");
    uint before = count_cur_conns();
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(c == NULL && count_cur_conns() == before,
            "a make_conn that could not watch its tube must leave the live "
            "count at %u, got %u (make_conn returned %p)",
            before, count_cur_conns(), (void *)c);

    tube_dref(t);
}


// prothandle can reach a conn twice in one event batch. The dead-fd
// guard is what makes the second close a no-op; without it the count
// wraps below zero and the -c gate locks the server out permanently.
void
cttest_count_cur_conns_decrements_only_on_the_first_close_of_a_conn(void)
{
    ccc_setup();
    Tube *t = ccc_tube("ccc-double-close");
    uint before = count_cur_conns();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    assertf(count_cur_conns() == before + 1, "setup: the conn must count");

    connclose(c);
    connclose(c);

    assertf(count_cur_conns() == before,
            "a second close must move nothing: baseline %u, got %u",
            before, count_cur_conns());

    tube_dref(t);
}


// A struct that comes back out of the slab pool is a new connection.
// Skipping the increment because the memory is familiar would let the
// live count sink below the number of open sockets.
void
cttest_count_cur_conns_counts_a_recycled_struct_as_one_live_connection(void)
{
    ccc_setup();
    Tube *t = ccc_tube("ccc-recycle");
    uint before = count_cur_conns();
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connclose(first);
    int pooled = -1;
    get_conn_pool_stats(&pooled);
    assertf(pooled >= 1, "setup: the closed struct must be pooled, got %d", pooled);

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");

    assertf(count_cur_conns() == before + 1,
            "one live connection built on a recycled struct is one: "
            "baseline %u, got %u", before, count_cur_conns());

    connclose(second);
    tube_dref(t);
}
