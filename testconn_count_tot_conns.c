// Angry tests for count_tot_conns (conn.c).
//
// The cumulative accept counter. Its one structural promise is that it
// only ever goes up — `stats` publishes it as total-connections and
// operators diff it over time, so a close that touches it, or a failed
// accept that inflates it, turns the rate into fiction.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
ctc_setup(void)
{
    fault_clear_all();
    progname = "testconn_count_tot_conns";
    prot_init();
    conn_pool_drain();
}

static Tube *
ctc_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// Six accepts, six closes. The closes must be invisible here.
void
cttest_count_tot_conns_counts_every_accept_and_forgets_no_close(void)
{
    ctc_setup();
    Tube *t = ctc_tube("ctc-cycle");
    uint before = count_tot_conns();

    for (int i = 0; i < 6; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        connclose(c);
    }

    assertf(count_tot_conns() == before + 6,
            "six accepts are six total connections however many closed: "
            "%u became %u", before, count_tot_conns());

    tube_dref(t);
}


// A recycled struct is still a new connection from the client's side.
void
cttest_count_tot_conns_counts_a_recycled_struct_as_a_new_connection(void)
{
    ctc_setup();
    Tube *t = ctc_tube("ctc-recycle");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connclose(first);
    uint before = count_tot_conns();

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");

    assertf(count_tot_conns() == before + 1,
            "taking a struct from the pool is still an accept: %u became %u",
            before, count_tot_conns());

    connclose(second);
    tube_dref(t);
}


// An accept that failed to allocate never reached the client. Counting
// it makes the total exceed the number of connections ever served.
void
cttest_count_tot_conns_ignores_an_accept_that_could_not_allocate(void)
{
    ctc_setup();
    Tube *t = ctc_tube("ctc-alloc-oom");
    uint before = count_tot_conns();
    fault_set(FAULT_CALLOC, 0, ENOMEM);

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(c == NULL && count_tot_conns() == before,
            "a refused allocation is not a connection: total %u became %u "
            "(make_conn returned %p)", before, count_tot_conns(), (void *)c);

    tube_dref(t);
}


// The relation between the two counters, not either value alone: while
// the live count oscillates, the total may never step backwards.
void
cttest_count_tot_conns_never_steps_back_while_the_live_count_oscillates(void)
{
    ctc_setup();
    Tube *t = ctc_tube("ctc-monotone");
    uint seen = count_tot_conns();
    uint regressions = 0;
    Conn *held = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(held, "setup: the held conn must allocate");

    for (int i = 0; i < 5; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        if (count_tot_conns() < seen)
            regressions++;
        seen = count_tot_conns();
        connclose(c);
        if (count_tot_conns() < seen)
            regressions++;
        seen = count_tot_conns();
    }

    assertf(regressions == 0,
            "the total accept count must never decrease: %u backward steps "
            "over five open/close pairs (total now %u, live %u)",
            regressions, count_tot_conns(), count_cur_conns());

    connclose(held);
    tube_dref(t);
}
