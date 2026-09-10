// Angry tests for count_cur_producers (conn.c).
//
// An unsigned counter with an increment guarded by a type bit and a
// decrement guarded by the same bit. Every attack below aims at making
// those two guards disagree, because the first disagreement in the
// downward direction wraps the value to ~4 billion in `stats`.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
ccp_setup(void)
{
    fault_clear_all();
    progname = "testconn_count_cur_producers";
    prot_init();
    conn_pool_drain();
}

static Tube *
ccp_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// Open, put, close, five times over. The counter must be exactly where
// it started after every single cycle, not merely at the end.
void
cttest_count_cur_producers_comes_home_after_every_produce_and_close(void)
{
    ccp_setup();
    Tube *t = ccp_tube("ccp-cycle");
    uint before = count_cur_producers();
    uint stray = before;

    for (int i = 0; i < 5; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        connsetproducer(c);
        connclose(c);
        if (count_cur_producers() != before)
            stray = count_cur_producers();
    }

    assertf(stray == before,
            "every produce/close cycle must be counter-neutral: baseline "
            "%u, a cycle left %u", before, stray);

    tube_dref(t);
}


// Most connections never put anything. Their close must not touch this
// counter at all — an unguarded decrement on a zero baseline wraps.
void
cttest_count_cur_producers_ignores_the_close_of_a_conn_that_never_produced(void)
{
    ccp_setup();
    Tube *t = ccp_tube("ccp-never");
    uint before = count_cur_producers();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    assertf(!(c->type & CONN_TYPE_PRODUCER), "setup: the conn must not be a producer");

    connclose(c);

    assertf(count_cur_producers() == before,
            "closing a conn that never produced must leave the counter at "
            "%u, got %u", before, count_cur_producers());

    tube_dref(t);
}


// A conn on both ledgers must give exactly one back to each. Reading the
// wrong bit here shows up as a producer count that tracks workers.
void
cttest_count_cur_producers_gives_back_exactly_one_for_a_conn_that_also_worked(void)
{
    ccp_setup();
    Tube *t = ccp_tube("ccp-both");
    uint pbefore = count_cur_producers(), wbefore = count_cur_workers();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    connsetproducer(c);
    connsetworker(c);
    assertf(count_cur_producers() == pbefore + 1
            && count_cur_workers() == wbefore + 1,
            "setup: both ledgers must claim the conn");

    connclose(c);

    assertf(count_cur_producers() == pbefore && count_cur_workers() == wbefore,
            "a conn on both ledgers gives one back to each: producers "
            "%u->%u, workers %u->%u",
            pbefore, count_cur_producers(), wbefore, count_cur_workers());

    tube_dref(t);
}


// The type bits live inside the range make_conn re-zeroes on a pool
// take. A struct that kept a stale PRODUCER bit would decrement the
// counter on a close for a connection that never put anything.
void
cttest_count_cur_producers_ignores_a_recycled_struct_that_never_produced(void)
{
    ccp_setup();
    Tube *t = ccp_tube("ccp-recycle");
    uint before = count_cur_producers();
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connsetproducer(first);
    connclose(first);
    assertf(count_cur_producers() == before, "setup: the first cycle must balance");
    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");
    assertf(!(second->type & CONN_TYPE_PRODUCER),
            "setup: a recycled conn must start with no type bits, got 0x%x",
            (unsigned)(unsigned char)second->type);

    connclose(second);

    assertf(count_cur_producers() == before,
            "a recycled struct that never produced must not give a producer "
            "back: baseline %u, got %u", before, count_cur_producers());

    tube_dref(t);
}
