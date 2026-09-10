// Angry tests for connsetworker (conn.c).
//
// Same shape as connsetproducer, and deliberately kept as its own file
// and its own literals: the two counters are separate promises and a
// single shared test would let one of them rot behind the other.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
csw_setup(void)
{
    fault_clear_all();
    progname = "testconn_connsetworker";
    prot_init();
    conn_pool_drain();
}

static Tube *
csw_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// Every reserve command marks the conn. A worker in a reserve loop calls
// this thousands of times; only the first may count.
void
cttest_connsetworker_counts_a_conn_once_however_often_it_is_marked(void)
{
    csw_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    uint before = count_cur_workers();

    connsetworker(&c);
    connsetworker(&c);
    connsetworker(&c);

    assertf(count_cur_workers() == before + 1,
            "three marks on one conn are one worker: %u became %u",
            before, count_cur_workers());
}


// A conn that both puts and reserves belongs on both ledgers, and
// neither mark may clear the other's bit.
void
cttest_connsetworker_and_connsetproducer_both_claim_the_same_conn(void)
{
    csw_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    uint pbefore = count_cur_producers(), wbefore = count_cur_workers();

    connsetproducer(&c);
    connsetworker(&c);

    assertf(count_cur_producers() == pbefore + 1
            && count_cur_workers() == wbefore + 1
            && (c.type & CONN_TYPE_PRODUCER)
            && (c.type & CONN_TYPE_WORKER),
            "a conn that produces and reserves counts on both ledgers: "
            "producers %u->%u, workers %u->%u, type 0x%x",
            pbefore, count_cur_producers(), wbefore, count_cur_workers(),
            (unsigned)(unsigned char)c.type);
}


// The counter must come home after every connection, or `stats` reports
// workers that left hours ago.
void
cttest_connsetworker_returns_the_counter_to_its_baseline_every_cycle(void)
{
    csw_setup();
    Tube *t = csw_tube("csw-cycle");
    uint before = count_cur_workers();
    uint worst = before;

    for (int i = 0; i < 5; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        connsetworker(c);
        connclose(c);
        if (count_cur_workers() != before)
            worst = count_cur_workers();
    }

    assertf(worst == before,
            "an open/reserve/close cycle must be counter-neutral: baseline "
            "%u, a cycle ended at %u", before, worst);

    tube_dref(t);
}


// The macro at dat.h:592 is what the reserve path uses. It must reach
// the function the first time round.
void
cttest_connsetworker_macro_marks_a_conn_that_has_never_reserved(void)
{
    csw_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    uint before = count_cur_workers();

    CONNSETWORKER(&c);

    assertf(count_cur_workers() == before + 1
            && (c.type & CONN_TYPE_WORKER),
            "the macro must mark and count a first-time worker: counter "
            "%u became %u, type 0x%x", before, count_cur_workers(),
            (unsigned)(unsigned char)c.type);
}
