// Angry tests for connsetproducer (conn.c).
//
// The whole function is an idempotence guard around a global counter that
// `stats` publishes as current-producers. Every attack here is aimed at
// the two ways that counter drifts: counting one conn twice, and losing
// the other type bits while setting this one.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
csp_setup(void)
{
    fault_clear_all();
    progname = "testconn_connsetproducer";
    prot_init();
    conn_pool_drain();
}

static Tube *
csp_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// A client that sends two puts calls this twice. Counting it twice
// inflates current-producers for the life of the process, because
// connclose only ever gives one back.
void
cttest_connsetproducer_counts_a_conn_once_however_often_it_is_marked(void)
{
    csp_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    uint before = count_cur_producers();

    connsetproducer(&c);
    connsetproducer(&c);
    connsetproducer(&c);

    assertf(count_cur_producers() == before + 1,
            "three marks on one conn are one producer: %u became %u",
            before, count_cur_producers());
}


// A worker that is also blocked in reserve puts a job. Both existing
// bits have to survive: losing WAITING strands the conn in every tube's
// waiting set with nothing that will ever remove it.
void
cttest_connsetproducer_leaves_the_worker_and_waiting_bits_standing(void)
{
    csp_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    c.type = CONN_TYPE_WORKER | CONN_TYPE_WAITING;

    connsetproducer(&c);

    assertf((c.type & (CONN_TYPE_WORKER | CONN_TYPE_WAITING))
            == (CONN_TYPE_WORKER | CONN_TYPE_WAITING),
            "marking a producer must only add a bit: type went from "
            "0x%x to 0x%x", CONN_TYPE_WORKER | CONN_TYPE_WAITING,
            (unsigned)(unsigned char)c.type);
}


// Open, produce, close, repeat. Any asymmetry between the increment here
// and the decrement in connclose shows up as a counter that walks away
// from its baseline one connection at a time.
void
cttest_connsetproducer_returns_the_counter_to_its_baseline_every_cycle(void)
{
    csp_setup();
    Tube *t = csp_tube("csp-cycle");
    uint before = count_cur_producers();
    uint worst = before;

    for (int i = 0; i < 5; i++) {
        Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(c, "setup: make_conn %d must succeed", i);
        connsetproducer(c);
        connclose(c);
        if (count_cur_producers() != before)
            worst = count_cur_producers();
    }

    assertf(worst == before,
            "an open/produce/close cycle must be counter-neutral: baseline "
            "%u, a cycle ended at %u", before, worst);

    tube_dref(t);
}


// The macro at dat.h:591 is the form every hot callsite actually uses.
// It must reach the function for a conn that is not a producer yet.
void
cttest_connsetproducer_macro_marks_a_conn_that_has_never_produced(void)
{
    csp_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    uint before = count_cur_producers();

    CONNSETPRODUCER(&c);

    assertf(count_cur_producers() == before + 1
            && (c.type & CONN_TYPE_PRODUCER),
            "the macro must mark and count a first-time producer: counter "
            "%u became %u, type 0x%x", before, count_cur_producers(),
            (unsigned)(unsigned char)c.type);
}
