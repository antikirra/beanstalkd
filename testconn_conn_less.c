// Angry tests for conn_less (conn.c).
//
// The tick heap's comparator. Everything the 4-ary heap does — siftup,
// siftdown, resift, remove — assumes a strict weak ordering, so a
// comparator that is reflexive, asymmetric-broken or that overflows on
// distant deadlines corrupts the heap silently: the root stops being the
// earliest wake-up and prottick parks past deadlines it holds.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
cls_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_less";
}


// The two ends of the signed range. A comparator written as a
// subtraction overflows here and reports the largest deadline as the
// earliest, which pins the heap root to a conn that must never wake.
void
cttest_conn_less_puts_the_lowest_tick_before_the_highest(void)
{
    cls_setup();
    Conn low, high;
    memset(&low, 0, sizeof low);
    memset(&high, 0, sizeof high);
    low.tickat = INT64_MIN;
    high.tickat = INT64_MAX;

    int r = conn_less(&low, &high);

    assertf(r != 0,
            "%" PRId64 " must sort before %" PRId64 ", got %d",
            low.tickat, high.tickat, r);
}


// The same pair the other way round. Both directions are needed: a
// comparator that always answers yes passes the first test alone.
void
cttest_conn_less_refuses_to_put_the_highest_tick_before_the_lowest(void)
{
    cls_setup();
    Conn low, high;
    memset(&low, 0, sizeof low);
    memset(&high, 0, sizeof high);
    low.tickat = INT64_MIN;
    high.tickat = INT64_MAX;

    int r = conn_less(&high, &low);

    assertf(r == 0,
            "%" PRId64 " must not sort before %" PRId64 ", got %d",
            high.tickat, low.tickat, r);
}


// Ticks are signed and conntickat can legitimately produce a negative
// one for an overdue reservation. Reinterpreting the comparison as
// unsigned sends every negative tick to the far end of the heap.
void
cttest_conn_less_puts_a_negative_tick_before_a_positive_one(void)
{
    cls_setup();
    Conn behind, ahead;
    memset(&behind, 0, sizeof behind);
    memset(&ahead, 0, sizeof ahead);
    behind.tickat = -5000000000LL;
    ahead.tickat = 5000000000LL;

    int r = conn_less(&behind, &ahead);

    assertf(r != 0,
            "%" PRId64 " must sort before %" PRId64 ", got %d",
            behind.tickat, ahead.tickat, r);
}


// Irreflexivity. heapresift compares an element against its own parent
// chain, and a comparator that says a conn precedes itself makes those
// walks non-terminating or order-destroying.
void
cttest_conn_less_never_puts_a_conn_before_itself(void)
{
    cls_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    c.tickat = 1234567890123LL;

    int r = conn_less(&c, &c);

    assertf(r == 0,
            "a conn cannot precede itself at tick %" PRId64 ", got %d",
            c.tickat, r);
}


// Asymmetry and transitivity across a triple, which is the property the
// heap's ordering argument actually rests on.
void
cttest_conn_less_keeps_a_strict_weak_ordering_across_a_triple(void)
{
    cls_setup();
    Conn x, y, z;
    memset(&x, 0, sizeof x);
    memset(&y, 0, sizeof y);
    memset(&z, 0, sizeof z);
    x.tickat = 100;
    y.tickat = 200;
    z.tickat = 300;

    int asymmetric = conn_less(&x, &y) && !conn_less(&y, &x)
                     && conn_less(&y, &z) && !conn_less(&z, &y);
    int transitive = conn_less(&x, &z) && !conn_less(&z, &x);

    assertf(asymmetric && transitive,
            "x<y<z must be asymmetric and transitive: asymmetry %d, "
            "transitivity %d", asymmetric, transitive);
}


// Only the tick participates. Two conns due at the same instant are
// interchangeable however different everything else about them is; a
// tie-break on another field is an ordering nobody promised and that
// changes as the heap moves elements around.
void
cttest_conn_less_looks_at_nothing_but_the_tick_time(void)
{
    cls_setup();
    Conn a, b;
    memset(&a, 0, sizeof a);
    memset(&b, 0, sizeof b);
    a.tickat = b.tickat = 987654321;
    a.tickpos = 0;
    b.tickpos = 99;
    a.gen = 1;
    b.gen = 500;
    a.state = STATE_WANT_COMMAND;
    b.state = STATE_SEND_JOB;
    a.type = CONN_TYPE_PRODUCER;
    b.type = CONN_TYPE_WORKER;

    int forward = conn_less(&a, &b);
    int backward = conn_less(&b, &a);

    assertf(forward == 0 && backward == 0,
            "two conns due at %" PRId64 " must be unordered whatever else "
            "differs: got %d forward, %d backward",
            a.tickat, forward, backward);
}
