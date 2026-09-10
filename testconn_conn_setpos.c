// Angry tests for conn_setpos (conn.c).
//
// The heap's position callback. It is the only thing that makes
// heapremove(&srv->conns, c->tickpos) remove THAT conn: a position that
// is stale, narrowed or not written at all removes somebody else's
// connection from the tick heap, and the victim's TTR is lost without a
// single visible error.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
csp2_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_setpos";
}

// Index of the first heap element whose recorded position does not match
// where it actually sits, or -1 when every element agrees.
static int
csp2_first_misplaced(Heap *h)
{
    for (size_t i = 0; i < h->len; i++)
        if (((Conn *)h->data[i])->tickpos != i)
            return (int)i;
    return -1;
}


// Positions are size_t. Narrowing the store to int or unsigned truncates
// a large index into one that belongs to a different element, and on a
// server with a big tick heap that is a removal aimed at the wrong conn.
void
cttest_conn_setpos_stores_the_largest_representable_index(void)
{
    csp2_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t want = SIZE_MAX;

    conn_setpos(&c, want);

    assertf(c.tickpos == want,
            "the position must be stored whole: want %zu, got %zu",
            want, c.tickpos);
}


// Position zero is the root, and it is the one value a "only store it if
// it is set" shortcut silently drops — leaving the root conn pointing at
// wherever it used to be.
void
cttest_conn_setpos_stores_the_root_index_over_a_stale_one(void)
{
    csp2_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    c.tickpos = 4242;

    conn_setpos(&c, 0);

    assertf(c.tickpos == 0,
            "moving a conn to the root must overwrite its old position, "
            "still reads %zu", c.tickpos);
}


// It writes one field. Anything else it touched would be a heap
// operation quietly rewriting a connection's state.
void
cttest_conn_setpos_touches_nothing_but_the_recorded_position(void)
{
    csp2_setup();
    Conn *c = malloc(sizeof *c);
    Conn *snapshot = malloc(sizeof *snapshot);
    assertf(c && snapshot, "setup: both conns must allocate");
    memset(c, 0x5A, sizeof *c);
    c->tickpos = 7;
    memcpy(snapshot, c, sizeof *c);

    conn_setpos(c, 19);

    snapshot->tickpos = 19;
    assertf(memcmp(c, snapshot, sizeof *c) == 0,
            "storing a position must change nothing else in the conn "
            "(position now %zu)", c->tickpos);

    free(c);
    free(snapshot);
}


// The invariant the heap contract rests on, checked after every single
// operation: an element that is not where it says it is turns the next
// heapremove into a removal of the wrong conn.
void
cttest_conn_setpos_keeps_every_element_at_the_index_it_records(void)
{
    csp2_setup();
    Heap h = {.less = conn_less, .setpos = conn_setpos};
    enum { N = 21 };
    static Conn cs[N];
    memset(cs, 0, sizeof cs);
    int bad_step = -1;

    for (int i = 0; i < N; i++) {
        cs[i].tickat = (int64)(N - i) * 1000;
        assertf(heapinsert(&h, &cs[i]), "setup: insert %d must succeed", i);
        if (csp2_first_misplaced(&h) >= 0 && bad_step < 0)
            bad_step = i;
    }
    for (int i = 0; i < N; i += 3) {
        cs[i].tickat = (int64)i;
        heapresift(&h, cs[i].tickpos);
        if (csp2_first_misplaced(&h) >= 0 && bad_step < 0)
            bad_step = 100 + i;
    }

    assertf(bad_step < 0,
            "every element must sit at the index it records: step code %d "
            "disagreed (heap holds %zu)", bad_step, h.len);

    free(h.data);
}


// The production use: a conn is removed from the middle of the heap by
// the position it recorded for itself, and the element swapped into its
// place must have had its own position rewritten.
void
cttest_conn_setpos_lets_a_middle_conn_be_removed_by_its_own_position(void)
{
    csp2_setup();
    Heap h = {.less = conn_less, .setpos = conn_setpos};
    enum { N = 21 };
    static Conn cs[N];
    memset(cs, 0, sizeof cs);
    for (int i = 0; i < N; i++) {
        cs[i].tickat = (int64)(N - i) * 1000;
        assertf(heapinsert(&h, &cs[i]), "setup: insert %d must succeed", i);
    }
    Conn *target = &cs[7];

    Conn *pulled = heapremove(&h, target->tickpos);

    assertf(pulled == target && csp2_first_misplaced(&h) < 0,
            "removing by the recorded position must unlink that conn and "
            "leave the rest consistent: pulled tick %" PRId64 ", wanted "
            "%" PRId64 ", first misplaced %d",
            pulled ? pulled->tickat : -1, target->tickat,
            csp2_first_misplaced(&h));

    free(h.data);
}
