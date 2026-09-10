// Angry tests for conn_waitpos_reserve (conn.c).
//
// This is the only allocator on the waiting-set fast path. Its two
// promises pull in opposite directions: on success the array must be
// able to mirror n watch entries, and on failure NOTHING may move —
// enqueue_waiting_conn's OOM rollback reads the hints that are already
// in the old block, so a pointer or a capacity that changed before the
// bail-out turns a survivable OOM into a NULL write or an out-of-range
// waiting-set removal.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
wr_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_waitpos_reserve";
}

// Hints are seeded with a value derived from their index so a lost,
// shifted or re-zeroed slot is distinguishable from a surviving one.
static void
wr_seed(Conn *c, size_t n, size_t base)
{
    for (size_t i = 0; i < n; i++)
        c->waitpos[i] = base + i;
}

// Index of the first hint that no longer reads back what wr_seed put
// there, or n when every slot survived.
static size_t
wr_first_rotted(Conn *c, size_t n, size_t base)
{
    for (size_t i = 0; i < n; i++)
        if (c->waitpos[i] != base + i)
            return i;
    return n;
}


// Growth must carry the hints that are already there: enqueue_waiting_conn
// writes waitpos[i] right after each ms_append and its rollback re-reads
// the earlier entries, so a grow that starts from a blank block unregisters
// the wrong conns from the wrong tubes.
void
cttest_conn_waitpos_reserve_keeps_every_earlier_hint_when_it_grows(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t seeded = 8, base = 4000;
    assertf(conn_waitpos_reserve(&c, seeded) == 1,
            "setup: the first reserve must succeed");
    wr_seed(&c, seeded, base);
    assertf(conn_waitpos_reserve(&c, seeded + 1) == 1,
            "setup: the growing reserve must succeed");

    size_t rotted = wr_first_rotted(&c, seeded, base);

    assertf(rotted == seeded,
            "growth must carry every hint across: slot %zu reads %zu, want %zu",
            rotted, rotted < seeded ? c.waitpos[rotted] : (size_t)0,
            base + rotted);

    free(c.waitpos);
}


// Doubling from an empty array reaches 8. A conn that watches 100 tubes
// asks for 100 in one step, and the promise is coverage, not doubling:
// an array that stops at 8 is written past by enqueue_waiting_conn.
void
cttest_conn_waitpos_reserve_covers_a_first_request_larger_than_its_default_block(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t want = 100;
    assertf(conn_waitpos_reserve(&c, want) == 1,
            "setup: the reserve must succeed");

    assertf(c.waitpos_cap >= want,
            "a reserve for %zu hints must leave room for all of them, "
            "capacity is %zu", want, c.waitpos_cap);

    free(c.waitpos);
}


// The very first slot on a conn that has never waited: capacity 0 is the
// one state where "n <= cap" and "n slots are available" disagree.
void
cttest_conn_waitpos_reserve_serves_a_single_slot_on_a_conn_that_never_waited(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t want = 1;

    assertf(conn_waitpos_reserve(&c, want) == 1,
            "setup: the reserve must succeed");

    assertf(c.waitpos_cap >= want,
            "a conn with no hint array asking for %zu slot must get it, "
            "capacity is %zu", want, c.waitpos_cap);

    free(c.waitpos);
}


// n == cap is the boundary between the no-op branch and the growth
// branch. The contract calls it a success no-op, so the capacity the
// conn already holds must come back unchanged.
void
cttest_conn_waitpos_reserve_leaves_the_capacity_alone_at_exactly_the_current_size(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    assertf(conn_waitpos_reserve(&c, 8) == 1, "setup: the seed reserve must succeed");
    size_t held = c.waitpos_cap;
    assertf(conn_waitpos_reserve(&c, held) == 1,
            "setup: a request for the capacity already held must succeed");

    assertf(c.waitpos_cap == held,
            "a request for the %zu slots already held must not resize the "
            "array, capacity moved to %zu", held, c.waitpos_cap);

    free(c.waitpos);
}


// The allocator says no. Returning 1 here makes enqueue_waiting_conn
// believe it may write watch.len hints into a shorter array.
void
cttest_conn_waitpos_reserve_reports_failure_when_the_allocator_refuses(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    assertf(conn_waitpos_reserve(&c, 8) == 1, "setup: the seed reserve must succeed");
    size_t held = c.waitpos_cap;
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    int r = conn_waitpos_reserve(&c, held + 1);

    assertf(r == 0,
            "an allocator that cannot provide %zu hints must be reported as "
            "failure, got %d with capacity %zu", held + 1, r, c.waitpos_cap);

    free(c.waitpos);
}


// Failure postcondition: the capacity must still describe the block the
// conn actually owns. A capacity written before the NULL check makes
// every later reserve a silent no-op over a too-small array.
void
cttest_conn_waitpos_reserve_keeps_the_old_capacity_when_the_allocator_refuses(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    assertf(conn_waitpos_reserve(&c, 8) == 1, "setup: the seed reserve must succeed");
    size_t held = c.waitpos_cap;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    conn_waitpos_reserve(&c, held + 1);
    assertf(fault_hits(FAULT_REALLOC) == 1, "setup: the fault must have fired");

    assertf(c.waitpos_cap == held,
            "a refused growth must leave the capacity describing the block "
            "the conn still owns: %zu became %zu", held, c.waitpos_cap);

    free(c.waitpos);
}


// Failure postcondition: the hints already written must still be
// readable. enqueue_waiting_conn's rollback loop walks waitpos[0..i)
// to unregister the tubes it already appended to.
void
cttest_conn_waitpos_reserve_keeps_the_old_hints_readable_when_the_allocator_refuses(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t seeded = 8, base = 7100;
    assertf(conn_waitpos_reserve(&c, seeded) == 1,
            "setup: the seed reserve must succeed");
    wr_seed(&c, seeded, base);
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    conn_waitpos_reserve(&c, seeded + 1);
    assertf(fault_hits(FAULT_REALLOC) == 1, "setup: the fault must have fired");

    size_t rotted = wr_first_rotted(&c, seeded, base);

    assertf(rotted == seeded,
            "a refused growth must leave every hint the rollback path reads "
            "intact: slot %zu reads %zu, want %zu",
            rotted, rotted < seeded ? c.waitpos[rotted] : (size_t)0,
            base + rotted);

    free(c.waitpos);
}


// The byte size is the only bound on the allocation, and it is a plain
// multiply. ms.c's grow() refuses a count whose byte size cannot be
// represented; this one has no such guard, so a count just past
// SIZE_MAX/sizeof(size_t) wraps the product down to a single slot while
// the recorded capacity stays astronomically large.
void
cttest_conn_waitpos_reserve_refuses_a_slot_count_whose_byte_size_overflows(void)
{
    wr_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    size_t want = SIZE_MAX / sizeof(size_t) + 2; /* want * sizeof(size_t) wraps to 8 */

    int r = conn_waitpos_reserve(&c, want);

    assertf(r == 0,
            "a slot count whose byte size wraps must be refused, not "
            "reported as %zu usable hints: returned %d with capacity %zu",
            want, r, c.waitpos_cap);

    free(c.waitpos);
}


// The whole reason the failure path must not move anything: the conn
// stays alive and enqueue_waiting_conn is retried (OP_WATCH re-runs it
// on the same conn). Every hint it then writes must resolve back to
// this conn in the tube it belongs to.
void
cttest_conn_waitpos_reserve_leaves_a_conn_able_to_wait_after_a_refused_growth(void)
{
    fault_clear_all();
    progname = "testconn_conn_waitpos_reserve";
    prot_init();
    conn_pool_drain();

    Tube *ta = tube_find_or_make("wpr-retry-a");
    Tube *tb = tube_find_or_make("wpr-retry-b");
    assertf(ta && tb, "setup: both tubes must allocate");
    tube_iref(ta);
    tube_iref(tb);
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, tb), "setup: the second watch must succeed");
    assertf(conn_waitpos_reserve(c, 8) == 1, "setup: the seed reserve must succeed");
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    assertf(conn_waitpos_reserve(c, c->waitpos_cap + 1) == 0,
            "setup: the injected growth failure must be reported");
    fault_clear_all();

    int enqueued = enqueue_waiting_conn(c);

    assertf(enqueued == 1
            && ta->waiting_conns.items[c->waitpos[0]] == c
            && tb->waiting_conns.items[c->waitpos[1]] == c,
            "after a refused growth the conn must still be able to wait "
            "with hints that resolve back to it: enqueue returned %d, "
            "hints %zu/%zu", enqueued, c->waitpos[0], c->waitpos[1]);

    remove_waiting_conn(c);
    connclose(c);
    tube_dref(ta);
    tube_dref(tb);
}
