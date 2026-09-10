// Angry tests for conn_defer_free_begin (conn.c).
//
// This is a use-after-reuse guard, not a performance knob. While an epoll
// batch drains, ep_buf can still hold events whose data.ptr targets a
// conn that closed earlier in the same batch; pooling that struct lets
// make_conn hand the same memory to a new client before the stale event
// is dispatched. Every test here is about the window really being shut.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
cdb_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_defer_free_begin";
    prot_init();
    conn_pool_drain();
}

static Tube *
cdb_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
cdb_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}


// Nesting is counted, so an inner window closing must not open the door
// early: a conn closed at any depth stays frozen until the outermost end.
void
cttest_conn_defer_free_begin_holds_every_close_until_the_outermost_end(void)
{
    cdb_setup();
    Tube *t = cdb_tube("cdb-nested");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    conn_defer_free_begin();
    connclose(cs[0]);
    conn_defer_free_begin();
    connclose(cs[1]);
    conn_defer_free_begin();
    connclose(cs[2]);
    conn_defer_free_end();
    int after_first_end = cdb_pooled();
    conn_defer_free_end();
    int after_second_end = cdb_pooled();

    conn_defer_free_end();

    assertf(after_first_end == 0 && after_second_end == 0 && cdb_pooled() == 3,
            "only the outermost end may release: pool read %d, then %d, "
            "then %d", after_first_end, after_second_end, cdb_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// The whole point: a struct closed inside the window must not be handed
// to a new connection while the batch is still draining.
void
cttest_conn_defer_free_begin_keeps_a_parked_struct_out_of_new_connections(void)
{
    cdb_setup();
    Tube *t = cdb_tube("cdb-noreuse");
    Conn *victim = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(victim, "setup: the victim must allocate");
    conn_defer_free_begin();
    connclose(victim);

    Conn *fresh[3];
    int collisions = 0;
    for (int i = 0; i < 3; i++) {
        fresh[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(fresh[i], "setup: fresh conn %d must allocate", i);
        if (fresh[i] == victim)
            collisions++;
    }

    assertf(collisions == 0,
            "a conn parked mid-drain must not be handed out again: %d of 3 "
            "new conns landed on the parked struct", collisions);

    for (int i = 0; i < 3; i++)
        connclose(fresh[i]);
    conn_defer_free_end();
    conn_pool_drain();
    tube_dref(t);
}


// The struct stays frozen with the dead-fd marker so a stale event can be
// recognised, and the generation counter must not move until the struct
// is genuinely reused — that pairing is what makes a stale reference
// detectable at all.
void
cttest_conn_defer_free_begin_freezes_a_parked_struct_until_it_is_reused(void)
{
    cdb_setup();
    Tube *t = cdb_tube("cdb-frozen");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    uint64 gen = c->gen;
    conn_defer_free_begin();
    connclose(c);
    assertf(c->sock.fd == -1 && c->gen == gen,
            "setup: the parked struct must stay frozen during the batch");

    conn_defer_free_end();
    Conn *again = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(again == c && again->gen == gen + 1,
            "the generation must advance only when the struct is really "
            "reused: %" PRIu64 " became %" PRIu64 " (same struct: %d)",
            gen, again ? again->gen : 0, again == c);

    connclose(again);
    conn_pool_drain();
    tube_dref(t);
}


// The depth counter has no floor. A stray end — one more than there were
// begins — drives it negative, and the next begin only brings it back to
// zero, so the window that follows protects nothing: a conn closed inside
// it goes straight into the pool and can be recycled mid-drain.
void
cttest_conn_defer_free_begin_still_defers_after_an_unmatched_end(void)
{
    cdb_setup();
    Tube *t = cdb_tube("cdb-stray");
    conn_defer_free_end();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    assertf(cdb_pooled() == 0, "setup: the pool must start empty");
    conn_defer_free_begin();

    connclose(c);

    assertf(cdb_pooled() == 0,
            "a conn closed inside a window must be parked, not pooled: the "
            "pool reports %d", cdb_pooled());

    conn_defer_free_end();
    conn_pool_drain();
    tube_dref(t);
}
