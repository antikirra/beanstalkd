// Angry tests for on_watch_remove (conn.c, static).
//
// The watch set's onremove does three things at once: it gives back the
// tube reference and the watching count that on_watch_insert took, and it
// mirrors ms_delete's swap into the parallel waitpos array. The mirror is
// the fragile half — its failure mode is a hint that still points into a
// waiting set but at somebody ELSE, and ms_remove_at's stale-hint fallback
// hides it right up until a conn is removed from the wrong slot. These
// tests check the hint exactly rather than checking that removal "works".

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
owr2_setup(void)
{
    fault_clear_all();
    progname = "testconn_on_watch_remove";
    prot_init();
    conn_pool_drain();
}

static Tube *
owr2_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static Conn *
owr2_conn(Tube *t)
{
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    return c;
}

// The OP_IGNORE sequence from prot.c: the conn leaves this tube's
// waiting set and this watch slot, and stays waiting on the rest.
static void
owr2_ignore(Conn *c, size_t wi)
{
    Tube *t = c->watch.items[wi];
    t->stat.waiting_ct--;
    ms_remove_at(&t->waiting_conns, c->waitpos[wi], c);
    ms_remove_at(&c->watch, wi, t);
}

// Index of the first watch slot whose hint no longer resolves back to
// this conn, or -1 when every hint is exact.
static int
owr2_first_bad_hint(Conn *c)
{
    for (size_t i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        if (c->waitpos[i] >= t->waiting_conns.len)
            return (int)i;
        if (t->waiting_conns.items[c->waitpos[i]] != c)
            return (int)i;
    }
    return -1;
}


// Removing a middle watch entry swaps the last one into the hole. The
// hint that belonged to the moved tube has to move with it, or the conn
// unregisters itself from the wrong position when it stops waiting.
void
cttest_on_watch_remove_mirrors_the_swap_into_the_hint_array(void)
{
    owr2_setup();
    Tube *ta = owr2_tube("owr2-mid-a");
    Tube *tb = owr2_tube("owr2-mid-b");
    Tube *tc = owr2_tube("owr2-mid-c");
    Conn *filler = make_conn(dup(2), STATE_WANT_COMMAND, tc, tc);
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(filler && c, "setup: two conns must allocate");
    assertf(ms_append(&c->watch, tb) && ms_append(&c->watch, tc),
            "setup: the extra watches must succeed");
    assertf(enqueue_waiting_conn(filler) == 1, "setup: the filler must wait");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");
    assertf(c->waitpos[2] == 1, "setup: the conn must be second in tube c");

    owr2_ignore(c, 0);

    assertf(owr2_first_bad_hint(c) == -1,
            "the hint of the swapped watch entry must follow it into slot "
            "0: watch now holds %zu tubes, hints %zu/%zu",
            c->watch.len, c->waitpos[0], c->waitpos[1]);

    remove_waiting_conn(c);
    remove_waiting_conn(filler);
    connclose(c);
    connclose(filler);
    tube_dref(ta);
    tube_dref(tb);
    tube_dref(tc);
}


// The hint array can be shorter than the watch set — a conn that waited
// on three tubes and then watched a fourth. The mirror reads the hint of
// the entry that moved, so the guard is the only thing keeping that read
// inside the block the conn actually owns; one step looser and it copies
// whatever follows the array into a live hint.
void
cttest_on_watch_remove_keeps_the_mirror_inside_the_hint_array_the_conn_owns(void)
{
    owr2_setup();
    Tube *ta = owr2_tube("owr2-edge-a");
    Tube *tb = owr2_tube("owr2-edge-b");
    Tube *tc = owr2_tube("owr2-edge-c");
    Tube *td = owr2_tube("owr2-edge-d");
    Conn *filler = owr2_conn(ta);
    Conn *c = owr2_conn(ta);
    assertf(ms_append(&c->watch, tb) && ms_append(&c->watch, tc),
            "setup: the extra watches must succeed");
    size_t slack = 5, sentinel = 0x5EED;
    c->waitpos = malloc(slack * sizeof *c->waitpos);
    assertf(c->waitpos, "setup: the hint array must allocate");
    for (size_t i = 0; i < slack; i++)
        c->waitpos[i] = sentinel;
    c->waitpos_cap = 3;
    assertf(enqueue_waiting_conn(filler) == 1, "setup: the filler must wait");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");
    /* Watched after the hints were sized: the array can no longer cover
       the whole watch set, which is exactly what the gate is for. */
    assertf(ms_append(&c->watch, td), "setup: the fourth watch must succeed");
    assertf(c->watch.len == 4 && c->waitpos_cap == 3,
            "setup: the watch set must outgrow the hint array, %zu vs %zu",
            c->watch.len, c->waitpos_cap);
    size_t kept = c->waitpos[0];
    assertf(kept != sentinel, "setup: the surviving hint must be a real one");

    ms_remove_at(&c->watch, 0, ta);

    assertf(c->waitpos[0] == kept,
            "a swap the hint array cannot cover must not be mirrored from "
            "outside it: hint %zu became %zu (sentinel is %zu)",
            kept, c->waitpos[0], sentinel);

    /* Unwind by hand: the conn is registered in ta, tb and tc but its
       watch set no longer lists ta, so remove_waiting_conn would work
       from hints that no longer describe it. */
    c->type &= ~CONN_TYPE_WAITING;
    ms_remove(&ta->waiting_conns, c);
    ms_remove(&tb->waiting_conns, c);
    ms_remove(&tc->waiting_conns, c);
    remove_waiting_conn(filler);
    connclose(c);
    connclose(filler);
    tube_dref(ta);
    tube_dref(tb);
    tube_dref(tc);
    tube_dref(td);
}


// A conn that never waited has no hint array at all. The bounds gate must
// suppress the mirror without suppressing the reference and counter work,
// which is the only part that matters for such a conn.
void
cttest_on_watch_remove_still_unwinds_a_tube_for_a_conn_that_never_waited(void)
{
    owr2_setup();
    Tube *home = owr2_tube("owr2-bare-home");
    Tube *extra = owr2_tube("owr2-bare-extra");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, extra), "setup: the extra watch must succeed");
    assertf(c->waitpos == NULL && c->waitpos_cap == 0,
            "setup: a conn that never waited has no hint array");
    uint before = home->watching_ct;

    ms_remove_at(&c->watch, 0, home);

    assertf(home->watching_ct == before - 1,
            "the watching count must still be given back for a conn with "
            "no hints: %u became %u", before, home->watching_ct);

    connclose(c);
    tube_dref(home);
    tube_dref(extra);
}


// The hint array is exactly as long as the watch set here, which puts the
// mirror's read index right at the gate's boundary. A gate that is one
// too strict silently skips a mirror that was perfectly in bounds and
// leaves a rotten hint behind.
void
cttest_on_watch_remove_mirrors_the_swap_when_the_hint_array_is_watch_sized(void)
{
    owr2_setup();
    Tube *ta = owr2_tube("owr2-tight-a");
    Tube *tb = owr2_tube("owr2-tight-b");
    Tube *tc = owr2_tube("owr2-tight-c");
    Conn *filler = make_conn(dup(2), STATE_WANT_COMMAND, tc, tc);
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(filler && c, "setup: two conns must allocate");
    assertf(ms_append(&c->watch, tb) && ms_append(&c->watch, tc),
            "setup: the extra watches must succeed");
    c->waitpos = malloc(3 * sizeof *c->waitpos);
    assertf(c->waitpos, "setup: the tight hint array must allocate");
    c->waitpos_cap = 3;
    assertf(enqueue_waiting_conn(filler) == 1, "setup: the filler must wait");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");
    assertf(c->waitpos_cap == 3, "setup: the hint array must stay watch-sized");

    owr2_ignore(c, 0);

    assertf(owr2_first_bad_hint(c) == -1,
            "an exactly watch-sized hint array must still be mirrored: "
            "watch holds %zu tubes, hints %zu/%zu",
            c->watch.len, c->waitpos[0], c->waitpos[1]);

    remove_waiting_conn(c);
    remove_waiting_conn(filler);
    connclose(c);
    connclose(filler);
    tube_dref(ta);
    tube_dref(tb);
    tube_dref(tc);
}


// Every hint must be exact after EVERY removal. Dropping the watch
// entries one at a time walks the swap through every position, and a
// hint that only rots in the middle of the sequence is invisible to a
// check done at the end.
void
cttest_on_watch_remove_keeps_the_hints_exact_after_each_dropped_tube(void)
{
    owr2_setup();
    Tube *ts[4];
    const char *names[4] = {"owr2-seq-a", "owr2-seq-b", "owr2-seq-c", "owr2-seq-d"};
    for (int i = 0; i < 4; i++)
        ts[i] = owr2_tube(names[i]);
    Conn *filler = make_conn(dup(2), STATE_WANT_COMMAND, ts[3], ts[3]);
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ts[0], ts[0]);
    assertf(filler && c, "setup: two conns must allocate");
    for (int i = 1; i < 4; i++)
        assertf(ms_append(&c->watch, ts[i]), "setup: watch %d must succeed", i);
    assertf(enqueue_waiting_conn(filler) == 1, "setup: the filler must wait");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");
    int bad_slot = -1, bad_step = -1;

    for (int step = 0; step < 3; step++) {
        owr2_ignore(c, 0);
        int slot = owr2_first_bad_hint(c);
        if (slot >= 0 && bad_step < 0) {
            bad_slot = slot;
            bad_step = step;
        }
    }

    assertf(bad_step < 0,
            "every surviving watch entry must keep an exact hint after each "
            "removal: slot %d rotted at step %d", bad_slot, bad_step);

    remove_waiting_conn(c);
    remove_waiting_conn(filler);
    connclose(c);
    connclose(filler);
    for (int i = 0; i < 4; i++)
        tube_dref(ts[i]);
}


// A full connection lifetime with several watched tubes: whatever the
// insert callback took, the remove callback must give back, or a tube
// either leaks forever or is freed while somebody still points at it.
void
cttest_on_watch_remove_returns_every_count_and_reference_it_was_lent(void)
{
    owr2_setup();
    Tube *ta = owr2_tube("owr2-balance-a");
    Tube *tb = owr2_tube("owr2-balance-b");
    uint wa = ta->watching_ct, wb = tb->watching_ct;
    uint ra = ta->refs, rb = tb->refs;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, tb), "setup: the second watch must succeed");
    assertf(ta->watching_ct == wa + 1 && tb->watching_ct == wb + 1,
            "setup: both tubes must be watched");

    connclose(c);

    assertf(ta->watching_ct == wa && tb->watching_ct == wb
            && ta->refs == ra && tb->refs == rb,
            "closing must unwind every watch: a watching %u->%u refs "
            "%u->%u, b watching %u->%u refs %u->%u",
            wa, ta->watching_ct, ra, ta->refs,
            wb, tb->watching_ct, rb, tb->refs);

    tube_dref(ta);
    tube_dref(tb);
}


// The hint array and the watch set grow independently: the array is
// sized by enqueue_waiting_conn, the watch set by every `watch`. A conn
// that waited on N tubes and then watched one more has watch.len ==
// waitpos_cap + 1, so dropping a watch leaves the swap SOURCE index
// exactly at waitpos_cap — one past the end of the block. The bound
// has to be strict; reading that slot is an out-of-bounds load that
// only a sanitizer or a wrong hint would ever reveal.
void
cttest_on_watch_remove_never_reads_past_the_hint_array(void)
{
    owr2_setup();

    enum { NWAIT = 8 };            // conn_waitpos_reserve's first size
    char name[32];
    Tube *first = owr2_tube("owr-edge-0");
    Conn *c = owr2_conn(first);

    for (int i = 1; i < NWAIT; i++) {
        snprintf(name, sizeof name, "owr-edge-%d", i);
        assertf(ms_append(&c->watch, owr2_tube(name)),
                "setup: watch tube %d", i);
    }
    assertf(c->watch.len == NWAIT,
            "setup: %zu tubes watched, want %d", c->watch.len, NWAIT);

    assertf(enqueue_waiting_conn(c), "setup: the conn must be able to wait");
    assertf(c->waitpos_cap == NWAIT,
            "setup: the hint array must be exactly %d long, got %zu",
            NWAIT, c->waitpos_cap);

    // One more watch: the watch set now outgrows the hint array.
    assertf(ms_append(&c->watch, owr2_tube("owr-edge-extra")),
            "setup: the extra watch");
    assertf(c->watch.len == NWAIT + 1,
            "setup: %zu tubes watched after the extra one", c->watch.len);

    // The swap source is now index NWAIT == waitpos_cap.
    Tube *dropped = c->watch.items[0];
    ms_remove_at(&c->watch, 0, dropped);

    assertf(c->watch.len == NWAIT,
            "the watch set must have shrunk by one, got %zu", c->watch.len);
    assertf(c->waitpos_cap == NWAIT,
            "the hint array must not have been resized behind our back, "
            "got %zu", c->waitpos_cap);

    // The hint that moved into slot 0 belongs to a tube this conn never
    // waited on, so its VALUE means nothing and the test does not judge
    // it. What must hold is that the copy read from inside the block:
    // the load at waitpos[waitpos_cap] is out of bounds, which the ASan
    // gate in Dockerfile.build turns into a failure. Everything the
    // conn still waits on must remain findable, which is what makes the
    // teardown below meaningful rather than a formality.
    for (size_t i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        if (t == dropped)
            continue;
        assertf(ms_contains(&t->waiting_conns, c) || t->waiting_conns.len == 0,
                "tube %s must either still hold this conn in its waiting "
                "set or hold nobody", t->name);
    }

    remove_waiting_conn(c);
    connclose(c);
    conn_pool_drain();
}
