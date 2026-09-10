// Angry tests for on_waiting_conn_remove (conn.c).
//
// Every tube's waiting_conns registers this as its onremove. ms_delete
// removes by swapping the tail into the hole, so exactly one conn changes
// position on every removal and exactly one cached hint must follow it.
// The failure mode is quiet by design — ms_remove_at degrades a stale hint
// to a linear scan — so these tests check the hint EXACTLY rather than
// checking that removal still happens to work.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
owr_setup(void)
{
    fault_clear_all();
    progname = "testconn_on_waiting_conn_remove";
    prot_init();
    conn_pool_drain();
}

static Tube *
owr_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

// The invariant this callback exists to preserve, checked exactly:
// for every waiting conn and every watched tube, the tube's waiting set
// must hold that conn at the cached index. Returns the index of the
// first watch slot that fails, or -1 when every hint resolves.
static int
owr_first_bad_hint(Conn *c)
{
    if (!conn_waiting(c))
        return -1;
    for (size_t i = 0; i < c->watch.len; i++) {
        Tube *t = c->watch.items[i];
        if (c->waitpos[i] >= t->waiting_conns.len)
            return (int)i;
        if (t->waiting_conns.items[c->waitpos[i]] != c)
            return (int)i;
    }
    return -1;
}


// The core promise: after a mid-set removal the conn that ms_delete
// swapped into the hole must find itself at the hole's index.
void
cttest_on_waiting_conn_remove_moves_the_hint_with_the_conn_it_relocated(void)
{
    owr_setup();
    Tube *t = owr_tube("owr-move");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *mid = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *last = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first && mid && last, "setup: three conns must allocate");
    assertf(enqueue_waiting_conn(first) && enqueue_waiting_conn(mid)
            && enqueue_waiting_conn(last), "setup: all three must wait");
    assertf(t->waiting_conns.len == 3, "setup: the tube must hold three waiters");

    remove_waiting_conn(first);

    assertf(owr_first_bad_hint(last) == -1,
            "the conn swapped into the hole must carry its hint along: "
            "hint %zu, tube holds %p there (conn is %p)",
            last->waitpos[0],
            (void *)(last->waitpos[0] < t->waiting_conns.len
                     ? t->waiting_conns.items[last->waitpos[0]] : NULL),
            (void *)last);

    remove_waiting_conn(mid);
    remove_waiting_conn(last);
    connclose(first);
    connclose(mid);
    connclose(last);
    tube_dref(t);
}


// Removing the tail moves nobody. ms_delete still leaves the removed
// item visible at a->items[a->len], so a guard that is off by one reads
// it and writes a hint for a conn that is no longer in the set.
void
cttest_on_waiting_conn_remove_writes_no_hint_when_the_tail_is_removed(void)
{
    owr_setup();
    Tube *t = owr_tube("owr-tail");
    Conn *a = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *b = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(a && b, "setup: two conns must allocate");
    assertf(enqueue_waiting_conn(a) && enqueue_waiting_conn(b),
            "setup: both must wait");
    assertf(t->waiting_conns.len == 2 && t->waiting_conns.items[1] == b,
            "setup: b must be the tail of the waiting set");
    size_t poison = 0xBEEF;
    b->waitpos[0] = poison;

    remove_waiting_conn(b);

    assertf(b->waitpos[0] == poison,
            "removing the tail moves no conn, so no hint may be written: "
            "the poison %zu became %zu", poison, b->waitpos[0]);

    remove_waiting_conn(a);
    connclose(a);
    connclose(b);
    tube_dref(t);
}


// A conn can sit in a waiting set with no hint array at all (it never
// went through enqueue_waiting_conn). The cap gate must suppress the
// write; the removal that triggered the callback must still complete.
void
cttest_on_waiting_conn_remove_completes_for_a_moved_conn_with_no_hint_array(void)
{
    owr_setup();
    Tube *t = owr_tube("owr-nohints");
    Conn *victim = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *bare = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(victim && bare, "setup: two conns must allocate");
    assertf(ms_append(&t->waiting_conns, victim), "setup: victim must enter the set");
    assertf(ms_append(&t->waiting_conns, bare), "setup: bare conn must enter the set");
    assertf(bare->waitpos == NULL && bare->waitpos_cap == 0,
            "setup: the bare conn must have no hint array");

    ms_remove_at(&t->waiting_conns, 0, victim);

    assertf(t->waiting_conns.len == 1 && t->waiting_conns.items[0] == bare,
            "the removal must still complete for a moved conn that never "
            "waited: set length %zu", t->waiting_conns.len);

    ms_remove_at(&t->waiting_conns, 0, bare);
    connclose(victim);
    connclose(bare);
    tube_dref(t);
}


// The moved conn may not watch this tube at all. Writing a hint anyway
// would corrupt the cached position it holds for a DIFFERENT tube.
void
cttest_on_waiting_conn_remove_leaves_a_moved_conn_that_ignores_this_tube_alone(void)
{
    owr_setup();
    Tube *home = owr_tube("owr-home");
    Tube *alien = owr_tube("owr-alien");
    Conn *victim = make_conn(dup(2), STATE_WANT_COMMAND, alien, alien);
    Conn *ahead = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    Conn *stranger = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(victim && ahead && stranger, "setup: three conns must allocate");
    assertf(enqueue_waiting_conn(ahead), "setup: the first waiter must enter home");
    assertf(enqueue_waiting_conn(stranger), "setup: the stranger must wait on home");
    /* The hint must differ from the index the callback would write, or a
       write that ignores the tube check would be indistinguishable. */
    size_t kept = stranger->waitpos[0];
    assertf(kept == 1, "setup: the stranger must be second in home, got %zu", kept);
    assertf(ms_append(&alien->waiting_conns, victim),
            "setup: victim must enter the alien set");
    assertf(ms_append(&alien->waiting_conns, stranger),
            "setup: stranger must be parked in the alien set too");

    ms_remove_at(&alien->waiting_conns, 0, victim);

    assertf(stranger->waitpos[0] == kept,
            "a moved conn that does not watch this tube must keep the hint "
            "it holds for the tube it does watch: %zu became %zu",
            kept, stranger->waitpos[0]);

    ms_remove_at(&alien->waiting_conns, 0, stranger);
    remove_waiting_conn(stranger);
    remove_waiting_conn(ahead);
    connclose(victim);
    connclose(ahead);
    connclose(stranger);
    tube_dref(home);
    tube_dref(alien);
}


// The invariant must hold after EVERY removal, not only at the end of a
// sequence: a hint that rots halfway through is repaired by chance on the
// next enqueue and the corruption window is invisible in a final check.
void
cttest_on_waiting_conn_remove_keeps_every_hint_exact_after_each_removal(void)
{
    owr_setup();
    enum { NCONN = 6 };
    Tube *ta = owr_tube("owr-churn-a");
    Tube *tb = owr_tube("owr-churn-b");
    Conn *cs[NCONN];
    for (int k = 0; k < NCONN; k++) {
        cs[k] = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
        assertf(cs[k], "setup: conn %d must allocate", k);
        if (k % 2 == 0)
            assertf(ms_append(&cs[k]->watch, tb), "setup: conn %d must watch b", k);
        assertf(enqueue_waiting_conn(cs[k]) == 1, "setup: conn %d must wait", k);
    }
    int order[NCONN] = {2, 5, 0, 4, 1, 3};
    int bad_conn = -1, bad_slot = -1, bad_step = -1;

    for (int step = 0; step < NCONN; step++) {
        remove_waiting_conn(cs[order[step]]);
        for (int k = 0; k < NCONN && bad_conn < 0; k++) {
            int slot = owr_first_bad_hint(cs[k]);
            if (slot >= 0) {
                bad_conn = k;
                bad_slot = slot;
                bad_step = step;
            }
        }
    }

    assertf(bad_conn < 0,
            "every surviving waiter must still be findable at its cached "
            "position after each removal: conn %d slot %d rotted at step %d",
            bad_conn, bad_slot, bad_step);

    for (int k = 0; k < NCONN; k++)
        connclose(cs[k]);
    tube_dref(ta);
    tube_dref(tb);
}
