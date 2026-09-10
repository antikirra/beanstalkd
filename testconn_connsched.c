// Angry tests for connsched (conn.c).
//
// It reconciles one conn's membership of the server's tick heap with the
// instant conntickat computed. Three branches, three distinct disasters:
// a missed resift leaves the heap unordered so prottick parks past a
// deadline; a missed removal wakes prottick forever on a conn with
// nothing to do; and a refused insert makes a conn's timers invisible
// unless the degraded flag is raised for conn_sched_recover.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
csd_setup(Server *s)
{
    fault_clear_all();
    progname = "testconn_connsched";
    prot_init();
    conn_pool_drain();
    memset(s, 0, sizeof *s);
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;
    s->sock.fd = -1;
    s->sock.added = 1;
    srv.idle_timeout = 0;
    now = 500000000000LL;
}

static Tube *
csd_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

// Index of the first conn whose membership flag and heap position
// disagree, or -1 when every conn in the array is consistent.
static int
csd_first_inconsistent(Server *s, Conn **cs, int n)
{
    for (int i = 0; i < n; i++) {
        Conn *c = cs[i];
        if (!c)
            continue;
        if (c->in_conns) {
            if (c->tickpos >= s->conns.len || s->conns.data[c->tickpos] != c)
                return i;
        } else {
            for (size_t k = 0; k < s->conns.len; k++)
                if (s->conns.data[k] == c)
                    return i;
        }
    }
    return -1;
}


// A conn whose wake-up moves earlier than everybody else's must end up
// at the root, or prottick sleeps past its deadline.
void
cttest_connsched_puts_the_earliest_wake_up_at_the_heap_root(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-root");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
        cs[i]->pending_timeout = 100 + i * 100;
        connsched(cs[i]);
        assertf(cs[i]->in_conns, "setup: conn %d must be scheduled", i);
    }
    cs[1]->pending_timeout = 1;

    connsched(cs[1]);

    assertf(s.conns.data[0] == cs[1],
            "the conn with the nearest wake-up must sit at the root: root "
            "tickat is %" PRId64 ", the moved conn's is %" PRId64,
            ((Conn *)s.conns.data[0])->tickat, cs[1]->tickat);

    for (int i = 0; i < 3; i++)
        connclose(cs[i]);
    free(s.conns.data);
    tube_dref(t);
}


// Called twice with nothing changed in between — which is what happens
// on every reply — it must not add the conn a second time. A duplicate
// entry means one of the two positions is stale and heapremove will
// unschedule the wrong connection.
void
cttest_connsched_keeps_one_entry_when_the_wake_up_did_not_move(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-idempotent");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 30;
    connsched(c);
    assertf(s.conns.len == 1, "setup: the conn must be scheduled once");
    size_t pos = c->tickpos;
    int64 tick = c->tickat;

    connsched(c);

    assertf(s.conns.len == 1 && c->tickpos == pos && c->tickat == tick,
            "rescheduling an unchanged conn must change nothing: heap holds "
            "%zu, position %zu (was %zu), tick %" PRId64 " (was %" PRId64 ")",
            s.conns.len, c->tickpos, pos, c->tickat, tick);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// When the last reason to wake disappears the conn must leave the heap.
// A conn left behind with an overdue tick wakes prottick on every pass
// for the rest of its life.
void
cttest_connsched_takes_out_a_conn_whose_last_timer_disappeared(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-drop");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
        cs[i]->pending_timeout = 20 + i * 20;
        connsched(cs[i]);
    }
    assertf(s.conns.len == 3, "setup: all three must be scheduled");
    cs[1]->pending_timeout = -1;

    connsched(cs[1]);

    assertf(!cs[1]->in_conns && s.conns.len == 2 && cs[1]->tickat == 0,
            "a conn with nothing left to wake for must leave the heap: "
            "in_conns %d, heap holds %zu, tickat %" PRId64,
            cs[1]->in_conns, s.conns.len, cs[1]->tickat);

    for (int i = 0; i < 3; i++)
        connclose(cs[i]);
    free(s.conns.data);
    tube_dref(t);
}


// Removal reshuffles the heap. Every conn still in it must remain
// findable at the position recorded for it, because that position is the
// only handle connclose and connsched have.
void
cttest_connsched_keeps_the_survivors_findable_at_their_recorded_positions(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-positions");
    enum { N = 6 };
    Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
        cs[i]->pending_timeout = (N - i) * 7;
        connsched(cs[i]);
    }
    assertf(s.conns.len == N, "setup: every conn must be scheduled");
    cs[2]->pending_timeout = -1;

    connsched(cs[2]);

    assertf(csd_first_inconsistent(&s, cs, N) < 0,
            "after a removal every conn's flag and position must still "
            "agree: conn %d disagrees (heap holds %zu)",
            csd_first_inconsistent(&s, cs, N), s.conns.len);

    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    free(s.conns.data);
    tube_dref(t);
}


// A refused insert makes the conn's timers invisible to prottick, and the
// heap cannot enumerate what is missing from it. The degraded flag is the
// only thing that gets the conn back, so recovery must actually find it.
void
cttest_connsched_flags_the_heap_so_recovery_can_retry_a_refused_insert(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-degraded");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 45;
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    connsched(c);
    assertf(!c->in_conns && fault_hits(FAULT_REALLOC) == 1,
            "setup: the insert must have been refused");
    conn_sched_recover();

    assertf(c->in_conns == 1 && s.conns.len == 1 && s.conns.data[0] == c,
            "a conn dropped by a refused insert must be recoverable: "
            "in_conns %d, heap holds %zu", c->in_conns, s.conns.len);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// The conns that are already in the heap must survive somebody else's
// failed insert untouched — a partial failure that corrupts the heap
// costs every scheduled connection, not just the victim.
void
cttest_connsched_leaves_the_scheduled_conns_intact_when_an_insert_is_refused(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-partial");
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
        cs[i]->pending_timeout = 15 + i * 15;
    }
    /* The heap grows on the first and third insert; kill the second growth. */
    fault_set(FAULT_REALLOC, 1, ENOMEM);
    connsched(cs[0]);
    connsched(cs[1]);
    connsched(cs[2]);
    assertf(fault_hits(FAULT_REALLOC) == 1, "setup: exactly one insert must fail");

    assertf(!cs[2]->in_conns && s.conns.len == 2
            && s.conns.data[cs[0]->tickpos] == cs[0]
            && s.conns.data[cs[1]->tickpos] == cs[1],
            "a refused insert must cost only its own conn: victim in_conns "
            "%d, heap holds %zu, positions %zu/%zu",
            cs[2]->in_conns, s.conns.len, cs[0]->tickpos, cs[1]->tickpos);

    for (int i = 0; i < 3; i++)
        connclose(cs[i]);
    free(s.conns.data);
    tube_dref(t);
}


// A struct that comes back from the slab pool must be scheduled as a new
// member. Trusting a stale membership flag would send connsched down the
// resift branch with a position that belongs to a connection that closed.
void
cttest_connsched_schedules_a_recycled_struct_as_a_fresh_member(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-recycle");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    first->srv = &s;
    first->pending_timeout = 25;
    connsched(first);
    assertf(first->in_conns, "setup: the first conn must be scheduled");
    connclose(first);
    assertf(s.conns.len == 0, "setup: closing must empty the heap");
    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");
    second->srv = &s;
    second->pending_timeout = 25;

    connsched(second);

    assertf(second->in_conns == 1 && s.conns.len == 1
            && s.conns.data[0] == second,
            "a recycled conn must enter the heap as a new member: in_conns "
            "%d, heap holds %zu", second->in_conns, s.conns.len);

    connclose(second);
    free(s.conns.data);
    tube_dref(t);
}


// The structural invariant, checked after every single step of a mixed
// sequence rather than at the end: a flag and a position that disagree
// even momentarily is a heapremove aimed at the wrong connection.
void
cttest_connsched_keeps_membership_and_position_in_step_through_a_sequence(void)
{
    Server s;
    csd_setup(&s);
    Tube *t = csd_tube("csd-sequence");
    enum { N = 5 };
    Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
    }
    int timeouts[10] = {30, 5, 90, 1, 60, -1, 12, -1, 3, 45};
    int bad_conn = -1, bad_step = -1;

    for (int step = 0; step < 10; step++) {
        Conn *c = cs[step % N];
        c->pending_timeout = timeouts[step];
        connsched(c);
        int bad = csd_first_inconsistent(&s, cs, N);
        if (bad >= 0 && bad_step < 0) {
            bad_conn = bad;
            bad_step = step;
        }
    }

    assertf(bad_step < 0,
            "membership and recorded position must agree after every "
            "reschedule: conn %d disagreed at step %d (heap holds %zu)",
            bad_conn, bad_step, s.conns.len);

    for (int i = 0; i < N; i++)
        connclose(cs[i]);
    free(s.conns.data);
    tube_dref(t);
}
