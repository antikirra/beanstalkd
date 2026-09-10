// Angry tests for conntickat (conn.c, static inline).
//
// Reached through connsched, which stores the value it computes into
// c->tickat and puts the conn in (or takes it out of) the server's tick
// heap. Two things make this function dangerous: a tick that is too LATE
// silently loses a TTR, and a tick that is too EARLY costs nothing on the
// reservation branches but produces an unbounded busy loop on the idle
// branch, because conn_timeout refuses to act on a tick its own five-term
// gate does not agree with. Every idle test below is written against
// conn_timeout's gate (prot.c:3053-3065), never against this code.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
tka_setup(Server *s)
{
    fault_clear_all();
    progname = "testconn_conntickat";
    prot_init();
    memset(s, 0, sizeof *s);
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;
    s->sock.fd = -1;
    s->sock.added = 1;
    srv.idle_timeout = 0;
}

static void
tka_conn(Conn *c, Server *s)
{
    memset(c, 0, sizeof *c);
    c->srv = s;
    c->state = STATE_WANT_COMMAND;
    c->pending_timeout = -1;
    job_list_reset(&c->reserved_jobs);
}

static void
tka_reserve(Conn *c, Job *j, int64 deadline)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->r.deadline_at = deadline;
    j->r.state = Reserved;
    j->reserver = c;
    job_list_insert(&c->reserved_jobs, j);
    c->soonest_job = NULL;
}

// The scheduled instant, or 0 when the conn was found to have nothing to
// wake for. Reading tickat while out of the heap is meaningless (dat.h:527),
// so membership is what distinguishes "no tick" from "tick at 0".
static int64
tka_tick(Conn *c)
{
    connsched(c);
    return c->in_conns ? c->tickat : 0;
}


// A waiting worker holding a reservation must be woken one safety margin
// before the deadline, so DEADLINE_SOON can still reach the client while
// the job is legally theirs.
void
cttest_conntickat_wakes_a_waiting_conn_one_margin_before_its_deadline(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 700000000000LL;
    srv.idle_timeout = 30000000000LL;
    int64 margin = 1000000000LL, deadline = now + 30000000000LL;
    c.type |= CONN_TYPE_WAITING;
    tka_reserve(&c, &j, deadline);

    int64 tick = tka_tick(&c);

    assertf(tick == deadline - margin,
            "a waiting conn must wake one margin early: want %" PRId64 ", "
            "got %" PRId64 " (deadline %" PRId64 ")",
            deadline - margin, tick, deadline);

    free(s.conns.data);
}


// The same conn without the waiting bit gets no margin: the margin exists
// for the reply the client is blocked on, not for the reservation.
void
cttest_conntickat_wakes_a_conn_that_is_not_waiting_at_its_deadline(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 700000000000LL;
    srv.idle_timeout = 30000000000LL;
    int64 deadline = now + 30000000000LL;
    tka_reserve(&c, &j, deadline);

    int64 tick = tka_tick(&c);

    assertf(tick == deadline,
            "a conn that is not waiting gets no margin: want %" PRId64 ", "
            "got %" PRId64, deadline, tick);

    free(s.conns.data);
}


// Two candidate wake-ups, the pending one at zero seconds. The tick is
// the earlier of the two, not whichever clause was evaluated last.
void
cttest_conntickat_takes_a_zero_second_timeout_over_a_distant_reservation(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 900000000000LL;
    tka_reserve(&c, &j, now + 3600000000000LL);
    c.pending_timeout = 0;

    int64 tick = tka_tick(&c);

    assertf(tick == now,
            "a zero-second reserve timeout is due immediately: want "
            "%" PRId64 ", got %" PRId64, now, tick);

    free(s.conns.data);
}


// The mirror case: the reservation is the earlier of the two and must not
// be overwritten by the pending timeout computed after it.
void
cttest_conntickat_takes_a_near_reservation_over_a_distant_timeout(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 900000000000LL;
    int64 deadline = now + 2000000000LL;
    tka_reserve(&c, &j, deadline);
    c.pending_timeout = 3600;

    int64 tick = tka_tick(&c);

    assertf(tick == deadline,
            "the nearer reservation must win: want %" PRId64 ", got "
            "%" PRId64, deadline, tick);

    free(s.conns.data);
}


// pending_timeout is an int and the scale factor is a billion. Without a
// widening cast the product overflows for any timeout past ~2 seconds'
// worth of int range, and the conn is scheduled at a wildly wrong instant.
void
cttest_conntickat_scales_a_timeout_of_int_max_seconds_without_overflow(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 1000000000000LL;
    c.pending_timeout = INT_MAX;
    int64 want = now + (int64)INT_MAX * 1000000000LL;

    int64 tick = tka_tick(&c);

    assertf(tick == want,
            "a timeout of %d seconds must scale in 64 bits: want %" PRId64
            ", got %" PRId64, INT_MAX, want, tick);

    free(s.conns.data);
}


// The -I clause. The deadline is measured from the last completed
// command, not from now, or a client that never speaks again is reaped a
// full idle period late on every tick.
void
cttest_conntickat_schedules_an_idle_conn_at_its_inactivity_deadline(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 30000000000LL;
    c.last_activity_at = now - 5000000000LL;
    int64 want = c.last_activity_at + srv.idle_timeout;

    int64 tick = tka_tick(&c);

    assertf(tick == want,
            "the idle deadline runs from the last command: want %" PRId64
            ", got %" PRId64, want, tick);

    free(s.conns.data);
}


// conn_timeout refuses to close a conn that is not in STATE_WANT_COMMAND.
// Scheduling one anyway is the busy loop the comment in conn.c warns
// about: the tick fires, nothing happens, the same tick is rescheduled.
void
cttest_conntickat_leaves_a_conn_mid_command_out_of_the_idle_clause(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 30000000000LL;
    c.last_activity_at = now - 5000000000LL;
    c.state = STATE_WANT_DATA;

    int64 tick = tka_tick(&c);

    assertf(tick == 0,
            "a conn in the middle of a command is not idle: it must not be "
            "scheduled, got %" PRId64, tick);

    free(s.conns.data);
}


// A worker blocked in reserve is not idle. conn_timeout excludes it, so
// conntickat must too.
void
cttest_conntickat_leaves_a_waiting_conn_out_of_the_idle_clause(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 30000000000LL;
    c.last_activity_at = now - 5000000000LL;
    c.type |= CONN_TYPE_WAITING;

    int64 tick = tka_tick(&c);

    assertf(tick == 0,
            "a conn parked in a waiting set is not idle: it must not be "
            "scheduled, got %" PRId64, tick);

    free(s.conns.data);
}


// -I off means no idle wake-ups at all. A conn otherwise fully eligible
// must stay out of the heap.
void
cttest_conntickat_adds_no_wake_up_when_the_idle_timeout_is_disabled(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 0;
    c.last_activity_at = now - 100000000000LL;

    int64 tick = tka_tick(&c);

    assertf(tick == 0,
            "with -I off an otherwise idle conn must not be scheduled, got "
            "%" PRId64, tick);

    free(s.conns.data);
}


// A conn holding a reservation is excluded from the idle clause even
// when its inactivity deadline is the nearer of the two. Including it
// would schedule a tick conn_timeout declines to act on.
void
cttest_conntickat_drops_the_idle_deadline_once_the_conn_holds_a_reservation(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 4000000000LL;
    c.last_activity_at = now - 3000000000LL; /* idle due in 1s */
    int64 deadline = now + 20000000000LL;    /* reservation due in 20s */
    tka_reserve(&c, &j, deadline);

    int64 tick = tka_tick(&c);

    assertf(tick == deadline,
            "a conn holding a job is never idle, so only the reservation "
            "may set the tick: want %" PRId64 ", got %" PRId64,
            deadline, tick);

    free(s.conns.data);
}


// Same exclusion for a conn with an outstanding reserve-with-timeout,
// again with the idle deadline deliberately the nearer one.
void
cttest_conntickat_drops_the_idle_deadline_while_a_reserve_timeout_is_pending(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;
    srv.idle_timeout = 4000000000LL;
    c.last_activity_at = now - 3000000000LL; /* idle due in 1s */
    c.pending_timeout = 20;                  /* timeout due in 20s */
    int64 want = now + 20000000000LL;

    int64 tick = tka_tick(&c);

    assertf(tick == want,
            "a conn awaiting a reserve timeout is not idle: want %" PRId64
            ", got %" PRId64, want, tick);

    free(s.conns.data);
}


// A conn with no reservation, no timeout and no idle deadline has
// nothing to wake for and must not occupy the tick heap.
void
cttest_conntickat_leaves_a_conn_with_nothing_to_do_out_of_the_heap(void)
{
    Server s;
    Conn c;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 400000000000LL;

    int64 tick = tka_tick(&c);

    assertf(tick == 0 && s.conns.len == 0,
            "an idle-free conn with no timers must stay out of the heap: "
            "tick %" PRId64 ", heap holds %zu", tick, s.conns.len);

    free(s.conns.data);
}


// An overdue reservation still has work to do — conn_timeout's expiry
// loop is what releases it. Reporting "nothing to wake for" strands the
// job in the Reserved state until the conn closes.
void
cttest_conntickat_still_schedules_a_reservation_that_is_already_overdue(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 900000000000LL;
    int64 deadline = now - 60000000000LL;
    tka_reserve(&c, &j, deadline);

    int64 tick = tka_tick(&c);

    assertf(tick == deadline,
            "an overdue reservation must be scheduled immediately, not "
            "dropped: want %" PRId64 ", got %" PRId64 " (in heap: %d)",
            deadline, tick, c.in_conns);

    free(s.conns.data);
}


// The returned timestamp is the only signal connsched has. A conn whose
// margin-adjusted instant happens to land on zero has a live TTR and
// still gets reported as "nothing to wake for", which evicts it from the
// heap and loses the reservation for good.
void
cttest_conntickat_keeps_a_conn_with_a_live_reservation_in_the_tick_heap(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 0;
    int64 margin = 1000000000LL;
    c.type |= CONN_TYPE_WAITING;
    tka_reserve(&c, &j, margin);

    connsched(&c);

    assertf(c.in_conns == 1,
            "a conn holding a reservation due at %" PRId64 " must stay "
            "scheduled: in_conns %d, heap holds %zu",
            margin, c.in_conns, s.conns.len);

    free(s.conns.data);
}


// The wake-up must be the instant conndeadlinesoon starts saying yes.
// Any other margin makes conn_timeout fire on a condition that is not
// yet true and reschedule the same instant forever.
void
cttest_conntickat_wakes_a_waiting_conn_exactly_when_its_deadline_turns_soon(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 800000000000LL;
    int64 deadline = now + 45000000000LL;
    c.type |= CONN_TYPE_WAITING;
    tka_reserve(&c, &j, deadline);
    int64 tick = tka_tick(&c);
    assertf(tick > now, "setup: the wake-up must be in the future");

    now = tick;

    assertf(conndeadlinesoon(&c) != 0,
            "at the scheduled instant %" PRId64 " the deadline %" PRId64
            " must already count as soon", tick, deadline);

    free(s.conns.data);
}


// Evaluated twice with the clock frozen it must give the same answer: a
// tick derived from a fresh clock reading drifts on every event and the
// conn is never woken at the instant it was scheduled for.
void
cttest_conntickat_gives_the_same_instant_twice_under_a_frozen_clock(void)
{
    Server s;
    Conn c;
    Job j;
    tka_setup(&s);
    tka_conn(&c, &s);
    now = 600000000000LL;
    tka_reserve(&c, &j, now + 15000000000LL);
    int64 first = tka_tick(&c);
    assertf(first != 0, "setup: the conn must be scheduled");

    int64 second = tka_tick(&c);

    assertf(second == first,
            "with the clock frozen the tick must not move: %" PRId64
            " became %" PRId64, first, second);

    free(s.conns.data);
}
