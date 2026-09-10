// Angry tests for conn_set_soonestjob (conn.c, static inline).
//
// Reached through its only caller pair — conn_reserve_job writes the memo,
// connsoonestjob reads it. The single promise worth attacking is that the
// memo NEVER regresses to a later deadline: every TTR expiry the server
// performs is driven off it, so a memo that drifts forward loses a
// reservation until the conn happens to close.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
css_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_set_soonestjob";
    prot_init();
}

static void
css_conn(Conn *c)
{
    memset(c, 0, sizeof *c);
    c->pending_timeout = -1;
    job_list_reset(&c->reserved_jobs);
}

static void
css_job(Job *j, Tube *t, int64 ttr)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->tube = t;
    j->r.ttr = ttr;
}

// The independently computed minimum over everything the conn holds, so
// the memo is compared against a walk rather than against itself.
static int64
css_true_minimum(Conn *c)
{
    int64 best = INT64_MAX;
    for (Job *j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next)
        if (j->r.deadline_at < best)
            best = j->r.deadline_at;
    return best;
}


// A later reservation must not displace an earlier one. This is the
// direction that loses a TTR: the memo points at a deadline further out
// than one the conn actually holds, so the nearer job never expires.
void
cttest_conn_set_soonestjob_refuses_to_regress_to_a_later_deadline(void)
{
    css_setup();
    Tube *t = tube_find_or_make("css-regress");
    assertf(t, "setup: the tube must allocate");
    tube_iref(t);
    Conn c;
    Job near, far;
    css_conn(&c);
    now = 5000000000LL;
    int64 near_ttr = 1000000000LL, far_ttr = 90000000000LL;
    css_job(&near, t, near_ttr);
    css_job(&far, t, far_ttr);
    conn_reserve_job(&c, &near);
    conn_reserve_job(&c, &far);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == now + near_ttr,
            "the memo must stay on the earlier deadline %" PRId64 ", got "
            "%" PRId64, now + near_ttr, got ? got->r.deadline_at : 0);

    tube_dref(t);
}


// The opposite direction: a strictly earlier reservation must take over,
// or the conn wakes a full TTR too late for it.
void
cttest_conn_set_soonestjob_advances_to_a_strictly_earlier_deadline(void)
{
    css_setup();
    Tube *t = tube_find_or_make("css-advance");
    assertf(t, "setup: the tube must allocate");
    tube_iref(t);
    Conn c;
    Job far, near;
    css_conn(&c);
    now = 5000000000LL;
    int64 far_ttr = 90000000000LL, near_ttr = 1000000000LL;
    css_job(&far, t, far_ttr);
    css_job(&near, t, near_ttr);
    conn_reserve_job(&c, &far);
    conn_reserve_job(&c, &near);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == now + near_ttr,
            "the memo must move to the earlier deadline %" PRId64 ", got "
            "%" PRId64, now + near_ttr, got ? got->r.deadline_at : 0);

    tube_dref(t);
}


// Two reservations share the minimum and a later one is taken after
// them. Which of the tied pair the memo holds is not promised; that the
// reported deadline is the shared minimum is.
void
cttest_conn_set_soonestjob_reports_the_shared_deadline_when_two_reservations_tie(void)
{
    css_setup();
    Tube *t = tube_find_or_make("css-tie");
    assertf(t, "setup: the tube must allocate");
    tube_iref(t);
    Conn c;
    Job first, second, behind;
    css_conn(&c);
    now = 7000000000LL;
    int64 tied_ttr = 2000000000LL, behind_ttr = 40000000000LL;
    css_job(&first, t, tied_ttr);
    css_job(&second, t, tied_ttr);
    css_job(&behind, t, behind_ttr);
    conn_reserve_job(&c, &first);
    conn_reserve_job(&c, &second);
    conn_reserve_job(&c, &behind);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == now + tied_ttr,
            "a tie at %" PRId64 " must survive a later reservation at "
            "%" PRId64 ", got %" PRId64,
            now + tied_ttr, now + behind_ttr, got ? got->r.deadline_at : 0);

    tube_dref(t);
}


// The memo compared against a walk of the whole reserved list, with the
// minimum buried in the middle of the reservation order so neither
// "first wins" nor "last wins" can pass by accident.
void
cttest_conn_set_soonestjob_matches_an_independent_walk_of_every_reservation(void)
{
    css_setup();
    Tube *t = tube_find_or_make("css-walk");
    assertf(t, "setup: the tube must allocate");
    tube_iref(t);
    Conn c;
    Job js[5];
    int64 ttrs[5] = {40000000000LL, 12000000000LL, 3000000000LL,
                     60000000000LL, 25000000000LL};
    css_conn(&c);
    now = 11000000000LL;
    for (int i = 0; i < 5; i++) {
        css_job(&js[i], t, ttrs[i]);
        conn_reserve_job(&c, &js[i]);
    }
    int64 want = css_true_minimum(&c);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == want,
            "the memo must equal the minimum over every reservation "
            "(%" PRId64 "), got %" PRId64, want, got ? got->r.deadline_at : 0);

    tube_dref(t);
}
