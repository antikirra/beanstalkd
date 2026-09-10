// Angry tests for connsoonestjob (conn.c).
//
// It answers one question — which reservation expires first — and every
// TTR decision in the server is built on the answer. The field it caches
// into is documented as pure memoization (dat.h:530), so the cache is not
// allowed to change the answer; these tests attack the answer, never the
// cache.

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
sj_setup(void)
{
    fault_clear_all();
    progname = "testconn_connsoonestjob";
}

static void
sj_conn(Conn *c)
{
    memset(c, 0, sizeof *c);
    c->pending_timeout = -1;
    job_list_reset(&c->reserved_jobs);
}

static void
sj_reserve(Conn *c, Job *j, int64 deadline)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->r.deadline_at = deadline;
    j->r.state = Reserved;
    j->reserver = c;
    job_list_insert(&c->reserved_jobs, j);
}


// A conn with nothing reserved. The list header is a Job-shaped
// sentinel, so a loop that stops on NULL instead of on the header
// hands the caller the header itself and every deadline read after
// that is garbage.
void
cttest_connsoonestjob_reports_nothing_for_a_conn_holding_no_reservation(void)
{
    sj_setup();
    Conn c;
    sj_conn(&c);

    Job *got = connsoonestjob(&c);

    assertf(got == NULL,
            "a conn with an empty reserved list has no soonest job, got %p "
            "(the list header is at %p)", (void *)got, (void *)&c.reserved_jobs);
}


// The answer must be a property of the set, not of the order the jobs
// were reserved in. Three conns, same three deadlines, three orders.
void
cttest_connsoonestjob_finds_the_same_minimum_whatever_the_reservation_order(void)
{
    sj_setup();
    int64 early = 1000, middle = 2000, late = 3000;
    Conn asc, desc, mixed;
    Job ja[3], jd[3], jm[3];
    sj_conn(&asc);
    sj_conn(&desc);
    sj_conn(&mixed);
    sj_reserve(&asc, &ja[0], early);
    sj_reserve(&asc, &ja[1], middle);
    sj_reserve(&asc, &ja[2], late);
    sj_reserve(&desc, &jd[0], late);
    sj_reserve(&desc, &jd[1], middle);
    sj_reserve(&desc, &jd[2], early);
    sj_reserve(&mixed, &jm[0], middle);
    sj_reserve(&mixed, &jm[1], early);
    sj_reserve(&mixed, &jm[2], late);

    Job *a = connsoonestjob(&asc);
    Job *d = connsoonestjob(&desc);
    Job *m = connsoonestjob(&mixed);

    assertf(a && d && m
            && a->r.deadline_at == early
            && d->r.deadline_at == early
            && m->r.deadline_at == early,
            "the earliest deadline is %" PRId64 " in every order, got "
            "%" PRId64 "/%" PRId64 "/%" PRId64,
            early, a ? a->r.deadline_at : -1, d ? d->r.deadline_at : -1,
            m ? m->r.deadline_at : -1);
}


// Deadlines are absolute nanosecond timestamps and nothing clamps them
// to a positive range. A comparison written as a subtraction overflows
// across the extremes of int64 and inverts the order silently.
void
cttest_connsoonestjob_orders_deadlines_at_the_extremes_of_the_signed_range(void)
{
    sj_setup();
    Conn c;
    Job low, high;
    sj_conn(&c);
    int64 lowest = INT64_MIN + 5, highest = INT64_MAX - 5;
    sj_reserve(&c, &high, highest);
    sj_reserve(&c, &low, lowest);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == lowest,
            "the minimum across the whole int64 range is %" PRId64 ", got "
            "%" PRId64, lowest, got ? got->r.deadline_at : 0);
}


// Two jobs share the minimum and a third, later one is reserved between
// them. Which of the tied pair wins is not promised; the deadline is.
void
cttest_connsoonestjob_reports_the_shared_value_when_two_deadlines_tie(void)
{
    sj_setup();
    Conn c;
    Job first, later, second;
    sj_conn(&c);
    int64 tied = 500000, behind = 900000;
    sj_reserve(&c, &first, tied);
    sj_reserve(&c, &later, behind);
    sj_reserve(&c, &second, tied);

    Job *got = connsoonestjob(&c);

    assertf(got && got->r.deadline_at == tied,
            "a tie at %" PRId64 " must not let the later deadline "
            "%" PRId64 " win, got %" PRId64,
            tied, behind, got ? got->r.deadline_at : 0);
}


// The function selects the earliest deadline; it does not filter out
// deadlines that have already gone by. conn_timeout's expiry loop is
// driven entirely by what comes back here, so a filtered-out overdue
// job never gets released.
void
cttest_connsoonestjob_still_returns_a_reservation_whose_deadline_has_passed(void)
{
    sj_setup();
    Conn c;
    Job overdue;
    sj_conn(&c);
    now = 1000000000;
    int64 gone = now - 60000000000LL;
    sj_reserve(&c, &overdue, gone);

    Job *got = connsoonestjob(&c);

    assertf(got == &overdue,
            "an overdue reservation is still the soonest one, got %p "
            "(want %p)", (void *)got, (void *)&overdue);
}


