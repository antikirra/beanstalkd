// Angry tests for conndeadlinesoon (conn.c).
//
// The predicate behind DEADLINE_SOON and behind the reserve gate that
// refuses to hand a worker a second job while its first is about to
// expire. It is a single comparison against a one-second margin, so the
// attack surface is the boundary itself and the empty-list guard.

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
cds_setup(void)
{
    fault_clear_all();
    progname = "testconn_conndeadlinesoon";
}

static void
cds_conn(Conn *c)
{
    memset(c, 0, sizeof *c);
    c->pending_timeout = -1;
    job_list_reset(&c->reserved_jobs);
}

static void
cds_reserve(Conn *c, Job *j, int64 deadline)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->r.deadline_at = deadline;
    j->r.state = Reserved;
    j->reserver = c;
    job_list_insert(&c->reserved_jobs, j);
    c->soonest_job = NULL;
}


// Exactly one second left. This is the instant conntickat schedules the
// wake-up for, so the predicate has to be true here or the tick fires,
// finds nothing to do and is rescheduled for the same instant.
void
cttest_conndeadlinesoon_says_yes_with_exactly_one_second_left(void)
{
    cds_setup();
    Conn c;
    Job j;
    cds_conn(&c);
    now = 600000000000LL;
    int64 margin = 1000000000LL;
    cds_reserve(&c, &j, now + margin);

    int soon = conndeadlinesoon(&c);

    assertf(soon != 0,
            "a deadline exactly one second out is already soon: clock "
            "%" PRId64 ", deadline %" PRId64 ", got %d",
            now, j.r.deadline_at, soon);
}


// One nanosecond further out. The other side of the same boundary: a
// margin that has crept wider hands out DEADLINE_SOON to workers whose
// job is not in danger yet.
void
cttest_conndeadlinesoon_says_no_one_nanosecond_outside_the_margin(void)
{
    cds_setup();
    Conn c;
    Job j;
    cds_conn(&c);
    now = 600000000000LL;
    int64 margin = 1000000000LL;
    cds_reserve(&c, &j, now + margin + 1);

    int soon = conndeadlinesoon(&c);

    assertf(soon == 0,
            "a deadline one nanosecond past the margin is not soon yet: "
            "clock %" PRId64 ", deadline %" PRId64 ", got %d",
            now, j.r.deadline_at, soon);
}


// A conn with nothing reserved. There is no deadline to compare against,
// and the answer must come back without reading one.
void
cttest_conndeadlinesoon_says_no_for_a_conn_holding_no_reservation(void)
{
    cds_setup();
    Conn c;
    cds_conn(&c);
    now = 600000000000LL;

    int soon = conndeadlinesoon(&c);

    assertf(soon == 0,
            "a conn with no reservation has no deadline approaching, got %d",
            soon);
}


// Already overdue. conn_timeout relies on this staying true past the
// deadline: the comparison is one-sided, not a window.
void
cttest_conndeadlinesoon_still_says_yes_after_the_deadline_has_passed(void)
{
    cds_setup();
    Conn c;
    Job j;
    cds_conn(&c);
    now = 600000000000LL;
    cds_reserve(&c, &j, now - 90000000000LL);

    int soon = conndeadlinesoon(&c);

    assertf(soon != 0,
            "a deadline %" PRId64 "ns in the past is still soon, got %d",
            now - j.r.deadline_at, soon);
}


// Two reservations where only the earlier one is inside the margin, and
// the later one was reserved last. The decision follows the minimum
// deadline, not the most recent reservation.
void
cttest_conndeadlinesoon_follows_the_earliest_of_several_reservations(void)
{
    cds_setup();
    Conn c;
    Job near, far;
    cds_conn(&c);
    now = 600000000000LL;
    cds_reserve(&c, &near, now + 500000000LL);
    cds_reserve(&c, &far, now + 3600000000000LL);

    int soon = conndeadlinesoon(&c);

    assertf(soon != 0,
            "the nearest reservation (%" PRId64 ") decides, not the last "
            "one taken (%" PRId64 "): got %d",
            near.r.deadline_at, far.r.deadline_at, soon);
}
