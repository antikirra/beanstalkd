// Angry tests for conn_reserve_job (conn.c).
//
// One call performs the whole reservation: the deadline the TTR machinery
// will fire on, the two counters the `stats` command reports, the list
// membership that decides who owns the job, and the clearing of the
// pending reserve timeout. Each of those is attacked separately here, so
// a red test names the promise that broke rather than "reserve is wrong".

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
crj_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_reserve_job";
    prot_init();
    conn_pool_drain();
}

static Tube *
crj_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static void
crj_conn(Conn *c)
{
    memset(c, 0, sizeof *c);
    c->pending_timeout = -1;
    job_list_reset(&c->reserved_jobs);
}

static void
crj_job(Job *j, Tube *t, int64 ttr)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->tube = t;
    j->r.ttr = ttr;
    j->r.pri = 100;
    j->r.state = Ready;
}


// The deadline is the whole TTR contract. It must come from the cached
// clock the rest of the tick uses, not from a fresh reading, and it must
// be exactly one TTR out.
void
cttest_conn_reserve_job_sets_the_deadline_one_ttr_after_the_cached_clock(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-deadline");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 1234567890123LL;
    int64 ttr = 1000000000LL;
    crj_job(&j, t, ttr);

    conn_reserve_job(&c, &j);

    assertf(j.r.deadline_at == now + ttr,
            "the deadline must be exactly one ttr past the cached clock: "
            "want %" PRId64 ", got %" PRId64 " (drift %" PRId64 "ns)",
            now + ttr, j.r.deadline_at, j.r.deadline_at - (now + ttr));

    tube_dref(t);
}


// A zero TTR is legal on the wire. The job is due the instant it is
// handed out; substituting any floor turns "expire now" into a job the
// worker keeps for a second it was never granted.
void
cttest_conn_reserve_job_gives_a_zero_ttr_job_an_immediately_due_deadline(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-zero-ttr");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 42000000000LL;
    crj_job(&j, t, 0);

    conn_reserve_job(&c, &j);

    assertf(j.r.deadline_at == now,
            "a zero ttr is due at the cached clock %" PRId64 ", got "
            "%" PRId64, now, j.r.deadline_at);

    tube_dref(t);
}


// now + ttr is an unguarded signed add. A ttr near the top of the range
// wraps the deadline into the past, and conn_timeout then bounces the job
// straight back to ready — the client is handed a job it loses instantly.
void
cttest_conn_reserve_job_must_not_wrap_a_huge_ttr_into_a_past_deadline(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-huge-ttr");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 9000000000LL;
    crj_job(&j, t, INT64_MAX);

    conn_reserve_job(&c, &j);

    assertf(j.r.deadline_at > now,
            "a reservation must never be due before it was made: clock is "
            "%" PRId64 ", deadline came out as %" PRId64,
            now, j.r.deadline_at);

    tube_dref(t);
}


// A conn that reached reserve through the timeout path can be carrying
// pending_timeout == 0 — the falsy value that a truthiness test would
// skip. Leaving it set makes conntickat schedule an immediate wake-up
// forever for a conn that is no longer waiting for anything.
void
cttest_conn_reserve_job_clears_a_pending_timeout_of_zero_seconds(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-pending-zero");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 3000000000LL;
    crj_job(&j, t, 5000000000LL);
    c.pending_timeout = 0;

    conn_reserve_job(&c, &j);

    assertf(c.pending_timeout == -1,
            "a conn holding a job is no longer waiting on a timeout: "
            "pending_timeout is %d, want -1", c.pending_timeout);

    tube_dref(t);
}


// The tube's reserved count is what `stats-tube` reports. The rule is one
// increment per call, so three reservations must move it by exactly three.
void
cttest_conn_reserve_job_adds_one_tube_reservation_for_every_call(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-tube-count");
    Conn c;
    Job js[3];
    crj_conn(&c);
    now = 8000000000LL;
    uint64 before = t->stat.reserved_ct;
    for (int i = 0; i < 3; i++) {
        crj_job(&js[i], t, 5000000000LL + i);
        conn_reserve_job(&c, &js[i]);
    }

    assertf(t->stat.reserved_ct == before + 3,
            "three reservations must move the tube counter by three: "
            "%" PRIu64 " became %" PRIu64, before, t->stat.reserved_ct);

    tube_dref(t);
}


// Per-job reserve_ct counts reservations, not distinct jobs: the same job
// handed out twice was reserved twice. Idempotence is not promised here,
// the counting rule is.
void
cttest_conn_reserve_job_counts_every_reservation_of_the_same_job(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-reserve-ct");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 6000000000LL;
    crj_job(&j, t, 5000000000LL);
    uint32 before = j.r.reserve_ct;

    conn_reserve_job(&c, &j);
    conn_reserve_job(&c, &j);

    assertf(j.r.reserve_ct == before + 2,
            "two reservations of one job must be counted twice: %u became %u",
            before, j.r.reserve_ct);

    tube_dref(t);
}


// The deadline this function writes and the margin conndeadlinesoon
// applies are the same one-second constant. A one-second TTR therefore
// lands exactly on the margin: the reservation is "soon" the moment it
// is made, which is what makes DEADLINE_SOON reachable at all.
void
cttest_conn_reserve_job_lands_a_one_second_ttr_on_the_deadline_margin(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-margin");
    Conn c;
    Job j;
    crj_conn(&c);
    now = 500000000000LL;
    int64 one_second = 1000000000LL;
    crj_job(&j, t, one_second);

    conn_reserve_job(&c, &j);

    assertf(conndeadlinesoon(&c) != 0,
            "a one-second reservation made at %" PRId64 " is already inside "
            "the one-second margin: deadline %" PRId64,
            now, j.r.deadline_at);

    tube_dref(t);
}


// The tube counter this function bumps is decremented again by the
// re-enqueue path when the conn closes. Reserve and release must leave
// `stats-tube current-jobs-reserved` exactly where it started.
void
cttest_conn_reserve_job_leaves_the_tube_count_balanced_once_the_conn_closes(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-balance");
    Server s;
    memset(&s, 0, sizeof s);
    s.conns.less = conn_less;
    s.conns.setpos = conn_setpos;
    s.sock.fd = -1;
    s.sock.added = 1;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    now = 4000000000LL;
    uint64 before = t->stat.reserved_ct;
    Job js[2];
    for (int i = 0; i < 2; i++) {
        crj_job(&js[i], t, 5000000000LL);
        conn_reserve_job(c, &js[i]);
    }
    assertf(t->stat.reserved_ct == before + 2, "setup: both must be counted");

    connclose(c);

    assertf(t->stat.reserved_ct == before,
            "closing the conn must give back every reservation it counted: "
            "%" PRIu64 " ended at %" PRIu64, before, t->stat.reserved_ct);

    free(s.conns.data);
    tube_dref(t);
}


// "A job lives in exactly one place." Handing an already-reserved job to
// a second conn must not splice it out of the first conn's list — the
// first conn would then close without ever releasing it.
void
cttest_conn_reserve_job_must_not_move_a_job_out_of_its_current_owners_list(void)
{
    crj_setup();
    Tube *t = crj_tube("crj-two-owners");
    Conn owner, thief;
    Job j;
    crj_conn(&owner);
    crj_conn(&thief);
    now = 2000000000LL;
    crj_job(&j, t, 5000000000LL);
    conn_reserve_job(&owner, &j);
    assertf(owner.reserved_jobs.next == &j, "setup: the owner must hold the job");

    conn_reserve_job(&thief, &j);

    assertf(owner.reserved_jobs.next == &j
            && owner.reserved_jobs.prev == &j,
            "a job already reserved elsewhere must stay in its owner's list: "
            "owner now holds next=%p prev=%p (job is %p)",
            (void *)owner.reserved_jobs.next, (void *)owner.reserved_jobs.prev,
            (void *)&j);

    tube_dref(t);
}
