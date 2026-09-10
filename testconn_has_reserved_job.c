// Angry tests for has_reserved_job (conn.c, static).
//
// A one-line predicate over an intrusive list header, reached through
// connsched: it decides whether conntickat consults connsoonestjob at
// all, and whether connclose bothers re-enqueueing. Its dangerous input
// is a header that is not a valid empty list — an all-zero header reads
// as NON-empty, which sends conntickat walking a NULL chain.

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
hrj_setup(Server *s)
{
    fault_clear_all();
    progname = "testconn_has_reserved_job";
    prot_init();
    conn_pool_drain();
    memset(s, 0, sizeof *s);
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;
    s->sock.fd = -1;
    s->sock.added = 1;
    srv.idle_timeout = 0;
}

static Tube *
hrj_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static void
hrj_reserve(Conn *c, Job *j, Tube *t, int64 deadline)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->tube = t;
    j->r.deadline_at = deadline;
    j->r.state = Reserved;
    j->reserver = c;
    job_list_insert(&c->reserved_jobs, j);
    c->soonest_job = NULL;
}


// A struct that comes back from the slab pool has had its whole hot
// prefix zeroed, and an all-zero list header is not an empty list — it
// is a header pointing at nothing. If make_conn did not re-form the
// header, this predicate says "yes, there are jobs" and conntickat
// follows a NULL next pointer.
void
cttest_has_reserved_job_treats_a_recycled_conn_as_holding_nothing(void)
{
    Server s;
    hrj_setup(&s);
    Tube *t = hrj_tube("hrj-recycle");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    first->srv = &s;
    now = 500000000000LL;
    Job j;
    hrj_reserve(first, &j, t, now + 10000000000LL);
    connclose(first);
    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second, "setup: the second make_conn must succeed");
    second->srv = &s;

    connsched(second);

    assertf(second->in_conns == 0 && s.conns.len == 0,
            "a recycled conn holds no reservation, so it has nothing to "
            "wake for: in_conns %d, heap holds %zu",
            second->in_conns, s.conns.len);

    connclose(second);
    free(s.conns.data);
    tube_dref(t);
}


// One job in, one job out. The predicate is a pure function of the list
// contents, so the conn must leave the tick heap again once it holds
// nothing — a conn stuck in the heap wakes prottick on a dead deadline
// for as long as it lives.
void
cttest_has_reserved_job_flips_back_to_empty_when_the_last_job_leaves(void)
{
    Server s;
    hrj_setup(&s);
    Tube *t = hrj_tube("hrj-flip");
    Conn c;
    memset(&c, 0, sizeof c);
    c.srv = &s;
    c.state = STATE_WANT_COMMAND;
    c.pending_timeout = -1;
    job_list_reset(&c.reserved_jobs);
    now = 500000000000LL;
    Job j;
    hrj_reserve(&c, &j, t, now + 10000000000LL);
    connsched(&c);
    assertf(c.in_conns == 1, "setup: a conn holding a job must be scheduled");
    assertf(job_list_remove(&j) == &j, "setup: the job must unlink");
    c.soonest_job = NULL;

    connsched(&c);

    assertf(c.in_conns == 0 && s.conns.len == 0,
            "with its last job gone the conn has nothing to wake for: "
            "in_conns %d, heap holds %zu", c.in_conns, s.conns.len);

    free(s.conns.data);
    tube_dref(t);
}


// Emptiness must track the list at every step of a sequence, not just at
// its ends: a predicate that latches after the first insert keeps the
// conn in the heap for the rest of its life.
void
cttest_has_reserved_job_tracks_the_list_at_every_step_of_a_sequence(void)
{
    Server s;
    hrj_setup(&s);
    Tube *t = hrj_tube("hrj-steps");
    Conn c;
    memset(&c, 0, sizeof c);
    c.srv = &s;
    c.state = STATE_WANT_COMMAND;
    c.pending_timeout = -1;
    job_list_reset(&c.reserved_jobs);
    now = 500000000000LL;
    Job js[3];
    int wrong_step = -1;

    for (int i = 0; i < 3; i++) {
        hrj_reserve(&c, &js[i], t, now + 10000000000LL + i);
        connsched(&c);
        if (c.in_conns != 1 && wrong_step < 0)
            wrong_step = i;
    }
    for (int i = 0; i < 3; i++) {
        assertf(job_list_remove(&js[i]) == &js[i], "setup: job %d must unlink", i);
        c.soonest_job = NULL;
        connsched(&c);
        int want = (i < 2) ? 1 : 0;
        if (c.in_conns != want && wrong_step < 0)
            wrong_step = 10 + i;
    }

    assertf(wrong_step < 0,
            "membership of the tick heap must follow the reserved list at "
            "every step: step %d disagreed (in_conns %d, heap holds %zu)",
            wrong_step, c.in_conns, s.conns.len);

    free(s.conns.data);
    tube_dref(t);
}
