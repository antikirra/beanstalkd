// Angry tests for conn_ready (conn.c).
//
// The predicate that decides whether a worker can be served immediately
// or must be parked in every watched tube's waiting set. Two conditions
// must hold on the SAME tube — jobs ready AND not paused — and the scan
// covers the whole watch set, not just the tube the worker happens to
// have watched first.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
crd_setup(void)
{
    fault_clear_all();
    progname = "testconn_conn_ready";
    prot_init();
    conn_pool_drain();
    now = 400000000000LL;
}

static Tube *
crd_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static void
crd_make_ready(Tube *t, Job *j, uint32 pri)
{
    memset(j, 0, sizeof *j);
    job_list_reset(j);
    j->tube = t;
    j->r.pri = pri;
    j->r.state = Ready;
    assertf(heapinsert(&t->ready, j), "setup: the ready job must be queued");
}


// The scan must cover the whole watch set. A worker watching three tubes
// where only the third has work is the exact case a first-entry-only loop
// gets wrong, and it gets it wrong by parking a worker that could have
// been served immediately.
void
cttest_conn_ready_finds_work_in_the_last_watched_tube(void)
{
    crd_setup();
    Tube *empty_a = crd_tube("crd-last-a");
    Tube *empty_b = crd_tube("crd-last-b");
    Tube *stocked = crd_tube("crd-last-c");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, empty_a, empty_a);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, empty_b) && ms_append(&c->watch, stocked),
            "setup: the extra watches must succeed");
    Job j;
    crd_make_ready(stocked, &j, 50);

    int ready = conn_ready(c);

    assertf(ready != 0,
            "work in the last of %zu watched tubes is still work, got %d",
            c->watch.len, ready);

    connclose(c);
    tube_dref(empty_a);
    tube_dref(empty_b);
    tube_dref(stocked);
}


// An empty watch set has nothing to scan. Reading one entry past the end
// of a set whose items array is still NULL is the failure this pins.
void
cttest_conn_ready_reports_no_work_for_an_empty_watch_set(void)
{
    crd_setup();
    Conn c;
    memset(&c, 0, sizeof c);
    ms_init(&c.watch, NULL, NULL);
    job_list_reset(&c.reserved_jobs);

    int ready = conn_ready(&c);

    assertf(ready == 0,
            "a conn watching nothing has no work available, got %d", ready);
}


// A paused tube has jobs but must not serve them. Both conditions have to
// hold on the same tube, so ready jobs in a paused tube plus an unpaused
// empty tube add up to nothing.
void
cttest_conn_ready_ignores_ready_jobs_sitting_in_a_paused_tube(void)
{
    crd_setup();
    Tube *paused = crd_tube("crd-paused");
    Tube *open_empty = crd_tube("crd-open-empty");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, paused, paused);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, open_empty), "setup: the second watch must succeed");
    Job j;
    crd_make_ready(paused, &j, 50);
    paused->pause = 60000000000LL;
    paused->unpause_at = now + paused->pause;

    int ready = conn_ready(c);

    assertf(ready == 0,
            "a paused tube's jobs are not available and an unpaused empty "
            "tube has none, got %d", ready);

    connclose(c);
    tube_dref(paused);
    tube_dref(open_empty);
}


// One ready job is enough. A comparison that demands more than one leaves
// a worker parked while a job sits waiting for it.
void
cttest_conn_ready_reports_work_for_a_tube_holding_a_single_ready_job(void)
{
    crd_setup();
    Tube *t = crd_tube("crd-single");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    int empty_answer = conn_ready(c);
    Job j;
    crd_make_ready(t, &j, 50);

    int ready = conn_ready(c);

    assertf(empty_answer == 0 && ready != 0,
            "exactly one ready job is available work: empty answered %d, "
            "one job answered %d", empty_answer, ready);

    connclose(c);
    tube_dref(t);
}


// The answer must be recomputed every time. A memoized result turns
// `pause-tube` into a command that only takes effect for connections that
// arrive afterwards.
void
cttest_conn_ready_tracks_the_pause_flag_every_time_it_is_asked(void)
{
    crd_setup();
    Tube *t = crd_tube("crd-toggle");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    Job j;
    crd_make_ready(t, &j, 50);
    int wrong_step = -1;

    for (int step = 0; step < 6; step++) {
        int paused = step % 2;
        t->pause = paused ? 60000000000LL : 0;
        t->unpause_at = paused ? now + t->pause : 0;
        int want_work = !paused;
        if ((conn_ready(c) != 0) != want_work && wrong_step < 0)
            wrong_step = step;
    }

    assertf(wrong_step < 0,
            "the answer must follow the pause flag on every call: step %d "
            "disagreed (pause %" PRId64 ")", wrong_step, t->pause);

    connclose(c);
    tube_dref(t);
}


// ms_append does not deduplicate, so the same tube can occupy two watch
// slots. The predicate is over the SET of watched tubes: a duplicate must
// not turn a paused tube into an available one.
void
cttest_conn_ready_gives_the_same_answer_when_a_tube_is_watched_twice(void)
{
    crd_setup();
    Tube *t = crd_tube("crd-dup");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    assertf(ms_append(&c->watch, t), "setup: the duplicate watch must succeed");
    assertf(c->watch.len == 2, "setup: the tube must occupy two watch slots");
    Job j;
    crd_make_ready(t, &j, 50);
    t->pause = 60000000000LL;
    t->unpause_at = now + t->pause;

    int ready = conn_ready(c);

    assertf(ready == 0,
            "watching one paused tube twice is still one paused tube, got %d",
            ready);

    connclose(c);
    tube_dref(t);
}
