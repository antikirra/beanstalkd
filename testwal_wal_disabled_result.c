// Angry tests for wal_disabled_result (walg.c) — the one-line decision
// that separates "the wal is off, pretend everything is fine" from "the
// wal is off, so tell the client the truth".
//
// It is shared by walwrite and reserve, and it is the whole of invariant
// #14 on the disabled path: under -D a dead wal must produce a real error
// instead of an ack for a record that will never exist. It has exactly
// one input, and that is the property worth pinning.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
wd_setup(void)
{
    fault_clear_all();
    progname = "testwal_wal_disabled_result";
}

// A wal that is switched off and carries no files at all: every call
// under test must return before it looks at w->cur.
static void
wd_wal(Wal *w, int durable)
{
    memset(w, 0, sizeof *w);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 0;
    w->durable_sync = durable;
}

static Job *
wd_job(const char *tubename, int body)
{
    Tube *t = make_tube(tubename);
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = 9;
    j->r.pri = 1;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'w', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}


// Under -D a disabled wal must refuse the write. Returning success here
// is what lets dur_flush_all ack a client for a record that was never
// written anywhere.
void
cttest_wal_disabled_result_refuses_a_write_under_durable_mode(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 1);
    Job *j = wd_job("durable", 10);

    int r = walwrite(&w, j);

    assertf(r == 0,
            "a disabled wal under -D must refuse the write so the client "
            "gets an error; walwrite returned %d", r);
}


// The refusal has to reach the reservation calls too, because that is
// where a put is turned away before anything is promised to the client.
void
cttest_wal_disabled_result_refuses_a_put_reservation_under_durable_mode(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 1);
    Job *j = wd_job("durableput", 10);

    int r = walresvput(&w, j);

    assertf(r == 0,
            "a disabled wal under -D must refuse to reserve for a put; "
            "walresvput returned %d", r);
}


// Same for the delete/update slot: a state change that cannot be logged
// must not look reservable.
void
cttest_wal_disabled_result_refuses_an_update_reservation_under_durable_mode(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 1);

    int r = walresvupdate(&w);

    assertf(r == 0,
            "a disabled wal under -D must refuse to reserve for an update; "
            "walresvupdate returned %d", r);
}


// Without -D the legacy contract stands: nothing persists anyway, so the
// write "succeeds" and the server keeps serving. Breaking this turns a
// degraded wal into a broken server for every non-durable deployment.
void
cttest_wal_disabled_result_keeps_the_legacy_success_for_a_write(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 0);
    Job *j = wd_job("legacy", 10);

    int r = walwrite(&w, j);

    assertf(r == 1,
            "a disabled wal without -D keeps the legacy silent success; "
            "walwrite returned %d", r);
}


// The legacy reservation return is 1, not the requested size. Callers
// store it in j->walresv and hand it back to walresvreturn, so a
// byte-count-shaped lie here corrupts the reservation accounting the
// moment the wal comes back.
void
cttest_wal_disabled_result_returns_one_rather_than_the_request_size(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 0);
    Job *j = wd_job("legacyput", 10);

    int r = walresvput(&w, j);

    assertf(r == 1,
            "the disabled-wal reservation is the legacy 1, never a byte "
            "count; walresvput returned %d", r);
}


// A refusal that moved a counter would be worse than the refusal itself:
// reserved bytes that nothing owns depress ratio() and stall compaction
// permanently.
void
cttest_wal_disabled_result_reserves_no_bytes_on_a_disabled_wal(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 0);
    Job *j = wd_job("counters", 10);

    walresvput(&w, j);
    walresvupdate(&w);

    assertf(w.resv == 0,
            "nothing was reserved, so w->resv must still be 0, got %"
            PRId64, w.resv);
}


// ...and the record counter the operator reads in `stats` must not move
// for a record that was never written.
void
cttest_wal_disabled_result_counts_no_record_on_a_disabled_wal(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 0);
    Job *j = wd_job("nrec", 10);

    walwrite(&w, j);
    walwrite(&w, j);

    assertf(w.nrec == 0,
            "two refused writes are zero records, stats says %" PRId64,
            w.nrec);
}


// The verdict depends on the durable flag and on nothing else. Wiring it
// to any other piece of wal state would make a perfectly good -D server
// start ghost-acking as soon as that other state happened to be set.
void
cttest_wal_disabled_result_depends_on_nothing_but_the_durable_flag(void)
{
    wd_setup();
    Wal w;
    wd_wal(&w, 0);
    w.wantsync = 1;
    w.commit_failed = 1;
    w.nfile = 9;
    w.resv = 4096;
    w.syncrate = 1000;
    Job *j = wd_job("metamorphic", 10);

    int legacy = walwrite(&w, j);
    w.durable_sync = 1;
    int durable = walwrite(&w, j);

    assertf(legacy == 1 && durable == 0,
            "flipping only durable_sync must flip only the verdict: got "
            "%d without -D and %d with it", legacy, durable);
}
