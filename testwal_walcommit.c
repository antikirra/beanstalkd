// Angry tests for walcommit (walg.c) — the end-of-tick group commit that
// turns a batch of staged records into durable ones.
//
// Its verdict is what dur_flush_all turns into acks, so every way of
// returning the wrong one is a correctness bug wearing a performance
// mask: a poisoned tick reported as success acks records that were
// truncated away, and an idle tick that syncs anyway pays for durability
// nobody asked for.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
wc_setup(void)
{
    fault_clear_all();
    progname = "testwal_walcommit";
    now = 0;
}

static void
wc_wal(Wal *w, Job *l, int durable)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    w->durable_sync = durable;
}

static Job *
wc_job(const char *tubename, uint64 id, int body)
{
    Tube *t = make_tube(tubename);
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 2;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'c', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static void
wc_stage(Wal *w, uint64 id)
{
    Job *j = wc_job("commit", id, 12);
    int n = walresvput(w, j);
    assertf(n > 0, "setup: reservation for job %" PRIu64 " failed", id);
    j->walresv = n;
    assertf(walwrite(w, j) != 0, "setup: staging job %" PRIu64 " failed", id);
}

static void
wc_stage_many(Wal *w, int n)
{
    for (int i = 0; i < n; i++) wc_stage(w, 500 + (uint64)i);
}


// A tick whose records were lost mid-flight must fail its commit even
// though the wal is already disabled. The "disabled, so nothing to
// commit" fast path would report success and every client in the batch
// would be acked for a record that no longer exists.
void
cttest_walcommit_fails_the_tick_a_write_failure_poisoned(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 0);
    wc_stage(&w, 401);
    Job *j = wc_job("commit", 402, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);
    walwrite(&w, j);
    assertf(w.use == 0, "setup: the failed write must have disabled the wal");

    int r = walcommit(&w);

    assertf(r == 0,
            "the poisoned tick must be reported as a failed commit, "
            "walcommit returned %d", r);
}


// The poison covers one tick. Every later tick is protected by the
// disabled wal itself, so failing forever would only convert one lost
// batch into a permanently erroring server.
void
cttest_walcommit_clears_the_poison_after_reporting_it_once(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 0);
    Job *j = wc_job("commit", 403, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);
    walwrite(&w, j);
    assertf(walcommit(&w) == 0, "setup: the poisoned tick must fail once");

    int r = walcommit(&w);

    assertf(r == 1,
            "the tick after the poisoned one has nothing to fail about, "
            "walcommit returned %d", r);
}


// A wal that is merely off — no lost records, no poison — has nothing to
// commit and must say so. Failing here would make an ordinary disabled
// wal error out on every tick of the main loop.
void
cttest_walcommit_succeeds_on_a_wal_that_is_merely_disabled(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 0);
    w.use = 0;

    int r = walcommit(&w);

    assertf(r == 1,
            "a disabled wal with no lost records commits nothing and "
            "succeeds, walcommit returned %d", r);
}


// A failing group sync means the staged bytes never became durable. The
// wal has to go down rather than let the tick be acked.
void
cttest_walcommit_disables_the_wal_when_the_group_sync_fails(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 1);
    wc_stage(&w, 404);
    fault_set(FAULT_FDATASYNC, 0, EIO);

    walcommit(&w);

    assertf(w.use == 0,
            "a failed group sync must disable the wal, use is still %d",
            w.use);
}


// ...and it must report the failure, which is what turns into
// INTERNAL_ERROR for every client waiting on this batch.
void
cttest_walcommit_returns_zero_when_the_group_sync_fails(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 1);
    wc_stage(&w, 405);
    fault_set(FAULT_FDATASYNC, 0, EIO);

    int r = walcommit(&w);

    assertf(r == 0,
            "a failed group sync must be reported, walcommit returned %d", r);
}


// The rollback takes the bytes off the tail AND out of the accounting.
// Leaving them reserved-but-spent skews ratio() for the rest of the
// process's life.
void
cttest_walcommit_returns_the_staged_bytes_to_the_reservation_on_failure(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 1);
    Job *j = wc_job("rollback", 406, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    int64 reserved = w.resv;
    assertf(walwrite(&w, j) != 0, "setup: the record must stage");
    fault_set(FAULT_FDATASYNC, 0, EIO);

    walcommit(&w);

    assertf(w.resv == reserved,
            "the rolled-back bytes must go back to the reservation: resv "
            "was %" PRId64 " before staging and is %" PRId64 " after the "
            "failed commit", reserved, w.resv);
}


// An idle tick costs nothing. The main loop calls this on every epoll
// drain, so an unconditional fdatasync here is a silent throughput
// collapse with no functional symptom.
void
cttest_walcommit_issues_no_sync_when_nothing_was_staged(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 1);
    fault_clear_all();

    int r = walcommit(&w);

    assertf(r == 1 && fault_calls(FAULT_FDATASYNC) == 0,
            "an empty tick must succeed without syncing: walcommit "
            "returned %d after %d fdatasync() call(s)",
            r, fault_calls(FAULT_FDATASYNC));
}


// One sync per tick, whatever the batch size. That bound is the whole
// point of group commit; one sync per record would be correct and
// unusable.
void
cttest_walcommit_syncs_once_for_a_whole_batch(void)
{
    wc_setup();
    Wal w;
    Job list;
    wc_wal(&w, &list, 1);
    wc_stage_many(&w, 4);
    fault_clear_all();

    walcommit(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "four staged records must cost exactly one fdatasync, got %d",
            fault_calls(FAULT_FDATASYNC));
}
