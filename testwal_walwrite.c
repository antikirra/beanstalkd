// Angry tests for walwrite (walg.c) — the staging half of the group
// commit, and the place where a write failure has to turn into a
// disabled wal exactly once.
//
// Three things it must get right and nothing else checks: the record
// shape depends on whether the job already has a full record on disk, a
// failed stage must not be counted, and the failure must poison this
// tick so the batch reply is an error rather than a set of ghost acks.

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
ww_setup(void)
{
    fault_clear_all();
    progname = "testwal_walwrite";
    now = 0;
}

static void
ww_wal(Wal *w, Job *l, int durable)
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
ww_job(const char *tubename, uint64 id, int body)
{
    Tube *t = make_tube(tubename);
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 6;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'k', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

// Reserves and stages one job, insisting that both halves succeed — used
// where the staging is setup rather than the thing under test.
static void
ww_stage(Wal *w, const char *tubename, uint64 id, int body)
{
    Job *j = ww_job(tubename, id, body);
    int n = walresvput(w, j);
    assertf(n > 0, "setup: reservation for job %" PRIu64 " failed", id);
    j->walresv = n;
    assertf(walwrite(w, j) != 0, "setup: staging job %" PRIu64 " failed", id);
}

static void
ww_stage_many(Wal *w, int n)
{
    for (int i = 0; i < n; i++) ww_stage(w, "many", 900 + (uint64)i, 12);
}


// A writev that fails means the record is not in the file. Reporting
// success would let the client be acked for a job nobody can replay.
void
cttest_walwrite_returns_zero_when_the_record_cannot_be_staged(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *j = ww_job("fail", 801, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);

    int r = walwrite(&w, j);

    assertf(r == 0, "a failed writev must be reported, walwrite returned %d", r);
}


// ...and it must take the wal down with it, because from here on the log
// and memory disagree and every later record would compound the lie.
void
cttest_walwrite_disables_the_wal_when_the_record_cannot_be_staged(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *j = ww_job("disable", 802, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);

    walwrite(&w, j);

    assertf(w.use == 0,
            "a staging failure must disable the wal, use is still %d", w.use);
}


// The record counter is what `stats` reports as cmd-put durability
// evidence. A record that was never written may not appear in it.
void
cttest_walwrite_counts_no_record_for_a_failed_stage(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *j = ww_job("norec", 803, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    int64 before = w.nrec;
    fault_set(FAULT_WRITEV, 0, EIO);

    walwrite(&w, j);

    assertf(w.nrec == before,
            "a failed stage counts nothing: nrec went from %" PRId64 " to %"
            PRId64, before, w.nrec);
}


// Records staged earlier in the same tick were orphaned by the file
// close. The tick has to be poisoned so the batch reply is an error, not
// a set of acks for records that vanished.
void
cttest_walwrite_poisons_the_tick_it_failed_in(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    ww_stage(&w, "earlier", 804, 12);
    Job *j = ww_job("poison", 805, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);
    walwrite(&w, j);

    int c = walcommit(&w);

    assertf(c == 0,
            "the tick that lost records must fail its group commit, "
            "walcommit returned %d", c);
}


// The poison is one-shot. A wal that fails every commit forever would
// turn one bad tick into a permanently erroring server even though every
// later walwrite already refuses on its own.
void
cttest_walwrite_poison_is_reported_only_once(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *j = ww_job("once", 806, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);
    walwrite(&w, j);
    assertf(walcommit(&w) == 0, "setup: the poisoned tick must fail once");

    int c = walcommit(&w);

    assertf(c == 1,
            "the tick after the poisoned one has nothing to fail about, "
            "walcommit returned %d", c);
}


// Every staged record is counted exactly once. The counter is
// operator-visible, so an off-by-one is a reporting bug that survives
// forever because nothing else reads it.
void
cttest_walwrite_counts_every_staged_record_exactly_once(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    int64 before = w.nrec;
    int staged = 5;

    ww_stage_many(&w, staged);

    assertf(w.nrec == before + staged,
            "%d staged records must add %d to nrec: it went from %" PRId64
            " to %" PRId64, staged, staged, before, w.nrec);
}


// A job that already owns a full record on disk gets a short one: the
// difference between the two shapes is exactly the tube name and the
// body. Swapping the branches keeps every return value the same and
// corrupts recovery instead.
void
cttest_walwrite_stages_a_short_record_for_a_job_that_already_has_a_file(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    int body = 12;
    Job *j = ww_job("tw", 807, body);
    int namelen = j->tube->name_len;
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    assertf(j->file == NULL, "setup: the job must start with no file");

    int before = w.cur->resv;
    assertf(walwrite(&w, j) != 0, "setup: the full record must stage");
    int after_full = w.cur->resv;
    assertf(j->file != NULL, "setup: a full record files the job");
    assertf(walwrite(&w, j) != 0, "setup: the short record must stage");
    int after_short = w.cur->resv;

    assertf((before - after_full) - (after_full - after_short)
            == namelen + body,
            "the full record must cost the tube name and the body more "
            "than the short one: full took %d bytes, short took %d, "
            "expected a difference of %d",
            before - after_full, after_full - after_short, namelen + body);
}


// With no next file to rotate into, the record cannot be staged at all.
// Writing past the reservation would let the tail of one binlog run into
// space another file's accounting already owns.
void
cttest_walwrite_refuses_when_the_reservation_is_spent_and_no_file_follows(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *j = ww_job("spent", 808, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    assertf(w.cur == w.tail && w.cur->next == NULL,
            "setup: there must be no file to rotate into");
    w.cur->resv = 0;

    int r = walwrite(&w, j);

    assertf(r == 0,
            "with the reservation spent and no next file, the record "
            "cannot be staged; walwrite returned %d", r);
}


// Once disabled, every later call takes the disabled path — it must not
// close the current file a second time or poison another tick.
void
cttest_walwrite_takes_the_disabled_path_after_it_has_already_failed(void)
{
    ww_setup();
    Wal w;
    Job list;
    ww_wal(&w, &list, 0);
    Job *first = ww_job("dead", 809, 12);
    first->walresv = walresvput(&w, first);
    assertf(first->walresv > 0, "setup: reservation must succeed");
    fault_set(FAULT_WRITEV, 0, EIO);
    walwrite(&w, first);
    assertf(w.use == 0, "setup: the wal must be disabled by now");
    Job *second = ww_job("dead", 810, 12);

    int r = walwrite(&w, second);

    assertf(r == 1,
            "a disabled non-durable wal keeps the legacy silent success on "
            "every later call; walwrite returned %d", r);
}
