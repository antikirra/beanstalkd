// Angry tests for filermjob (file.c) — it unlinks a job from its file
// and reverses that job's WAL accounting. It is the function that
// drives compaction: the reference it releases is what eventually lets
// walgc unlink a binlog, and the bytes it returns are what the
// compaction ratio is computed from.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static void
rm_setup(void)
{
    fault_clear_all();
    progname = "testfile_filermjob";
}

static Job *
rm_job(Tube *t, uint64 id, int64 walused)
{
    Job *j = allocate_job(4);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.body_size = 4;
    j->r.state = Ready;
    memcpy(j->body, "ab\r\n", 4);
    j->walused = walused;
    return j;
}

static void
rm_wal(Wal *w, File *f)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    f->w = w;
    f->refs = 1;
    w->head = f;
    w->tail = f;
    w->cur = f;
}

static int
rm_walk(File *f, uint64 *out, int max)
{
    int n = 0;
    for (Job *p = f->jlist.fnext; p && p != &f->jlist && n < max; p = p->fnext)
        out[n++] = p->r.id;
    return n;
}


// Contract (3): accounting drift must not be able to push alive below
// zero — a negative live-bytes figure poisons the compaction ratio for
// the life of the process.
void
cttest_filermjob_clamps_alive_at_zero_when_a_job_claims_more_than_exists(void)
{
    rm_setup();
    Wal w;
    File f;
    rm_wal(&w, &f);
    Tube *t = make_tube("clamptube");
    assertf(t != NULL, "setup: make_tube");

    Job *j = rm_job(t, 41, 1000);
    fileaddjob(&f, j);
    w.alive = 10;                 // the counters have drifted apart

    filermjob(&f, j);

    assertf(w.alive == 0,
            "removing the only job cannot leave live bytes behind and must "
            "never go negative, got %"PRId64, w.alive);
}


// The same recovery, but the Wal owns a second file whose jobs are
// perfectly accounted for. Zeroing the global counter to repair one
// file's drift throws away every other file's live bytes and tells the
// compactor the whole WAL is dead space.
void
cttest_filermjob_underflow_must_not_erase_another_files_live_bytes(void)
{
    rm_setup();
    Wal w;
    File a;
    rm_wal(&w, &a);
    File b;
    memset(&b, 0, sizeof b);
    b.w = &w;
    b.refs = 1;
    a.next = &b;
    w.tail = &b;
    w.nfile = 2;

    Tube *t = make_tube("crossfiletube");
    assertf(t != NULL, "setup: make_tube");
    Job *ja = rm_job(t, 51, 100);
    Job *jb = rm_job(t, 52, 500);
    fileaddjob(&a, ja);
    fileaddjob(&b, jb);
    w.alive = 600;

    ja->walused = 1000;           // drift on file A only

    filermjob(&a, ja);

    assertf(w.alive == 500,
            "file B's job is still live and its 500 bytes must survive A's "
            "accounting repair, got %"PRId64, w.alive);
}


void
cttest_filermjob_on_a_foreign_file_leaves_the_job_registered(void)
{
    rm_setup();
    Wal w;
    File a;
    rm_wal(&w, &a);
    File other;
    memset(&other, 0, sizeof other);
    other.w = &w;

    Tube *t = make_tube("foreigntube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = rm_job(t, 61, 90);
    fileaddjob(&a, j);

    filermjob(&other, j);

    assertf(j->file == &a,
            "a removal aimed at the wrong file must change nothing: the "
            "job still lives in the file that holds its record");
}


void
cttest_filermjob_with_a_null_file_leaves_the_job_registered(void)
{
    rm_setup();
    Wal w;
    File f;
    rm_wal(&w, &f);
    Tube *t = make_tube("nullrmtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = rm_job(t, 71, 90);
    fileaddjob(&f, j);

    filermjob(NULL, j);

    assertf(j->file == &f,
            "filermjob(NULL, j) is a no-op and must leave the job where it "
            "was");
}


// Contract (5): exactly one reference per removal. Releasing none pins
// the binlog; releasing two frees it while jobs still live in it.
void
cttest_filermjob_releases_exactly_one_reference(void)
{
    rm_setup();
    Wal w;
    File f;
    rm_wal(&w, &f);
    Tube *t = make_tube("refrmtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j1 = rm_job(t, 81, 90);
    Job *j2 = rm_job(t, 82, 90);
    Job *j3 = rm_job(t, 83, 90);
    fileaddjob(&f, j1);
    fileaddjob(&f, j2);
    fileaddjob(&f, j3);
    w.alive = 270;

    uint before = f.refs;
    filermjob(&f, j2);

    assertf(f.refs == before - 1,
            "removing one job releases exactly one reference: was %u, now "
            "%u", before, f.refs);
}


// Contract (2): splicing the middle out of the ring must leave the two
// survivors linked to each other and to the sentinel.
void
cttest_filermjob_keeps_the_list_consistent_after_removing_the_middle_job(void)
{
    rm_setup();
    Wal w;
    File f;
    rm_wal(&w, &f);
    Tube *t = make_tube("middlermtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j1 = rm_job(t, 91, 90);
    Job *j2 = rm_job(t, 92, 90);
    Job *j3 = rm_job(t, 93, 90);
    fileaddjob(&f, j1);
    fileaddjob(&f, j2);
    fileaddjob(&f, j3);
    w.alive = 270;

    filermjob(&f, j2);

    uint64 want[2] = {91, 93};
    uint64 got[8];
    int n = rm_walk(&f, got, 8);

    assertf(n == 2 && memcmp(got, want, sizeof want) == 0,
            "the ring must close over the removed job: walked %d", n);
}


// Contract (4): the job no longer occupies bytes in any file, so its
// own counter has to say so — otherwise a later removal subtracts them
// from alive a second time.
void
cttest_filermjob_zeroes_the_jobs_used_bytes(void)
{
    rm_setup();
    Wal w;
    File f;
    rm_wal(&w, &f);
    Tube *t = make_tube("usedrmtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = rm_job(t, 101, 90);
    fileaddjob(&f, j);
    w.alive = 90;

    filermjob(&f, j);

    assertf(j->walused == 0,
            "an unlinked job occupies no bytes in any file, got %"PRId64,
            j->walused);
}
