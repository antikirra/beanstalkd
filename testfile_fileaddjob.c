// Angry tests for fileaddjob (file.c) — it registers a job as living in
// a file, lazily initialising the intrusive list's sentinel and taking
// a reference. Two invariants hang off it: the doubly linked ring stays
// walkable in both directions, and f->refs counts the jobs registered
// on f. Compaction and reaping are built on both.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static void
aj_setup(void)
{
    fault_clear_all();
    progname = "testfile_fileaddjob";
}

static Job *
aj_job(Tube *t, uint64 id)
{
    Job *j = allocate_job(4);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.body_size = 4;
    j->r.state = Ready;
    memcpy(j->body, "ab\r\n", 4);
    return j;
}

static int
aj_walk_forward(File *f, uint64 *out, int max)
{
    int n = 0;
    for (Job *p = f->jlist.fnext; p && p != &f->jlist && n < max; p = p->fnext)
        out[n++] = p->r.id;
    return n;
}

static int
aj_walk_backward(File *f, uint64 *out, int max)
{
    int n = 0;
    for (Job *p = f->jlist.fprev; p && p != &f->jlist && n < max; p = p->fprev)
        out[n++] = p->r.id;
    return n;
}


// Registered at the tail, in order, on a File whose sentinel starts out
// entirely zeroed — the lazy-init branch walread's calloc'd Files take.
void
cttest_fileaddjob_appends_jobs_in_insertion_order(void)
{
    aj_setup();
    File f;
    memset(&f, 0, sizeof f);
    Tube *t = make_tube("addjobtube");
    assertf(t != NULL, "setup: make_tube");

    fileaddjob(&f, aj_job(t, 1));
    fileaddjob(&f, aj_job(t, 2));
    fileaddjob(&f, aj_job(t, 3));

    uint64 want[3] = {1, 2, 3};
    uint64 got[8];
    int n = aj_walk_forward(&f, got, 8);

    assertf(n == 3 && memcmp(got, want, sizeof want) == 0,
            "the file's job list must hold the three jobs in insertion "
            "order and close back on its sentinel: walked %d", n);
}


// The backward links are half the ring and nothing else in the tree
// walks them, yet filermjob splices through both.
void
cttest_fileaddjob_links_the_ring_backwards_as_well(void)
{
    aj_setup();
    File f;
    memset(&f, 0, sizeof f);
    Tube *t = make_tube("addjobbacktube");
    assertf(t != NULL, "setup: make_tube");

    fileaddjob(&f, aj_job(t, 11));
    fileaddjob(&f, aj_job(t, 12));
    fileaddjob(&f, aj_job(t, 13));

    uint64 want[3] = {13, 12, 11};
    uint64 got[8];
    int n = aj_walk_backward(&f, got, 8);

    assertf(n == 3 && memcmp(got, want, sizeof want) == 0,
            "walking the ring backwards must visit the same three jobs in "
            "reverse: walked %d", n);
}


void
cttest_fileaddjob_takes_one_reference_per_job(void)
{
    aj_setup();
    File f;
    memset(&f, 0, sizeof f);
    f.refs = 1;
    Tube *t = make_tube("addjobreftube");
    assertf(t != NULL, "setup: make_tube");

    uint before = f.refs;
    fileaddjob(&f, aj_job(t, 21));
    fileaddjob(&f, aj_job(t, 22));
    fileaddjob(&f, aj_job(t, 23));

    assertf(f.refs == before + 3,
            "three registered jobs must hold three references: was %u, "
            "now %u", before, f.refs);
}


