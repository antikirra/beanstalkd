// Angry tests for needfree (walg.c) — the promise that w->tail has n
// free bytes.
//
// Every caller treats a non-zero return as a guarantee and subtracts n
// from w->tail->free without checking again. That makes needfree the one
// place where "I created a file, so it must fit now" has to actually be
// true: a file smaller than the request satisfies neither half.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
nf_setup(void)
{
    fault_clear_all();
    progname = "testwal_needfree";
    now = 0;
}

static void
nf_wal(Wal *w, Job *l, int filesize)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = filesize;
    w->use = 1;
    walinit(w, l);
}

static Job *
nf_job(uint64 id, int body)
{
    Tube *t = make_tube("n");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'n', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int
nf_minfree(Wal *w)
{
    int m = INT_MAX;
    for (File *f = w->head; f; f = f->next)
        if (f->free < m) m = f->free;
    return m;
}


// A record that cannot fit in a whole binlog cannot be reserved,
// however many binlogs are created. Creating one and reporting success
// anyway means the caller subtracts bytes from a file that never had
// them.
void
cttest_needfree_refuses_a_reservation_larger_than_a_whole_binlog(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 1024);
    Job *j = nf_job(101, 2000);

    int r = walresvput(&w, j);

    assertf(r == 0,
            "no 1024-byte binlog can hold this record, so the reservation "
            "must be refused; walresvput returned %d", r);
}


// ...and whatever the verdict, no file may end up owing space it does
// not have. A negative free count feeds straight into ratio() and into
// every later reservation decision.
void
cttest_needfree_never_leaves_a_binlog_owing_free_space(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 1024);
    Job *j = nf_job(102, 2000);

    walresvput(&w, j);

    assertf(nf_minfree(&w) >= 0,
            "a file's free count can never go below zero: the emptiest "
            "file reports %d free bytes", nf_minfree(&w));
}


// The same invariant on a wal whose files are big enough: this is the
// postcondition every caller of needfree relies on, checked after a
// reservation that really did have to grow the wal.
void
cttest_needfree_keeps_every_binlog_solvent_across_a_growing_reservation(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 4096);
    Job *first = nf_job(103, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = nf_job(104, 3900);

    walresvput(&w, second);

    assertf(nf_minfree(&w) >= 0,
            "growing the wal must leave every file solvent: the emptiest "
            "file reports %d free bytes", nf_minfree(&w));
}


// When the new file cannot be created the guarantee cannot be met, and
// the wal must look exactly as it did before the attempt.
void
cttest_needfree_adds_no_file_when_the_new_binlog_cannot_be_created(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 4096);
    Job *first = nf_job(105, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    int before = w.nfile;
    Job *second = nf_job(106, 3900);
    fault_set(FAULT_OPEN, 0, EACCES);

    walresvput(&w, second);

    assertf(w.nfile == before,
            "the file could not be created, so the count must not move: "
            "nfile went from %d to %d", before, w.nfile);
}


// A failed creation must not burn the sequence number either: every
// later binlog would be shifted by one, and the stranded number is gone
// for the life of the directory.
void
cttest_needfree_leaves_the_sequence_unchanged_when_creation_fails(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 4096);
    Job *first = nf_job(107, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    int before = w.next;
    Job *second = nf_job(108, 3900);
    fault_set(FAULT_OPEN, 0, EACCES);

    walresvput(&w, second);

    assertf(w.next == before,
            "no file was created, so the next sequence must not advance: "
            "it went from %d to %d", before, w.next);
}


// ...and the caller's reservation has to be rolled back with it.
void
cttest_needfree_rolls_the_reservation_back_when_creation_fails(void)
{
    nf_setup();
    Wal w;
    Job list;
    nf_wal(&w, &list, 4096);
    Job *first = nf_job(109, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    int64 before = w.resv;
    Job *second = nf_job(110, 3900);
    fault_set(FAULT_OPEN, 0, EACCES);

    walresvput(&w, second);

    assertf(w.resv == before,
            "a refused reservation must leave the total untouched: resv "
            "went from %" PRId64 " to %" PRId64, before, w.resv);
}
