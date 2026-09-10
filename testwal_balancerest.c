// Angry tests for balancerest (walg.c) — the recursive half of balance
// that walks the file chain making every reservation congruent.
//
// It is the only code that may move reserved bytes between files, and it
// reports success by returning 1 whether or not the move it decided on
// actually moved anything. When the file it is correcting *is* the tail,
// the correction is a move from a file to itself.

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
br_setup(void)
{
    fault_clear_all();
    progname = "testwal_balancerest";
    now = 0;
}

static int
br_z(void)
{
    return (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32));
}

static void
br_wal(Wal *w, Job *l)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
}

static Job *
br_job(uint64 id, int body)
{
    Tube *t = make_tube("r");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'r', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int64
br_sumresv(Wal *w)
{
    int64 s = 0;
    for (File *f = w->head; f; f = f->next) s += f->resv;
    return s;
}

// Files whose reserved plus free bytes no longer add up to the capacity
// a fresh binlog has. Redistribution moves bytes between the two columns
// of one file and between files; it never changes a file's capacity.
static int
br_wrong_capacity(Wal *w)
{
    int n = 0;
    int cap = w->filesize - (int)sizeof(int);
    for (File *f = w->head; f; f = f->next)
        if (f->resv + f->free != cap) n++;
    return n;
}

static int
br_files(Wal *w)
{
    int n = 0;
    for (File *f = w->head; f && n < 100000; f = f->next) n++;
    return n;
}

static void
br_reserve_many(Wal *w, int n, int body)
{
    for (int i = 0; i < n; i++) {
        Job *j = br_job(400 + (uint64)i, body);
        assertf(walresvput(w, j) > 0, "setup: reservation %d failed", i);
    }
}


// The tail is the file the correction pulls bytes from. When the file
// being corrected is the tail itself, "move c bytes from the tail to b"
// is a move from a file to itself, and the invariant it was supposed to
// establish is reported as established without a single byte changing.
void
cttest_balancerest_leaves_the_tail_a_whole_number_of_delete_slots(void)
{
    br_setup();
    Wal w;
    Job list;
    br_wal(&w, &list);
    Job *first = br_job(301, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = br_job(302, 3900);

    assertf(walresvput(&w, second) > 0, "setup: the second reservation must succeed");

    assertf(w.tail->resv % br_z() == 0,
            "the last file's reservation must be a whole number of %d-byte "
            "delete slots, it holds %d bytes", br_z(), w.tail->resv);
}


// Redistribution never creates or destroys reserved bytes. The wal total
// and the per-file totals are two views of one number.
void
cttest_balancerest_conserves_the_reserved_bytes_it_redistributes(void)
{
    br_setup();
    Wal w;
    Job list;
    br_wal(&w, &list);

    br_reserve_many(&w, 12, 900);

    assertf(br_sumresv(&w) == w.resv,
            "the files hold %" PRId64 " reserved bytes while the wal says %"
            PRId64, br_sumresv(&w), w.resv);
}


// Nor does it change any file's capacity: bytes move between the
// reserved and free columns of a file, and between files, but the sum
// per file is fixed at what a fresh binlog holds.
void
cttest_balancerest_keeps_every_file_at_its_original_capacity(void)
{
    br_setup();
    Wal w;
    Job list;
    br_wal(&w, &list);

    br_reserve_many(&w, 12, 900);

    assertf(br_wrong_capacity(&w) == 0,
            "%d file(s) no longer account for their whole %d-byte "
            "capacity", br_wrong_capacity(&w), w.filesize - (int)sizeof(int));
}


// The walk is linear in the number of files and must terminate on a
// chain of any length a real directory can reach. A recursion that
// forgets to advance runs out of stack instead of returning.
void
cttest_balancerest_walks_a_long_file_chain_and_returns(void)
{
    br_setup();
    Wal w;
    Job list;
    br_wal(&w, &list);

    br_reserve_many(&w, 60, 3900);

    assertf(br_files(&w) == w.nfile,
            "the chain the walk traversed holds %d files while nfile says "
            "%d", br_files(&w), w.nfile);
}
