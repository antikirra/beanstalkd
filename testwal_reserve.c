// Angry tests for reserve (walg.c) — the accountant behind every public
// reservation call.
//
// Its contract is unusually precise for this file: exactly n or exactly
// 0, with every counter it touched restored on failure. A reservation
// that half-applies is invisible immediately and permanent afterwards,
// because nothing ever recomputes w->resv from the files.

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
rs_setup(void)
{
    fault_clear_all();
    progname = "testwal_reserve";
    now = 0;
}

static void
rs_wal(Wal *w, Job *l, int filesize)
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
rs_job(uint64 id, int body)
{
    Tube *t = make_tube("s");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 's', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int64
rs_sumresv(Wal *w)
{
    int64 s = 0;
    for (File *f = w->head; f; f = f->next) s += f->resv;
    return s;
}


// The fast path takes the bytes out of the current file's free space,
// all of them and no more.
void
cttest_reserve_takes_the_requested_bytes_out_of_the_free_space(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *j = rs_job(701, 100);
    int before = w.cur->free;

    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    assertf(w.cur->free == before - n,
            "the reservation must cost exactly what it returned: free went "
            "from %d to %d for a reservation of %d", before, w.cur->free, n);
}


// ...and puts the same bytes into the file's reserved column.
void
cttest_reserve_puts_the_requested_bytes_into_the_reserved_space(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *j = rs_job(702, 100);
    int before = w.cur->resv;

    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    assertf(w.cur->resv == before + n,
            "the file's reservation must grow by exactly what was "
            "returned: resv went from %d to %d for a reservation of %d",
            before, w.cur->resv, n);
}


// The wal-wide total has to move by the same amount, or the number
// ratio() reads stops describing the files it is computed against.
void
cttest_reserve_grows_the_wal_total_by_exactly_what_it_returned(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *j = rs_job(703, 100);
    int64 before = w.resv;

    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    assertf(w.resv == before + n,
            "the wal total must grow by exactly the reservation: it went "
            "from %" PRId64 " to %" PRId64 " for %d bytes",
            before, w.resv, n);
}


// A request that cannot fit in any binlog the wal is allowed to create
// must be refused. Accepting it is how a file ends up owing space and
// how every later reservation creates yet another binlog.
void
cttest_reserve_refuses_a_request_no_binlog_could_hold(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 1024);
    Job *j = rs_job(704, 2000);

    int r = walresvput(&w, j);

    assertf(r == 0,
            "the record is larger than a whole binlog, so the reservation "
            "must be refused; it returned %d", r);
}


// ...and a refused request must not have moved the total.
void
cttest_reserve_takes_no_bytes_for_a_request_it_cannot_honour(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 1024);
    Job *j = rs_job(705, 2000);
    int64 before = w.resv;

    walresvput(&w, j);

    assertf(w.resv == before,
            "a refused reservation must leave the total alone: resv went "
            "from %" PRId64 " to %" PRId64, before, w.resv);
}


// Failing part-way through the slow path is the dangerous case: the
// bytes have already been booked against the tail by then, and the undo
// is the only thing that takes them back.
void
cttest_reserve_restores_the_total_when_the_wal_cannot_grow(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *first = rs_job(706, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    int64 before = w.resv;
    Job *second = rs_job(707, 3900);
    // Refuse every open from here on: the point under test is the
    // rollback after growth is denied, not how many files the wal
    // tries before it gives up.
    fault_set(FAULT_OPEN, 0, EACCES);

    int r = walresvput(&w, second);
    assertf(r == 0, "setup: the reservation must fail, it returned %d", r);

    assertf(w.resv == before,
            "a failed reservation must restore the total: resv went from %"
            PRId64 " to %" PRId64, before, w.resv);
}


// ...and the tail's own free space with it, or the wal believes a file
// is fuller than it is for the rest of its life.
void
cttest_reserve_restores_the_tail_free_space_when_the_wal_cannot_grow(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *first = rs_job(708, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = rs_job(709, 3900);
    // Refuse every open from here on: the point under test is the
    // rollback after growth is denied, not how many files the wal
    // tries before it gives up.
    fault_set(FAULT_OPEN, 0, EACCES);
    int r = walresvput(&w, second);
    assertf(r == 0, "setup: the reservation must fail, it returned %d", r);

    assertf(rs_sumresv(&w) == w.resv,
            "after the rollback the files hold %" PRId64 " reserved bytes "
            "while the wal says %" PRId64, rs_sumresv(&w), w.resv);
}


// Reserve and give back is a round trip. Any drift is a reservation leak
// that compaction reads as live space forever.
void
cttest_reserve_round_trips_with_the_matching_return(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *j = rs_job(710, 100);
    int64 before = w.resv;
    int free_before = w.cur->free;

    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");
    walresvreturn(&w, n);

    assertf(w.resv == before && w.cur->free == free_before,
            "reserving and returning n bytes must leave every counter "
            "where it was: resv %" PRId64 " -> %" PRId64 ", free %d -> %d",
            before, w.resv, free_before, w.cur->free);
}


// A disabled wal reserves nothing at all, whatever it returns.
void
cttest_reserve_takes_no_bytes_on_a_disabled_wal(void)
{
    rs_setup();
    Wal w;
    Job list;
    rs_wal(&w, &list, 4096);
    Job *j = rs_job(711, 100);
    w.use = 0;
    int64 before = w.resv;

    walresvput(&w, j);
    walresvupdate(&w);

    assertf(w.resv == before,
            "a disabled wal must not book anything: resv went from %" PRId64
            " to %" PRId64, before, w.resv);
}
