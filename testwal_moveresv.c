// Angry tests for moveresv (walg.c) — four lines that shuffle reserved
// bytes from one file to another.
//
// It validates nothing: not the amount, not the source, not even that
// the two files differ. Everything that keeps it honest lives in its
// callers, so the only way to attack it is through the invariants it is
// supposed to preserve on every reservation the wal ever makes.

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
mv_setup(void)
{
    fault_clear_all();
    progname = "testwal_moveresv";
    now = 0;
}

static int
mv_z(void)
{
    return (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32));
}

static void
mv_wal(Wal *w, Job *l)
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
mv_job(uint64 id, int body)
{
    Tube *t = make_tube("v");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'v', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static void
mv_reserve_many(Wal *w, int n, int body)
{
    for (int i = 0; i < n; i++) {
        Job *j = mv_job(500 + (uint64)i, body);
        assertf(walresvput(w, j) > 0, "setup: reservation %d failed", i);
    }
}

static int64
mv_sumresv(Wal *w)
{
    int64 s = 0;
    for (File *f = w->head; f; f = f->next) s += f->resv;
    return s;
}

static int
mv_wrong_capacity(Wal *w)
{
    int n = 0;
    int cap = w->filesize - (int)sizeof(int);
    for (File *f = w->head; f; f = f->next)
        if (f->resv + f->free != cap) n++;
    return n;
}

static int
mv_negative(Wal *w)
{
    int n = 0;
    for (File *f = w->head; f; f = f->next)
        if (f->resv < 0 || f->free < 0) n++;
    return n;
}

static int
mv_over_capacity(Wal *w)
{
    int n = 0;
    int cap = w->filesize - (int)sizeof(int);
    for (File *f = w->head; f; f = f->next)
        if (f->resv > cap) n++;
    return n;
}

static int
mv_uneven_after_cur(Wal *w)
{
    int n = 0;
    int started = 0;
    for (File *f = w->head; f; f = f->next) {
        if (started && f->resv % mv_z() != 0) n++;
        if (f == w->cur) started = 1;
    }
    return n;
}


// Every move takes n bytes off one file and puts n on another. Across a
// whole run of reservations the per-file totals must still add up to the
// wal's own total, or one of the two is lying.
void
cttest_moveresv_conserves_the_reserved_total_across_a_reservation_run(void)
{
    mv_setup();
    Wal w;
    Job list;
    mv_wal(&w, &list);

    mv_reserve_many(&w, 20, 700);

    assertf(mv_sumresv(&w) == w.resv,
            "the files hold %" PRId64 " reserved bytes while the wal says %"
            PRId64, mv_sumresv(&w), w.resv);
}


// A move changes which column a byte is in, never how many bytes a file
// has. Dropping either half of the pair on either file breaks this the
// first time a reservation has to be rebalanced.
void
cttest_moveresv_keeps_every_file_at_its_original_capacity(void)
{
    mv_setup();
    Wal w;
    Job list;
    mv_wal(&w, &list);

    mv_reserve_many(&w, 20, 700);

    assertf(mv_wrong_capacity(&w) == 0,
            "%d file(s) no longer account for their whole %d-byte capacity",
            mv_wrong_capacity(&w), w.filesize - (int)sizeof(int));
}


// No file may end up owing bytes in either column. A negative count is
// what makes ratio() and every size check downstream meaningless.
void
cttest_moveresv_never_leaves_a_file_owing_reserved_or_free_bytes(void)
{
    mv_setup();
    Wal w;
    Job list;
    mv_wal(&w, &list);

    mv_reserve_many(&w, 20, 700);

    assertf(mv_negative(&w) == 0,
            "%d file(s) hold a negative reserved or free count",
            mv_negative(&w));
}


// ...and none may end up holding more reserved bytes than it physically
// has, which is the same sign error seen from the other side.
void
cttest_moveresv_never_reserves_more_than_a_file_can_hold(void)
{
    mv_setup();
    Wal w;
    Job list;
    mv_wal(&w, &list);

    mv_reserve_many(&w, 20, 700);

    assertf(mv_over_capacity(&w) == 0,
            "%d file(s) claim more reserved bytes than their %d-byte "
            "capacity", mv_over_capacity(&w), w.filesize - (int)sizeof(int));
}


// A move whose source and destination are the same file changes nothing.
// It must not be the move a caller relies on to establish an invariant,
// because the caller then reports success on a chain that is still
// wrong.
void
cttest_moveresv_must_not_pass_a_self_move_off_as_a_redistribution(void)
{
    mv_setup();
    Wal w;
    Job list;
    mv_wal(&w, &list);
    Job *first = mv_job(601, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = mv_job(602, 3900);

    assertf(walresvput(&w, second) > 0, "setup: the second reservation must succeed");

    assertf(mv_uneven_after_cur(&w) == 0,
            "%d file(s) behind the current one were left holding a "
            "remainder that no delete record can ever use",
            mv_uneven_after_cur(&w));
}
