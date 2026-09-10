// Angry tests for walresvreturn (walg.c) — the other half of every
// reservation.
//
// Reserved bytes that are never returned are invisible: nothing
// recomputes w->resv, nothing warns, and the only symptom is that
// ratio() reads a wal as fuller than it is and compaction gradually
// stops. The function returns void, so a caller that hands back n has no
// way to learn that only part of it came home.

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
rr_setup(void)
{
    fault_clear_all();
    progname = "testwal_walresvreturn";
    now = 0;
}

static void
rr_wal(Wal *w, Job *l)
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
rr_job(uint64 id, int body)
{
    Tube *t = make_tube("t");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 't', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int64
rr_sumresv(Wal *w)
{
    int64 s = 0;
    for (File *f = w->head; f; f = f->next) s += f->resv;
    return s;
}


// The simplest round trip there is: what reserve took, this gives back.
void
cttest_walresvreturn_restores_the_wal_total_after_a_round_trip(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1101, 100);
    int64 before = w.resv;
    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    walresvreturn(&w, n);

    assertf(w.resv == before,
            "returning the whole reservation must restore the total: resv "
            "went from %" PRId64 " to %" PRId64, before, w.resv);
}


// ...down to the current file's free space.
void
cttest_walresvreturn_restores_the_current_file_free_space(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1102, 100);
    int before = w.cur->free;
    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    walresvreturn(&w, n);

    assertf(w.cur->free == before,
            "the bytes must go back to free space: it went from %d to %d",
            before, w.cur->free);
}


// ...and its reserved space.
void
cttest_walresvreturn_restores_the_current_file_reservation(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1103, 100);
    int before = w.cur->resv;
    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");

    walresvreturn(&w, n);

    assertf(w.cur->resv == before,
            "the file must stop holding the returned bytes: resv went from "
            "%d to %d", before, w.cur->resv);
}


// Returning nothing changes nothing.
void
cttest_walresvreturn_ignores_a_zero_byte_request(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1104, 100);
    assertf(walresvput(&w, j) > 0, "setup: the reservation must succeed");
    int64 before = w.resv;

    walresvreturn(&w, 0);

    assertf(w.resv == before,
            "a zero-byte return is a no-op: resv went from %" PRId64 " to %"
            PRId64, before, w.resv);
}


// A negative return would be a reservation in disguise. It must be
// refused outright rather than added to a file's reserved column.
void
cttest_walresvreturn_ignores_a_negative_request(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1105, 100);
    assertf(walresvput(&w, j) > 0, "setup: the reservation must succeed");
    int before = w.cur->free;

    walresvreturn(&w, -1000);

    assertf(w.cur->free == before,
            "a negative return must not move a single byte: free went from "
            "%d to %d", before, w.cur->free);
}


// After the wal has grown, a reservation's bytes no longer all sit on
// the current file. Handing them back must still return every one of
// them: what cannot be found on cur or tail is silently dropped today,
// and w->resv overstates the wal from then on.
void
cttest_walresvreturn_gives_every_reserved_byte_back_across_a_growth(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *first = rr_job(1106, 3900);
    int a = walresvput(&w, first);
    assertf(a > 0, "setup: the first reservation must fit");
    Job *second = rr_job(1107, 3900);
    int b = walresvput(&w, second);
    assertf(b > 0, "setup: the second reservation must succeed");

    walresvreturn(&w, a);
    walresvreturn(&w, b);

    assertf(w.resv == 0,
            "every reserved byte was handed back, so the wal must hold "
            "none: resv is %" PRId64, w.resv);
}


// Whatever it manages to return, the two accountings must still describe
// the same bytes afterwards.
void
cttest_walresvreturn_keeps_the_two_accountings_in_step(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *first = rr_job(1108, 3900);
    int a = walresvput(&w, first);
    assertf(a > 0, "setup: the first reservation must fit");
    Job *second = rr_job(1109, 3900);
    int b = walresvput(&w, second);
    assertf(b > 0, "setup: the second reservation must succeed");

    walresvreturn(&w, a);
    walresvreturn(&w, b);

    assertf(rr_sumresv(&w) == w.resv,
            "the files hold %" PRId64 " reserved bytes while the wal says %"
            PRId64, rr_sumresv(&w), w.resv);
}


// A disabled wal abandons the return entirely. Whatever that costs, it
// must not leave the per-file counters and the wal total disagreeing.
void
cttest_walresvreturn_leaves_a_disabled_wal_self_consistent(void)
{
    rr_setup();
    Wal w;
    Job list;
    rr_wal(&w, &list);
    Job *j = rr_job(1110, 100);
    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");
    w.use = 0;

    walresvreturn(&w, n);

    assertf(rr_sumresv(&w) == w.resv,
            "the files hold %" PRId64 " reserved bytes while the wal says %"
            PRId64, rr_sumresv(&w), w.resv);
}
