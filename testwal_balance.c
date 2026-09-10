// Angry tests for balance (walg.c) — the three reservation invariants
// that keep a delete record's slot available in every file.
//
// The invariants are stated in the source and checked nowhere. They
// exist so that every job whose full record lives in file X can always
// have its delete record written somewhere, which is what lets X ever be
// reaped. A wal that quietly violates them looks completely healthy
// right up to the point where compaction stops reclaiming anything.

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
bl_setup(void)
{
    fault_clear_all();
    progname = "testwal_balance";
    now = 0;
}

// The delete-record slot size: the modulus every reservation invariant is
// stated in terms of.
static int
bl_z(void)
{
    return (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32));
}

static void
bl_wal(Wal *w, Job *l)
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
bl_job(uint64 id, int body)
{
    Tube *t = make_tube("b");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'b', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int64
bl_sumresv(Wal *w)
{
    int64 s = 0;
    for (File *f = w->head; f; f = f->next) s += f->resv;
    return s;
}

// Number of files after the current one whose reservation is not a whole
// number of delete slots — invariant 3, counted directly.
static int
bl_uneven_after_cur(Wal *w)
{
    int n = 0;
    int started = 0;
    for (File *f = w->head; f; f = f->next) {
        if (started && f->resv % bl_z() != 0) n++;
        if (f == w->cur) started = 1;
    }
    return n;
}

// One job's whole wal life, spending its reservation exactly.
static int
bl_cycle(Wal *w, uint64 id)
{
    Job *j = bl_job(id, 12);
    int n = walresvput(w, j);
    assertf(n > 0, "setup: reservation for job %" PRIu64 " failed", id);
    j->walresv = n;
    assertf(walwrite(w, j) != 0, "setup: full record for %" PRIu64, id);
    j->r.state = Invalid;
    assertf(walwrite(w, j) != 0, "setup: delete record for %" PRIu64, id);
    return n;
}

static void
bl_fill_to_the_brim(Wal *w)
{
    int n = bl_cycle(w, 3000);
    for (int i = 1; i < 500 && w->cur->free >= n; i++)
        bl_cycle(w, 3000 + (uint64)i);
    assertf(w->cur->free < n,
            "setup: the current file still has %d free bytes", w->cur->free);
}


// Invariant 1: after a reservation the current file holds at least what
// was asked for. Everything downstream — walwrite's rotation check, the
// delete slot, the tail accounting — assumes it.
void
cttest_balance_leaves_the_current_file_holding_at_least_the_request(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(201, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = bl_job(202, 3900);

    int n = walresvput(&w, second);
    assertf(n > 0, "setup: the second reservation must succeed");

    assertf(w.cur->resv >= n,
            "the current file must hold the whole reservation: it has %d "
            "reserved bytes against a request of %d", w.cur->resv, n);
}


// Invariant 2: the current file's surplus over the request is a whole
// number of delete slots, so the next delete always has somewhere to go.
void
cttest_balance_leaves_the_current_surplus_a_whole_number_of_delete_slots(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(203, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = bl_job(204, 3900);

    int n = walresvput(&w, second);
    assertf(n > 0, "setup: the second reservation must succeed");

    assertf((w.cur->resv - n) % bl_z() == 0,
            "the surplus over the request must be a whole number of %d-byte "
            "delete slots: resv is %d against a request of %d",
            bl_z(), w.cur->resv, n);
}


// Invariant 3: every file behind the current one holds a whole number of
// delete slots and nothing else. A remainder stranded there is space
// that can never be used for the record it was set aside for.
void
cttest_balance_leaves_every_later_file_a_whole_number_of_delete_slots(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(205, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = bl_job(206, 3900);

    assertf(walresvput(&w, second) > 0, "setup: the second reservation must succeed");

    assertf(bl_uneven_after_cur(&w) == 0,
            "%d file(s) after the current one hold a reservation that is "
            "not a whole number of %d-byte delete slots",
            bl_uneven_after_cur(&w), bl_z());
}


// Reserved space is only ever moved between files, never created or
// destroyed. The per-file counters and the wal total are two views of the
// same bytes and must agree after every reservation.
void
cttest_balance_conserves_the_reserved_bytes_it_redistributes(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(207, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = bl_job(208, 3900);

    assertf(walresvput(&w, second) > 0, "setup: the second reservation must succeed");

    assertf(bl_sumresv(&w) == w.resv,
            "the per-file reservations must add up to the wal total: files "
            "hold %" PRId64 " bytes, the wal says %" PRId64,
            bl_sumresv(&w), w.resv);
}


// The rotation loop is documented to run at most once per reservation.
// Twice would skip a file that still had reserved space in it.
void
cttest_balance_rotates_at_most_once_for_one_reservation(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    bl_fill_to_the_brim(&w);
    int before = w.cur->seq;

    bl_cycle(&w, 3900);

    assertf(w.cur->seq == before + 1,
            "one reservation may advance the current file by one at most: "
            "cur went from binlog.%d to binlog.%d", before, w.cur->seq);
}


// A reservation that fails part-way through the redistribution must
// leave the total exactly as it found it. Bytes stranded as reserved are
// invisible, permanent, and depress the compaction metric forever.
void
cttest_balance_restores_the_reserved_total_when_the_wal_cannot_grow(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(209, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    int64 before = w.resv;
    Job *second = bl_job(210, 3900);
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


// ...and the per-file view must be restored with it, or the two
// accountings drift apart with nothing to notice.
void
cttest_balance_keeps_the_two_accountings_in_step_after_a_failure(void)
{
    bl_setup();
    Wal w;
    Job list;
    bl_wal(&w, &list);
    Job *first = bl_job(211, 3900);
    assertf(walresvput(&w, first) > 0, "setup: the first reservation must fit");
    Job *second = bl_job(212, 3900);
    // Refuse every open from here on: the point under test is the
    // rollback after growth is denied, not how many files the wal
    // tries before it gives up.
    fault_set(FAULT_OPEN, 0, EACCES);

    int r = walresvput(&w, second);
    assertf(r == 0, "setup: the reservation must fail, it returned %d", r);

    assertf(bl_sumresv(&w) == w.resv,
            "after the rollback the files hold %" PRId64 " reserved bytes "
            "while the wal says %" PRId64, bl_sumresv(&w), w.resv);
}
