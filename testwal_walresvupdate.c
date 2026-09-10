// Angry tests for walresvupdate (walg.c) — the space one state change or
// delete needs.
//
// The same number appears twice in this file: here, as a sum of three
// sizeofs, and in balancerest, as the modulus z that every per-file
// reservation is kept congruent to. Nothing ties the two together, and a
// mismatch between them silently breaks the invariant that guarantees a
// delete can always be logged.

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
ru_setup(void)
{
    fault_clear_all();
    progname = "testwal_walresvupdate";
    now = 0;
}

static void
ru_wal(Wal *w, Job *l)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
}

// Books delete slots until the wal has had to grow, so the congruence
// machinery in balance/balancerest has actually run.
static void
ru_grow(Wal *w)
{
    for (int i = 0; i < 500; i++) {
        if (w->nfile > 1) return;
        if (walresvupdate(w) == 0) return;
    }
    assertf(0, "setup: the wal neither grew nor refused a reservation");
}

// Books delete slots until the current file cannot hold one more, so the
// very next reservation has to grow the wal.
static void
ru_fill_to_the_brim(Wal *w)
{
    int z = walresvupdate(w);
    assertf(z > 0, "setup: the first reservation must succeed");
    for (int i = 1; i < 500 && w->cur->free >= z; i++)
        assertf(walresvupdate(w) > 0, "setup: reservation %d failed", i);
    assertf(w->cur->free < z,
            "setup: the current file still has %d free bytes", w->cur->free);
}

// Files whose reservation is not a whole number of `z`-byte slots.
static int
ru_uneven(Wal *w, int z)
{
    int n = 0;
    for (File *f = w->head; f; f = f->next)
        if (f->resv % z != 0) n++;
    return n;
}


// One delete record is a length, a job record and a checksum. Nothing
// else, and none of them optional.
void
cttest_walresvupdate_books_exactly_one_delete_record(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    int expected = (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32));

    int n = walresvupdate(&w);

    assertf(n == expected,
            "a delete slot is %d bytes, walresvupdate booked %d",
            expected, n);
}


// The bytes come out of the current file, all of them.
void
cttest_walresvupdate_takes_its_bytes_from_the_current_file(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    int before = w.cur->free;

    int n = walresvupdate(&w);
    assertf(n > 0, "setup: the reservation must succeed");

    assertf(w.cur->free == before - n,
            "the delete slot must cost the current file exactly %d bytes: "
            "free went from %d to %d", n, before, w.cur->free);
}


// The size this call returns is the modulus every per-file reservation
// is kept congruent to. If the two ever disagree, files end up holding
// remainders no delete record can use.
void
cttest_walresvupdate_matches_the_modulus_the_reservations_are_kept_to(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    int z = walresvupdate(&w);
    assertf(z > 0, "setup: the first reservation must succeed");

    ru_grow(&w);

    assertf(ru_uneven(&w, z) == 0,
            "after a run of %d-byte delete reservations, %d file(s) hold a "
            "reservation that is not a whole number of them",
            z, ru_uneven(&w, z));
}


// A delete that cannot be booked must be refused. Accepting it means a
// job that can never be retired from the log.
void
cttest_walresvupdate_refuses_when_the_wal_cannot_grow(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    ru_fill_to_the_brim(&w);
    fault_set(FAULT_OPEN, 0, EACCES);

    int r = walresvupdate(&w);

    assertf(r == 0,
            "the wal is full and cannot create a file, so the delete slot "
            "must be refused; walresvupdate returned %d", r);
}


// ...and the refusal must leave the accounting exactly where it was.
void
cttest_walresvupdate_books_nothing_when_it_refuses(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    ru_fill_to_the_brim(&w);
    fault_set(FAULT_OPEN, 0, EACCES);
    int64 before = w.resv;

    int r = walresvupdate(&w);
    assertf(r == 0, "setup: the reservation must be refused, it returned %d", r);

    assertf(w.resv == before,
            "a refused delete slot books nothing: resv went from %" PRId64
            " to %" PRId64, before, w.resv);
}


// Under -D a disabled wal must refuse: a delete that cannot be logged is
// a delete the client must not be told about.
void
cttest_walresvupdate_refuses_on_a_disabled_wal_under_durable_mode(void)
{
    ru_setup();
    Wal w;
    Job list;
    ru_wal(&w, &list);
    w.use = 0;
    w.durable_sync = 1;

    int r = walresvupdate(&w);

    assertf(r == 0,
            "a disabled wal under -D must refuse the delete slot, "
            "walresvupdate returned %d", r);
}
