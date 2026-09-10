// Angry tests for walresvmigrate (walg.c) — the space one job needs when
// compaction rewrites it into the current binlog.
//
// It books a full record and nothing else, because the delete record for
// that job was already booked when it was put. That makes it the third
// copy of the same arithmetic (walresvput, walresvupdate, this) with
// nothing tying the three together, and the only caller is a migration
// nobody watches.

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

// The tube name and body the seeded records carry, which together fix
// the exact size of the record a migration has to book for.
static const char rm_tube[] = "g";
enum { rm_body = 8 };

static void
rm_setup(void)
{
    fault_clear_all();
    progname = "testwal_walresvmigrate";
    now = 0;
}

static void
rm_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static void
rm_seed(int seq, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    rm_write(fd, &ver, sizeof ver);

    int nl = (int)strlen(rm_tube);
    char body[rm_body];
    memset(body, 'g', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 8;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 4;
    jr.state = Ready;

    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &nl, sizeof nl);
    c = wal_crc32c(c, rm_tube, (size_t)nl);
    c = wal_crc32c(c, &jr, sizeof jr);
    c = wal_crc32c(c, body, sizeof body);
    c ^= WAL_CRC32C_XOR;
    unsigned char tr[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };

    rm_write(fd, &nl, sizeof nl);
    rm_write(fd, rm_tube, (size_t)nl);
    rm_write(fd, &jr, sizeof jr);
    rm_write(fd, body, sizeof body);
    rm_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
rm_wal(Wal *w, Job *l, int old)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    for (int i = 1; i <= old; i++) rm_seed(i, 1200 + (uint64)i);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == old + 1, "setup: expected %d files, got %d",
            old + 1, w->nfile);
    now = 1000000;
}

static Job *
rm_find(Job *l, uint64 id)
{
    for (Job *j = l->next; j != l; j = j->next)
        if (j->r.id == id) return j;
    return NULL;
}


// The booking is exactly one full record: a length, the tube name, a job
// record, the body and a checksum. Nothing rounded, nothing padded, and
// no second delete slot on top of the one the put already booked.
void
cttest_walresvmigrate_books_exactly_one_full_record(void)
{
    rm_setup();
    Wal w;
    Job list;
    rm_wal(&w, &list, 2);
    int record = (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32))
               + (int)strlen(rm_tube) + rm_body;
    int before = w.cur->free;

    walmaint(&w);
    assertf(w.nmig == 1, "setup: exactly one job must have migrated");

    assertf(before - w.cur->free == record,
            "the migration must book the %d bytes its record occupies, it "
            "booked %d", record, before - w.cur->free);
}


// The booking is spent to the byte by the write it was made for: after a
// successful migration the current file holds no leftover reservation
// from it.
void
cttest_walresvmigrate_books_exactly_what_the_migrating_write_spends(void)
{
    rm_setup();
    Wal w;
    Job list;
    rm_wal(&w, &list, 2);
    int before = w.cur->resv;

    walmaint(&w);
    assertf(w.nmig == 1, "setup: exactly one job must have migrated");

    assertf(w.cur->resv == before,
            "the migrated record must spend its whole booking: the file "
            "still holds %d leftover reserved bytes", w.cur->resv - before);
}


// A replayed record can carry any body size that got past the reader. A
// size whose record cannot be expressed as an int must be refused, not
// narrowed into a small — or negative — reservation the writer then
// overruns.
void
cttest_walresvmigrate_refuses_a_record_whose_size_overflows_an_int(void)
{
    rm_setup();
    Wal w;
    Job list;
    rm_wal(&w, &list, 2);
    Job *j = rm_find(&list, 1201);
    assertf(j != NULL, "setup: the head file's job must be replayed");
    j->r.body_size = INT_MAX;
    int64 before = w.resv;

    walmaint(&w);

    assertf(w.resv == before,
            "a record that cannot be sized must not be booked at all: resv "
            "went from %" PRId64 " to %" PRId64, before, w.resv);
}


// ...and the migration must not be counted as done either, or compaction
// believes it reclaimed a file it never touched.
void
cttest_walresvmigrate_migrates_nothing_it_could_not_book(void)
{
    rm_setup();
    Wal w;
    Job list;
    rm_wal(&w, &list, 2);
    Job *j = rm_find(&list, 1201);
    assertf(j != NULL, "setup: the head file's job must be replayed");
    j->r.body_size = INT_MAX;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_WRITEV) == 0,
            "nothing was booked, so nothing may be written: %d writev() "
            "call(s) escaped", fault_calls(FAULT_WRITEV));
}


// A migration books a full record; a put books that same full record
// plus a delete slot. The two constants live in different functions and
// are edited separately, so the relation between them is the thing worth
// pinning.
void
cttest_walresvmigrate_is_a_put_reservation_without_the_delete_slot(void)
{
    rm_setup();
    Wal w;
    Job list;
    rm_wal(&w, &list, 2);
    Job *j = rm_find(&list, 1201);
    assertf(j != NULL, "setup: the head file's job must be replayed");
    int before = w.cur->free;

    walmaint(&w);
    assertf(w.nmig == 1, "setup: exactly one job must have migrated");
    int migrated = before - w.cur->free;

    Job *twin = allocate_job(rm_body);
    assertf(twin != NULL, "setup: allocate_job");
    TUBE_ASSIGN(twin->tube, j->tube);
    twin->r.body_size = rm_body;
    int put = walresvput(&w, twin);
    int update = walresvupdate(&w);
    assertf(put > 0 && update > 0, "setup: both reservations must succeed");

    assertf(migrated == put - update,
            "a migration books a put minus a delete slot: it booked %d "
            "against %d - %d", migrated, put, update);
}
