// Angry tests for walresvput (walg.c) — the space a put has to book
// before the client can be told anything.
//
// The number is a sum of six terms spread across three functions that
// are edited independently (walresvput, walresvmigrate, walresvupdate).
// A term added to one and forgotten in the others produces a reservation
// that is a few bytes short, which shows up as a wal that overruns a file
// under load and never in a test that only checks the happy path.

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
rp_setup(void)
{
    fault_clear_all();
    progname = "testwal_walresvput";
    now = 0;
}

static void
rp_wal(Wal *w, Job *l, int filesize)
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
rp_job(const char *tubename, uint64 id, int body)
{
    Tube *t = make_tube(tubename);
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'p', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static void
rp_longname(char *buf, size_t n, int len)
{
    assertf((size_t)len < n, "setup: name buffer too small");
    memset(buf, 'x', (size_t)len);
    buf[len] = '\0';
}


// A put books a full record and the delete record that will one day
// retire it: two lengths, two job records, two checksums, the tube name
// and the body.
void
cttest_walresvput_books_a_full_record_and_its_delete_record(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    int body = 12;
    Job *j = rp_job("pp", 901, body);
    int namelen = j->tube->name_len;
    int expected = (int)(2 * sizeof(int) + 2 * sizeof(Jobrec)
                       + 2 * sizeof(uint32)) + namelen + body;

    int n = walresvput(&w, j);

    assertf(n == expected,
            "a put must book %d bytes for a %d-byte body in a %d-byte tube "
            "name, it booked %d", expected, body, namelen, n);
}


// The reservation tracks the tube name byte for byte. A constant, a
// rounded-up bound or a forgotten term all show up here and nowhere
// else.
void
cttest_walresvput_scales_byte_for_byte_with_the_tube_name(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    char longname[MAX_TUBE_NAME_LEN];
    rp_longname(longname, sizeof longname, MAX_TUBE_NAME_LEN - 1);
    Job *shortnamed = rp_job("x", 902, 10);
    Job *longnamed = rp_job(longname, 903, 10);
    int gap = longnamed->tube->name_len - shortnamed->tube->name_len;

    int a = walresvput(&w, shortnamed);
    int b = walresvput(&w, longnamed);

    assertf(b - a == gap,
            "a tube name %d bytes longer must cost %d bytes more: %d "
            "against %d", gap, gap, b, a);
}


// ...and byte for byte with the body.
void
cttest_walresvput_scales_byte_for_byte_with_the_body(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 8192);
    int small = 10;
    int large = 1000;
    Job *a_job = rp_job("y", 904, small);
    Job *b_job = rp_job("y", 905, large);

    int a = walresvput(&w, a_job);
    int b = walresvput(&w, b_job);

    assertf(b - a == large - small,
            "a body %d bytes longer must cost %d bytes more: %d against %d",
            large - small, large - small, b, a);
}


// The strongest form of the promise: the reservation is exactly what the
// writer goes on to spend. Reserve for a put, write the full record and
// then its delete record, and the file's reservation must be back where
// it started — not a byte over, not a byte short.
void
cttest_walresvput_books_exactly_what_the_writer_spends(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    Job *j = rp_job("z", 906, 12);
    int before = w.cur->resv;
    int n = walresvput(&w, j);
    assertf(n > 0, "setup: the reservation must succeed");
    j->walresv = n;

    assertf(walwrite(&w, j) != 0, "setup: the full record must stage");
    assertf(walwrite(&w, j) != 0, "setup: the delete record must stage");

    assertf(w.cur->resv == before,
            "a full record plus its delete record must spend the whole "
            "reservation: the file still holds %d of the %d reserved bytes",
            w.cur->resv - before, n);
}


// A body that pushes the total past what an int can hold must be
// refused, not truncated into a small positive reservation that the
// writer then overruns.
void
cttest_walresvput_refuses_a_total_that_would_overflow_an_int(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    Job *j = rp_job("q", 907, 12);
    j->r.body_size = INT_MAX;

    int r = walresvput(&w, j);

    assertf(r == 0,
            "a body of INT_MAX cannot be booked alongside two job records, "
            "so the put must be refused; it returned %d", r);
}


// ...and the refusal must not have booked anything on the way to
// deciding.
void
cttest_walresvput_books_nothing_for_an_overflowing_total(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    Job *j = rp_job("q", 908, 12);
    j->r.body_size = INT_MAX;
    int64 before = w.resv;

    walresvput(&w, j);

    assertf(w.resv == before,
            "a refused put books nothing: resv went from %" PRId64 " to %"
            PRId64, before, w.resv);
}


// A record that no binlog could ever hold must be refused rather than
// accepted into a file that does not have the room.
void
cttest_walresvput_refuses_a_record_larger_than_a_whole_binlog(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 1024);
    Job *j = rp_job("q", 909, 2000);

    int r = walresvput(&w, j);

    assertf(r == 0,
            "no 1024-byte binlog can hold this record, so the put must be "
            "refused; it returned %d", r);
}


// The put reservation is the migration reservation plus the update
// reservation, exactly. The three constants live in three functions and
// nothing else ties them together.
void
cttest_walresvput_exceeds_an_update_reservation_by_one_full_record(void)
{
    rp_setup();
    Wal w;
    Job list;
    rp_wal(&w, &list, 4096);
    int body = 12;
    Job *j = rp_job("pp", 910, body);
    int namelen = j->tube->name_len;
    int fullrecord = (int)(sizeof(int) + sizeof(Jobrec) + sizeof(uint32))
                   + namelen + body;

    int put = walresvput(&w, j);
    int update = walresvupdate(&w);

    assertf(put - update == fullrecord,
            "a put is one delete slot plus one full record (%d bytes): the "
            "put booked %d and the update booked %d",
            fullrecord, put, update);
}
