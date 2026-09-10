// Angry tests for usenext (walg.c) — the rotation from one binlog to the
// next.
//
// Rotation is the last moment anything can reach the outgoing file: once
// filewclose drops the descriptor, no later fsync can touch it, so every
// record staged into it under group commit has to be committed first.
// The other half of its contract is what it must NOT do — "there is no
// next file yet" is a reservation-path condition, not an I/O failure.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
un_setup(void)
{
    fault_clear_all();
    progname = "testwal_usenext";
    now = 0;
}

static void
un_wal(Wal *w, Job *l, int durable)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    w->durable_sync = durable;
}

static Job *
un_job(uint64 id, int body)
{
    Tube *t = make_tube("u");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'u', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

// One job's whole wal life: reserve for a put and its delete, stage the
// full record, then stage the delete. That spends the reservation
// exactly, which is what lets the current file's reservation reach zero
// and a rotation become possible.
static int
un_cycle(Wal *w, uint64 id)
{
    Job *j = un_job(id, 12);
    int n = walresvput(w, j);
    assertf(n > 0, "setup: reservation for job %" PRIu64 " failed", id);
    j->walresv = n;
    assertf(walwrite(w, j) != 0, "setup: full record for %" PRIu64, id);
    j->r.state = Invalid;
    assertf(walwrite(w, j) != 0, "setup: delete record for %" PRIu64, id);
    return n;
}

// Stages jobs until the current file can no longer hold one more
// reservation, so the very next one has to rotate.
static void
un_fill_to_the_brim(Wal *w)
{
    int n = un_cycle(w, 1000);
    for (int i = 1; i < 500 && w->cur->free >= n; i++)
        un_cycle(w, 1000 + (uint64)i);
    assertf(w->cur->free < n,
            "setup: the current file still has %d free bytes", w->cur->free);
}

static int
un_wopen(Wal *w)
{
    int n = 0;
    for (File *f = w->head; f; f = f->next) n += f->iswopen ? 1 : 0;
    return n;
}


// A rotation that has to commit the outgoing file and cannot must take
// the wal down: the records staged into that file were truncated away by
// the commit rollback, and nobody may be acked for them.
void
cttest_usenext_disables_the_wal_when_the_rotation_commit_fails(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 1);
    un_fill_to_the_brim(&w);
    Job *j = un_job(2001, 12);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 1, EIO);

    walresvput(&w, j);

    assertf(w.use == 0,
            "the rotation commit failed and its staged records are gone, "
            "so the wal must be disabled; use is %d", w.use);
}


// ...and the reservation that triggered the rotation must be refused, so
// the put never gets as far as being acked.
void
cttest_usenext_refuses_the_reservation_when_the_rotation_commit_fails(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 1);
    un_fill_to_the_brim(&w);
    Job *j = un_job(2002, 12);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 1, EIO);

    int r = walresvput(&w, j);

    assertf(r == 0,
            "a reservation whose rotation failed cannot be honoured, "
            "walresvput returned %d", r);
}


// The tick has to be poisoned too. Records staged into the old file
// earlier in this same tick were ftruncated away by the rollback, and the
// group commit is the only thing that can stop them being acked.
void
cttest_usenext_poisons_the_tick_when_the_rotation_commit_fails(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 1);
    un_fill_to_the_brim(&w);
    Job *j = un_job(2003, 12);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 1, EIO);
    walresvput(&w, j);

    int c = walcommit(&w);

    assertf(c == 0,
            "the tick whose staged records were rolled back must fail its "
            "group commit, walcommit returned %d", c);
}


// A successful rotation moves to the file that follows the outgoing one,
// not to the head and not to some other end of the list.
void
cttest_usenext_advances_the_current_file_to_its_successor(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 0);
    un_fill_to_the_brim(&w);
    int before = w.cur->seq;

    un_cycle(&w, 2004);

    assertf(w.cur->seq == before + 1,
            "the rotation must land on the next file in the list: cur went "
            "from binlog.%d to binlog.%d", before, w.cur->seq);
}


// Only the current file is open for writing. Rotations that forget to
// close the outgoing file accumulate one descriptor per rotation, and a
// busy wal rotates constantly.
void
cttest_usenext_leaves_only_the_current_binlog_open_for_writing(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 0);
    un_fill_to_the_brim(&w);
    un_cycle(&w, 2005);
    un_fill_to_the_brim(&w);
    un_cycle(&w, 2006);

    assertf(un_wopen(&w) == 1,
            "after two rotations exactly one binlog may still be open for "
            "writing, %d are", un_wopen(&w));
}


// "There is no next file" changes nothing at all: the current file stays
// current, and nothing may be left pointing at a successor that does not
// exist.
void
cttest_usenext_leaves_the_current_file_alone_with_no_successor(void)
{
    un_setup();
    Wal w;
    Job list;
    un_wal(&w, &list, 0);
    Job *j = un_job(2007, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: reservation must succeed");
    assertf(w.cur == w.tail && w.cur->next == NULL,
            "setup: there must be no file to rotate into");
    File *before = w.cur;
    w.cur->resv = 0;

    walwrite(&w, j);

    assertf(w.cur == before,
            "a refused rotation must leave the current file exactly where "
            "it was");
}


