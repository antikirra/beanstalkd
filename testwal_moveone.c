// Angry tests for moveone (walg.c) — the single-job migration that lets
// an old binlog become reapable.
//
// It removes a job from one file and writes it into another, and for the
// window in between the job exists in neither. Everything interesting
// about it is what happens when the write at the end of that window
// fails: whether the only surviving copy is still on disk, and whether
// the space it reserved comes back.

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
mo_setup(void)
{
    fault_clear_all();
    progname = "testwal_moveone";
    now = 0;
}

static void
mo_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static void
mo_seed(int seq, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    mo_write(fd, &ver, sizeof ver);

    const char *tube = "o";
    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'o', sizeof body);
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
    c = wal_crc32c(c, tube, (size_t)nl);
    c = wal_crc32c(c, &jr, sizeof jr);
    c = wal_crc32c(c, body, sizeof body);
    c ^= WAL_CRC32C_XOR;
    unsigned char tr[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };

    mo_write(fd, &nl, sizeof nl);
    mo_write(fd, tube, (size_t)nl);
    mo_write(fd, &jr, sizeof jr);
    mo_write(fd, body, sizeof body);
    mo_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
mo_seeds(int n)
{
    for (int i = 1; i <= n; i++) mo_seed(i, 200 + (uint64)i);
}

static void
mo_wal(Wal *w, Job *l, int old)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    mo_seeds(old);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == old + 1, "setup: expected %d files, got %d",
            old + 1, w->nfile);
    now = 1000000;
}

static int
mo_exists(int seq)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    return stat(path, &st) == 0;
}

static Job *
mo_find(Job *l, uint64 id)
{
    for (Job *j = l->next; j != l; j = j->next)
        if (j->r.id == id) return j;
    return NULL;
}


// The migration takes the job out of the old file before writing it into
// the new one. If that write fails, the old file is the only place the
// job still exists — it must not be unlinked out from under it.
void
cttest_moveone_keeps_the_source_binlog_when_the_migration_write_fails(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 3);
    fault_clear_all();
    fault_set(FAULT_WRITEV, 0, EIO);

    walmaint(&w);

    assertf(mo_exists(1),
            "the migrating write failed, so binlog.1 still holds the only "
            "copy of job 201 and must survive; it was unlinked");
}


// Whatever the migration reserved is either spent by the write or given
// back. A reservation that stays booked against a wal that just died
// inflates w->resv for the rest of the process's life, and ratio() reads
// w->resv on every maintenance pass.
void
cttest_moveone_conserves_the_reservation_when_the_migration_fails(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 3);
    int64 before = w.resv;
    fault_clear_all();
    fault_set(FAULT_WRITEV, 0, EIO);

    walmaint(&w);

    assertf(w.resv == before,
            "the failed migration must not keep its reservation: resv went "
            "from %" PRId64 " to %" PRId64, before, w.resv);
}


// A head that is already adjacent to the file being written has nothing
// worth moving: rewriting it would reclaim no space at all.
void
cttest_moveone_writes_nothing_when_the_head_is_next_to_the_current_file(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 1);
    assertf(w.head->next == w.cur, "setup: the head must sit next to cur");
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_WRITEV) == 0,
            "there is nothing to migrate, so nothing may be rewritten: %d "
            "writev() call(s) escaped", fault_calls(FAULT_WRITEV));
}


// ...and it must not take a reservation for the move it is not making.
void
cttest_moveone_reserves_nothing_when_there_is_nothing_to_move(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 1);
    assertf(w.head->next == w.cur, "setup: the head must sit next to cur");
    int64 before = w.resv;

    walmaint(&w);

    assertf(w.resv == before,
            "nothing was migrated, so nothing may be reserved: resv went "
            "from %" PRId64 " to %" PRId64, before, w.resv);
}


// One call, one job. The migration counter is what the operator sees in
// `stats`, and a job counted twice hides how much work compaction is
// really doing.
void
cttest_moveone_moves_exactly_one_job_per_migration(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 2);

    walmaint(&w);

    assertf(w.nmig == 1,
            "only binlog.1's job can be migrated before the head reaches "
            "the current file: nmig is %" PRId64, w.nmig);
}


// The same bytes in a different file are still the same bytes. A
// migration that changes the live-byte count feeds ratio() a number that
// drifts a little further from the truth on every compaction.
void
cttest_moveone_keeps_the_live_byte_count_unchanged(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 2);
    int64 before = w.alive;

    walmaint(&w);

    assertf(w.alive == before,
            "a migration moves bytes, it does not create or destroy them: "
            "alive went from %" PRId64 " to %" PRId64, before, w.alive);
}


// After the move, the job belongs to the file that now holds its record.
// A job still filed under the old binlog keeps that binlog's refcount up
// forever and compaction never reclaims a thing.
void
cttest_moveone_files_the_migrated_job_under_the_current_binlog(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 2);
    Job *j = mo_find(&list, 201);
    assertf(j != NULL, "setup: job 201 must be replayed");

    walmaint(&w);

    assertf(j->file == w.cur,
            "the migrated job's record now lives in the current binlog, so "
            "that is the file it must be filed under");
}


// And the point of the whole exercise: the drained binlog goes away.
void
cttest_moveone_reaps_the_source_binlog_once_its_last_job_moved(void)
{
    mo_setup();
    Wal w;
    Job list;
    mo_wal(&w, &list, 2);

    walmaint(&w);

    assertf(!mo_exists(1),
            "binlog.1 lost its only job to the migration and must be "
            "reaped; it is still on disk");
}
