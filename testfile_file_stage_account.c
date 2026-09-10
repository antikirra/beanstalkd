// Angry tests for file_stage_account (file.c).
//
// The helper exists so that staging and the commit-failure rollback
// stay in lockstep: five counters move on the way in and exactly the
// same five have to move back on the way out. Its promise is that
// RELATION, not any individual assignment, so the tests below measure
// the relation — including the round trip through a forced commit
// failure, which is the only place the lockstep is observable.

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
sa_setup(void)
{
    fault_clear_all();
    progname = "testfile_file_stage_account";
}

static int
sa_binlog(char *path, size_t n, const char *name)
{
    int k = snprintf(path, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create binlog: %s", strerror(errno));
    assertf(fd > 2, "setup: the wrapped syscalls skip fds 0-2");
    int ver = Walver;
    assertf(write(fd, &ver, sizeof ver) == (ssize_t)sizeof ver,
            "setup: version header");
    return fd;
}

static void
sa_wal(Wal *w, File *f, int fd, int durable, int64 resv)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 1 << 20;
    w->use = 1;
    w->durable_sync = durable;
    w->resv = resv;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->free = (1 << 20) - 4;
    f->resv = (int)resv;
    f->refs = 1;
    w->head = f;
    w->tail = f;
    w->cur = f;
}

static Job *
sa_job(Tube *t, uint64 id, int body_size)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 41;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    memset(j->body, 's', (size_t)body_size);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static int64
sa_absdiff(int64 a, int64 b)
{
    return a > b ? a - b : b - a;
}

static off_t
sa_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// Bytes leave the reservation as they land on disk. Measured against
// the file so a wrong sign or a wrong operand cannot be masked by the
// counter it is compared with.
void
cttest_file_stage_account_debits_the_reservation_by_the_staged_bytes(void)
{
    sa_setup();
    char path[512];
    int fd = sa_binlog(path, sizeof path, "resvdebit.binlog");

    Wal w;
    File f;
    sa_wal(&w, &f, fd, 0, 1 << 20);
    Tube *t = make_tube("resvdebittube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = sa_job(t, 801, 9);

    off_t before = sa_size(fd);
    int64 resv_before = w.resv;
    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    off_t grew = sa_size(fd) - before;

    assertf(resv_before - w.resv == (int64)grew,
            "the reservation must shrink by exactly the bytes staged: "
            "file grew %lld, resv moved %"PRId64,
            (long long)grew, resv_before - w.resv);

    close(fd);
}


void
cttest_file_stage_account_credits_alive_by_the_staged_bytes(void)
{
    sa_setup();
    char path[512];
    int fd = sa_binlog(path, sizeof path, "alivecredit.binlog");

    Wal w;
    File f;
    sa_wal(&w, &f, fd, 0, 1 << 20);
    Tube *t = make_tube("alivecredittube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = sa_job(t, 802, 9);

    off_t before = sa_size(fd);
    int64 alive_before = w.alive;
    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    off_t grew = sa_size(fd) - before;

    assertf(w.alive - alive_before == (int64)grew,
            "alive must grow by exactly the bytes staged: file grew %lld, "
            "alive moved %"PRId64, (long long)grew, w.alive - alive_before);

    close(fd);
}


// The documented lockstep, exercised as a round trip: stage a record,
// force the commit to fail, and require every one of the five counters
// the helper touched to be exactly where it started. A dropped line or
// a flipped sign on either side of the pair shows up as drift.
void
cttest_file_stage_account_reverses_exactly_when_the_commit_fails(void)
{
    sa_setup();
    char path[512];
    int fd = sa_binlog(path, sizeof path, "lockstep.binlog");

    Wal w;
    File f;
    sa_wal(&w, &f, fd, 1, 1 << 20);
    Tube *t = make_tube("locksteptube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = sa_job(t, 803, 9);

    int64 w_resv = w.resv, w_alive = w.alive;
    int f_resv = f.resv, f_bytes = f.uncommitted_bytes;
    int f_alive = f.uncommitted_alive;

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the commit must fail");

    int64 drift = sa_absdiff(w.resv, w_resv)
                + sa_absdiff(w.alive, w_alive)
                + sa_absdiff(f.resv, f_resv)
                + sa_absdiff(f.uncommitted_bytes, f_bytes)
                + sa_absdiff(f.uncommitted_alive, f_alive);

    assertf(drift == 0,
            "staging and rollback must be exact inverses: w.resv %"PRId64
            "->%"PRId64", w.alive %"PRId64"->%"PRId64", f.resv %d->%d, "
            "uncommitted_bytes %d->%d, uncommitted_alive %d->%d",
            w_resv, w.resv, w_alive, w.alive, f_resv, f.resv,
            f_bytes, f.uncommitted_bytes, f_alive, f.uncommitted_alive);

    close(fd);
}


// The exact-fit boundary: a reservation consumed to the last byte must
// land on zero, never one short and never one over.
void
cttest_file_stage_account_leaves_an_exactly_filled_reservation_at_zero(void)
{
    sa_setup();
    char path[512];
    int fd = sa_binlog(path, sizeof path, "exactfit.binlog");

    Tube *t = make_tube("exactfittube");
    assertf(t != NULL, "setup: make_tube");
    int body_size = 9;
    int64 record = 4 + (int64)t->name_len + (int64)sizeof(Jobrec)
                 + body_size + 4;

    Wal w;
    File f;
    sa_wal(&w, &f, fd, 0, record);
    Job *j = sa_job(t, 804, body_size);

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");

    assertf(w.resv == 0,
            "a reservation matching the record exactly must be consumed to "
            "zero, got %"PRId64, w.resv);

    close(fd);
}
