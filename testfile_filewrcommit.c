// Angry tests for filewrcommit (file.c) — the group-commit boundary
// (invariant #16). One fdatasync covers every record staged since the
// last call; on failure the tail goes away AND the global counters have
// to follow it, or memory and disk start telling different stories.

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
fc_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewrcommit";
}

static int
fc_binlog(char *path, size_t n, const char *name)
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
fc_wal(Wal *w, File *f, int fd, int durable)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 1 << 20;
    w->use = 1;
    w->durable_sync = durable;
    w->resv = 1 << 20;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->free = (1 << 20) - 4;
    f->resv = 1 << 20;
    f->refs = 1;
    w->head = f;
    w->tail = f;
    w->cur = f;
}

static Job *
fc_job(Tube *t, uint64 id, int body_size, char fill)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 71;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    memset(j->body, fill, (size_t)body_size);
    j->body[body_size-2] = '\r';
    j->body[body_size-1] = '\n';
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
fc_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// Contract (1): the main loop calls this on every tick. With nothing
// staged it must cost nothing — an fdatasync per idle tick is a silent
// throughput regression with no functional symptom at all.
void
cttest_filewrcommit_with_nothing_staged_issues_no_sync(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "idlecommit.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);

    fault_clear_all();
    filewrcommit(&f);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "an empty batch must not sync: %d fdatasync calls escaped",
            fault_calls(FAULT_FDATASYNC));

    close(fd);
}


// ...and it must report success, or every idle tick would look like a
// WAL failure and disable the log.
void
cttest_filewrcommit_with_nothing_staged_reports_success(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "idleverdict.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);

    int r = filewrcommit(&f);

    assertf(r == 1,
            "an empty batch is trivially committed and must report "
            "success, got %d", r);

    close(fd);
}


// Contract (2)/(4): the counters are the batch. Leaving them behind on
// success makes the NEXT commit believe it owns bytes that are already
// durable.
void
cttest_filewrcommit_drains_the_staged_counters_on_success(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "drain.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);
    Tube *t = make_tube("draintube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fc_job(t, 1101, 6, 'd');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    assertf(filewrcommit(&f) == 1, "setup: the commit must succeed");

    assertf(f.uncommitted_bytes == 0 && f.uncommitted_alive == 0,
            "a committed batch is over: uncommitted_bytes %d, "
            "uncommitted_alive %d", f.uncommitted_bytes, f.uncommitted_alive);

    close(fd);
}


// Contract (4): the counters are drained on the failure path too. A
// stale count would let the next commit truncate a second batch's worth
// off the tail — this time out of records that were already durable.
void
cttest_filewrcommit_a_second_commit_after_a_failure_truncates_nothing_further(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "twicefail.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);
    Tube *t = make_tube("twicefailtube");
    assertf(t != NULL, "setup: make_tube");
    Job *keep = fc_job(t, 1102, 6, 'k');
    Job *lose = fc_job(t, 1103, 9, 'l');

    assertf(filewrjobfull(&f, keep) == 1, "setup: first stage");
    assertf(filewrcommit(&f) == 1, "setup: first commit");
    assertf(filewrjobfull(&f, lose) == 1, "setup: second stage");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the second commit must fail");

    off_t after_failure = fc_size(fd);
    fault_set(FAULT_FDATASYNC, 0, EIO);
    filewrcommit(&f);

    assertf(fc_size(fd) == after_failure,
            "the failed batch is already gone: a following commit must not "
            "eat more of the file — was %lld bytes, now %lld",
            (long long)after_failure, (long long)fc_size(fd));

    close(fd);
}


// Contract (3): alive reverts by uncommitted_alive, never by the whole
// batch. Only a batch that MIXES shapes can tell the two apart — a
// short record already undid its own contribution and a delete record
// already subtracted its job's full-record bytes, so reverting
// everything would subtract both a second time.
void
cttest_filewrcommit_restores_alive_to_the_live_bytes_after_a_mixed_batch(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "mixedbatch.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);
    Tube *t = make_tube("mixedtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j1 = fc_job(t, 1104, 6, 'a');
    Job *j2 = fc_job(t, 1105, 8, 'b');
    Job *j3 = fc_job(t, 1106, 10, 'c');
    Job *j4 = fc_job(t, 1107, 12, 'd');

    off_t p0 = fc_size(fd);
    assertf(filewrjobfull(&f, j1) == 1, "setup: stage j1");
    off_t p1 = fc_size(fd);
    assertf(filewrjobfull(&f, j2) == 1, "setup: stage j2");
    off_t p2 = fc_size(fd);
    assertf(filewrjobfull(&f, j3) == 1, "setup: stage j3");
    assertf(filewrcommit(&f) == 1, "setup: first commit");

    int64 s1 = (int64)(p1 - p0);
    int64 s2 = (int64)(p2 - p1);

    j2->r.state = Buried;
    assertf(filewrjobshort(&f, j2) == 1, "setup: stage the bury update");
    j3->r.state = Invalid;
    assertf(filewrjobshort(&f, j3) == 1, "setup: stage the delete");
    assertf(filewrjobfull(&f, j4) == 1, "setup: stage j4");

    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the mixed commit must fail");

    assertf(w.alive == s1 + s2,
            "after the rolled-back batch only the two surviving full "
            "records are alive: expected %"PRId64", got %"PRId64,
            s1 + s2, w.alive);

    close(fd);
}


// uncommitted_bytes is an int. With -z up to 1 GiB and -s up to
// INT_MAX, a tick can stage past its range, and a total that has gone
// negative slips through the `cur < total` guard and makes the rollback
// ftruncate the file LARGER than it was — the opposite of removing a
// batch.
void
cttest_filewrcommit_a_negative_staged_count_must_not_extend_the_binlog(void)
{
    fc_setup();
    char path[512];
    int fd = fc_binlog(path, sizeof path, "negtotal.binlog");

    Wal w;
    File f;
    fc_wal(&w, &f, fd, 1);
    Tube *t = make_tube("negtotaltube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fc_job(t, 1108, 6, 'n');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    off_t before = fc_size(fd);
    f.uncommitted_bytes = -100;      // what an overflowed counter leaves

    fault_set(FAULT_FDATASYNC, 0, EIO);
    filewrcommit(&f);

    assertf(fc_size(fd) <= before,
            "a rollback may only ever shrink the binlog: it grew from "
            "%lld to %lld bytes",
            (long long)before, (long long)fc_size(fd));

    close(fd);
}
