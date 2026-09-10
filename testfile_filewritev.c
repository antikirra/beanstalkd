// Angry tests for filewritev (file.c) — the staging step every WAL
// record goes through: compute the record's size from the iovec vector,
// hand it to writev_all, and only then apply the accounting.
//
// filewritev is static; filewrjobfull and filewrjobshort are its only
// callers, so they are the doors used here.

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
fv_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewritev";
}

static int
fv_binlog(char *path, size_t n, const char *name)
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
fv_wal(Wal *w, File *f, int fd, int durable)
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
fv_job(Tube *t, uint64 id, int body_size)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 31;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    if (body_size > 0) memset(j->body, 'v', (size_t)body_size);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
fv_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// Contract (2): a failed writev applies no accounting at all. The
// existing writev-failure test watches resv/alive and the per-job
// counters; the staging counters are the pair nothing watches, and they
// are exactly what the commit rollback later acts on.
void
cttest_filewritev_stages_no_uncommitted_bytes_when_the_write_fails(void)
{
    fv_setup();
    char path[512];
    int fd = fv_binlog(path, sizeof path, "stagefail.binlog");

    Wal w;
    File f;
    fv_wal(&w, &f, fd, 0);
    Tube *t = make_tube("stagefailtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fv_job(t, 701, 6);

    int bytes_before = f.uncommitted_bytes;
    int alive_before = f.uncommitted_alive;
    fault_set(FAULT_WRITEV, 0, EIO);
    int r = filewrjobfull(&f, j);
    assertf(r == 0, "setup: the staging write must fail, got %d", r);

    assertf(f.uncommitted_bytes == bytes_before
            && f.uncommitted_alive == alive_before,
            "a record that never reached the file must stage nothing: "
            "uncommitted_bytes %d->%d, uncommitted_alive %d->%d",
            bytes_before, f.uncommitted_bytes,
            alive_before, f.uncommitted_alive);

    close(fd);
}


// The record's cost is computed by summing the iovec vector. Measured
// against the file, a total taken over the wrong iovcnt has nowhere to
// hide.
void
cttest_filewritev_charges_the_file_exactly_the_bytes_it_appended(void)
{
    fv_setup();
    char path[512];
    int fd = fv_binlog(path, sizeof path, "stagesize.binlog");

    Wal w;
    File f;
    fv_wal(&w, &f, fd, 0);
    Tube *t = make_tube("stagesizetube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fv_job(t, 702, 9);

    off_t before = fv_size(fd);
    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    off_t grew = fv_size(fd) - before;

    assertf((off_t)f.uncommitted_bytes == grew,
            "the staged count must equal the bytes appended: file grew "
            "%lld, counter says %d", (long long)grew, f.uncommitted_bytes);

    close(fd);
}


// The same bytes, seen from the job's side. Three views of one number;
// two of them agreeing is not evidence, so each is pinned to the file.
void
cttest_filewritev_charges_the_job_the_bytes_the_file_grew(void)
{
    fv_setup();
    char path[512];
    int fd = fv_binlog(path, sizeof path, "stagejob.binlog");

    Wal w;
    File f;
    fv_wal(&w, &f, fd, 0);
    Tube *t = make_tube("stagejobtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fv_job(t, 703, 9);

    off_t before = fv_size(fd);
    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    off_t grew = fv_size(fd) - before;

    assertf(j->walused == (int64)grew,
            "the job must be charged the bytes its record occupies: file "
            "grew %lld, walused is %"PRId64, (long long)grew, j->walused);

    close(fd);
}


// Invariant #16: staging never syncs. The deferred fdatasync is
// filewrcommit's job, and a sync smuggled into the staging path turns
// group commit back into one sync per record with no visible symptom
// other than the machine getting slower.
void
cttest_filewritev_never_syncs_while_staging(void)
{
    fv_setup();
    char path[512];
    int fd = fv_binlog(path, sizeof path, "stagenosync.binlog");

    Wal w;
    File f;
    fv_wal(&w, &f, fd, 1);          // durable mode: the tempting one
    Tube *t = make_tube("stagesynctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j1 = fv_job(t, 704, 6);
    Job *j2 = fv_job(t, 705, 6);
    Job *j3 = fv_job(t, 706, 6);

    fault_clear_all();
    assertf(filewrjobfull(&f, j1) == 1, "setup: stage 1");
    assertf(filewrjobfull(&f, j2) == 1, "setup: stage 2");
    assertf(filewrjobshort(&f, j3) == 1, "setup: stage 3");

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "staging must never sync, even in durable mode: %d fdatasync "
            "calls escaped", fault_calls(FAULT_FDATASYNC));

    close(fd);
}


// A zero-length body puts an empty iovec in the middle of the vector.
// The resume cursor advances with `>=` for exactly this reason; a `>`
// there stalls on the empty entry, and the total must still be the true
// sum.
void
cttest_filewritev_an_empty_body_iovec_is_still_counted_exactly(void)
{
    fv_setup();
    char path[512];
    int fd = fv_binlog(path, sizeof path, "stageempty.binlog");

    Wal w;
    File f;
    fv_wal(&w, &f, fd, 0);
    Tube *t = make_tube("stageemptytube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = fv_job(t, 707, 0);

    off_t before = fv_size(fd);
    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    off_t grew = fv_size(fd) - before;

    assertf((off_t)f.uncommitted_bytes == grew,
            "a zero-length body iovec must neither stall the write nor "
            "mis-count it: file grew %lld, counter says %d",
            (long long)grew, f.uncommitted_bytes);

    close(fd);
}
