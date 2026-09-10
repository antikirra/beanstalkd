// Angry tests for filewrite_commit_durable (file.c) — the step that
// turns "the bytes are in the page cache" into "the bytes are on the
// disk", and undoes them when it cannot.
//
// #C2: a failed commit has to look exactly like "the writev never
// happened". It is static, so filewrcommit is the door.

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
cd_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewrite_commit_durable";
}

static int
cd_binlog(char *path, size_t n, const char *name)
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
cd_wal(Wal *w, File *f, int fd, int durable)
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
cd_job(Tube *t, uint64 id, int body_size, char fill)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 61;
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
cd_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// Contract (1): outside durable mode the commit does not sync at all —
// staged bytes stay in the page cache and the periodic walmaint sync
// handles them. Counted, so an inverted test cannot hide.
void
cttest_filewrite_commit_durable_skips_the_sync_when_durability_is_off(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "nosync.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 0);
    Tube *t = make_tube("nosynctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = cd_job(t, 1001, 6, 'n');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    fault_clear_all();
    assertf(filewrcommit(&f) == 1, "setup: the commit must report success");

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "with durable_sync off the commit must not sync: %d fdatasync "
            "calls escaped", fault_calls(FAULT_FDATASYNC));

    close(fd);
}


// ...and it must not roll anything back either: without a sync there is
// no failure to recover from, so the staged bytes stay on disk.
void
cttest_filewrite_commit_durable_keeps_the_staged_bytes_when_durability_is_off(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "nosynckeep.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 0);
    Tube *t = make_tube("nosynckeeptube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = cd_job(t, 1002, 6, 'k');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    off_t staged = cd_size(fd);
    assertf(filewrcommit(&f) == 1, "setup: the commit must report success");

    assertf(cd_size(fd) == staged,
            "a non-durable commit must leave the file alone: was %lld, "
            "now %lld", (long long)staged, (long long)cd_size(fd));

    close(fd);
}


// Contract (3): the failed batch is removed from the tail — exactly the
// batch, no more and no less.
void
cttest_filewrite_commit_durable_removes_exactly_the_failed_batch(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "batchgone.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 1);
    Tube *t = make_tube("batchgonetube");
    assertf(t != NULL, "setup: make_tube");
    Job *kept = cd_job(t, 1003, 6, 'a');
    Job *lost = cd_job(t, 1004, 9, 'b');

    assertf(filewrjobfull(&f, kept) == 1, "setup: first stage");
    assertf(filewrcommit(&f) == 1, "setup: first commit");
    off_t committed = cd_size(fd);

    assertf(filewrjobfull(&f, lost) == 1, "setup: second stage");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the second commit must fail");

    assertf(cd_size(fd) == committed,
            "the failed batch must be gone and the committed record kept: "
            "expected %lld bytes, got %lld",
            (long long)committed, (long long)cd_size(fd));

    close(fd);
}


// The same promise stated the way a restart sees it: a batch whose sync
// failed must be unrecoverable, and the batch before it must not be.
void
cttest_filewrite_commit_durable_leaves_the_failed_batch_unreplayable(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "batchreplay.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 1);
    Tube *t = make_tube("default");
    assertf(t != NULL, "setup: make_tube");
    Job *kept = cd_job(t, 1005, 6, 'a');
    Job *lost = cd_job(t, 1006, 9, 'b');

    assertf(filewrjobfull(&f, kept) == 1, "setup: first stage");
    assertf(filewrcommit(&f) == 1, "setup: first commit");
    assertf(filewrjobfull(&f, lost) == 1, "setup: second stage");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the second commit must fail");
    close(fd);

    Wal rw = {0};
    File rf = {0};
    Job list = {0};
    rf.w = &rw;
    rf.path = path;
    rf.fd = open(path, O_RDONLY);
    assertf(rf.fd >= 0, "setup: reopen binlog: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&rf, &rw);
    rw.cur = &rf;
    fileread(&rf, &list);
    close(rf.fd);

    assertf(job_find(1006) == NULL,
            "the server was told this batch failed, so the disk must not "
            "hand job 1006 back on the next restart");
}


// Contract (4): if the counter claims more bytes than the file holds,
// truncating by it would eat records that were committed long ago. The
// guard refuses — proven by counting ftruncate, not by the file size,
// which a refusal and a no-op truncate would leave identical.
void
cttest_filewrite_commit_durable_refuses_to_truncate_into_committed_data(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "guardtrunc.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 1);
    Tube *t = make_tube("guardtrunctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = cd_job(t, 1007, 6, 'g');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    f.uncommitted_bytes = 1 << 20;   // a counter that outran the file

    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EIO);
    assertf(filewrcommit(&f) == 0, "setup: the commit must fail");

    assertf(fault_calls(FAULT_FTRUNCATE) == 0,
            "a staged count larger than the file must stop the rollback "
            "before it truncates: %d ftruncate calls escaped",
            fault_calls(FAULT_FTRUNCATE));

    close(fd);
}


// Contract (2): EINTR is transient. Treating it as permanent throws away
// a batch that was about to become durable.
void
cttest_filewrite_commit_durable_retries_an_interrupted_sync(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "eintrsync.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 1);
    Tube *t = make_tube("eintrsynctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = cd_job(t, 1008, 6, 'e');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    fault_set(FAULT_FDATASYNC, 0, EINTR);
    int r = filewrcommit(&f);

    assertf(r == 1,
            "an interrupted fdatasync must be retried, not reported as a "
            "lost batch, got %d", r);

    close(fd);
}


// A ghost ack is the one outcome this path exists to prevent: after a
// sync that failed, no amount of trouble in the rollback may turn the
// answer back into success.
void
cttest_filewrite_commit_durable_never_reports_success_after_a_failed_sync(void)
{
    cd_setup();
    char path[512];
    int fd = cd_binlog(path, sizeof path, "noghostack.binlog");

    Wal w;
    File f;
    cd_wal(&w, &f, fd, 1);
    Tube *t = make_tube("noghosttube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = cd_job(t, 1009, 6, 'q');

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    fault_set(FAULT_FTRUNCATE, 0, EIO);
    int r = filewrcommit(&f);

    assertf(r == 0,
            "the data never became durable, so the commit must fail even "
            "when its rollback does too, got %d", r);

    close(fd);
}
