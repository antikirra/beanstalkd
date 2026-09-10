// Angry tests for filewrjobshort (file.c) — the writer of the v8 short
// (state-update) record.
//
// Its whole reason to exist is #622: short-record bytes are dead space.
// The counters therefore have to move in a very specific pattern — the
// file grows, the reservation is consumed, but alive and walused come
// back to where they were, and uncommitted_alive excludes the bytes so
// a commit rollback cannot subtract them twice.

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
ws_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewrjobshort";
}

static int
ws_binlog(char *path, size_t n, const char *name)
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
ws_wal(Wal *w, File *f, int fd)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 1 << 20;
    w->use = 1;
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
ws_job(Tube *t, uint64 id, int body_size, byte state)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 21;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = state;
    memset(j->body, 'k', (size_t)body_size);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
ws_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// #622: the bytes are real on disk but dead to the compactor. alive
// must come out of the call exactly where it went in, or every state
// update inflates the live-bytes figure and compaction stops firing.
void
cttest_filewrjobshort_leaves_alive_unchanged_while_the_binlog_grows(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortalive.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortalivetube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 601, 6, Buried);

    off_t before = ws_size(fd);
    int64 alive_before = w.alive;
    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    assertf(ws_size(fd) > before, "setup: the record must reach the file");

    assertf(w.alive == alive_before,
            "a short record is dead space: alive must be unchanged, was "
            "%"PRId64", now %"PRId64, alive_before, w.alive);

    close(fd);
}


void
cttest_filewrjobshort_leaves_the_jobs_used_bytes_unchanged(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortused.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortusedtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 602, 6, Buried);
    j->walused = 91;

    int64 before = j->walused;
    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(j->walused == before,
            "a short record must not be charged to the job: walused was "
            "%"PRId64", now %"PRId64, before, j->walused);

    close(fd);
}


// Contract (4): the bytes ARE on disk, so the staging counter has to
// see them — measured against the file itself, never against another
// counter, so the hand-recomputed `total` cannot drift from the record.
void
cttest_filewrjobshort_counts_its_bytes_as_uncommitted(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortuncomm.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortuncommtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 603, 6, Buried);

    off_t before = ws_size(fd);
    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    off_t grew = ws_size(fd) - before;

    assertf((off_t)f.uncommitted_bytes == grew,
            "uncommitted_bytes must equal the bytes appended: file grew "
            "%lld, counter says %d", (long long)grew, f.uncommitted_bytes);

    close(fd);
}


// Contract (3): the commit rollback reverts uncommitted_alive, so a
// short record must leave nothing there — otherwise a failed commit
// subtracts these bytes from alive a second time and drives it negative.
void
cttest_filewrjobshort_excludes_its_bytes_from_the_revertible_alive_total(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortuncommalive.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortuatube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 604, 6, Buried);

    int before = f.uncommitted_alive;
    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(f.uncommitted_alive == before,
            "short-record bytes were never added to alive, so nothing of "
            "them may be revertible: was %d, now %d",
            before, f.uncommitted_alive);

    close(fd);
}


// The reader recomputes this checksum over [namelen=0][Jobrec] and
// rejects the whole record if the writer's coverage differs by a single
// byte. Pinned against a checksum this test folds itself.
void
cttest_filewrjobshort_trailer_covers_the_namelen_and_the_jobrec(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortcrc.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortcrctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 605, 6, Buried);

    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    int nl = 0;
    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &nl, sizeof nl);
    c = wal_crc32c(c, &j->r, sizeof j->r);
    c ^= WAL_CRC32C_XOR;
    unsigned char want[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };

    unsigned char got[4];
    off_t at = ws_size(fd) - 4;
    assertf(pread(fd, got, 4, at) == 4, "setup: pread trailer");

    assertf(memcmp(got, want, 4) == 0,
            "the short-record trailer must cover namelen and the whole "
            "Jobrec, little-endian");

    close(fd);
}


// Contract (5): only a delete detaches. Detaching on an ordinary state
// update would drop the reference the job's FULL record holds and let
// walgc unlink a binlog that still owns live data.
void
cttest_filewrjobshort_keeps_a_buried_job_on_its_file(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortburied.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortburiedtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 606, 6, Buried);
    fileaddjob(&f, j);

    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(j->file == &f,
            "a bury is a state update, not a delete: the job must stay on "
            "the file that holds its full record");

    close(fd);
}


void
cttest_filewrjobshort_detaches_a_deleted_job_from_its_file(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortdelete.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortdeltube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 607, 6, Invalid);
    fileaddjob(&f, j);

    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(j->file == NULL,
            "a delete record must unlink the job from its file so the "
            "binlog becomes reapable");

    close(fd);
}


// The compensation exists to undo what filewritev did. When filewritev
// did nothing, applying it anyway walks alive down by a record's worth
// on every failed write.
void
cttest_filewrjobshort_applies_no_compensation_when_the_write_fails(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortfail.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    w.alive = 1000;
    Tube *t = make_tube("shortfailtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 608, 6, Buried);

    int64 before = w.alive;
    fault_set(FAULT_WRITEV, 0, EIO);
    int r = filewrjobshort(&f, j);
    assertf(r == 0, "setup: the staging write must fail, got %d", r);

    assertf(w.alive == before,
            "no bytes were staged, so nothing may be compensated: alive "
            "was %"PRId64", now %"PRId64, before, w.alive);

    close(fd);
}


// A delete for a job whose file reference is already gone hits
// filermjob's `if (!f) return;` guard, so its walused is never zeroed
// and its full record's bytes stay counted as live forever — the exact
// inverse of the #622 bug this function exists to fix.
void
cttest_filewrjobshort_deleting_an_orphaned_job_must_release_its_bytes(void)
{
    ws_setup();
    char path[512];
    int fd = ws_binlog(path, sizeof path, "shortorphan.binlog");

    Wal w;
    File f;
    ws_wal(&w, &f, fd);
    Tube *t = make_tube("shortorphantube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = ws_job(t, 609, 6, Invalid);
    // Its full record's binlog is gone: no file, but its bytes are
    // still on the books.
    j->file = NULL;
    j->walused = 91;
    w.alive = 91;

    int r = filewrjobshort(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(w.alive == 0,
            "a deleted job's bytes are dead space wherever they live: "
            "alive must be 0, got %"PRId64, w.alive);

    close(fd);
}
