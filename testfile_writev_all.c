// Angry tests for writev_all (file.c) — the only thing standing between
// a half-finished writev and a binlog whose tail CRC-fails on the next
// restart (#700).
//
// It is static; filewrjobfull is the door. The attacks below drive the
// three exits its contract names: the offset probe, the resume loop,
// and the best-effort rollback.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
wv_setup(void)
{
    fault_clear_all();
    progname = "testfile_writev_all";
}

static int
wv_binlog(char *path, size_t n, const char *name)
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
wv_wal(Wal *w, File *f, int fd)
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
wv_job(Tube *t, uint64 id, int body_size, char fill)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 51;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    memset(j->body, fill, (size_t)body_size);
    if (body_size >= 2) {
        j->body[body_size-2] = '\r';
        j->body[body_size-1] = '\n';
    }
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
wv_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// A descriptor that cannot be seeked cannot be rolled back, so the
// contract refuses before a single byte is committed to it. Proven by
// counting the writev calls, not by the return value alone.
void
cttest_writev_all_writes_nothing_to_a_non_seekable_descriptor(void)
{
    wv_setup();
    int pipefd[2];
    assertf(pipe(pipefd) == 0, "setup: pipe: %s", strerror(errno));
    assertf(pipefd[1] > 2, "setup: the wrapped syscalls skip fds 0-2");

    Wal w;
    File f;
    wv_wal(&w, &f, pipefd[1]);
    Tube *t = make_tube("pipetube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wv_job(t, 901, 6, 'p');

    fault_clear_all();
    int r = filewrjobfull(&f, j);
    assertf(r == 0, "setup: the stage must fail on a pipe, got %d", r);

    assertf(fault_calls(FAULT_WRITEV) == 0,
            "a record that cannot be rolled back must not be written at "
            "all: %d writev calls escaped", fault_calls(FAULT_WRITEV));

    close(pipefd[0]);
    close(pipefd[1]);
}


// The same exit, seen from the accounting side: a failed offset probe
// is a plain write failure and must cost the caller nothing.
void
cttest_writev_all_applies_no_accounting_when_the_offset_probe_fails(void)
{
    wv_setup();
    int pipefd[2];
    assertf(pipe(pipefd) == 0, "setup: pipe: %s", strerror(errno));

    Wal w;
    File f;
    wv_wal(&w, &f, pipefd[1]);
    Tube *t = make_tube("pipeacctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wv_job(t, 902, 6, 'p');

    int64 resv_before = w.resv;
    int r = filewrjobfull(&f, j);
    assertf(r == 0, "setup: the stage must fail on a pipe, got %d", r);

    assertf(w.resv == resv_before,
            "an unseekable descriptor consumed no reservation: was "
            "%"PRId64", now %"PRId64, resv_before, w.resv);

    close(pipefd[0]);
    close(pipefd[1]);
}


// #700: after a failed write the file must be exactly as long as it was
// before the call. The existing test watches only the return value, so
// deleting the whole rollback block survives it.
void
cttest_writev_all_restores_the_file_length_after_a_failed_write(void)
{
    wv_setup();
    char path[512];
    int fd = wv_binlog(path, sizeof path, "rollbacklen.binlog");

    Wal w;
    File f;
    wv_wal(&w, &f, fd);
    Tube *t = make_tube("rblentube");
    assertf(t != NULL, "setup: make_tube");
    Job *good = wv_job(t, 903, 6, 'g');
    Job *bad = wv_job(t, 904, 6, 'b');

    assertf(filewrjobfull(&f, good) == 1, "setup: first stage");
    off_t before = wv_size(fd);

    fault_set(FAULT_WRITEV, 0, EIO);
    assertf(filewrjobfull(&f, bad) == 0, "setup: the second stage must fail");

    assertf(wv_size(fd) == before,
            "a failed write must leave the binlog at its previous length: "
            "was %lld, now %lld",
            (long long)before, (long long)wv_size(fd));

    close(fd);
}


// The offset is the other half of the rollback: a descriptor left past
// the end of the data writes the next record into a hole.
void
cttest_writev_all_restores_the_file_offset_after_a_failed_write(void)
{
    wv_setup();
    char path[512];
    int fd = wv_binlog(path, sizeof path, "rollbackoff.binlog");

    Wal w;
    File f;
    wv_wal(&w, &f, fd);
    Tube *t = make_tube("rbofftube");
    assertf(t != NULL, "setup: make_tube");
    Job *good = wv_job(t, 905, 6, 'g');
    Job *bad = wv_job(t, 906, 6, 'b');

    assertf(filewrjobfull(&f, good) == 1, "setup: first stage");
    off_t before = lseek(fd, 0, SEEK_CUR);
    assertf(before > 0, "setup: lseek probe");

    fault_set(FAULT_WRITEV, 0, EIO);
    assertf(filewrjobfull(&f, bad) == 0, "setup: the second stage must fail");

    assertf(lseek(fd, 0, SEEK_CUR) == before,
            "a failed write must leave the descriptor where it started: "
            "was %lld, now %lld",
            (long long)before, (long long)lseek(fd, 0, SEEK_CUR));

    close(fd);
}


// #700 end to end, with a write that really is cut in half. RLIMIT_FSIZE
// lets part of the record through and then refuses the rest, which is
// the shape the resume loop and the rollback were written for. What
// matters is not which of the two paths ran but what the next restart
// sees: a binlog that stops cleanly, with no torn record at its tail.
void
cttest_writev_all_leaves_no_torn_record_at_the_tail_after_a_cut_write(void)
{
    wv_setup();
    char path[512];
    int fd = wv_binlog(path, sizeof path, "torn.binlog");

    Wal w;
    File f;
    wv_wal(&w, &f, fd);
    Tube *t = make_tube("default");
    assertf(t != NULL, "setup: make_tube");
    Job *good = wv_job(t, 907, 6, 'g');
    Job *cut = wv_job(t, 908, 6, 'c');

    assertf(filewrjobfull(&f, good) == 1, "setup: first stage");
    off_t before = wv_size(fd);

    struct sigaction ign, old;
    memset(&ign, 0, sizeof ign);
    memset(&old, 0, sizeof old);
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    assertf(sigaction(SIGXFSZ, &ign, &old) == 0, "setup: ignore SIGXFSZ");

    struct rlimit lim, oldlim;
    assertf(getrlimit(RLIMIT_FSIZE, &oldlim) == 0, "setup: getrlimit");
    lim = oldlim;
    lim.rlim_cur = (rlim_t)before + 20;   // room for part of the record
    assertf(setrlimit(RLIMIT_FSIZE, &lim) == 0, "setup: setrlimit");

    int r = filewrjobfull(&f, cut);

    assertf(setrlimit(RLIMIT_FSIZE, &oldlim) == 0, "setup: restore rlimit");
    assertf(sigaction(SIGXFSZ, &old, NULL) == 0, "setup: restore SIGXFSZ");
    assertf(r == 0, "setup: the cut stage must fail, got %d", r);
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
    int verdict = fileread(&rf, &list);
    close(rf.fd);

    assertf(verdict == 0,
            "a write cut short must leave no partial record behind: replay "
            "reported %d instead of a clean pass", verdict);
}


// The rollback is documented as best effort, but the RETURN VALUE is
// not: it is what makes the caller disable the WAL instead of acking a
// client for bytes that never landed.
void
cttest_writev_all_reports_failure_when_the_rollback_truncate_also_fails(void)
{
    wv_setup();
    char path[512];
    int fd = wv_binlog(path, sizeof path, "rbtruncfail.binlog");

    Wal w;
    File f;
    wv_wal(&w, &f, fd);
    Tube *t = make_tube("rbtrunctube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wv_job(t, 909, 6, 'z');

    fault_set(FAULT_WRITEV, 0, EIO);
    fault_set(FAULT_FTRUNCATE, 0, EIO);
    int r = filewrjobfull(&f, j);

    assertf(r == 0,
            "a write that failed and could not be rolled back is still a "
            "failure, got %d", r);

    close(fd);
}
