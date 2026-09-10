// Angry tests for walinit (walg.c) — the one call that stands between a
// directory full of binlogs and a running server.
//
// It has to do three things in the right order (scan, replay, open a new
// writable file) and leave a list whose head/cur/tail relationship walgc,
// moveone and usenext all quietly depend on. Getting any of it wrong is
// invisible until the next restart, when the data is gone.

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
#include <sys/wait.h>

static void
wi_setup(void)
{
    fault_clear_all();
    progname = "testwal_walinit";
}

static void
wi_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Seeds <dir>/binlog.<seq> with a version header and one full v8 record.
static void
wi_seed(const char *dir, int seq, const char *tube, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", dir, seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wi_write(fd, &ver, sizeof ver);

    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'q', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 7;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 3;
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

    wi_write(fd, &nl, sizeof nl);
    wi_write(fd, tube, (size_t)nl);
    wi_write(fd, &jr, sizeof jr);
    wi_write(fd, body, sizeof body);
    wi_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wi_wal(Wal *w, char *dir)
{
    memset(w, 0, sizeof *w);
    w->dir = dir;
    w->filesize = 4096;
    w->use = 1;
}

static Job *
wi_list(Job *l)
{
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    return l;
}

static int
wi_count(Job *l)
{
    int n = 0;
    for (Job *j = l->next; j != l; j = j->next) n++;
    return n;
}

static void
wi_subdir(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: subdir path did not fit");
    assertf(mkdir(buf, 0700) == 0, "setup: mkdir %s: %s", buf, strerror(errno));
}


// A fresh directory yields exactly one file: the one the server is about
// to write into. Two would mean a wasted preallocation on every start.
void
cttest_walinit_creates_exactly_one_binlog_in_a_fresh_directory(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());

    walinit(&w, wi_list(&list));

    assertf(w.nfile == 1,
            "a fresh wal dir must end up holding exactly one file, got %d",
            w.nfile);
}


// The writable file is the tail of the list. walgc walks from the head
// and stops at cur; if cur were anywhere but the tail, the files behind
// it could never be reaped.
void
cttest_walinit_leaves_the_current_file_at_the_tail(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());
    wi_seed(ctdir(), 1, "a", 11);
    wi_seed(ctdir(), 2, "b", 12);
    wi_seed(ctdir(), 3, "c", 13);

    walinit(&w, wi_list(&list));

    assertf(w.cur == w.tail,
            "the newly created file must be both the current and the last "
            "file: cur is binlog.%d, tail is binlog.%d",
            w.cur ? w.cur->seq : -1, w.tail ? w.tail->seq : -1);
}


// Replayed files keep their on-disk order in the list, oldest first.
// walgc reaps from the head, so a reversed list would unlink the newest
// binlog while the oldest one stayed forever.
void
cttest_walinit_leaves_the_oldest_binlog_at_the_head(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());
    wi_seed(ctdir(), 1, "a", 21);
    wi_seed(ctdir(), 2, "b", 22);
    wi_seed(ctdir(), 3, "c", 23);

    walinit(&w, wi_list(&list));

    assertf(w.head && w.head->seq == 1,
            "the oldest replayed binlog must lead the list, got binlog.%d",
            w.head ? w.head->seq : -1);
}


// The new writable file follows the highest sequence that was on disk.
// Reusing an existing number would overwrite jobs that have not been
// replayed out of it yet.
void
cttest_walinit_writes_into_the_sequence_after_the_highest_replayed_one(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());
    wi_seed(ctdir(), 1, "a", 31);
    wi_seed(ctdir(), 2, "b", 32);
    wi_seed(ctdir(), 3, "c", 33);

    walinit(&w, wi_list(&list));

    assertf(w.cur && w.cur->seq == 4,
            "binlog.3 is the highest existing sequence, so the writable "
            "file must be binlog.4, got binlog.%d", w.cur ? w.cur->seq : -1);
}


// Every job in every existing binlog reaches the caller's list. Replaying
// only the newest file is the shape of bug that loses everything older
// than the last rotation.
void
cttest_walinit_replays_the_jobs_of_every_existing_binlog(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());
    wi_seed(ctdir(), 1, "a", 41);
    wi_seed(ctdir(), 2, "b", 42);
    wi_seed(ctdir(), 3, "c", 43);

    walinit(&w, wi_list(&list));

    assertf(wi_count(&list) == 3,
            "three seeded binlogs hold three jobs; replay produced %d",
            wi_count(&list));
}


// The new binlog is preallocated to the configured size, which is what
// the reservation arithmetic assumes it can hand out.
void
cttest_walinit_preallocates_the_new_binlog_to_the_configured_size(void)
{
    wi_setup();
    Wal w;
    Job list;
    wi_wal(&w, ctdir());

    walinit(&w, wi_list(&list));

    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.1", ctdir());
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    assertf(stat(path, &st) == 0, "setup: stat %s: %s", path, strerror(errno));

    assertf(st.st_size == (off_t)w.filesize,
            "the new binlog must be preallocated to filesize (%d), on-disk "
            "size is %lld", w.filesize, (long long)st.st_size);
}


// A server that cannot create its first writable file has no wal at all.
// Continuing would leave w->cur NULL and every later walwrite dereferencing
// it. The failure is fatal, so it is observed from a child process.
void
cttest_walinit_refuses_to_continue_without_a_writable_file(void)
{
    wi_setup();
    fflush(NULL);
    pid_t pid = fork();
    assertf(pid >= 0, "setup: fork: %s", strerror(errno));
    if (pid == 0) {
        Wal w;
        Job list;
        wi_wal(&w, ctdir());
        fault_set(FAULT_OPEN, 0, EACCES);
        walinit(&w, wi_list(&list));
        _exit(0);
    }
    int st = 0;
    assertf(waitpid(pid, &st, 0) == pid, "setup: waitpid: %s", strerror(errno));

    assertf(WIFEXITED(st) && WEXITSTATUS(st) != 0,
            "walinit must not return with no writable file: child exited "
            "with status 0x%x", st);
}


// Two Wals in one process own two directories. Nothing in the scan may
// be shared between them, or a second wal would inherit the first one's
// sequence and write over a file it does not own.
void
cttest_walinit_keeps_two_wal_instances_independent(void)
{
    wi_setup();
    char adir[512], bdir[512];
    wi_subdir(adir, sizeof adir, "a");
    wi_subdir(bdir, sizeof bdir, "b");
    wi_seed(adir, 1, "a", 51);
    wi_seed(adir, 2, "b", 52);
    wi_seed(adir, 3, "c", 53);
    Wal wa, wb;
    Job la, lb;
    wi_wal(&wa, adir);
    wi_wal(&wb, bdir);

    walinit(&wa, wi_list(&la));
    walinit(&wb, wi_list(&lb));

    assertf(wb.cur && wb.cur->seq == 1,
            "the second wal's directory is empty, so its first file is "
            "binlog.1 regardless of the first wal; got binlog.%d",
            wb.cur ? wb.cur->seq : -1);
}
