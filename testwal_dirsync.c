// Angry tests for dirsync (walg.c) — the fsync that makes a binlog's
// *name* survive a crash, as opposed to its contents.
//
// It is best-effort by design, which is precisely why it is dangerous:
// every failure mode it has is silent. It also owns a descriptor that is
// either handed to the fsync thread or closed here, and exactly one of
// those must happen, on every one of the three paths through it.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
ds_setup(void)
{
    fault_clear_all();
    progname = "testwal_dirsync";
}

static void
ds_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static void
ds_seed(int seq, const char *tube, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    ds_write(fd, &ver, sizeof ver);

    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'd', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 3;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 5;
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

    ds_write(fd, &nl, sizeof nl);
    ds_write(fd, tube, (size_t)nl);
    ds_write(fd, &jr, sizeof jr);
    ds_write(fd, body, sizeof body);
    ds_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
ds_wal(Wal *w)
{
    memset(w, 0, sizeof *w);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
}

static Job *
ds_list(Job *l)
{
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    return l;
}

static Job *
ds_find(Job *l, uint64 id)
{
    for (Job *j = l->next; j != l; j = j->next)
        if (j->r.id == id) return j;
    return NULL;
}

static int
ds_exists(int seq)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    return stat(path, &st) == 0;
}

static int
ds_fdmark(void)
{
    int fd = open("/dev/null", O_RDONLY);
    assertf(fd >= 0, "setup: /dev/null probe: %s", strerror(errno));
    assertf(close(fd) == 0, "setup: close /dev/null probe");
    return fd;
}

// Reaps the head binlog by removing the job that pins it, which is the
// only way production ever reaches dirsync from walgc.
static void
ds_reap(Job *list, uint64 id)
{
    Job *j = ds_find(list, id);
    assertf(j && j->file, "setup: job %" PRIu64 " must be replayed and filed", id);
    filermjob(j->file, j);
}


// With no fsync thread running the caller does the sync itself. One
// unlinked binlog, one directory sync — anything less and the unlink can
// be undone by a crash the server already reported as complete.
void
cttest_dirsync_syncs_the_directory_itself_with_no_fsync_thread(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 701);
    ds_seed(2, "beta", 702);
    walinit(&w, ds_list(&list));
    fault_clear_all();

    ds_reap(&list, 701);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "the reaped name must be made durable inline, got %d "
            "fdatasync() call(s)", fault_calls(FAULT_FDATASYNC));
}


// When the fsync thread is running but its slot is already occupied,
// dirsync falls back to syncing inline. The fallback exists to keep
// durability, so skipping the sync because the thread was busy is the
// one thing it may not do.
void
cttest_dirsync_syncs_inline_when_the_fsync_thread_is_busy(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 711);
    ds_seed(2, "beta", 712);
    walinit(&w, ds_list(&list));
    assertf(pthread_mutex_init(&w.sync_mu, NULL) == 0, "setup: mutex init");
    assertf(pthread_cond_init(&w.sync_cond, NULL) == 0, "setup: cond init");
    w.sync_fd = dup(w.cur->fd);
    assertf(w.sync_fd >= 0, "setup: dup: %s", strerror(errno));
    w.sync_on = 1;
    fault_clear_all();

    ds_reap(&list, 711);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "a busy fsync thread must not cost the directory its sync, got "
            "%d fdatasync() call(s)", fault_calls(FAULT_FDATASYNC));
}


// An interrupted fdatasync is not a failure; it is a signal. The
// directory still has to end up on stable storage.
void
cttest_dirsync_retries_the_directory_sync_after_an_interrupted_call(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 721);
    ds_seed(2, "beta", 722);
    walinit(&w, ds_list(&list));
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EINTR);

    ds_reap(&list, 721);

    assertf(fault_calls(FAULT_FDATASYNC) == 2,
            "EINTR must be retried, so the directory sync costs two calls, "
            "got %d", fault_calls(FAULT_FDATASYNC));
}


// A directory that cannot even be opened is a durability problem, never
// a correctness one: the reap it was asked to make durable has already
// happened and must stand.
void
cttest_dirsync_completes_the_reap_when_the_directory_cannot_be_opened(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 731);
    ds_seed(2, "beta", 732);
    walinit(&w, ds_list(&list));
    fault_clear_all();
    fault_set(FAULT_OPEN, 0, EACCES);

    ds_reap(&list, 731);

    assertf(!ds_exists(1),
            "the unlink happened before the directory sync, so binlog.1 "
            "must be gone whatever the sync did");
}


// ...and the failed open must not leave a descriptor behind either.
void
cttest_dirsync_leaks_no_descriptor_when_the_directory_cannot_be_opened(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 741);
    ds_seed(2, "beta", 742);
    walinit(&w, ds_list(&list));
    int before = ds_fdmark();
    fault_set(FAULT_OPEN, 0, EACCES);

    ds_reap(&list, 741);

    assertf(ds_fdmark() == before,
            "a failed directory open must leak nothing: the next free fd "
            "moved from %d to %d", before, ds_fdmark());
}


// The inline path closes the descriptor it opened. One leak per reaped
// run is enough to exhaust a long-running server's fd table.
void
cttest_dirsync_closes_the_descriptor_it_synced_inline(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 751);
    ds_seed(2, "beta", 752);
    ds_seed(3, "gamma", 753);
    walinit(&w, ds_list(&list));
    int before = ds_fdmark();

    ds_reap(&list, 751);
    ds_reap(&list, 752);

    assertf(ds_fdmark() == before,
            "every inline directory sync must close its descriptor: the "
            "next free fd moved from %d to %d", before, ds_fdmark());
}


// The hand-off path gives the descriptor away instead of closing it. The
// thread owns it from then on, and after the join nothing may be left
// open — whichever of the two paths each individual call happened to
// take.
void
cttest_dirsync_leaks_no_descriptor_through_the_fsync_thread(void)
{
    ds_setup();
    Wal w;
    Job list;
    ds_wal(&w);
    ds_seed(1, "alpha", 761);
    ds_seed(2, "beta", 762);
    ds_seed(3, "gamma", 763);
    ds_seed(4, "delta", 764);
    walinit(&w, ds_list(&list));
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    int before = ds_fdmark();

    ds_reap(&list, 761);
    ds_reap(&list, 762);
    ds_reap(&list, 763);
    walsyncstop(&w);

    assertf(ds_fdmark() == before,
            "every handed-off descriptor must be closed by the thread: the "
            "next free fd moved from %d to %d", before, ds_fdmark());
}
