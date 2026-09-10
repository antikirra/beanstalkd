// Angry tests for walsyncstart (walg.c) — the idempotent start of the
// per-wal fsync thread, and the fallback it promises when the thread
// cannot be created.
//
// The interesting half is the failure path: it destroys the mutex and
// cond it just initialised specifically so a later retry is legal, and it
// leaves the wal in synchronous mode rather than in a mode where nobody
// syncs at all. Both promises are invisible on a machine that can always
// spawn a thread.

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
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
ss_setup(void)
{
    fault_clear_all();
    progname = "testwal_walsyncstart";
    now = 0;
}

static void
ss_wal(Wal *w, Job *l, char *dir)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = dir;
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    now = 1;
}

static void
ss_subdir(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: subdir path did not fit");
    assertf(mkdir(buf, 0700) == 0, "setup: mkdir %s: %s", buf, strerror(errno));
}

static void
ss_post(Wal *w, int fd)
{
    assertf(pthread_mutex_lock(&w->sync_mu) == 0, "setup: lock");
    assertf(w->sync_fd < 0, "setup: the fsync slot must be free before a post");
    w->sync_fd = fd;
    assertf(pthread_cond_signal(&w->sync_cond) == 0, "setup: signal");
    assertf(pthread_mutex_unlock(&w->sync_mu) == 0, "setup: unlock");
}

static void
ss_wait_taken(Wal *w)
{
    struct timespec ts = {0, 1000000};
    for (int i = 0; i < 5000; i++) {
        assertf(pthread_mutex_lock(&w->sync_mu) == 0, "setup: lock");
        int busy = w->sync_fd >= 0;
        assertf(pthread_mutex_unlock(&w->sync_mu) == 0, "setup: unlock");
        if (!busy) return;
        nanosleep(&ts, NULL);
    }
    assertf(0, "setup: the fsync thread never took the posted descriptor");
}

// One full hand-off, waited out. A completed round also proves the
// previous round's bookkeeping has landed.
static void
ss_round(Wal *w)
{
    int fd = dup(w->cur->fd);
    assertf(fd >= 0, "setup: dup: %s", strerror(errno));
    ss_post(w, fd);
    ss_wait_taken(w);
}

static void
ss_failing_round(Wal *w)
{
    fault_set(FAULT_FDATASYNC, 0, EIO);
    ss_round(w);
}


// A thread that could not be created leaves the wal in synchronous
// fallback, and the flag that says so must say so. Setting it optimistically
// would send every later sync to a thread that does not exist.
void
cttest_walsyncstart_leaves_the_thread_off_when_creation_fails(void)
{
    ss_setup();
    Wal w;
    Job list;
    ss_wal(&w, &list, ctdir());
    fault_set(FAULT_PTHREAD_CREATE, 0, EAGAIN);

    walsyncstart(&w);

    assertf(w.sync_on == 0,
            "pthread_create failed, so the wal must be in synchronous "
            "fallback; sync_on is %d", w.sync_on);
}


// The fallback is about performance, never about durability: with no
// thread, the periodic sync must still reach the disk on the very next
// maintenance pass.
void
cttest_walsyncstart_keeps_syncing_inline_when_the_thread_cannot_start(void)
{
    ss_setup();
    Wal w;
    Job list;
    ss_wal(&w, &list, ctdir());
    fault_set(FAULT_PTHREAD_CREATE, 0, EAGAIN);
    walsyncstart(&w);
    w.wantsync = 1;
    w.syncrate = 0;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "a wal in synchronous fallback still owes one fsync per "
            "interval, got %d fdatasync() call(s)",
            fault_calls(FAULT_FDATASYNC));
}


// Destroying the mutex and cond on the failure path exists so a second
// attempt can initialise them again legally. Nothing verifies that today,
// and a re-init of a live mutex is undefined behaviour, not an error.
void
cttest_walsyncstart_works_on_a_second_attempt_after_a_failed_creation(void)
{
    ss_setup();
    Wal w;
    Job list;
    ss_wal(&w, &list, ctdir());
    fault_set(FAULT_PTHREAD_CREATE, 0, EAGAIN);
    walsyncstart(&w);
    assertf(w.sync_on == 0, "setup: the first start must have failed");
    fault_clear_all();

    walsyncstart(&w);
    ss_round(&w);
    walsyncstop(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "the retried start must give a working thread: expected one "
            "handed-off sync, got %d fdatasync() call(s)",
            fault_calls(FAULT_FDATASYNC));
}


// Calling start on a wal that is already running is a no-op. Re-running
// the initialisation would zero sync_err and erase a durability failure
// the main thread has not seen yet.
void
cttest_walsyncstart_does_not_erase_a_latched_error_on_a_redundant_start(void)
{
    ss_setup();
    Wal w;
    Job list;
    ss_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    ss_failing_round(&w);
    ss_round(&w);

    walsyncstart(&w);
    int r = walmaint(&w);

    assertf(r == 0,
            "a redundant start must not swallow the pending fsync failure; "
            "walmaint returned %d", r);

    // Join before w leaves scope: the worker parks in pthread_cond_wait
    // on a mutex that lives in THIS frame, so returning with the thread
    // still running leaves it waiting on memory the caller may reuse.
    walsyncstop(&w);
}


// Each wal owns its own thread, mutex and cond. Stopping one must leave
// the other's hand-off path working.
void
cttest_walsyncstart_gives_each_wal_its_own_fsync_thread(void)
{
    ss_setup();
    char adir[512], bdir[512];
    ss_subdir(adir, sizeof adir, "one");
    ss_subdir(bdir, sizeof bdir, "two");
    Wal wa, wb;
    Job la, lb;
    ss_wal(&wa, &la, adir);
    ss_wal(&wb, &lb, bdir);
    walsyncstart(&wa);
    walsyncstart(&wb);
    assertf(wa.sync_on == 1 && wb.sync_on == 1,
            "setup: both fsync threads must be running");
    fault_clear_all();

    walsyncstop(&wa);
    ss_round(&wb);
    walsyncstop(&wb);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "stopping one wal must not disturb the other's hand-off: "
            "expected one sync, got %d", fault_calls(FAULT_FDATASYNC));
}
