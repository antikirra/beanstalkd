// Angry tests for the fsync worker loop (walg.c) — the only thread in
// beanstalkd besides the event loop.
//
// Its whole job is to take a descriptor off the main thread, sync it,
// close it and remember whether that worked. Every one of those four
// steps has a failure mode that is invisible from the main thread: a
// descriptor that is never closed, a shutdown that discards an accepted
// request, a slot that is never cleared, an error that is never latched.

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
st_setup(void)
{
    fault_clear_all();
    progname = "testwal_sync_thread_fn";
    now = 0;
}

static void
st_wal(Wal *w, Job *l, char *dir)
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
st_subdir(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: subdir path did not fit");
    assertf(mkdir(buf, 0700) == 0, "setup: mkdir %s: %s", buf, strerror(errno));
}

static int
st_fdmark(void)
{
    int fd = open("/dev/null", O_RDONLY);
    assertf(fd >= 0, "setup: /dev/null probe: %s", strerror(errno));
    assertf(close(fd) == 0, "setup: close /dev/null probe");
    return fd;
}

// Hands one descriptor to the worker using the documented producer
// protocol: the slot is only ever written while it holds -1.
static void
st_post(Wal *w, int fd)
{
    assertf(pthread_mutex_lock(&w->sync_mu) == 0, "setup: lock");
    assertf(w->sync_fd < 0, "setup: the fsync slot must be free before a post");
    w->sync_fd = fd;
    assertf(pthread_cond_signal(&w->sync_cond) == 0, "setup: signal");
    assertf(pthread_mutex_unlock(&w->sync_mu) == 0, "setup: unlock");
}

// Waits for the worker to take the posted descriptor, with a ceiling so
// a thread that never wakes fails the run instead of hanging it.
static void
st_wait_taken(Wal *w)
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

// Posts one descriptor and returns once the worker has taken it. Because
// the worker only takes a new descriptor after finishing the previous
// one's bookkeeping, a completed round also proves the round before it
// has fully landed.
static void
st_round(Wal *w)
{
    int fd = dup(w->cur->fd);
    assertf(fd >= 0, "setup: dup: %s", strerror(errno));
    st_post(w, fd);
    st_wait_taken(w);
}

static void
st_failing_round(Wal *w)
{
    fault_set(FAULT_FDATASYNC, 0, EIO);
    st_round(w);
}

static void
st_failing_rounds(Wal *w, int n)
{
    for (int i = 0; i < n; i++) st_failing_round(w);
}


// The worker owns every descriptor it accepts, success or failure. A
// return that skips the close leaks one descriptor per failed sync, and
// the failures come in storms.
void
cttest_sync_thread_closes_every_descriptor_it_was_handed(void)
{
    st_setup();
    Wal w;
    Job list;
    st_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    int before = st_fdmark();

    st_failing_rounds(&w, 50);
    walsyncstop(&w);

    assertf(st_fdmark() == before,
            "fifty failed syncs must leave no descriptor behind: the next "
            "free fd moved from %d to %d", before, st_fdmark());
}


// A descriptor the worker has already accepted is a durability promise.
// Shutting down is not permission to drop it.
void
cttest_sync_thread_syncs_a_pending_descriptor_before_shutting_down(void)
{
    st_setup();
    Wal w;
    Job list;
    st_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    fault_clear_all();
    int fd = dup(w.cur->fd);
    assertf(fd >= 0, "setup: dup: %s", strerror(errno));

    st_post(&w, fd);
    walsyncstop(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "the descriptor posted just before the stop must still be "
            "synced, got %d fdatasync() call(s)",
            fault_calls(FAULT_FDATASYNC));
}


// The slot is cleared before the sync starts, so the producer is never
// locked out by a descriptor that is already being worked on.
void
cttest_sync_thread_accepts_a_second_descriptor_without_a_restart(void)
{
    st_setup();
    Wal w;
    Job list;
    st_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    fault_clear_all();

    st_round(&w);
    st_round(&w);
    walsyncstop(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 2,
            "two posted descriptors are two syncs, got %d fdatasync() "
            "call(s)", fault_calls(FAULT_FDATASYNC));
}


// A failure inside the worker is invisible until the main thread asks.
// It must still be there when it does, or a wal that has stopped syncing
// looks perfectly healthy.
void
cttest_sync_thread_latches_a_failed_sync_for_the_main_thread(void)
{
    st_setup();
    Wal w;
    Job list;
    st_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");

    st_failing_round(&w);
    st_round(&w);
    int r = walmaint(&w);

    assertf(r == 0,
            "an fsync that failed on the worker thread must be reported to "
            "the main thread; walmaint returned %d", r);

    // Join before w leaves scope. The worker parks in pthread_cond_wait
    // on a mutex that lives in THIS frame, so returning with the thread
    // still running leaves it waiting on memory the caller is free to
    // reuse — TSan sees the wake-up as a lock on a destroyed mutex, and
    // it is one.
    walsyncstop(&w);
}


// Two failures before anyone looks may be coalesced into one report, but
// the second must never overwrite the first with success.
void
cttest_sync_thread_does_not_erase_an_error_with_a_later_failure(void)
{
    st_setup();
    Wal w;
    Job list;
    st_wal(&w, &list, ctdir());
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");

    st_failing_round(&w);
    st_failing_round(&w);
    st_round(&w);
    int r = walmaint(&w);

    assertf(r == 0,
            "two failed syncs still owe the main thread one report; "
            "walmaint returned %d", r);

    walsyncstop(&w);   // see the note above: w lives on this frame
}


// The worker reads nothing but its own wal's four fields. Two wals with
// two threads must not be able to see each other's verdict.
void
cttest_sync_thread_keeps_two_wals_errors_apart(void)
{
    st_setup();
    char adir[512], bdir[512];
    st_subdir(adir, sizeof adir, "sick");
    st_subdir(bdir, sizeof bdir, "well");
    Wal wa, wb;
    Job la, lb;
    st_wal(&wa, &la, adir);
    st_wal(&wb, &lb, bdir);
    walsyncstart(&wa);
    walsyncstart(&wb);
    assertf(wa.sync_on == 1 && wb.sync_on == 1,
            "setup: both fsync threads must be running");

    st_failing_round(&wa);
    st_round(&wa);
    int r = walmaint(&wb);

    assertf(r == 1,
            "the failing wal's error must not surface on the healthy one; "
            "walmaint on the healthy wal returned %d", r);

    walsyncstop(&wa);  // see the note above: both wals live on this frame
    walsyncstop(&wb);
}
