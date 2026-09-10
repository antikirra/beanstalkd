// Angry tests for walsyncstop (walg.c) — the symmetric half of
// walsyncstart.
//
// Stopping is where a background thread gets to lose things quietly: a
// descriptor it had already accepted, a durability failure nobody has
// read yet, or the ability to be started again at all. It also has to be
// safe on a wal that never started a thread, because that is exactly the
// state a fallback leaves behind.

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
sp_setup(void)
{
    fault_clear_all();
    progname = "testwal_walsyncstop";
    now = 0;
}

static void
sp_wal(Wal *w, Job *l)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    now = 1;
}

static int
sp_fdmark(void)
{
    int fd = open("/dev/null", O_RDONLY);
    assertf(fd >= 0, "setup: /dev/null probe: %s", strerror(errno));
    assertf(close(fd) == 0, "setup: close /dev/null probe");
    return fd;
}

static void
sp_post(Wal *w, int fd)
{
    assertf(pthread_mutex_lock(&w->sync_mu) == 0, "setup: lock");
    assertf(w->sync_fd < 0, "setup: the fsync slot must be free before a post");
    w->sync_fd = fd;
    assertf(pthread_cond_signal(&w->sync_cond) == 0, "setup: signal");
    assertf(pthread_mutex_unlock(&w->sync_mu) == 0, "setup: unlock");
}

static void
sp_wait_taken(Wal *w)
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

static int
sp_dup(Wal *w)
{
    int fd = dup(w->cur->fd);
    assertf(fd >= 0, "setup: dup: %s", strerror(errno));
    return fd;
}

static void
sp_round(Wal *w)
{
    sp_post(w, sp_dup(w));
    sp_wait_taken(w);
}

static void
sp_failing_round(Wal *w)
{
    fault_set(FAULT_FDATASYNC, 0, EIO);
    sp_round(w);
}

// n complete start / hand-off / stop cycles on one wal.
static void
sp_cycles(Wal *w, int n)
{
    for (int i = 0; i < n; i++) {
        walsyncstart(w);
        assertf(w->sync_on == 1, "setup: cycle %d failed to start a thread", i);
        sp_round(w);
        walsyncstop(w);
    }
}


// The stop joins the thread, and the thread closes what it owns. Nothing
// posted before the stop may outlive it as an open descriptor.
void
cttest_walsyncstop_leaves_no_descriptor_open_behind_it(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    int before = sp_fdmark();

    sp_post(&w, sp_dup(&w));
    walsyncstop(&w);

    assertf(sp_fdmark() == before,
            "the pending descriptor must be closed by the time the stop "
            "returns: the next free fd moved from %d to %d",
            before, sp_fdmark());
}


// A wal that never started a thread is the state the pthread_create
// fallback leaves behind, and shutdown calls stop unconditionally.
// Joining an uninitialised thread handle there would take the server
// down on a clean exit.
void
cttest_walsyncstop_leaves_a_never_started_wal_startable(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    assertf(w.sync_on == 0, "setup: no thread has been started yet");
    fault_clear_all();

    walsyncstop(&w);
    walsyncstart(&w);
    sp_round(&w);
    walsyncstop(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "a stop on a thread-less wal must change nothing, so the later "
            "start still syncs: expected one hand-off, got %d fdatasync() "
            "call(s)", fault_calls(FAULT_FDATASYNC));
}


// Stopping twice must not join twice or destroy an already-destroyed
// mutex, and the wal must still be startable afterwards.
void
cttest_walsyncstop_can_be_called_twice_and_still_restart(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    walsyncstop(&w);
    walsyncstop(&w);
    fault_clear_all();

    walsyncstart(&w);
    sp_round(&w);
    walsyncstop(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "a doubled stop must leave the wal startable: expected one "
            "handed-off sync, got %d fdatasync() call(s)",
            fault_calls(FAULT_FDATASYNC));
}


// Ten cycles, ten syncs. A stop that forgets to clear sync_on turns every
// later start into a no-op, and the syncs simply stop happening while
// every call still returns success.
void
cttest_walsyncstop_survives_ten_start_stop_cycles(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    fault_clear_all();

    sp_cycles(&w, 10);

    assertf(fault_calls(FAULT_FDATASYNC) == 10,
            "ten cycles owe ten handed-off syncs, got %d",
            fault_calls(FAULT_FDATASYNC));
}


// ...and ten cycles must not accumulate descriptors either.
void
cttest_walsyncstop_leaks_no_descriptor_across_ten_cycles(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    int before = sp_fdmark();

    sp_cycles(&w, 10);

    assertf(sp_fdmark() == before,
            "ten start/stop cycles must leave the fd table where they "
            "found it: the next free fd moved from %d to %d",
            before, sp_fdmark());
}


// An fsync that failed on the worker thread is a durability failure the
// main thread has not been told about yet. Shutting the thread down must
// not be the thing that converts it into a clean exit.
void
cttest_walsyncstop_does_not_discard_an_unreported_sync_error(void)
{
    sp_setup();
    Wal w;
    Job list;
    sp_wal(&w, &list);
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    sp_failing_round(&w);
    sp_round(&w);

    walsyncstop(&w);
    int r = walmaint(&w);

    assertf(r == 0,
            "the fsync failure was never reported before the stop, so it "
            "must still be reported after it; walmaint returned %d", r);
}
