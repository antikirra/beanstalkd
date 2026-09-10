// Angry tests for walsync (walg.c) — the periodic durability step of
// non-durable mode.
//
// Everything it can get wrong is silent. Stamping the clock for a sync
// it decided not to issue skips a whole interval with no fsync at all;
// syncing a descriptor that is no longer the current file is a no-op
// that still reports success; and an error latched by the fsync thread
// is only ever reported here, once.

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
wy_setup(void)
{
    fault_clear_all();
    progname = "testwal_walsync";
    now = 0;
}

static void
wy_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static void
wy_seed(int seq, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wy_write(fd, &ver, sizeof ver);

    const char *tube = "y";
    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'y', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 8;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 4;
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

    wy_write(fd, &nl, sizeof nl);
    wy_write(fd, tube, (size_t)nl);
    wy_write(fd, &jr, sizeof jr);
    wy_write(fd, body, sizeof body);
    wy_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wy_wal(Wal *w, Job *l, int old)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    for (int i = 1; i <= old; i++) wy_seed(i, 1300 + (uint64)i);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
}

// Marks the fsync thread's slot as occupied without running a thread, so
// the hand-off path finds itself with nowhere to put the descriptor.
static void
wy_occupy(Wal *w)
{
    assertf(pthread_mutex_init(&w->sync_mu, NULL) == 0, "setup: mutex init");
    assertf(pthread_cond_init(&w->sync_cond, NULL) == 0, "setup: cond init");
    w->sync_fd = dup(w->cur->fd);
    assertf(w->sync_fd >= 0, "setup: dup: %s", strerror(errno));
    w->sync_on = 1;
}

static void
wy_post(Wal *w, int fd)
{
    assertf(pthread_mutex_lock(&w->sync_mu) == 0, "setup: lock");
    assertf(w->sync_fd < 0, "setup: the fsync slot must be free before a post");
    w->sync_fd = fd;
    assertf(pthread_cond_signal(&w->sync_cond) == 0, "setup: signal");
    assertf(pthread_mutex_unlock(&w->sync_mu) == 0, "setup: unlock");
}

static void
wy_wait_taken(Wal *w)
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

static void
wy_round(Wal *w)
{
    int fd = dup(w->cur->fd);
    assertf(fd >= 0, "setup: dup: %s", strerror(errno));
    wy_post(w, fd);
    wy_wait_taken(w);
}


// A round skipped because the fsync thread was busy is a round with no
// fsync in it. Stamping the clock anyway silently swallows a whole sync
// interval, so the next call has to try again at the same clock reading.
void
cttest_walsync_retries_immediately_after_the_fsync_thread_was_busy(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 0);
    wy_occupy(&w);
    w.wantsync = 1;
    w.syncrate = 1000000;
    w.lastsync = 0;
    now = 1000000;
    walmaint(&w);
    assertf(w.sync_fd >= 0, "setup: the busy slot must still be occupied");
    assertf(close(w.sync_fd) == 0, "setup: close the occupying descriptor");
    w.sync_fd = -1;

    walmaint(&w);

    assertf(w.sync_fd >= 0,
            "the skipped round must be retried at the same clock reading, "
            "but no descriptor was handed off");
}


// With periodic syncing switched off nothing may be synced, however far
// the clock has moved. A -f 0 server explicitly opted out of paying for
// this.
void
cttest_walsync_issues_no_sync_when_periodic_syncing_is_off(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 0);
    w.wantsync = 0;
    w.syncrate = 1000000;
    now = 100000000;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "periodic syncing is off, so nothing may be synced: %d "
            "fdatasync() call(s) escaped", fault_calls(FAULT_FDATASYNC));
}


// The interval is inclusive: a sync is due once exactly syncrate
// nanoseconds have passed.
void
cttest_walsync_fires_once_the_interval_has_exactly_elapsed(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 0);
    w.wantsync = 1;
    w.syncrate = 1000000;
    w.lastsync = 0;
    now = 1000000;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "the interval has exactly elapsed, so one sync is due, got %d "
            "fdatasync() call(s)", fault_calls(FAULT_FDATASYNC));
}


// ...and not a nanosecond earlier, or the syncrate the operator
// configured is not the rate they get.
void
cttest_walsync_does_not_fire_one_nanosecond_before_the_interval(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 0);
    w.wantsync = 1;
    w.syncrate = 1000000;
    w.lastsync = 0;
    now = 999999;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "the interval has not elapsed yet, so no sync is due: %d "
            "fdatasync() call(s) escaped", fault_calls(FAULT_FDATASYNC));
}


// The sync has to reach the file records are being written into. A
// counter alone cannot tell "synced the current file" from "synced some
// other file's descriptor", so the head is given a descriptor no sync
// can succeed on.
void
cttest_walsync_syncs_the_current_file_and_not_some_other_one(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 1);
    assertf(w.head != w.cur, "setup: the head must not be the current file");
    w.head->fd = -1;
    w.wantsync = 1;
    w.syncrate = 0;
    now = 1;
    fault_clear_all();

    int r = walmaint(&w);

    assertf(r == 1 && fault_calls(FAULT_FDATASYNC) == 1,
            "the sync must target the current file's descriptor: walmaint "
            "returned %d after %d fdatasync() call(s)",
            r, fault_calls(FAULT_FDATASYNC));
}


// An error latched by the fsync thread is reported here and then
// cleared. Reporting it forever would turn one bad sync into a server
// that fails every maintenance pass for the rest of its life.
void
cttest_walsync_reports_a_latched_async_failure_exactly_once(void)
{
    wy_setup();
    Wal w;
    Job list;
    wy_wal(&w, &list, 0);
    walsyncstart(&w);
    assertf(w.sync_on == 1, "setup: the fsync thread must be running");
    fault_set(FAULT_FDATASYNC, 0, EIO);
    wy_round(&w);
    wy_round(&w);
    now = 1;
    assertf(walmaint(&w) == 0, "setup: the latched failure must be reported");

    int r = walmaint(&w);

    assertf(r == 1,
            "the latched failure was already reported, so the next pass is "
            "clean; walmaint returned %d", r);
    // Join before w leaves scope: the worker parks in pthread_cond_wait
    // on a mutex that lives in THIS frame, so returning with the thread
    // still running leaves it waiting on memory the caller may reuse.
    walsyncstop(&w);
}


