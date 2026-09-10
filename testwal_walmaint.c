// Angry tests for walmaint (walg.c) — the periodic maintenance the main
// loop calls on every tick.
//
// It owes the caller two things: it must not do compaction work more
// often than once per millisecond of the global clock, and it must
// report a wal that died during the call. The first is a latency
// promise; the second is the difference between a maintenance pass that
// noticed the wal is gone and one that says everything is fine.

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
wm_setup(void)
{
    fault_clear_all();
    progname = "testwal_walmaint";
    now = 0;
}

static void
wm_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Seeds <ctdir>/binlog.<seq> with a version header and one full v8
// record, so the replayed file holds exactly one live job.
static void
wm_seed(int seq, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wm_write(fd, &ver, sizeof ver);

    const char *tube = "m";
    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'm', sizeof body);
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

    wm_write(fd, &nl, sizeof nl);
    wm_write(fd, tube, (size_t)nl);
    wm_write(fd, &jr, sizeof jr);
    wm_write(fd, body, sizeof body);
    wm_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wm_seeds(int n)
{
    for (int i = 1; i <= n; i++) wm_seed(i, 300 + (uint64)i);
}

// A wal with `old` migratable binlogs behind the writable one, i.e. a
// fill ratio far above the compaction threshold.
static void
wm_wal(Wal *w, Job *l, int old)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    wm_seeds(old);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == old + 1, "setup: expected %d files, got %d",
            old + 1, w->nfile);
    assertf(w->alive > 0, "setup: the seeded jobs must be counted as live");
}


// Compaction is rate-limited to once per millisecond of the global
// clock. Inside the window it must not run, or a pipelining client turns
// every command into a compaction pass.
void
cttest_walmaint_skips_compaction_inside_the_rate_limit_window(void)
{
    wm_setup();
    Wal w;
    Job list;
    wm_wal(&w, &list, 4);
    now = 999999;

    walmaint(&w);

    assertf(w.nmig == 0,
            "the rate-limit interval has not elapsed, so nothing may "
            "migrate: nmig is %" PRId64, w.nmig);
}


// The other side of the same boundary: once the interval has elapsed,
// compaction has to actually happen, or the wal grows without bound
// while every maintenance call reports success.
void
cttest_walmaint_compacts_once_the_rate_limit_window_has_elapsed(void)
{
    wm_setup();
    Wal w;
    Job list;
    wm_wal(&w, &list, 4);
    now = 1000000;

    walmaint(&w);

    assertf(w.nmig > 0,
            "the interval has elapsed and the wal is mostly dead space, so "
            "compaction must run: nmig is %" PRId64, w.nmig);
}


// A wal that was already off before the call is not this call's failure.
// Reporting one would make an ordinary degraded server look like it is
// failing on every tick.
void
cttest_walmaint_reports_success_on_an_already_disabled_wal(void)
{
    wm_setup();
    Wal w;
    Job list;
    wm_wal(&w, &list, 4);
    w.use = 0;
    now = 1000000;

    int r = walmaint(&w);

    assertf(r == 1,
            "a wal that was already disabled is not a maintenance failure, "
            "walmaint returned %d", r);
}


// ...and it must not do the work either. Syncing a file handle that
// belongs to a dead wal is at best wasted and at worst a sync of a
// descriptor that has been recycled.
void
cttest_walmaint_does_no_work_on_an_already_disabled_wal(void)
{
    wm_setup();
    Wal w;
    Job list;
    wm_wal(&w, &list, 4);
    w.use = 0;
    w.wantsync = 1;
    w.syncrate = 0;
    now = 1000000;
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "a disabled wal must not be synced: %d fdatasync() call(s) "
            "escaped", fault_calls(FAULT_FDATASYNC));
}


// When compaction kills the wal, the maintenance pass has to say so. A
// batch where one migration succeeded and the next one destroyed the wal
// is not a successful maintenance pass, and the caller has no other way
// to find out.
void
cttest_walmaint_reports_failure_when_compaction_disabled_the_wal(void)
{
    wm_setup();
    Wal w;
    Job list;
    wm_wal(&w, &list, 4);
    now = 1000000;
    fault_clear_all();
    fault_set(FAULT_WRITEV, 1, EIO);

    int r = walmaint(&w);

    assertf(r == 0,
            "the second migration disabled the wal, so this pass failed: "
            "walmaint returned %d with use=%d", r, w.use);
}


