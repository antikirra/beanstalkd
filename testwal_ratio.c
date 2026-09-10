// Angry tests for ratio (walg.c) — the one number that decides whether
// compaction runs at all.
//
// It is arithmetic with no I/O and no callers outside walcompact, which
// is exactly why nothing tests it: every mistake in it turns into
// "compaction quietly stopped happening" or "compaction runs forever",
// both of which look like a healthy server for a long time.

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
rt_setup(void)
{
    fault_clear_all();
    progname = "testwal_ratio";
    now = 0;
}

static void
rt_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static void
rt_seed(int seq, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    rt_write(fd, &ver, sizeof ver);

    const char *tube = "r";
    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'r', sizeof body);
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

    rt_write(fd, &nl, sizeof nl);
    rt_write(fd, tube, (size_t)nl);
    rt_write(fd, &jr, sizeof jr);
    rt_write(fd, body, sizeof body);
    rt_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
rt_seeds(int n)
{
    for (int i = 1; i <= n; i++) rt_seed(i, 100 + (uint64)i);
}

static void
rt_wal(Wal *w, Job *l, int old)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    rt_seeds(old);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == old + 1, "setup: expected %d files, got %d",
            old + 1, w->nfile);
    now = 1000000;
}


// Nothing live and nothing reserved is a division by zero waiting to
// happen, and it is the state every server is in for the first moments
// after it starts.
void
cttest_ratio_starts_no_compaction_when_nothing_is_live(void)
{
    rt_setup();
    Wal w;
    Job list;
    rt_wal(&w, &list, 0);
    assertf(w.alive == 0 && w.resv == 0,
            "setup: a fresh wal has nothing live and nothing reserved");

    walmaint(&w);

    assertf(w.nmig == 0,
            "an empty wal has no dead space to reclaim: nmig is %" PRId64,
            w.nmig);
}


// The positive control: dead space dominating is exactly the state
// compaction exists for. Without this, a metric stuck at zero would
// satisfy every other test in this file.
void
cttest_ratio_starts_compaction_when_the_dead_space_dominates(void)
{
    rt_setup();
    Wal w;
    Job list;
    rt_wal(&w, &list, 4);

    walmaint(&w);

    assertf(w.nmig > 0,
            "four nearly-empty binlogs is a fill ratio well above the "
            "threshold: nmig is %" PRId64, w.nmig);
}


// As the live bytes approach the allocated size the metric falls to
// zero. This monotonicity is the property the `>= 2` threshold relies
// on: a metric that ignores the live term never stops compacting.
void
cttest_ratio_stops_compaction_when_the_live_bytes_fill_the_allocation(void)
{
    rt_setup();
    Wal w;
    Job list;
    rt_wal(&w, &list, 4);
    w.alive = (int64)w.nfile * (int64)w.filesize;

    walmaint(&w);

    assertf(w.nmig == 0,
            "every allocated byte is live, so there is no dead space to "
            "reclaim: nmig is %" PRId64, w.nmig);
}


// Accounting drift after a rollback can leave more claimed as live than
// was ever allocated. That is a nonsensical state, and the safe reading
// of it is "nothing to compact" — never a large positive pressure
// derived from an absolute value or an unsigned wrap.
void
cttest_ratio_stops_compaction_when_the_accounting_exceeds_the_allocation(void)
{
    rt_setup();
    Wal w;
    Job list;
    rt_wal(&w, &list, 4);
    w.alive = 2 * (int64)w.nfile * (int64)w.filesize;

    walmaint(&w);

    assertf(w.nmig == 0,
            "more bytes are claimed live than were ever allocated, so the "
            "pressure is not positive: nmig is %" PRId64, w.nmig);
}


// A large -s with a handful of files puts the allocated term far past
// what an int can hold. The 64-bit multiply exists for exactly this, and
// losing it turns a wal that desperately needs compacting into one that
// reports no pressure at all.
void
cttest_ratio_keeps_compacting_when_the_allocation_exceeds_int_max(void)
{
    rt_setup();
    Wal w;
    Job list;
    rt_wal(&w, &list, 3);
    w.filesize = 1 << 30;
    w.nfile = 8;
    w.alive = 2147483648LL;

    walmaint(&w);

    assertf(w.nmig > 0,
            "eight one-gigabyte files against two gigabytes of live data "
            "is a pressure of three: compaction must run, nmig is %" PRId64,
            w.nmig);
}
