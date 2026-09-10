// Angry tests for walcompact (walg.c) — the migration driver behind
// walmaint.
//
// Its contract is a single bit: 0 if and only if the wal was disabled
// during the attempt. That bit is the only channel through which a wal
// that died in the middle of a maintenance pass can be reported, and it
// is computed from a loop counter rather than from the wal's state.

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
wp_setup(void)
{
    fault_clear_all();
    progname = "testwal_walcompact";
    now = 0;
}

static void
wp_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Seeds <ctdir>/binlog.<seq> with a version header and one full v8
// record whose body is `bodylen` bytes.
static void
wp_seed(int seq, uint64 id, int bodylen)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wp_write(fd, &ver, sizeof ver);

    const char *tube = "p";
    int nl = (int)strlen(tube);
    char *body = malloc((size_t)bodylen);
    assertf(body != NULL, "setup: body buffer");
    memset(body, 'p', (size_t)bodylen);
    body[bodylen-2] = '\r';
    body[bodylen-1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 8;
    jr.ttr = 120000000000LL;
    jr.body_size = bodylen;
    jr.created_at = 4;
    jr.state = Ready;

    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &nl, sizeof nl);
    c = wal_crc32c(c, tube, (size_t)nl);
    c = wal_crc32c(c, &jr, sizeof jr);
    c = wal_crc32c(c, body, (size_t)bodylen);
    c ^= WAL_CRC32C_XOR;
    unsigned char tr[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };

    wp_write(fd, &nl, sizeof nl);
    wp_write(fd, tube, (size_t)nl);
    wp_write(fd, &jr, sizeof jr);
    wp_write(fd, body, (size_t)bodylen);
    wp_write(fd, tr, sizeof tr);
    free(body);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wp_seeds(int n, int bodylen)
{
    for (int i = 1; i <= n; i++) wp_seed(i, 700 + (uint64)i, bodylen);
}

static void
wp_wal(Wal *w, Job *l, int old, int bodylen)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    wp_seeds(old, bodylen);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == old + 1, "setup: expected %d files, got %d",
            old + 1, w->nfile);
    assertf(w->head != w->cur && w->head->next != w->cur,
            "setup: there must be a migratable file behind the head");
    now = 1000000;
}


// A batch that disables the wal is a failed compaction, however many
// migrations happened to succeed before it. Deriving the verdict from a
// loop counter reports a dead wal as a healthy maintenance pass.
void
cttest_walcompact_reports_failure_when_a_migration_disabled_the_wal(void)
{
    wp_setup();
    Wal w;
    Job list;
    wp_wal(&w, &list, 6, 8);
    fault_clear_all();
    fault_set(FAULT_WRITEV, 2, EIO);

    int r = walmaint(&w);

    assertf(r == 0,
            "two migrations succeeded and the third destroyed the wal; the "
            "pass must report failure but returned %d with use=%d",
            r, w.use);
}


// Below the fill-ratio threshold there is nothing worth moving. A wal
// whose files are mostly live must not rewrite them: the migration is
// pure write amplification with no space reclaimed.
void
cttest_walcompact_migrates_nothing_below_the_ratio_threshold(void)
{
    wp_setup();
    Wal w;
    Job list;
    wp_wal(&w, &list, 2, 3900);

    walmaint(&w);

    assertf(w.nmig == 0,
            "the wal is almost entirely live, so compaction must not move "
            "anything: nmig is %" PRId64, w.nmig);
}


// ...and it must not even reach for the head file. "Nothing to do" has
// to be free, because it is the common case on every tick.
void
cttest_walcompact_writes_nothing_below_the_ratio_threshold(void)
{
    wp_setup();
    Wal w;
    Job list;
    wp_wal(&w, &list, 2, 3900);
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_WRITEV) == 0,
            "below the threshold nothing is rewritten: %d writev() call(s) "
            "escaped", fault_calls(FAULT_WRITEV));
}


// Compaction converges: it keeps migrating while the dead space
// dominates and stops when the head is adjacent to the file being
// written. A loop that stops early leaks binlogs forever; one that never
// stops hangs the event loop.
void
cttest_walcompact_converges_until_the_head_is_next_to_the_current_file(void)
{
    wp_setup();
    Wal w;
    Job list;
    wp_wal(&w, &list, 10, 8);

    walmaint(&w);

    assertf(w.nfile == 2,
            "every migratable binlog must be drained and reaped, leaving "
            "the current file and its predecessor: nfile is %d", w.nfile);
}


// The positive control for the threshold pair above: with the dead space
// dominating, compaction has to run. A threshold that never fires is
// indistinguishable from one that fires correctly unless both sides are
// pinned.
void
cttest_walcompact_migrates_when_the_dead_space_dominates(void)
{
    wp_setup();
    Wal w;
    Job list;
    wp_wal(&w, &list, 4, 8);

    walmaint(&w);

    assertf(w.nmig > 0,
            "four nearly-empty binlogs behind the writer is exactly what "
            "compaction is for: nmig is %" PRId64, w.nmig);
}
