// Angry tests for walread (walg.c) — the replay loop that turns binlogs
// back into jobs on startup.
//
// Its failure mode is the quietest one in the server: a file that cannot
// be opened or read is skipped, and the jobs it held simply are not
// there any more. Every test here is built so that a swallowed file is
// visible as a missing job rather than as a warning nobody reads.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

// walread has external linkage but no declaration in dat.h; walinit is
// its only production caller.

static void
wr_setup(void)
{
    fault_clear_all();
    progname = "testwal_walread";
}

static void
wr_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Seeds <ctdir>/binlog.<seq> with a version header and one full v8
// record for job `id` in tube `tube`.
static void
wr_seed(int seq, const char *tube, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wr_write(fd, &ver, sizeof ver);

    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'z', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 5;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 1;
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

    wr_write(fd, &nl, sizeof nl);
    wr_write(fd, tube, (size_t)nl);
    wr_write(fd, &jr, sizeof jr);
    wr_write(fd, body, sizeof body);
    wr_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wr_wal(Wal *w, int next)
{
    memset(w, 0, sizeof *w);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    w->next = next;
}

static Job *
wr_list(Job *l)
{
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    return l;
}

static int
wr_count(Job *l)
{
    int n = 0;
    for (Job *j = l->next; j != l; j = j->next) n++;
    return n;
}

// The id of the only replayed job, or 0 when the replay produced
// anything other than exactly one job.
static uint64
wr_only_id(Job *l)
{
    if (wr_count(l) != 1) return 0;
    return l->next->r.id;
}

// The number the next open() would return, as a proxy for "how many
// descriptors are in use": fd allocation is lowest-available, so this is
// stable exactly while nothing has been leaked.
static int
wr_fdmark(void)
{
    int fd = open("/dev/null", O_RDONLY);
    assertf(fd >= 0, "setup: /dev/null probe: %s", strerror(errno));
    assertf(close(fd) == 0, "setup: close /dev/null probe");
    return fd;
}


// A binlog that cannot be opened costs its own jobs, which is bad
// enough. It must not cost the jobs of every later file too.
void
cttest_walread_replays_the_next_binlog_after_one_cannot_be_opened(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 3);
    wr_seed(1, "alpha", 101);
    wr_seed(2, "beta", 202);

    fault_set(FAULT_OPEN, 0, EACCES);
    walread(&w, wr_list(&list), 1);

    assertf(wr_only_id(&list) == 202,
            "binlog.1 was unopenable, so replay must still deliver job 202 "
            "from binlog.2 and nothing else; got %d job(s), first id %"
            PRIu64, wr_count(&list), wr_count(&list) ? list.next->r.id : 0);
}


// A read error inside one file is reported and the loop moves on; the
// healthy file behind it is fully replayed.
void
cttest_walread_continues_past_a_binlog_whose_read_fails(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 3);
    wr_seed(1, "alpha", 111);
    wr_seed(2, "beta", 222);

    fault_set(FAULT_READ, 0, EIO);
    walread(&w, wr_list(&list), 1);

    assertf(wr_only_id(&list) == 222,
            "a read error in binlog.1 must not abandon binlog.2: expected "
            "job 222 alone, got %d job(s), first id %" PRIu64,
            wr_count(&list), wr_count(&list) ? list.next->r.id : 0);
}


// A directory that happens to be named binlog.3 opens fine and then
// fails every read. Recovery must treat it as a damaged file and carry
// on, not as the end of the replay.
void
cttest_walread_continues_past_a_binlog_that_is_really_a_directory(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 5);
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.3", ctdir());
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    assertf(mkdir(path, 0700) == 0, "setup: mkdir %s: %s", path, strerror(errno));
    wr_seed(4, "gamma", 404);

    walread(&w, wr_list(&list), 3);

    assertf(wr_only_id(&list) == 404,
            "an unreadable entry in the range must not stop the replay: "
            "expected job 404 from binlog.4, got %d job(s), first id %"
            PRIu64, wr_count(&list), wr_count(&list) ? list.next->r.id : 0);
}


// Every job in every file of the range reaches the caller's list.
void
cttest_walread_replays_every_job_in_the_range(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 4);
    wr_seed(1, "one", 1001);
    wr_seed(2, "two", 1002);
    wr_seed(3, "three", 1003);

    walread(&w, wr_list(&list), 1);

    assertf(wr_count(&list) == 3,
            "three seeded binlogs hold three jobs; the replay produced %d",
            wr_count(&list));
}


// Every replayed file is registered with the Wal, or walgc can never
// reap it and the compaction accounting under-counts what is on disk.
void
cttest_walread_registers_every_binlog_it_replayed(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 3);
    wr_seed(1, "one", 2001);
    wr_seed(2, "two", 2002);

    walread(&w, wr_list(&list), 1);

    assertf(w.nfile == 2,
            "both replayed binlogs must be registered with the wal: nfile "
            "is %d", w.nfile);
}


// min == w->next is the fresh-directory case: there is nothing to
// replay, and the loop must not reach for a file at all.
void
cttest_walread_opens_nothing_when_the_replay_range_is_empty(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 5);

    fault_clear_all();
    walread(&w, wr_list(&list), 5);

    assertf(fault_calls(FAULT_OPEN) == 0,
            "an empty range must not open anything: %d open() call(s) "
            "escaped", fault_calls(FAULT_OPEN));
}


// A scan that found no files hands back INT_MAX. Nothing may be
// registered with the Wal on the strength of a sequence that does not
// exist.
void
cttest_walread_registers_no_file_when_the_scan_found_nothing(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 1);

    walread(&w, wr_list(&list), INT_MAX);

    assertf(w.nfile == 0,
            "nothing was replayed, so no file may be registered: nfile is "
            "%d", w.nfile);
}


// Replay reads each binlog once and hands the descriptor back. A
// per-file leak here is unbounded in the number of binlogs a long-lived
// directory has accumulated, and it happens before the server has even
// started serving.
void
cttest_walread_closes_every_descriptor_it_opened(void)
{
    wr_setup();
    Wal w;
    Job list;
    wr_wal(&w, 4);
    wr_seed(1, "one", 3001);
    wr_seed(2, "two", 3002);
    wr_seed(3, "three", 3003);
    int before = wr_fdmark();

    walread(&w, wr_list(&list), 1);

    assertf(wr_fdmark() == before,
            "every replay descriptor must be closed: the next free fd "
            "moved from %d to %d", before, wr_fdmark());
}
