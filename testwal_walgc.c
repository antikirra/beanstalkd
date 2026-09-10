// Angry tests for walgc (walg.c) — the reaper that unlinks binlogs once
// nothing references them any more.
//
// It is the only code in the server that deletes data on purpose, and it
// runs from filedecref, i.e. from the middle of unrelated operations. The
// two things it must never do — reap the file being written, and forget a
// file it failed to delete — both leave no trace until much later.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <dirent.h>
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
wg_setup(void)
{
    fault_clear_all();
    progname = "testwal_walgc";
}

static void
wg_write(int fd, const void *p, size_t n)
{
    ssize_t r = write(fd, p, n);
    assertf(r == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Seeds <ctdir>/binlog.<seq> with a version header and one full v8
// record, so the replayed file ends up holding exactly one live job.
static void
wg_seed(int seq, const char *tube, uint64 id)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));

    int ver = Walver;
    wg_write(fd, &ver, sizeof ver);

    int nl = (int)strlen(tube);
    char body[8];
    memset(body, 'g', sizeof body);
    body[sizeof body - 2] = '\r';
    body[sizeof body - 1] = '\n';

    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 4;
    jr.ttr = 120000000000LL;
    jr.body_size = (int32)sizeof body;
    jr.created_at = 2;
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

    wg_write(fd, &nl, sizeof nl);
    wg_write(fd, tube, (size_t)nl);
    wg_write(fd, &jr, sizeof jr);
    wg_write(fd, body, sizeof body);
    wg_write(fd, tr, sizeof tr);
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
wg_wal(Wal *w)
{
    memset(w, 0, sizeof *w);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
}

static Job *
wg_list(Job *l)
{
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    return l;
}

static Job *
wg_find(Job *l, uint64 id)
{
    for (Job *j = l->next; j != l; j = j->next)
        if (j->r.id == id) return j;
    return NULL;
}

static int
wg_exists(int seq)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", ctdir(), seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    return stat(path, &st) == 0;
}

// Counts the binlog.* entries actually present in the wal directory.
static int
wg_ondisk(void)
{
    DIR *d = opendir(ctdir());
    assertf(d != NULL, "setup: opendir %s: %s", ctdir(), strerror(errno));
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)))
        if (strncmp(e->d_name, "binlog.", 7) == 0) n++;
    assertf(closedir(d) == 0, "setup: closedir");
    return n;
}

static int
wg_listed(Wal *w)
{
    int n = 0;
    for (File *f = w->head; f && n < 1000; f = f->next) n++;
    return n;
}


// The file w->cur points at carries the writer's reference, but the
// WAL-disable paths drop it while cur still points there. Reaping it
// would free the struct out from under w->cur and unlink the binlog that
// holds the records of the tick that just failed.
void
cttest_walgc_never_reaps_the_file_the_wal_is_writing_to(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    walinit(&w, wg_list(&list));
    assertf(w.head == w.cur, "setup: a fresh wal has one file");

    filewclose(w.cur);

    assertf(wg_exists(1),
            "dropping the writer's reference must not unlink the file "
            "w->cur still points at: binlog.1 is gone");
}


// A head binlog whose last job has been removed is dead weight and must
// leave the disk.
void
cttest_walgc_unlinks_a_head_binlog_that_lost_its_last_job(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 601);
    wg_seed(2, "beta", 602);
    walinit(&w, wg_list(&list));
    Job *a = wg_find(&list, 601);
    assertf(a && a->file, "setup: job 601 must be replayed and filed");

    filermjob(a->file, a);

    assertf(!wg_exists(1),
            "binlog.1 holds no jobs and is not the current file, so it "
            "must be unlinked; it is still on disk");
}


// Reaping is a leading run, not a sweep: the first file that still holds
// a job stops it, whatever lies behind that file.
void
cttest_walgc_stops_at_the_first_binlog_that_still_holds_a_job(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 611);
    wg_seed(2, "beta", 612);
    wg_seed(3, "gamma", 613);
    walinit(&w, wg_list(&list));
    Job *a = wg_find(&list, 611);
    assertf(a && a->file, "setup: job 611 must be replayed and filed");

    filermjob(a->file, a);

    assertf(wg_exists(2),
            "binlog.2 still holds job 612 and must survive the reap of "
            "binlog.1; it was unlinked anyway");
}


// The file list and the counter that describes it are read by ratio()
// on every maintenance pass. They must agree after every reap.
void
cttest_walgc_keeps_nfile_equal_to_the_length_of_the_file_list(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 621);
    wg_seed(2, "beta", 622);
    walinit(&w, wg_list(&list));
    Job *a = wg_find(&list, 621);
    assertf(a && a->file, "setup: job 621 must be replayed and filed");

    filermjob(a->file, a);

    assertf(wg_listed(&w) == w.nfile,
            "after a reap the list holds %d file(s) while nfile says %d",
            wg_listed(&w), w.nfile);
}


// Reaping a run of files costs one directory fsync, not one per file:
// the unlinks are only durable together, and the cost bound is what
// makes reaping affordable on the event-loop thread.
void
cttest_walgc_syncs_the_directory_once_for_a_run_of_reaped_files(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 631);
    wg_seed(2, "beta", 632);
    wg_seed(3, "gamma", 633);
    walinit(&w, wg_list(&list));
    Job *a = wg_find(&list, 631);
    Job *b = wg_find(&list, 632);
    assertf(a && a->file && b && b->file, "setup: both jobs must be filed");
    filermjob(b->file, b);   // binlog.2 is now ref-free but not the head
    fault_clear_all();

    filermjob(a->file, a);   // reaps binlog.1 and binlog.2 in one pass

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "two reaped files must cost exactly one directory sync, got %d",
            fault_calls(FAULT_FDATASYNC));
}


// Nothing reapable means nothing touched. A reaper that unlinks first and
// checks later is indistinguishable from this one on a healthy wal.
void
cttest_walgc_unlinks_nothing_while_the_head_still_holds_a_job(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 641);
    wg_seed(2, "beta", 642);
    walinit(&w, wg_list(&list));
    fault_clear_all();

    walgc(&w);

    assertf(fault_calls(FAULT_UNLINK) == 0,
            "no file is reapable, so nothing may be unlinked: %d unlink() "
            "call(s) escaped", fault_calls(FAULT_UNLINK));
}


// ...and it must not pay for a directory sync either. "Did nothing"
// has to be cheaper than "swept everything", or every filedecref in the
// server drags an fsync behind it.
void
cttest_walgc_does_not_sync_the_directory_when_it_reaped_nothing(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 651);
    wg_seed(2, "beta", 652);
    walinit(&w, wg_list(&list));
    fault_clear_all();

    walgc(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "nothing was reaped, so the directory must not be synced: %d "
            "fdatasync() call(s) escaped", fault_calls(FAULT_FDATASYNC));
}


// A binlog that could not be unlinked is still on disk and still costs
// its whole filesize. Dropping it from nfile while it lives on makes the
// wal's own accounting disagree with the directory, and ratio() — the
// input to every compaction decision — reads that accounting.
void
cttest_walgc_keeps_a_binlog_it_failed_to_unlink_in_its_accounting(void)
{
    wg_setup();
    Wal w;
    Job list;
    wg_wal(&w);
    wg_seed(1, "alpha", 661);
    wg_seed(2, "beta", 662);
    walinit(&w, wg_list(&list));
    Job *a = wg_find(&list, 661);
    assertf(a && a->file, "setup: job 661 must be replayed and filed");
    fault_set(FAULT_UNLINK, 0, EACCES);

    filermjob(a->file, a);

    assertf(wg_ondisk() == w.nfile,
            "the unlink failed, so binlog.1 still occupies the directory: "
            "%d binlog(s) on disk but nfile says %d",
            wg_ondisk(), w.nfile);
}
