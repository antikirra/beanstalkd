// Angry tests for filewclose (file.c) — the last thing that touches a
// write-open binlog: it drops the unused preallocated tail, takes the
// final chance to flush, closes the descriptor and releases the
// writer's reference.

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
wc_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewclose";
}

static int
wc_file(char *path, size_t n, const char *name, int bytes)
{
    int k = snprintf(path, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create file: %s", strerror(errno));
    assertf(fd > 2, "setup: the wrapped syscalls skip fds 0-2");
    if (bytes > 0) {
        char *buf = calloc(1, (size_t)bytes);
        assertf(buf != NULL, "setup: calloc");
        assertf(write(fd, buf, (size_t)bytes) == (ssize_t)bytes,
                "setup: fill file");
        free(buf);
    }
    return fd;
}

static void
wc_wal(Wal *w, File *f, int fd, int filesize, int durable)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = filesize;
    w->durable_sync = durable;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->refs = 3;            // survive the decref; walgc stays out of it
    f->jlist.fprev = &f->jlist;
    f->jlist.fnext = &f->jlist;
    w->head = f;
    w->tail = f;
    w->cur = f;
}

static off_t
wc_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// Contract (3): in durable mode with an empty batch, filewrcommit has
// already synced everything — a second sync here is pure overhead on
// every rotation.
void
cttest_filewclose_skips_the_sync_when_durable_and_nothing_is_staged(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closenosync.bin", 100);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 100, 1);
    f.free = 0;
    f.uncommitted_bytes = 0;

    fault_clear_all();
    filewclose(&f);

    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "nothing is staged and durable mode already synced: %d "
            "fdatasync calls escaped", fault_calls(FAULT_FDATASYNC));
}


// The other side of the same condition: a batch still staged has never
// been synced, and close() drops the only descriptor that could ever
// reach this binlog again.
void
cttest_filewclose_syncs_when_durable_with_a_batch_still_staged(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closesync.bin", 100);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 100, 1);
    f.free = 0;
    f.uncommitted_bytes = 88;

    fault_clear_all();
    filewclose(&f);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "staged-but-unsynced bytes must be flushed before the last "
            "descriptor closes: %d fdatasync calls",
            fault_calls(FAULT_FDATASYNC));
}


// Contract (2): a file with no free space has no tail to drop, and
// touching it anyway is how live records get truncated away.
void
cttest_filewclose_skips_the_truncate_when_no_space_is_free(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closefull.bin", 4096);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 4096, 0);
    f.free = 0;

    fault_clear_all();
    filewclose(&f);

    assertf(fault_calls(FAULT_FTRUNCATE) == 0,
            "an exactly filled binlog has no tail to drop: %d ftruncate "
            "calls escaped", fault_calls(FAULT_FTRUNCATE));
}


// ...and when there IS a tail, exactly the tail goes.
void
cttest_filewclose_drops_exactly_the_unused_preallocated_tail(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closetail.bin", 4096);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 4096, 0);
    f.free = 3996;          // 100 bytes of real records were written

    filewclose(&f);

    struct stat st;
    assertf(stat(path, &st) == 0, "setup: stat closed file");
    assertf(st.st_size == 100,
            "the file must be cut back to its written length of 100 "
            "bytes, got %lld", (long long)st.st_size);
}


// Contract (1): walg.c's WAL-disable path closes the same File twice on
// purpose. A second close must not close(-1) and must not release a
// second reference — that under-count is what pins or frees the wrong
// binlog later.
void
cttest_filewclose_is_idempotent_on_an_already_closed_file(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closetwice.bin", 100);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 100, 0);
    f.free = 0;

    filewclose(&f);
    uint after_first = f.refs;
    filewclose(&f);

    assertf(f.refs == after_first,
            "the second close must be a no-op: refs went %u -> %u",
            after_first, f.refs);
}


void
cttest_filewclose_on_null_leaves_a_later_close_working(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closenull.bin", 100);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 100, 0);
    f.free = 0;

    filewclose(NULL);
    filewclose(&f);

    assertf(f.fd == -1,
            "filewclose(NULL) is a no-op and must leave the next close "
            "working, got fd %d", f.fd);
}


// f->free is signed and walg.c only guards the fast path, so an
// over-committed reservation can leave it negative. `filesize - free`
// is then LARGER than the file, and the "drop the tail" truncate
// silently extends the binlog with a hole instead.
void
cttest_filewclose_with_an_overcommitted_reservation_must_not_grow_the_file(void)
{
    wc_setup();
    char path[512];
    int fd = wc_file(path, sizeof path, "closeneg.bin", 100);

    Wal w;
    File f;
    wc_wal(&w, &f, fd, 4096, 0);
    f.free = -100;

    off_t before = wc_size(fd);
    filewclose(&f);

    struct stat st;
    assertf(stat(path, &st) == 0, "setup: stat closed file");
    assertf(st.st_size <= before,
            "closing a binlog may only ever shorten it: it grew from %lld "
            "to %lld bytes", (long long)before, (long long)st.st_size);
}
