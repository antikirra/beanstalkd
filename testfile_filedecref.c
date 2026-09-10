// Angry tests for filedecref (file.c) — the release that makes a binlog
// reapable. Its counter is an unsigned int, so the difference between
// "refuses" and "wraps to 4294967295" is the difference between a
// reaped file and one pinned on disk forever, with no warning either
// way.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
dr_setup(void)
{
    fault_clear_all();
    progname = "testfile_filedecref";
}

static int
dr_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

// Creates a real binlog file and a heap File registered in w, so walgc
// may legitimately unlink and free both.
static File *
dr_file(Wal *w, char *keep, size_t n, const char *name)
{
    int k = snprintf(keep, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: path did not fit");
    int fd = open(keep, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create binlog: %s", strerror(errno));
    close(fd);

    File *f = calloc(1, sizeof(File));
    assertf(f != NULL, "setup: calloc File");
    f->w = w;
    f->path = strdup(keep);
    assertf(f->path != NULL, "setup: strdup path");
    fileadd(f, w);
    return f;
}


void
cttest_filedecref_on_null_leaves_the_next_release_correct(void)
{
    dr_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    f.w = &w;
    f.refs = 4;
    w.head = &f;
    w.tail = &f;
    w.cur = &f;

    filedecref(NULL);
    filedecref(&f);

    assertf(f.refs == 3,
            "filedecref(NULL) is a no-op and the next release is still "
            "exactly one: expected 3, got %u", f.refs);
}


// The whole point of the count reaching zero.
void
cttest_filedecref_to_zero_reaps_a_head_binlog(void)
{
    dr_setup();
    Wal w;
    memset(&w, 0, sizeof w);
    w.dir = ctdir();
    char keep[512];
    File *f = dr_file(&w, keep, sizeof keep, "binlog.7101");
    f->refs = 1;

    filedecref(f);

    assertf(!dr_exists(keep),
            "the last reference is gone, so the binlog must be unlinked");
}


// walgc's documented refusal, which walwrite's WAL-disable path depends
// on: the file w->cur still points at is never freed underneath it.
void
cttest_filedecref_never_unlinks_the_current_binlog(void)
{
    dr_setup();
    Wal w;
    memset(&w, 0, sizeof w);
    w.dir = ctdir();
    char keep[512];
    File *f = dr_file(&w, keep, sizeof keep, "binlog.7102");
    f->refs = 1;
    w.cur = f;

    filedecref(f);

    assertf(dr_exists(keep),
            "the current binlog must never be reaped: w->cur would be left "
            "dangling and fmt_stats reads through it");
}


// An over-release is a bug wherever it comes from, but the counter is
// unsigned: one decrement too many turns 0 into 4294967295, `refs < 1`
// stops being true, and the file is pinned out of walgc's reach for the
// life of the process. Every later binlog queues up behind it.
void
cttest_filedecref_must_not_wrap_the_refcount_below_zero(void)
{
    dr_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    f.w = &w;
    f.refs = 0;
    w.head = &f;
    w.tail = &f;
    w.cur = &f;          // keep the collector away from a stack File

    filedecref(&f);

    assertf(f.refs == 0,
            "a release must never raise the reference count: it went from "
            "0 to %u, pinning the binlog forever", f.refs);
}
