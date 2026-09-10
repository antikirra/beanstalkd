// Angry tests for fileincref (file.c). The counter is what keeps a
// binlog on disk: while it is above zero walgc must not reap the file.
// The unit itself is three lines, so the attacks aim at the promise
// around it — the NULL guard, exact pairing with filedecref, and the
// reaping refusal the count exists to produce.

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
ir_setup(void)
{
    fault_clear_all();
    progname = "testfile_fileincref";
}

static int
ir_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}


// The NULL guard is the promise; a test that merely calls it and
// survives proves nothing, so the call is followed by a real increment
// that has to still be exactly one.
void
cttest_fileincref_on_null_leaves_the_next_increment_correct(void)
{
    ir_setup();
    File f;
    memset(&f, 0, sizeof f);
    f.refs = 4;

    fileincref(NULL);
    fileincref(&f);

    assertf(f.refs == 5,
            "fileincref(NULL) is a no-op and the next increment is still "
            "exactly one: expected 5, got %u", f.refs);
}


void
cttest_fileincref_and_filedecref_are_exact_inverses(void)
{
    ir_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    f.w = &w;
    f.refs = 5;
    w.head = &f;
    w.tail = &f;
    w.cur = &f;

    fileincref(&f);
    filedecref(&f);

    assertf(f.refs == 5,
            "one reference taken and one released must leave the count "
            "where it started: expected 5, got %u", f.refs);
}


// The reason the counter exists: a referenced binlog survives the
// collector. Held with fileincref rather than by poking the field, so
// an increment that does not increment turns this red.
void
cttest_fileincref_pins_a_binlog_against_the_collector(void)
{
    ir_setup();
    char keep[512];
    int k = snprintf(keep, sizeof keep, "%s/binlog.7001", ctdir());
    assertf(k > 0 && (size_t)k < sizeof keep, "setup: path did not fit");
    int fd = open(keep, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create binlog: %s", strerror(errno));
    close(fd);

    Wal w;
    memset(&w, 0, sizeof w);
    w.dir = ctdir();
    File *f = calloc(1, sizeof(File));
    assertf(f != NULL, "setup: calloc File");
    f->w = &w;
    f->path = strdup(keep);
    assertf(f->path != NULL, "setup: strdup path");
    fileadd(f, &w);
    f->refs = 0;

    fileincref(f);
    walgc(&w);

    assertf(ir_exists(keep),
            "a binlog with a live reference must survive the collector");
}
