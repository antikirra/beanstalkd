// Angry tests for walscandir (walg.c) — the directory scan that decides
// which binlog sequence a restarting server picks up from.
//
// It is reached only through walinit(), and its whole output is two
// numbers: the seq replay starts at, and w->next (the seq of the first
// file the server will write). Both are invisible in the normal case and
// catastrophic when wrong: a next that is too low overwrites live data,
// a next that is too high replays from a file that was never written.
// The seq of the file walinit ends up writing to is the observable that
// carries both.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
ws_setup(void)
{
    fault_clear_all();
    progname = "testwal_walscandir";
}

// Creates <ctdir>/<name> as an empty file. walscandir never reads a
// binlog, so an empty one is a faithful stand-in for a real file as far
// as the scan is concerned.
static void
ws_touch(const char *name)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));
    assertf(close(fd) == 0, "setup: close %s", path);
}

static void
ws_wal(Wal *w)
{
    memset(w, 0, sizeof *w);
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
}

static Job *
ws_list(Job *l)
{
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    return l;
}


// The documented contract for a directory holding nothing: w->next is 1,
// so the very first file a fresh server writes is binlog.1. A binlog.0
// would be rejected by this same scanner on the next restart, which is
// silent, permanent data loss.
void
cttest_walscandir_names_the_first_binlog_one_in_an_empty_directory(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 1,
            "an empty wal dir must start at binlog.1, got binlog.%d",
            w.cur ? w.cur->seq : -1);
}


// strtol is far more forgiving than the on-disk name format: it skips
// leading whitespace and accepts a leading '+'. Neither is a name this
// server can ever write, so neither may steer w->next — a "binlog.+7"
// dropped in the directory would otherwise make recovery start reading
// at a binlog.7 that never existed.
void
cttest_walscandir_rejects_a_seq_suffix_only_strtol_would_accept(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.1");
    ws_touch("binlog.+7");
    ws_touch("binlog. 7");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 2,
            "only binlog.1 is a real sequence, so the next file is "
            "binlog.2; got binlog.%d", w.cur ? w.cur->seq : -1);
}


// 0 is not a valid first sequence: the writer never produces binlog.0,
// so finding one must not shift the numbering.
void
cttest_walscandir_ignores_a_zero_sequence_binlog(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.0");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 1,
            "binlog.0 is not a valid sequence, so the scan must behave as "
            "if the directory were empty and start at binlog.1; got "
            "binlog.%d", w.cur ? w.cur->seq : -1);
}


// "binlog." carries an empty suffix. strtol reports 0 for it and leaves
// its end pointer at the start, which is the only thing separating this
// from a legitimate binlog.0.
void
cttest_walscandir_ignores_an_empty_sequence_suffix(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 1,
            "a name with no sequence at all must not be scanned as a "
            "binlog; expected binlog.1, got binlog.%d",
            w.cur ? w.cur->seq : -1);
}


// INT_MAX is the one sequence that cannot be accepted: max+1 would
// overflow and w->next would land at INT_MIN, after which every name the
// server writes is negative and unreadable on the next restart.
void
cttest_walscandir_ignores_the_int_max_sequence(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.2147483647");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 1,
            "a seq of INT_MAX cannot be followed by max+1 and must be "
            "ignored; expected binlog.1, got binlog.%d",
            w.cur ? w.cur->seq : -1);
}


// INT_MAX-1 is the documented reason the guard is `n < INT_MAX` rather
// than a smaller cap. The greatest sequence a server can actually put
// into service is one lower still: the test above fixes binlog.INT_MAX
// as a name this same scanner ignores, so INT_MAX-1 is the last file
// the writer may create and INT_MAX-2 the last one it may find.
void
cttest_walscandir_accepts_the_greatest_usable_sequence(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.2147483645");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 2147483646,
            "binlog.2147483645 is a valid sequence, so the next file is "
            "binlog.2147483646; got binlog.%d", w.cur ? w.cur->seq : -1);
}


// A hole in the sequence (files reaped out of the middle, or a crash
// between two rotations) must not make the server reuse a number: the
// next file is max+1, never a count of what is on disk.
void
cttest_walscandir_never_reuses_a_sequence_that_falls_in_a_gap(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.1");
    ws_touch("binlog.5");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 6,
            "the next file follows the highest sequence present (5), not "
            "the number of files; expected binlog.6, got binlog.%d",
            w.cur ? w.cur->seq : -1);
}


// The scan must not care where in the directory listing the highest
// sequence turns up: readdir order is unspecified, and a scanner that
// keeps the last match instead of the largest one is order-dependent.
// Twelve names make an accidental "the last one happened to be right"
// pass unlikely, and 10 vs 9 catches a textual rather than numeric
// comparison.
void
cttest_walscandir_picks_the_numerically_highest_sequence(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.10");
    ws_touch("binlog.9");
    ws_touch("binlog.3");
    ws_touch("binlog.1");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 11,
            "10 is the highest sequence present even though 9 sorts after "
            "it as text; expected binlog.11, got binlog.%d",
            w.cur ? w.cur->seq : -1);
}


// Names that merely start with the prefix are not binlogs. "binlog.1x"
// has a numeric head and a trailing character, which is exactly what the
// end-pointer check exists to reject.
void
cttest_walscandir_rejects_a_sequence_with_a_trailing_character(void)
{
    ws_setup();
    Wal w;
    Job list;
    ws_wal(&w);
    ws_touch("binlog.1x");

    walinit(&w, ws_list(&list));

    assertf(w.cur && w.cur->seq == 1,
            "binlog.1x is not a binlog and must not set the sequence; "
            "expected binlog.1, got binlog.%d", w.cur ? w.cur->seq : -1);
}
