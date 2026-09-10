// Angry tests for durable_fsync (walg.c) — the two-line wrapper that
// decides whether a signal counts as a durability failure.
//
// It is shared by the fsync thread, walsync's inline path and dirsync, so
// its EINTR handling is the difference between "a SIGALRM arrived" and
// "the wal is dead". The periodic sync path is the cheapest door to it:
// walmaint's verdict is exactly durable_fsync's verdict.

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
df_setup(void)
{
    fault_clear_all();
    progname = "testwal_durable_fsync";
    now = 0;
}

// A wal whose periodic sync fires on the very next walmaint and whose
// compaction step is still inside its rate-limit window, so the only
// fdatasync in play is the one durable_fsync issues.
static void
df_wal(Wal *w, Job *l)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = ctdir();
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    w->wantsync = 1;
    w->syncrate = 0;
    now = 1;
}


// EINTR is not an I/O error. A wal that reports failure every time a
// signal lands during an fsync would disable itself on an idle server
// running under a debugger, a profiler or systemd's watchdog.
void
cttest_durable_fsync_reports_success_after_an_interrupted_sync(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EINTR);

    int r = walmaint(&w);

    assertf(r == 1,
            "an interrupted fdatasync must be retried, not reported as a "
            "durability failure; walmaint returned %d", r);
}


// ...and the retry has to be a real second syscall. Swallowing EINTR
// without calling again would report success for data that never reached
// stable storage.
void
cttest_durable_fsync_calls_the_kernel_again_after_an_interruption(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EINTR);

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 2,
            "one interruption costs exactly one extra fdatasync: expected "
            "2 calls, got %d", fault_calls(FAULT_FDATASYNC));
}


// A real I/O error is the opposite: it must reach the caller, because
// the caller is what turns it into a disabled wal instead of a lie.
void
cttest_durable_fsync_reports_a_media_error_to_its_caller(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EIO);

    int r = walmaint(&w);

    assertf(r == 0,
            "EIO from fdatasync means the data is not on stable storage; "
            "walmaint returned %d", r);
}


// A full filesystem is a different errno on the same path, and must not
// be quietly mapped onto a retry or onto success.
void
cttest_durable_fsync_reports_a_full_filesystem_to_its_caller(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, ENOSPC);

    int r = walmaint(&w);

    assertf(r == 0,
            "ENOSPC from fdatasync must not be swallowed; walmaint "
            "returned %d", r);
}


// Only EINTR is retried. Retrying anything else turns a dead disk into an
// endless loop on the event-loop thread.
void
cttest_durable_fsync_does_not_retry_a_media_error(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();
    fault_set(FAULT_FDATASYNC, 0, EIO);

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "a hard error must be reported after exactly one attempt, got "
            "%d fdatasync() call(s)", fault_calls(FAULT_FDATASYNC));
}


// The success path costs one syscall. Two would double the cost of every
// periodic sync on every wal in the process.
void
cttest_durable_fsync_issues_one_syscall_when_nothing_goes_wrong(void)
{
    df_setup();
    Wal w;
    Job list;
    df_wal(&w, &list);
    fault_clear_all();

    walmaint(&w);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "an uninterrupted sync is exactly one fdatasync, got %d",
            fault_calls(FAULT_FDATASYNC));
}
