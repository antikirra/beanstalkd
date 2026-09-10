// Angry tests for waldirlock (walg.c) — the advisory lock that stops two
// beanstalkd instances writing the same binlogs.
//
// Its only real promise can be tested from one process alone: that a
// second owner is refused. Everything about it is arranged around that —
// the descriptor is deliberately leaked, because closing any descriptor
// on that file drops the lock for the whole process.

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
#include <sys/wait.h>

static void
dl_setup(void)
{
    fault_clear_all();
    progname = "testwal_waldirlock";
}

static void
dl_subdir(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: subdir path did not fit");
    assertf(mkdir(buf, 0700) == 0, "setup: mkdir %s: %s", buf, strerror(errno));
}

static int
dl_lock(char *dir)
{
    Wal w;
    memset(&w, 0, sizeof w);
    w.dir = dir;
    return waldirlock(&w);
}

// waldirlock's verdict as seen by a separate process, which is the only
// place POSIX record locks actually conflict.
static int
dl_other_process(char *dir)
{
    fflush(NULL);
    pid_t pid = fork();
    assertf(pid >= 0, "setup: fork: %s", strerror(errno));
    if (pid == 0) _exit(dl_lock(dir) ? 1 : 0);
    int st = 0;
    assertf(waitpid(pid, &st, 0) == pid, "setup: waitpid: %s", strerror(errno));
    assertf(WIFEXITED(st), "setup: the child did not exit normally (0x%x)", st);
    return WEXITSTATUS(st);
}


// One instance per wal directory. This is the entire point of the
// function, and it can only be observed from a second process.
void
cttest_waldirlock_refuses_a_second_owner_of_the_directory(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "owned");
    assertf(dl_lock(dir) == 1, "setup: the first lock must be granted");

    int other = dl_other_process(dir);

    assertf(other == 0,
            "a second instance must not be able to lock a directory this "
            "one owns; it got a verdict of %d", other);
}


// The lock outlives the call: the descriptor is never closed precisely
// so the lock survives the return. A tidy-up that closed it would leave
// every directory unprotected while looking completely correct.
void
cttest_waldirlock_still_owns_the_directory_after_the_call_returns(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "kept");
    assertf(dl_lock(dir) == 1, "setup: the first lock must be granted");
    int scratch = open("/dev/null", O_RDONLY);
    assertf(scratch >= 0, "setup: open /dev/null: %s", strerror(errno));
    assertf(close(scratch) == 0, "setup: close /dev/null");

    int other = dl_other_process(dir);

    assertf(other == 0,
            "the lock must still be held long after waldirlock returned; a "
            "second instance got a verdict of %d", other);
}


// A lock file left behind by a cleanly exited instance is not a lock. It
// must be reused, not mistaken for a live owner — otherwise a server
// that shut down normally can never start again.
void
cttest_waldirlock_acquires_a_directory_whose_lock_file_already_exists(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "stale");
    char path[600];
    int k = snprintf(path, sizeof path, "%s/lock", dir);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: lock path did not fit");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create %s: %s", path, strerror(errno));
    assertf(close(fd) == 0, "setup: close %s", path);

    int r = dl_lock(dir);

    assertf(r == 1,
            "an existing lock file is not a live owner and must be locked "
            "normally; waldirlock returned %d", r);
}


// The lock lives inside the wal directory it protects, not somewhere
// shared: two directories are two independent instances.
void
cttest_waldirlock_locks_a_file_inside_the_directory_it_protects(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "inside");
    assertf(dl_lock(dir) == 1, "setup: the lock must be granted");
    char other[512];
    dl_subdir(other, sizeof other, "elsewhere");

    int r = dl_other_process(other);

    assertf(r == 1,
            "a different wal directory is a different lock and must be "
            "grantable; the second directory got a verdict of %d", r);
}


// A trailing slash is a legal spelling of the same directory. The lock
// has to land on the same file, or two instances spelling the path
// differently both believe they own it.
void
cttest_waldirlock_locks_the_same_file_when_the_path_ends_in_a_slash(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "slashed");
    char slashed[600];
    int k = snprintf(slashed, sizeof slashed, "%s/", dir);
    assertf(k > 0 && (size_t)k < sizeof slashed, "setup: path did not fit");
    assertf(dl_lock(slashed) == 1, "setup: the first lock must be granted");

    int other = dl_other_process(dir);

    assertf(other == 0,
            "the trailing slash names the same directory, so the second "
            "instance must be refused; it got a verdict of %d", other);
}


// A directory that does not exist cannot be locked, and the failure must
// be reported rather than assumed away.
void
cttest_waldirlock_refuses_a_directory_that_does_not_exist(void)
{
    dl_setup();
    char dir[600];
    int k = snprintf(dir, sizeof dir, "%s/nowhere", ctdir());
    assertf(k > 0 && (size_t)k < sizeof dir, "setup: path did not fit");

    int r = dl_lock(dir);

    assertf(r == 0,
            "there is no such directory, so there is no lock; waldirlock "
            "returned %d", r);
}


// A path too long for the filesystem must be refused cleanly, not
// truncated into a path that happens to exist.
void
cttest_waldirlock_refuses_an_over_long_directory_path(void)
{
    dl_setup();
    char dir[5000];
    memset(dir, 'd', sizeof dir - 1);
    dir[0] = '/';
    dir[sizeof dir - 1] = '\0';

    int r = dl_lock(dir);

    assertf(r == 0,
            "an unusable path cannot yield a lock; waldirlock returned %d",
            r);
}


// A lock path that is occupied by a directory cannot be opened for
// writing. Ignoring that and reporting success would run two instances
// against one wal.
void
cttest_waldirlock_refuses_when_the_lock_path_is_a_directory(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "blocked");
    char path[600];
    int k = snprintf(path, sizeof path, "%s/lock", dir);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: lock path did not fit");
    assertf(mkdir(path, 0700) == 0, "setup: mkdir %s: %s", path, strerror(errno));

    int r = dl_lock(dir);

    assertf(r == 0,
            "the lock path is a directory and cannot be opened for "
            "writing; waldirlock returned %d", r);
}


// main.c installs the signal handlers before it acquires the wal, so a
// SIGTERM or SIGUSR1 arriving during startup lands on this open(). An
// interrupted syscall is not a lock held elsewhere, and durable_fsync in
// the same file already retries EINTR — reporting it as a failed lock
// aborts a perfectly good start with exit(10).
void
cttest_waldirlock_does_not_report_an_interrupted_open_as_a_lost_lock(void)
{
    dl_setup();
    char dir[512];
    dl_subdir(dir, sizeof dir, "interrupted");
    fault_set(FAULT_OPEN, 0, EINTR);

    int r = dl_lock(dir);

    assertf(r == 1,
            "a signal during startup is not another instance holding the "
            "lock; waldirlock returned %d", r);
}
