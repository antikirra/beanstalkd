#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <limits.h>
#include <pthread.h>

static int reserve(Wal *w, int n);

// Platform-aware fsync: uses F_FULLFSYNC on macOS for true durability
// (standard fsync on macOS does not flush drive write caches),
// plain fsync elsewhere.
static int
durable_fsync(int fd)
{
#ifdef F_FULLFSYNC
    int r = fcntl(fd, F_FULLFSYNC, 0);
    if (r == -1 && errno == ENOTSUP)
        return fsync(fd);
    return r;
#else
    return fsync(fd);
#endif
}

// --- Per-Wal async fsync thread ---
//
// Moves the blocking fsync() call off the main event loop thread.
// The main thread passes a dup()'d fd to the fsync thread; the thread
// owns that fd and closes it after fsync. This avoids fd lifetime races:
// the main thread can close/rotate the original fd freely.
//
// All fsync state lives inside the Wal struct (not static globals),
// allowing multiple Wal instances to run independent fsync threads.
//
// Thread safety contract:
//   w->sync_fd, w->sync_stop, w->sync_err — shared, protected by w->sync_mu
//   w->sync_on — main-thread-only flag (set before thread start, cleared after join)
//   w->use, w->cur, w->* — main-thread-only (fsync thread never reads Wal fields)
//   The ONLY data crossing the thread boundary is the dup'd fd integer.

static void *
sync_thread_fn(void *arg)
{
    Wal *w = arg;

    pthread_mutex_lock(&w->sync_mu);
    for (;;) {
        while (w->sync_fd < 0 && !w->sync_stop)
            pthread_cond_wait(&w->sync_cond, &w->sync_mu);

        if (w->sync_stop) break;

        int fd = w->sync_fd;
        w->sync_fd = -1;
        pthread_mutex_unlock(&w->sync_mu);

        int r = durable_fsync(fd);
        close(fd);

        pthread_mutex_lock(&w->sync_mu);
        if (r == -1)
            w->sync_err = 1;
    }
    pthread_mutex_unlock(&w->sync_mu);
    return NULL;
}

void
walsyncstart(Wal *w)
{
    if (w->sync_on) return;
    pthread_mutex_init(&w->sync_mu, NULL);
    pthread_cond_init(&w->sync_cond, NULL);
    w->sync_fd = -1;
    w->sync_stop = 0;
    w->sync_err = 0;
    if (pthread_create(&w->sync_thread, NULL, sync_thread_fn, w) != 0) {
        twarnx("failed to start fsync thread, using synchronous fsync");
        return;
    }
    w->sync_on = 1;
}

void
walsyncstop(Wal *w)
{
    if (!w->sync_on) return;
    pthread_mutex_lock(&w->sync_mu);
    w->sync_stop = 1;
    pthread_cond_signal(&w->sync_cond);
    pthread_mutex_unlock(&w->sync_mu);
    pthread_join(w->sync_thread, NULL);
    w->sync_on = 0;
}


// Reads w->dir for files matching binlog.NNN,
// sets w->next to the next unused number, and
// returns the minimum number.
// If no files are found, sets w->next to 1 and
// returns a large number.
static int
walscandir(Wal *w)
{
    static char base[] = "binlog.";
    static const int len = sizeof(base) - 1;
    DIR *d;
    struct dirent *e;
    int min = 1<<30;
    int max = 0;
    int n;
    char *p;

    d = opendir(w->dir);
    if (!d) return min;

    while ((e = readdir(d))) {
        if (strncmp(e->d_name, base, len) == 0) {
            n = strtol(e->d_name+len, &p, 10);
            if (p && *p == '\0') {
                if (n > max) max = n;
                if (n < min) min = n;
            }
        }
    }

    closedir(d);
    w->next = max + 1;
    return min;
}


static void
dirsync(Wal *w)
{
    int fd = open(w->dir, O_RDONLY);
    if (fd >= 0) {
        durable_fsync(fd);
        close(fd);
    }
}


void
walgc(Wal *w)
{
    File *f;
    int did_unlink = 0;

    while (w->head && !w->head->refs) {
        f = w->head;
        w->head = f->next;
        if (w->tail == f) {
            w->tail = f->next; // also, f->next == NULL
        }

        w->nfile--;
        unlink(f->path);
        free(f->path);
        free(f);
        did_unlink = 1;
    }

    if (did_unlink) {
        dirsync(w);
    }
}


// returns 1 on success, 0 on error.
static int
usenext(Wal *w)
{
    File *f;

    f = w->cur;
    if (!f->next) {
        twarnx("there is no next wal file");
        return 0;
    }

    w->cur = f->next;
    filewclose(f);
    return 1;
}


static int
ratio(Wal *w)
{
    int64 n, d;

    d = w->alive + w->resv;
    n = (int64)w->nfile * (int64)w->filesize - d;
    if (!d) return 0;
    return n / d;
}


// Returns the number of bytes reserved or 0 on error.
static int
walresvmigrate(Wal *w, Job *j)
{
    int z = 0;

    // reserve only space for the migrated full job record
    // space for the delete is already reserved
    z += sizeof(int);
    z += j->tube->name_len;
    z += sizeof(Jobrec);
    z += j->r.body_size;

    return reserve(w, z);
}


// moveone migrates one job from the oldest wal file to the current.
// Returns 1 on success, 0 on failure or nothing to do.
static int
moveone(Wal *w)
{
    Job *j;

    if (w->head == w->cur || w->head->next == w->cur) {
        return 1; // no point in moving a job
    }

    j = w->head->jlist.fnext;
    if (!j || j == &w->head->jlist) {
        twarnx("head holds no jlist");
        return 1; // nothing to move
    }

    if (!walresvmigrate(w, j)) {
        return 1; // it will not fit, try again later
    }

    filermjob(w->head, j);
    w->nmig++;
    return walwrite(w, j);
}


// walcompact migrates jobs from old wal files. Returns 0 if WAL was disabled.
static int
walcompact(Wal *w)
{
    int r;

    for (r=ratio(w); r>=2; r--) {
        if (!moveone(w))
            return 0;
    }
    return 1;
}


static int
walsync(Wal *w)
{
    if (w->sync_on) {
        pthread_mutex_lock(&w->sync_mu);
        int err = w->sync_err;
        w->sync_err = 0;
        pthread_mutex_unlock(&w->sync_mu);
        if (err) {
            twarnx("async fsync failed");
            return 0;
        }
    }

    if (w->wantsync && now >= w->lastsync+w->syncrate) {
        w->lastsync = now;
        if (w->sync_on) {
            int fd = dup(w->cur->fd);
            if (fd == -1) {
                twarn("dup for async fsync");
                return 0;
            }
            pthread_mutex_lock(&w->sync_mu);
            if (w->sync_fd >= 0) {
                close(fd);
            } else {
                w->sync_fd = fd;
                pthread_cond_signal(&w->sync_cond);
            }
            pthread_mutex_unlock(&w->sync_mu);
        } else {
            if (durable_fsync(w->cur->fd) == -1) {
                twarn("fsync");
                return 0;
            }
        }
    }
    return 1;
}


// Walwrite writes j to the log w (if w is enabled).
// On failure, walwrite disables w and returns 0; on success, it returns 1.
// Unlke walresv*, walwrite should never fail because of a full disk.
// If w is disabled, then walwrite takes no action and returns 1.
int
walwrite(Wal *w, Job *j)
{
    int r = 0;

    if (!w->use) return 1;
    if (w->cur->resv > 0 || usenext(w)) {
        if (j->file) {
            r = filewrjobshort(w->cur, j);
        } else {
            r = filewrjobfull(w->cur, j);
        }
    }
    if (!r) {
        filewclose(w->cur);
        w->use = 0;
        return 0;
    }
    w->nrec++;
    return r;
}


// walmaint performs wal compaction and sync.
// Returns 1 on success, 0 if WAL was disabled due to error.
int
walmaint(Wal *w)
{
    if (!w->use) return 1;
    if (!walcompact(w)) return 0;
    if (!walsync(w)) return 0;
    return 1;
}


static int
makenextfile(Wal *w)
{
    File *f;

    f = new(File);
    if (!f) {
        twarnx("OOM");
        return 0;
    }

    if (!fileinit(f, w, w->next)) {
        free(f);
        twarnx("OOM");
        return 0;
    }

    filewopen(f);
    if (!f->iswopen) {
        free(f->path);
        free(f);
        return 0;
    }

    w->next++;
    fileadd(f, w);
    dirsync(w);
    return 1;
}


static void
moveresv(File *to, File *from, int n)
{
    from->resv -= n;
    from->free += n;
    to->resv += n;
    to->free -= n;
}


static int
needfree(Wal *w, int n)
{
    if (w->tail->free >= n) return n;
    if (makenextfile(w)) return n;
    return 0;
}


// Ensures:
//  1. b->resv is congruent to n (mod z).
//  2. x->resv is congruent to 0 (mod z) for each future file x.
// Assumes (and preserves) that b->resv >= n.
// Reserved space is conserved (neither created nor destroyed);
// we just move it around to preserve the invariant.
// We might have to allocate a new file.
// Returns 1 on success, otherwise 0. If there was a failure,
// w->tail is not updated.
static int
balancerest(Wal *w, File *b, int n)
{
    int rest, c, r;
    static const int z = sizeof(int) + sizeof(Jobrec);

    if (!b) return 1;

    rest = b->resv - n;
    r = rest % z;
    if (r == 0) return balancerest(w, b->next, 0);

    c = z - r;
    if (w->tail->resv >= c && b->free >= c) {
        moveresv(b, w->tail, c);
        return balancerest(w, b->next, 0);
    }

    if (needfree(w, r) != r) {
        twarnx("needfree");
        return 0;
    }
    moveresv(w->tail, b, r);
    return balancerest(w, b->next, 0);
}


// Ensures:
//  1. w->cur->resv >= n.
//  2. w->cur->resv is congruent to n (mod z).
//  3. x->resv is congruent to 0 (mod z) for each future file x.
// (where z is the size of a delete record in the wal).
// Reserved space is conserved (neither created nor destroyed);
// we just move it around to preserve the invariant.
// We might have to allocate a new file.
// Returns 1 on success, otherwise 0. If there was a failure,
// w->tail is not updated.
static int
balance(Wal *w, int n)
{
    // Invariant 1
    // (this loop will run at most once)
    while (w->cur->resv < n) {
        int m = w->cur->resv;

        int r = needfree(w, m);
        if (r != m) {
            twarnx("needfree");
            return 0;
        }

        moveresv(w->tail, w->cur, m);
        usenext(w);
    }

    // Invariants 2 and 3
    return balancerest(w, w->cur, n);
}


// Returns the number of bytes successfully reserved: either 0 or n.
static int
reserve(Wal *w, int n)
{
    int r;

    // return value must be nonzero but is otherwise ignored
    if (!w->use) return 1;

    if (w->cur->free >= n) {
        w->cur->free -= n;
        w->cur->resv += n;
        w->resv += n;
        return n;
    }

    r = needfree(w, n);
    if (r != n) {
        twarnx("needfree");
        return 0;
    }

    w->tail->free -= n;
    w->tail->resv += n;
    w->resv += n;
    if (!balance(w, n)) {
        // error; undo the reservation
        w->resv -= n;
        w->tail->resv -= n;
        w->tail->free += n;
        return 0;
    }

    return n;
}


// Returns the number of bytes reserved or 0 on error.
int
walresvput(Wal *w, Job *j)
{
    int z = 0;

    // reserve space for the initial job record
    z += sizeof(int);
    z += j->tube->name_len;
    z += sizeof(Jobrec);
    z += j->r.body_size;

    // plus space for a delete to come later
    z += sizeof(int);
    z += sizeof(Jobrec);

    return reserve(w, z);
}


// Returns the number of bytes reserved or 0 on error.
int
walresvupdate(Wal *w)
{
    int z = 0;
    z +=sizeof(int);
    z +=sizeof(Jobrec);
    return reserve(w, z);
}


// Returns the number of locks acquired: either 0 or 1.
int
waldirlock(Wal *w)
{
    int r;
    int fd;
    struct flock lk;
    char *path;
    size_t path_length;

    path_length = strlen(w->dir) + strlen("/lock") + 1;
    if ((path = malloc(path_length)) == NULL) {
        twarn("malloc");
        return 0;
    }
    snprintf(path, path_length, "%s/lock", w->dir);

    fd = open(path, O_WRONLY|O_CREAT, 0600);
    free(path);
    if (fd == -1) {
        twarn("open");
        return 0;
    }

    lk.l_type = F_WRLCK;
    lk.l_whence = SEEK_SET;
    lk.l_start = 0;
    lk.l_len = 0;
    r = fcntl(fd, F_SETLK, &lk);
    if (r) {
        twarn("fcntl");
        return 0;
    }

    // intentionally leak fd, since we never want to close it
    // and we'll never need it again
    return 1;
}


void
walread(Wal *w, Job *list, int min)
{
    int i;
    int err = 0;

    for (i = min; i < w->next; i++) {
        File *f = new(File);
        if (!f) {
            twarnx("OOM");
            exit(1);
        }

        if (!fileinit(f, w, i)) {
            free(f);
            twarnx("OOM");
            exit(1);
        }

        int fd = open(f->path, O_RDONLY);
        if (fd < 0) {
            twarn("open %s", f->path);
            free(f->path);
            free(f);
            continue;
        }

        f->fd = fd;
        fileadd(f, w);
        err |= fileread(f, list);
        if (close(fd) == -1)
            twarn("close");
    }

    if (err) {
        warnx("Errors reading one or more WAL files.");
        warnx("Continuing. You may be missing data.");
    }
}


void
walinit(Wal *w, Job *list)
{
    int min;

    min = walscandir(w);
    walread(w, list, min);

    // first writable file
    if (!makenextfile(w)) {
        twarnx("makenextfile");
        exit(1);
    }

    w->cur = w->tail;
}
