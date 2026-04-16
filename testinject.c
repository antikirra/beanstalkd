#define _GNU_SOURCE
#include "testinject.h"
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/uio.h>
#include <sys/types.h>

struct fault faults[FAULT_COUNT];

static int
default_err(int which)
{
    switch (which) {
    case FAULT_MALLOC:
    case FAULT_CALLOC:
    case FAULT_REALLOC:
        return ENOMEM;
    case FAULT_OPEN:
    case FAULT_UNLINK:
        return ENOENT;
    default:
        return EIO;
    }
}

void
fault_set(int which, int after, int err)
{
    faults[which].countdown = after + 1;
    faults[which].err = err;
    faults[which].hits = 0;
    // Intentionally DO NOT reset `calls` here: a test may want to
    // measure the number of wrapped calls before AND after arming.
    // Use fault_clear_all() to reset everything.
}

void
fault_clear(int which)
{
    faults[which].countdown = 0;
    faults[which].err = 0;
}

void
fault_clear_all(void)
{
    for (int i = 0; i < FAULT_COUNT; i++) {
        faults[i].countdown = 0;
        faults[i].err = 0;
        faults[i].hits = 0;
        faults[i].calls = 0;
    }
}

int
fault_hits(int which)
{
    return faults[which].hits;
}

int
fault_calls(int which)
{
    return faults[which].calls;
}

static int
fault_fire(int which)
{
    struct fault *f = &faults[which];
    f->calls++;
    if (f->countdown <= 0)
        return 0;
    if (f->countdown > 1) {
        f->countdown--;
        return 0;
    }
    // countdown == 1: fire and disarm.
    f->countdown = 0;
    f->hits++;
    errno = f->err ? f->err : default_err(which);
    return 1;
}


// --- Wrapped syscalls (only active in test binary via -Wl,--wrap) ---

extern void   *__real_malloc(size_t);
extern void   *__real_calloc(size_t, size_t);
extern void   *__real_realloc(void *, size_t);
extern ssize_t __real_write(int, const void *, size_t);
extern ssize_t __real_writev(int, const struct iovec *, int);
extern ssize_t __real_read(int, void *, size_t);
extern int     __real_open(const char *, int, mode_t);
extern int     __real_ftruncate(int, off_t);
extern int     __real_unlink(const char *);
extern int     __real_fdatasync(int);


void *
__wrap_malloc(size_t n)
{
    if (fault_fire(FAULT_MALLOC))
        return NULL;
    return __real_malloc(n);
}

void *
__wrap_calloc(size_t nmemb, size_t size)
{
    if (fault_fire(FAULT_CALLOC))
        return NULL;
    return __real_calloc(nmemb, size);
}

void *
__wrap_realloc(void *ptr, size_t size)
{
    if (fault_fire(FAULT_REALLOC))
        return NULL;
    return __real_realloc(ptr, size);
}

ssize_t
__wrap_write(int fd, const void *buf, size_t count)
{
    if (fd > 2 && fault_fire(FAULT_WRITE))
        return -1;
    return __real_write(fd, buf, count);
}

ssize_t
__wrap_writev(int fd, const struct iovec *iov, int iovcnt)
{
    if (fd > 2 && fault_fire(FAULT_WRITEV))
        return -1;
    return __real_writev(fd, iov, iovcnt);
}

ssize_t
__wrap_read(int fd, void *buf, size_t count)
{
    if (fault_fire(FAULT_READ))
        return -1;
    return __real_read(fd, buf, count);
}

int
__wrap_open(const char *path, int flags, ...)
{
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, mode_t);
        va_end(ap);
    }
    if (fault_fire(FAULT_OPEN))
        return -1;
    return __real_open(path, flags, mode);
}

int
__wrap_ftruncate(int fd, off_t length)
{
    if (fault_fire(FAULT_FTRUNCATE))
        return -1;
    return __real_ftruncate(fd, length);
}

int
__wrap_unlink(const char *path)
{
    if (fault_fire(FAULT_UNLINK))
        return -1;
    return __real_unlink(path);
}

int
__wrap_fdatasync(int fd)
{
    // Skip fd 0/1/2 to match the discipline used by __wrap_write /
    // __wrap_writev: a misbehaving test that accidentally issues
    // fdatasync(stdin) must not be silently "fixed" by an injected
    // failure, and conversely must not count toward fault_calls().
    if (fd <= 2)
        return __real_fdatasync(fd);
    if (fault_fire(FAULT_FDATASYNC))
        return -1;
    return __real_fdatasync(fd);
}
