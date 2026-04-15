#define _GNU_SOURCE
#include "dat.h"
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

static int  readrec(File*, Job *, int*);
static int  readrec5(File*, Job *, int*);
static int  readrec7(File*, Job *, int*);
static int  readfull(File*, void*, int, int*, char*);
static void warnpos(File*, int, char*, ...)
__attribute__((format(printf, 3, 4)));

FAlloc falloc = rawfalloc;

enum
{
    Walver5 = 5,
    Walver7 = 7
};

typedef struct Jobrec5 Jobrec5;

struct Jobrec5 {
    uint64 id;
    uint32 pri;
    uint64 delay; // usec
    uint64 ttr; // usec
    int32  body_size;
    uint64 created_at; // usec
    uint64 deadline_at; // usec
    uint32 reserve_ct;
    uint32 timeout_ct;
    uint32 release_ct;
    uint32 bury_ct;
    uint32 kick_ct;
    byte   state;

    char pad[1];
};

enum
{
    Jobrec5size = offsetof(Jobrec5, pad)
};
_Static_assert(Jobrec5size == 77, "Jobrec5 layout changed — breaks v5 WAL compatibility");

// rawfalloc allocates disk space of len bytes.
// It expects fd's offset to be 0; may also reset fd's offset to 0.
// Returns 0 on success, and a positive errno otherwise.
int
rawfalloc(int fd, int len)
{
    // fallocate(): single syscall, O(1), allocates without writing.
    // Falls through to write loop on EOPNOTSUPP (e.g. NFS, tmpfs).
    int r = fallocate(fd, 0, 0, len);
    if (r == 0)
        return 0;
    if (errno != EOPNOTSUPP && errno != ENOSYS)
        return errno;

    // Fallback: write zeroes in 4KB chunks for filesystems
    // that don't support fallocate (NFS, tmpfs).
    static char buf[4096] = {0};
    int i, w;

    for (i = 0; i < len; ) {
        int chunk = len - i < (int)sizeof(buf) ? len - i : (int)sizeof(buf);
        w = write(fd, buf, chunk);
        if (w == -1) {
            if (errno == EINTR)
                continue;
            return errno;
        }
        if (w == 0)
            return EIO;
        i += w;
    }
    lseek(fd, 0, 0);            // do not care if this fails
    return 0;
}

void
fileincref(File *f)
{
    if (!f) return;
    f->refs++;
}


void
filedecref(File *f)
{
    if (!f) return;
    f->refs--;
    if (f->refs < 1) {
        walgc(f->w);
    }
}


void
fileaddjob(File *f, Job *j)
{
    Job *h;

    h = &f->jlist;
    if (!h->fprev) h->fprev = h;
    j->file = f;
    j->fprev = h->fprev;
    j->fnext = h;
    h->fprev->fnext = j;
    h->fprev = j;
    fileincref(f);
}


void
filermjob(File *f, Job *j)
{
    if (!f) return;
    if (f != j->file) return;
    j->fnext->fprev = j->fprev;
    j->fprev->fnext = j->fnext;
    j->fnext = 0;
    j->fprev = 0;
    j->file = NULL;
    if (j->walused <= f->w->alive) {
        f->w->alive -= j->walused;
    } else {
        twarnx("filermjob: walused %"PRId64" > alive %"PRId64, j->walused, f->w->alive);
        f->w->alive = 0;
    }
    j->walused = 0;
    filedecref(f);
}


// Fileread reads jobs from f->path into list.
// It returns 0 on success, or 1 if any errors occurred.
int
fileread(File *f, Job *list)
{
    int err = 0, v;
    ReadBuf rb = {.pos = 0, .filled = 0};
    f->rbuf = &rb;

    if (!readfull(f, &v, sizeof(v), &err, "version")) {
        f->rbuf = NULL;
        return err;
    }
    switch (v) {
    case Walver:
        fileincref(f);
        while (readrec(f, list, &err));
        filedecref(f);
        f->rbuf = NULL;
        return err;
    case Walver7:
        fileincref(f);
        while (readrec7(f, list, &err));
        filedecref(f);
        f->rbuf = NULL;
        return err;
    case Walver5:
        fileincref(f);
        while (readrec5(f, list, &err));
        filedecref(f);
        f->rbuf = NULL;
        return err;
    }

    warnx("%s: unknown version: %d", f->path, v);
    f->rbuf = NULL;
    return 1;
}


// Readrec reads a v8 WAL record from f->fd into linked list l and verifies
// its CRC32C trailer. If an error occurs, it sets *err to 1.
// Returns 1 if a record was read, 0 on EOF or error.
//
// Record layout (v8):
//   full:  [int32 namelen] [char tubename[namelen]] [Jobrec jr] [body] [uint32 crc32c_le]
//   short: [int32 namelen=0] [Jobrec jr] [uint32 crc32c_le]
//
// CRC32C (Castagnoli) covers everything before the trailer; trailer is
// 4 bytes in little-endian byte order. Mismatch → record rejected, *err = 1.
static int
readrec(File *f, Job *l, int *err)
{
    int r, sz = 0;
    int namelen;
    Jobrec jr;
    Job *j;
    Tube *t;
    char tubename[MAX_TUBE_NAME_LEN];
    uint32 crc = WAL_CRC32C_INIT;
    unsigned char crc_bytes[4];
    char *body_buf = NULL;

    r = readfull(f, &namelen, sizeof(int), err, "namelen");
    if (!r) return 0;
    sz += r;
    crc = wal_crc32c(crc, &namelen, sizeof(int));

    if (namelen >= MAX_TUBE_NAME_LEN) {
        warnpos(f, -r, "namelen %d exceeds maximum of %d", namelen, MAX_TUBE_NAME_LEN - 1);
        *err = 1;
        return 0;
    }

    if (namelen < 0) {
        warnpos(f, -r, "namelen %d is negative", namelen);
        *err = 1;
        return 0;
    }

    if (namelen) {
        r = readfull(f, tubename, namelen, err, "tube name");
        if (!r) {
            return 0;
        }
        sz += r;
        crc = wal_crc32c(crc, tubename, namelen);
    }
    tubename[namelen] = '\0';

    r = readfull(f, &jr, sizeof(Jobrec), err, "job struct");
    if (!r) {
        return 0;
    }
    sz += r;
    crc = wal_crc32c(crc, &jr, sizeof(Jobrec));

    // Are we reading trailing zeroes? (fallocate zero-fills unused tail;
    // a genuine record always has jr.id > 0.)
    if (!jr.id) return 0;

    // Reject jobs with body_size < 2 from corrupted WAL.
    // Valid jobs always include \r\n trailer (body_size >= 2).
    if (namelen && jr.body_size < 2 && jr.state != Invalid) {
        warnpos(f, -sz, "job %"PRIu64" invalid body_size %d", jr.id, jr.body_size);
        *err = 1;
        return 0;
    }

    // For full records, read body into a temporary buffer and fold into CRC.
    // We cannot read directly into j->body yet because CRC verification must
    // pass BEFORE any existing job state is mutated. This preserves the
    // invariant that corrupted data never overwrites valid in-memory state.
    if (namelen) {
        body_buf = malloc(jr.body_size);
        if (!body_buf) {
            warnpos(f, -sz, "OOM body_buf");
            *err = 1;
            return 0;
        }
        r = readfull(f, body_buf, jr.body_size, err, "job body");
        if (!r) { free(body_buf); return 0; }
        sz += r;
        crc = wal_crc32c(crc, body_buf, jr.body_size);
    }

    // Read and verify the 4-byte little-endian CRC32C trailer.
    r = readfull(f, crc_bytes, sizeof crc_bytes, err, "crc trailer");
    if (!r) { free(body_buf); return 0; }
    sz += r;

    uint32 stored = (uint32)crc_bytes[0]
                  | ((uint32)crc_bytes[1] << 8)
                  | ((uint32)crc_bytes[2] << 16)
                  | ((uint32)crc_bytes[3] << 24);
    uint32 computed = crc ^ WAL_CRC32C_XOR;
    if (stored != computed) {
        warnpos(f, -sz, "job %"PRIu64" crc mismatch: computed 0x%08x stored 0x%08x",
                jr.id, computed, stored);
        *err = 1;
        free(body_buf);
        return 0;
    }

    // CRC is valid — safe to apply state changes.
    j = job_find(jr.id);
    if (!(j || namelen)) {
        // Short record for a job whose full record lived in a now-deleted
        // earlier file. The record itself is checksum-valid, so we
        // intentionally ignore it (job has been deleted or migrated).
        return 1;
    }

    switch (jr.state) {
    case Reserved:
        jr.state = Ready;
        /* Falls through */
    case Ready:
    case Buried:
    case Delayed:
        if (!j) {
            if ((size_t)jr.body_size > job_data_size_limit) {
                warnpos(f, -sz, "job %"PRIu64" is too big (%"PRId32" > %zu)",
                        jr.id,
                        jr.body_size,
                        job_data_size_limit);
                goto Error;
            }
            t = tube_find_or_make(tubename);
            if (!t) {
                warnpos(f, -sz, "OOM tube_find_or_make");
                goto Error;
            }
            j = make_job_with_id(jr.pri, jr.delay, jr.ttr, jr.body_size,
                                 t, jr.id);
            if (!j) {
                warnpos(f, -sz, "OOM make_job_with_id");
                goto Error;
            }
            job_list_reset(j);
            j->r.created_at = jr.created_at;
        }
        {
        int32 old_body_size = j->r.body_size;
        j->r = jr;

        // For short records, move job to tail of replay list to
        // preserve WAL ordering. Ensures buried jobs maintain
        // their burial order after restart (#668).
        if (!namelen) {
            job_list_remove(j);
        }
        job_list_insert(l, j);

        if (namelen) {
            if (jr.body_size != old_body_size) {
                warnpos(f, -sz, "job %"PRIu64" size changed", j->r.id);
                warnpos(f, -sz, "was %d, now %d", old_body_size, jr.body_size);
                goto Error;
            }
            memcpy(j->body, body_buf, j->r.body_size);

            // since this is a full record, we can move
            // the file pointer and decref the old
            // file, if any
            filermjob(j->file, j);
            fileaddjob(f, j);

            // Only count full records toward alive/walused.
            // Short records are redundant state updates; their bytes
            // are dead space eligible for compaction (#622).
            j->walused += sz;
            f->w->alive += sz;
        }

        free(body_buf);
        return 1;
        } /* end old_body_size scope */
    case Invalid:
        free(body_buf);
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    default:
        warnpos(f, -sz, "unknown job state: %d", jr.state);
        goto Error;
    }

Error:
    *err = 1;
    free(body_buf);
    if (j) {
        job_list_remove(j);
        filermjob(j->file, j);
        job_free(j);
    }
    return 0;
}


// Readrec7 reads a v7 WAL record. v7 has no CRC trailer; this function is
// an unchanged snapshot of the previous readrec implementation and exists
// to recover pre-v8 binlogs during migration. See dat.h:186 for procedure.
static int
readrec7(File *f, Job *l, int *err)
{
    int r, sz = 0;
    int namelen;
    Jobrec jr;
    Job *j;
    Tube *t;
    char tubename[MAX_TUBE_NAME_LEN];

    r = readfull(f, &namelen, sizeof(int), err, "namelen");
    if (!r) return 0;
    sz += r;
    if (namelen >= MAX_TUBE_NAME_LEN) {
        warnpos(f, -r, "namelen %d exceeds maximum of %d", namelen, MAX_TUBE_NAME_LEN - 1);
        *err = 1;
        return 0;
    }

    if (namelen < 0) {
        warnpos(f, -r, "namelen %d is negative", namelen);
        *err = 1;
        return 0;
    }

    if (namelen) {
        r = readfull(f, tubename, namelen, err, "tube name");
        if (!r) {
            return 0;
        }
        sz += r;
    }
    tubename[namelen] = '\0';

    r = readfull(f, &jr, sizeof(Jobrec), err, "job struct");
    if (!r) {
        return 0;
    }
    sz += r;

    // are we reading trailing zeroes?
    if (!jr.id) return 0;

    // Reject jobs with body_size < 2 from corrupted WAL.
    // Valid jobs always include \r\n trailer (body_size >= 2).
    if (namelen && jr.body_size < 2 && jr.state != Invalid) {
        warnpos(f, -sz, "job %"PRIu64" invalid body_size %d", jr.id, jr.body_size);
        *err = 1;
        return 0;
    }

    j = job_find(jr.id);
    if (!(j || namelen)) {
        // We read a short record without having seen a
        // full record for this job, so the full record
        // was in an earlier file that has been deleted.
        // Therefore the job itself has either been
        // deleted or migrated; either way, this record
        // should be ignored.
        return 1;
    }

    switch (jr.state) {
    case Reserved:
        jr.state = Ready;
        /* Falls through */
    case Ready:
    case Buried:
    case Delayed:
        if (!j) {
            if ((size_t)jr.body_size > job_data_size_limit) {
                warnpos(f, -r, "job %"PRIu64" is too big (%"PRId32" > %zu)",
                        jr.id,
                        jr.body_size,
                        job_data_size_limit);
                goto Error;
            }
            t = tube_find_or_make(tubename);
            if (!t) {
                warnpos(f, -r, "OOM tube_find_or_make");
                goto Error;
            }
            j = make_job_with_id(jr.pri, jr.delay, jr.ttr, jr.body_size,
                                 t, jr.id);
            if (!j) {
                warnpos(f, -r, "OOM make_job_with_id");
                goto Error;
            }
            job_list_reset(j);
            j->r.created_at = jr.created_at;
        }
        {
        int32 old_body_size = j->r.body_size;
        j->r = jr;

        // For short records, move job to tail of replay list to
        // preserve WAL ordering. Ensures buried jobs maintain
        // their burial order after restart (#668).
        if (!namelen) {
            job_list_remove(j);
        }
        job_list_insert(l, j);

        // full record; read the job body
        if (namelen) {
            if (jr.body_size != old_body_size) {
                warnpos(f, -r, "job %"PRIu64" size changed", j->r.id);
                warnpos(f, -r, "was %d, now %d", j->r.body_size, jr.body_size);
                goto Error;
            }
            r = readfull(f, j->body, j->r.body_size, err, "job body");
            if (!r) {
                goto Error;
            }
            sz += r;

            // since this is a full record, we can move
            // the file pointer and decref the old
            // file, if any
            filermjob(j->file, j);
            fileaddjob(f, j);

            // Only count full records toward alive/walused.
            // Short records are redundant state updates; their bytes
            // are dead space eligible for compaction (#622).
            j->walused += sz;
            f->w->alive += sz;
        }

        return 1;
        } /* end old_body_size scope */
    case Invalid:
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    default:
        warnpos(f, -r, "unknown job state: %d", jr.state);
        goto Error;
    }

Error:
    *err = 1;
    if (j) {
        job_list_remove(j);
        filermjob(j->file, j);
        job_free(j);
    }
    return 0;
}


// Readrec5 is like readrec, but it reads a record in "version 5"
// of the log format.
static int
readrec5(File *f, Job *l, int *err)
{
    int r, sz = 0;
    size_t namelen;
    Jobrec5 jr;
    Job *j;
    Tube *t;
    char tubename[MAX_TUBE_NAME_LEN];

    r = readfull(f, &namelen, sizeof(namelen), err, "v5 namelen");
    if (!r) return 0;
    sz += r;
    if (namelen >= MAX_TUBE_NAME_LEN) {
        warnpos(f, -r, "namelen %zu exceeds maximum of %d", namelen, MAX_TUBE_NAME_LEN - 1);
        *err = 1;
        return 0;
    }

    if (namelen) {
        r = readfull(f, tubename, namelen, err, "v5 tube name");
        if (!r) {
            return 0;
        }
        sz += r;
    }
    tubename[namelen] = '\0';

    r = readfull(f, &jr, Jobrec5size, err, "v5 job struct");
    if (!r) {
        return 0;
    }
    sz += r;

    // are we reading trailing zeroes?
    if (!jr.id) return 0;

    if (namelen && jr.body_size < 2 && jr.state != Invalid) {
        warnpos(f, -sz, "v5 job %"PRIu64" invalid body_size %d", jr.id, jr.body_size);
        *err = 1;
        return 0;
    }

    j = job_find(jr.id);
    if (!(j || namelen)) {
        // We read a short record without having seen a
        // full record for this job, so the full record
        // was in an earlier file that has been deleted.
        // Therefore the job itself has either been
        // deleted or migrated; either way, this record
        // should be ignored.
        return 1;
    }

    switch (jr.state) {
    case Reserved:
        jr.state = Ready;
        /* Falls through */
    case Ready:
    case Buried:
    case Delayed:
        if (!j) {
            if ((size_t)jr.body_size > job_data_size_limit) {
                warnpos(f, -r, "job %"PRIu64" is too big (%"PRId32" > %zu)",
                        jr.id,
                        jr.body_size,
                        job_data_size_limit);
                goto Error;
            }
            t = tube_find_or_make(tubename);
            if (!t) {
                warnpos(f, -r, "OOM tube_find_or_make");
                goto Error;
            }
            j = make_job_with_id(jr.pri, jr.delay, jr.ttr, jr.body_size,
                                 t, jr.id);
            if (!j) {
                warnpos(f, -r, "OOM make_job_with_id");
                goto Error;
            }
            job_list_reset(j);
        }
        {
        int32 old_body_size = j->r.body_size;
        j->r.id = jr.id;
        j->r.pri = jr.pri;
        j->r.delay = jr.delay * 1000; // us => ns
        j->r.ttr = jr.ttr * 1000; // us => ns
        j->r.body_size = jr.body_size;
        j->r.created_at = jr.created_at * 1000; // us => ns
        j->r.deadline_at = jr.deadline_at * 1000; // us => ns
        j->r.reserve_ct = jr.reserve_ct;
        j->r.timeout_ct = jr.timeout_ct;
        j->r.release_ct = jr.release_ct;
        j->r.bury_ct = jr.bury_ct;
        j->r.kick_ct = jr.kick_ct;
        j->r.state = jr.state;

        if (!namelen) {
            job_list_remove(j);
        }
        job_list_insert(l, j);

        // full record; read the job body
        if (namelen) {
            if (jr.body_size != old_body_size) {
                warnpos(f, -r, "job %"PRIu64" size changed", j->r.id);
                warnpos(f, -r, "was %"PRId32", now %"PRId32, j->r.body_size, jr.body_size);
                goto Error;
            }
            r = readfull(f, j->body, j->r.body_size, err, "v5 job body");
            if (!r) {
                goto Error;
            }
            sz += r;

            // since this is a full record, we can move
            // the file pointer and decref the old
            // file, if any
            filermjob(j->file, j);
            fileaddjob(f, j);

            j->walused += sz;
            f->w->alive += sz;
        }

        return 1;
        } /* end old_body_size scope */
    case Invalid:
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    default:
        warnpos(f, -r, "unknown v5 job state: %d", jr.state);
        goto Error;
    }

Error:
    *err = 1;
    if (j) {
        job_list_remove(j);
        filermjob(j->file, j);
        job_free(j);
    }
    return 0;
}


static int
readfull(File *f, void *c, int n, int *err, char *desc)
{
    ReadBuf *rb = f->rbuf;
    char *dst = (char *)c;
    int got = 0;

    while (got < n) {
        // Use buffered path if available.
        if (rb) {
            if (rb->pos < rb->filled) {
                int avail = rb->filled - rb->pos;
                int chunk = (n - got < avail) ? n - got : avail;
                memcpy(dst + got, rb->buf + rb->pos, chunk);
                rb->pos += chunk;
                got += chunk;
                continue;
            }
            // Refill buffer.
            int r = read(f->fd, rb->buf, sizeof(rb->buf));
            if (r == -1) {
                if (errno == EINTR) continue;
                twarn("read");
                warnpos(f, 0, "error reading %s", desc);
                *err = 1;
                return 0;
            }
            if (r == 0) {
                if (got == 0) return 0; // expected EOF
                warnpos(f, -got, "unexpected EOF reading %d bytes (got %d): %s", n, got, desc);
                *err = 1;
                return 0;
            }
            rb->pos = 0;
            rb->filled = r;
            continue;
        }

        // Unbuffered fallback.
        int r = read(f->fd, dst + got, n - got);
        if (r == -1) {
            if (errno == EINTR) continue;
            twarn("read");
            warnpos(f, 0, "error reading %s", desc);
            *err = 1;
            return 0;
        }
        if (r == 0) {
            warnpos(f, -got, "unexpected EOF reading %d bytes (got %d): %s", n, got, desc);
            *err = 1;
            return 0;
        }
        got += r;
    }
    return got;
}

static void
warnpos(File *f, int adj, char *fmt, ...)
{
    int off;
    va_list ap;

    off = lseek(f->fd, 0, SEEK_CUR);
    // Adjust for unread buffered data.
    if (f->rbuf)
        off -= (f->rbuf->filled - f->rbuf->pos);
    fprintf(stderr, "%s:%d: ", f->path, off+adj);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}


// Opens f for writing, writes a header, and initializes
// f->free and f->resv.
// Sets f->iswopen if successful.
void
filewopen(File *f)
{
    int fd, r;
    int n;
    int ver = Walver;

    fd = open(f->path, O_WRONLY|O_CREAT|O_CLOEXEC, 0400);
    if (fd < 0) {
        twarn("open %s", f->path);
        return;
    }

    r = falloc(fd, f->w->filesize);
    if (r) {
        if (close(fd) == -1)
            twarn("close");
        errno = r;
        twarn("falloc %s", f->path);
        r = unlink(f->path);
        if (r) {
            twarn("unlink %s", f->path);
        }
        return;
    }

    n = write(fd, &ver, sizeof(int));
    if (n < 0 || (size_t)n < sizeof(int)) {
        twarn("write %s", f->path);
        if (close(fd) == -1)
            twarn("close");
        unlink(f->path);
        return;
    }

    f->fd = fd;
    f->iswopen = 1;
    fileincref(f);
    f->free = f->w->filesize - n;
    f->resv = 0;
}


// filewritev writes multiple buffers to f in a single writev syscall.
// Updates WAL accounting on success. Returns 1 on success, 0 on failure.
__attribute__((hot)) static int
filewritev(File *f, Job *j, struct iovec *iov, int iovcnt)
{
    int total = 0;
    for (int i = 0; i < iovcnt; i++)
        total += iov[i].iov_len;

    // Fast path: single writev completes everything (common case).
    ssize_t r = writev(f->fd, iov, iovcnt);
    if (likely(r == total))
        goto done;

    // EINTR before any bytes written: retry from scratch.
    if (r == -1) {
        if (errno != EINTR) { twarn("writev"); return 0; }
        r = 0;
    } else if (unlikely(r <= 0)) {
        twarn("writev");
        return 0;
    }

    // Slow path: partial write, advance iov and retry.
    {
        int written = (int)r;
        while (iovcnt > 0 && (size_t)r >= iov[0].iov_len) {
            r -= iov[0].iov_len; iov++; iovcnt--;
        }
        if (iovcnt > 0 && r > 0) {
            iov[0].iov_base = (char *)iov[0].iov_base + r;
            iov[0].iov_len -= r;
        }
        while (written < total) {
            r = writev(f->fd, iov, iovcnt);
            if (r == -1 && errno == EINTR) continue;
            if (unlikely(r <= 0)) { twarn("writev"); return 0; }
            written += r;
            while (iovcnt > 0 && (size_t)r >= iov[0].iov_len) {
                r -= iov[0].iov_len; iov++; iovcnt--;
            }
            if (iovcnt > 0 && r > 0) {
                iov[0].iov_base = (char *)iov[0].iov_base + r;
                iov[0].iov_len -= r;
            }
        }
    }

done:
    f->w->resv -= total;
    f->resv -= total;
    j->walresv -= total;
    j->walused += total;
    f->w->alive += total;
    return 1;
}


__attribute__((hot)) int
filewrjobshort(File *f, Job *j)
{
    int nl = 0; // name len 0 indicates short record

    // CRC32C (v8 trailer) over [nl, j->r], serialized little-endian.
    uint32 crc = WAL_CRC32C_INIT;
    crc = wal_crc32c(crc, &nl,   sizeof nl);
    crc = wal_crc32c(crc, &j->r, sizeof j->r);
    crc ^= WAL_CRC32C_XOR;
    unsigned char crc_bytes[4] = {
        (unsigned char)(crc      ),
        (unsigned char)(crc >>  8),
        (unsigned char)(crc >> 16),
        (unsigned char)(crc >> 24),
    };

    struct iovec iov[3] = {
        { .iov_base = &nl,       .iov_len = sizeof nl },
        { .iov_base = &j->r,     .iov_len = sizeof j->r },
        { .iov_base = crc_bytes, .iov_len = sizeof crc_bytes },
    };

    int r = filewritev(f, j, iov, 3);
    if (!r) return 0;

    // Short records are state updates for an existing job whose
    // authoritative data lives in a full record (in j->file).
    // Undo the alive/walused accounting from filewritev to prevent
    // phantom bytes that suppress compaction ratio (#622).
    int total = sizeof(int) + sizeof(Jobrec) + sizeof crc_bytes;
    j->walused -= total;
    f->w->alive -= total;

    if (j->r.state == Invalid) {
        filermjob(j->file, j);
    }

    return r;
}


int
filewrjobfull(File *f, Job *j)
{
    int nl = j->tube->name_len;

    // CRC32C (v8 trailer) over [nl, tube name, j->r, body], serialized LE.
    uint32 crc = WAL_CRC32C_INIT;
    crc = wal_crc32c(crc, &nl,           sizeof nl);
    crc = wal_crc32c(crc, j->tube->name, nl);
    crc = wal_crc32c(crc, &j->r,         sizeof j->r);
    crc = wal_crc32c(crc, j->body,       j->r.body_size);
    crc ^= WAL_CRC32C_XOR;
    unsigned char crc_bytes[4] = {
        (unsigned char)(crc      ),
        (unsigned char)(crc >>  8),
        (unsigned char)(crc >> 16),
        (unsigned char)(crc >> 24),
    };

    struct iovec iov[5] = {
        { .iov_base = &nl,           .iov_len = sizeof nl },
        { .iov_base = j->tube->name, .iov_len = nl },
        { .iov_base = &j->r,         .iov_len = sizeof j->r },
        { .iov_base = j->body,       .iov_len = j->r.body_size },
        { .iov_base = crc_bytes,     .iov_len = sizeof crc_bytes },
    };

    int r = filewritev(f, j, iov, 5);
    if (r)
        fileaddjob(f, j);
    return r;
}


void
filewclose(File *f)
{
    if (!f) return;
    if (!f->iswopen) return;
    if (f->free) {
        errno = 0;
        if (ftruncate(f->fd, f->w->filesize - f->free) != 0) {
            twarn("ftruncate");
        }
    }
    if (close(f->fd) == -1)
        twarn("close");
    f->fd = -1;
    f->iswopen = 0;
    filedecref(f);
}


int
fileinit(File *f, Wal *w, int n)
{
    f->w = w;
    f->seq = n;
    f->path = fmtalloc("%s/binlog.%d", w->dir, n);
    return !!f->path;
}


// Adds f to the linked list in w,
// updating w->tail and w->head as necessary.
Wal*
fileadd(File *f, Wal *w)
{
    if (w->tail) {
        w->tail->next = f;
    }
    w->tail = f;
    if (!w->head) {
        w->head = f;
    }
    w->nfile++;
    return w;
}
