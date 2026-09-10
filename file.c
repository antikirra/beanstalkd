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
static int  readrec7(File*, Job *, int*);
static int  readfull(File*, void*, int, int*, char*);
static void warnpos(File*, int, char*, ...)
__attribute__((format(printf, 3, 4)));

// An all-zero Jobrec (jr.id == 0) normally means we reached the
// fallocate-zeroed tail of the binlog and replay stops cleanly. But a
// zeroed record MID-file (torn write, partial fsync, misaligned replay)
// looks identical, and replay would then silently drop every record
// that follows it. Peek at the unread buffered remainder: if it holds
// any nonzero byte this was not a tail, so say so. This is a bounded,
// best-effort diagnostic — data still in the kernel beyond the current
// buffer is not scanned, and the stop-at-zero semantics are unchanged.
// Returns 1 when the zero region was positively shown NOT to be a
// fallocate tail — live record bytes follow it inside the buffer just
// read. That is detected corruption: replay stops here either way, so
// the verdict is the only thing that reaches the operator.
static int
warn_if_not_tail(File *f)
{
    ReadBuf *rb = f->rbuf;
    if (!rb) return 0;
    for (int i = rb->pos; i < rb->filled; i++) {
        if (rb->buf[i]) {
            warnpos(f, 0, "zero record with nonzero data beyond it; "
                    "not a fallocate tail — ignoring the rest of the file");
            return 1;
        }
    }
    return 0;
}

FAlloc falloc = rawfalloc;

enum
{
    Walver7 = 7
};

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
    // refs is unsigned: one release too many turns 0 into UINT_MAX,
    // `refs < 1` stops holding, and the binlog is pinned out of walgc's
    // reach for the life of the process, with every later binlog queued
    // up behind it. Treat the over-release as the zero it meant.
    if (f->refs == 0) {
        twarnx("filedecref: refcount underflow on %s",
               f->path ? f->path : "(unnamed binlog)");
        walgc(f->w);
        return;
    }
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


// Sum of the wal bytes every job still on a file accounts for. Walked
// only on the repair path in filermjob, where the alternative is
// discarding the total outright.
static int64
alive_bytes(Wal *w)
{
    int64 total = 0;

    for (File *f = w->head; f; f = f->next) {
        Job *h = &f->jlist;
        if (!h->fnext) continue;   // list never initialised
        for (Job *k = h->fnext; k != h; k = k->fnext)
            total += k->walused;
    }
    return total;
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
        // The two counts have drifted. Zeroing the total would throw
        // away the live bytes every OTHER file still holds, so rebuild
        // it from what the files actually carry — j is already off its
        // list, so it is not counted.
        twarnx("filermjob: walused %"PRId64" > alive %"PRId64, j->walused, f->w->alive);
        f->w->alive = alive_bytes(f->w);
    }
    j->walused = 0;
    filedecref(f);
}


// Fileread reads jobs from f->path into list.
// It returns 0 on success, or 1 if any errors occurred.
// Releases the reference fileread took for the duration of the replay.
// Deliberately not filedecref: a binlog whose jobs were all deleted ends
// the replay at zero references, and filedecref would hand it to walgc,
// which frees the very struct fileread still has to write through and
// that its caller is still holding in w->head. The file stays
// registered; the next filedecref anywhere collects it normally.
static void
fileread_release(File *f)
{
    if (f->refs) f->refs--;
}


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
        f->rbuf = NULL;          // rb is this frame's; never outlive it
        fileread_release(f);
        return err;
    case Walver7:
        fileincref(f);
        while (readrec7(f, list, &err));
        f->rbuf = NULL;
        fileread_release(f);
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

    // A name off disk must clear the same bar as one off the wire: a
    // tube whose name carries CRLF or a colon turns every stats and
    // list reply that prints it into YAML the client reads as extra
    // fields. Reject the record; anything an earlier valid record
    // recovered stays untouched.
    if (namelen && !is_valid_tube(tubename, MAX_TUBE_NAME_LEN - 1)) {
        warnpos(f, -sz, "record names an unusable tube (%d bytes)", namelen);
        *err = 1;
        return 0;
    }

    r = readfull(f, &jr, sizeof(Jobrec), err, "job struct");
    if (!r) {
        return 0;
    }
    sz += r;
    crc = wal_crc32c(crc, &jr, sizeof(Jobrec));

    // UINT64_MAX is not a usable id: make_job_with_id would advance the
    // counter past it to 0, and 0 is the value that marks the fallocate
    // tail — the next job written would end every later replay at its
    // own record. Refuse the record; whatever an earlier valid record
    // recovered stays untouched.
    if (unlikely(jr.id == UINT64_MAX)) {
        warnpos(f, -sz, "job id %" PRIu64 " is out of range", jr.id);
        *err = 1;
        return 0;
    }

    // Are we reading trailing zeroes? (fallocate zero-fills unused tail;
    // a genuine record always has jr.id > 0.)
    if (!jr.id) {
        // Corruption that was positively identified must reach the exit
        // code too: "no errors" on a binlog whose tail was just
        // discarded is a failure wearing a legitimate mask.
        if (warn_if_not_tail(f)) *err = 1;
        return 0;
    }

    // Reject jobs with body_size < 2 from corrupted WAL.
    // Valid jobs always include \r\n trailer (body_size >= 2).
    if (namelen && jr.body_size < 2 && jr.state != Invalid) {
        warnpos(f, -sz, "job %"PRIu64" invalid body_size %d", jr.id, jr.body_size);
        *err = 1;
        return 0;
    }

    // Sanity cap on body_size BEFORE allocating. A corrupted or
    // pathological binlog could carry jr.body_size = INT_MAX, which
    // malloc would then attempt to honor. v7 reader bounds to 64 for
    // markers; we accept the full job_data_size_limit here for real
    // jobs and cap the legacy Invalid-marker specifically at 64 to
    // mirror v7 (the marker body was always 2 bytes) (#717).
    if (namelen) {
        int64 max_body = (jr.state == Invalid) ? 64 : (int64)job_data_size_limit;
        if (jr.body_size < 0 || (int64)jr.body_size > max_body) {
            warnpos(f, -sz,
                    "record %"PRIu64" body_size %d out of range (state=%d, max=%"PRId64")",
                    jr.id, jr.body_size, jr.state, max_body);
            *err = 1;
            return 0;
        }
    }

    // For full records, read body into a temporary buffer and fold into CRC.
    // We cannot read directly into j->body yet because CRC verification must
    // pass BEFORE any existing job state is mutated. This preserves the
    // invariant that corrupted data never overwrites valid in-memory state.
    // body_size 0 is legal for a legacy truncate marker, and readfull
    // returns 0 for a zero-byte read — which every caller reads as
    // end-of-file. Skip the read instead of ending replay on a record
    // the file still has data behind (#714 sibling).
    if (namelen && jr.body_size > 0) {
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

        // A short record is a state update for an existing job; its
        // body_size must equal the size set by the job's full record
        // (the writer snapshots the immutable j->r). Reject a mismatch
        // BEFORE `j->r = jr`: otherwise the bogus size is copied into
        // j->r and the Error path's job_free would file j->body into
        // the wrong size-class pool slab (heap overflow on reuse).
        // Unreachable here under a valid CRC trailer; kept as free
        // defense in depth mirroring the v7 reader, where it is
        // load-bearing.
        if (!namelen && jr.body_size != j->r.body_size) {
            warnpos(f, -sz, "job %"PRIu64" short-record body_size changed (was %d, now %d)",
                    jr.id, j->r.body_size, jr.body_size);
            goto Error;
        }
        {
        int32 old_body_size = j->r.body_size;

        // A full record for an existing job must carry the body_size the
        // job's current allocation was created with. Checking this AFTER
        // `j->r = jr` is too late: the Error path's job_free files
        // j->body into the size-class pool keyed by j->r.body_size, so a
        // corrupt (larger) size would pool a small slab under a large
        // class and the next allocate_job of that class would overflow
        // it. Reject BEFORE the assignment, like the short-record check
        // above. (Here it is defense in depth — a corrupt record never
        // passes the CRC trailer — but in the v7 reader the identical
        // check is load-bearing.)
        if (namelen && jr.body_size != old_body_size) {
            warnpos(f, -sz, "job %"PRIu64" full-record body_size changed (was %d, now %d)",
                    jr.id, old_body_size, jr.body_size);
            goto Error;
        }
        j->r = jr;

        // For short records, move job to tail of replay list to
        // preserve WAL ordering. Ensures buried jobs maintain
        // their burial order after restart (#668).
        if (!namelen) {
            job_list_remove(j);
        }
        job_list_insert(l, j);

        if (namelen) {
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
        if (namelen > 0) {
            // Legacy truncate marker (the truncate command was removed).
            // CRC is already verified; the cutoff it carried is ignored,
            // so previously truncated jobs replay as live — the accepted
            // downgrade semantic. Warn for each marker found, but do not
            // fail replay.
            warnpos(f, -sz, "ignoring legacy truncate marker (tube=%s cutoff=%"PRIu64")",
                    tubename, jr.id);
            return 1;
        }
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    default:
        warnpos(f, -sz, "unknown job state: %d", jr.state);
        // Nothing of this record has been applied yet, so j (if there is
        // one) still holds exactly what an earlier, valid record
        // recovered. Falling into Error would turn one unreplayable
        // record into the loss of a live job.
        *err = 1;
        free(body_buf);
        return 0;
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
// to recover pre-v8 binlogs during migration. See the Walver workflow
// comment above struct Jobrec in dat.h for the procedure.
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

    // Same rule as the v8 reader above: a tube name off disk must be
    // one this server would have accepted on the wire.
    if (namelen && !is_valid_tube(tubename, MAX_TUBE_NAME_LEN - 1)) {
        warnpos(f, -sz, "record names an unusable tube (%d bytes)", namelen);
        *err = 1;
        return 0;
    }

    r = readfull(f, &jr, sizeof(Jobrec), err, "job struct");
    if (!r) {
        return 0;
    }
    sz += r;

    // Same id bound as the v8 reader above.
    if (unlikely(jr.id == UINT64_MAX)) {
        warnpos(f, -sz, "job id %" PRIu64 " is out of range", jr.id);
        *err = 1;
        return 0;
    }

    // are we reading trailing zeroes?
    if (!jr.id) {
        // Corruption that was positively identified must reach the exit
        // code too: "no errors" on a binlog whose tail was just
        // discarded is a failure wearing a legitimate mask.
        if (warn_if_not_tail(f)) *err = 1;
        return 0;
    }

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

        // A short record is a state update for an existing job; its
        // body_size must equal the size set by the job's full record
        // (the writer snapshots the immutable j->r), so a mismatch is
        // corruption. v7 has no CRC trailer: without this check a
        // bit-flipped body_size would be copied into j->r below and
        // later poison the size-class pool — job_free pools the small
        // j->body slab under the bogus (large) class, and a future
        // allocate_job hands that slab to a real large PUT whose body
        // write overflows it. Reject BEFORE `j->r = jr` so the Error
        // path frees j by its true size. Full records already get the
        // symmetric "size changed" check below; short records were the
        // unvalidated gap.
        if (!namelen && jr.body_size != j->r.body_size) {
            warnpos(f, -sz, "job %"PRIu64" short-record body_size changed (was %d, now %d)",
                    jr.id, j->r.body_size, jr.body_size);
            goto Error;
        }
        {
        int32 old_body_size = j->r.body_size;

        // Full records need the same guard as the short-record check
        // above, and for the same reason it must fire BEFORE `j->r = jr`:
        // a corrupt full record re-stating an existing job with a larger
        // body_size would otherwise be copied into j->r, and the Error
        // path's job_free would pool the small j->body slab under the
        // bogus large size class — the next allocate_job of that class
        // overflows it. v7 has no CRC trailer, so this check is the only
        // thing standing between a bit-flip and the pool poisoning.
        if (namelen && jr.body_size != old_body_size) {
            warnpos(f, -sz, "job %"PRIu64" full-record body_size changed (was %d, now %d)",
                    jr.id, old_body_size, jr.body_size);
            goto Error;
        }
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
        // Legacy truncate marker (Invalid + namelen > 0). The truncate
        // command was removed; consume the marker body so the file
        // position stays aligned for the next readrec7 call (#714), then
        // ignore the cutoff — previously truncated jobs replay as live
        // (accepted downgrade semantic). The writer used a 2-byte "\r\n"
        // body; bound defensively.
        if (namelen > 0) {
            if (jr.body_size < 0 || jr.body_size > 64) {
                warnpos(f, -sz, "v7 marker body_size %d out of expected range",
                        jr.body_size);
                // A marker carries a cutoff, never a job: the job whose
                // id it happens to collide with was recovered by its own
                // full record and must survive this one being garbage.
                *err = 1;
                return 0;
            }
            char mbody[64];
            if (jr.body_size > 0) {
                int rb = readfull(f, mbody, jr.body_size, err, "marker body");
                if (!rb) goto Error;
            }
            warnpos(f, -sz, "ignoring legacy truncate marker (tube=%s cutoff=%"PRIu64")",
                    tubename, jr.id);
            return 1;
        }
        if (j) {
            job_list_remove(j);
            filermjob(j->file, j);
            job_free(j);
        }
        return 1;
    default:
        warnpos(f, -r, "unknown job state: %d", jr.state);
        // As in readrec: the record is rejected, but the job an earlier
        // record recovered is not this record's to destroy.
        *err = 1;
        return 0;
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

    // Every reader reaches this through fileread, which owns the buffer
    // for the length of the replay. There used to be an unbuffered
    // fallback here for the rb == NULL case; nothing could reach it, so
    // it was never exercised — an untested read path in the WAL reader
    // is worse than an explicit refusal.
    if (unlikely(!rb)) {
        warnpos(f, 0, "internal: read of %s outside a replay", desc);
        *err = 1;
        return 0;
    }

    while (got < n) {
        if (rb->pos < rb->filled) {
            int avail = rb->filled - rb->pos;
            int chunk = (n - got < avail) ? n - got : avail;
            memcpy(dst + got, rb->buf + rb->pos, chunk);
            rb->pos += chunk;
            got += chunk;
            continue;
        }

        // Refill.
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

    // A binlog too small for its own header would publish a negative
    // f->free, and that feeds both the reservation arithmetic and the
    // closing truncate: "drop the unused tail" becomes "extend the
    // file". -s accepts any size from 1 byte up, so this is reachable.
    if (f->w->filesize < (int)sizeof(int)) {
        twarnx("binlog size %d is too small for the version header",
               f->w->filesize);
        return;
    }

    // O_TRUNC: a file of this name left behind by an earlier life of
    // the server keeps its old records past the freshly written header
    // otherwise, and replay brings those jobs back from a binlog this
    // writer never wrote.
    fd = open(f->path, O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0400);
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

    // rawfalloc's fallback (filesystems without fallocate) writes the
    // zeroes itself and leaves the offset at the end of them, so a bare
    // write() would stamp the header past the data instead of at 0.
    n = lseek(fd, 0, SEEK_SET) == 0 ? write(fd, &ver, sizeof(int)) : -1;
    if (n < 0 || (size_t)n < sizeof(int)) {
        twarn("write %s", f->path);
        if (close(fd) == -1)
            twarn("close");
        unlink(f->path);
        return;
    }

    f->fd = fd;
    f->woff = (off_t)n;      // the header is all that is written so far
    f->woff_valid = 1;
    f->iswopen = 1;
    fileincref(f);
    f->free = f->w->filesize - n;
    f->resv = 0;
}


// writev_all writes `total` bytes from iov to fd with EINTR / partial-write
// retry. On failure, truncates the file back to the pre-write offset so
// that on recovery, readrec does NOT stumble on half-written record
// bytes: a torn trailer would CRC-fail and abort fileread, silently
// dropping every record that followed in the same binlog (#700).
// Returns 1 on success, 0 on failure (file rolled back when possible).
static int
writev_all(File *f, struct iovec *iov, int iovcnt, int total)
{
    int fd = f->fd;

    // The rollback below needs the offset this record started at. Ask
    // the kernel only once per fd: from then on every writer here keeps
    // f->woff in step, which takes one syscall per staged record out of
    // the durable hot path.
    if (unlikely(!f->woff_valid)) {
        off_t cur = lseek(fd, 0, SEEK_CUR);
        if (unlikely(cur < 0)) { twarn("lseek before writev"); return 0; }
        f->woff = cur;
        f->woff_valid = 1;
    }
    off_t before = f->woff;

    ssize_t r = writev(fd, iov, iovcnt);
    if (likely(r == total)) { f->woff = before + total; return 1; }

    int written;
    if (r == -1) {
        if (errno != EINTR) { twarn("writev"); goto rollback; }
        written = 0;
    } else if (unlikely(r <= 0)) {
        twarn("writev");
        goto rollback;
    } else {
        written = (int)r;
        while (iovcnt > 0 && (size_t)r >= iov[0].iov_len) {
            r -= iov[0].iov_len; iov++; iovcnt--;
        }
        if (iovcnt > 0 && r > 0) {
            iov[0].iov_base = (char *)iov[0].iov_base + r;
            iov[0].iov_len -= r;
        }
    }
    while (written < total) {
        r = writev(fd, iov, iovcnt);
        if (r == -1 && errno == EINTR) continue;
        if (unlikely(r <= 0)) { twarn("writev"); goto rollback; }
        written += r;
        while (iovcnt > 0 && (size_t)r >= iov[0].iov_len) {
            r -= iov[0].iov_len; iov++; iovcnt--;
        }
        if (iovcnt > 0 && r > 0) {
            iov[0].iov_base = (char *)iov[0].iov_base + r;
            iov[0].iov_len -= r;
        }
    }
    f->woff = before + total;
    return 1;

rollback:
    // Best-effort: leave the WAL in a readable state. If ftruncate fails
    // (rare: ENOSPC already on a filesystem that ate our writev), the
    // binlog tail will contain torn bytes — caller should treat this as
    // WAL-disabled and let the next restart deal with a CRC fail at the
    // tail, which stops at the first bad record rather than propagating.
    if (ftruncate(fd, before) == -1) {
        twarn("ftruncate rollback after writev fail");
        f->woff_valid = 0;   // the file is not where either side thinks
    } else if (lseek(fd, before, SEEK_SET) < 0) {
        twarn("lseek after writev rollback");
        f->woff_valid = 0;
    } else {
        f->woff = before;
    }
    return 0;
}

// Durable-commits the tail of f: fdatasync + ftruncate rollback on failure.
// Mirrors writev_all's atomic-or-rollback pattern so that, on error, the
// `total` bytes just written are removed from the end of the file. This
// keeps server memory and on-disk state consistent: the caller has not
// yet applied accounting updates, so a failure here looks identical to
// "writev never happened" (#C2).
//
// EINTR retries indefinitely (fdatasync is never interrupted for long in
// practice; if it is, the process is being signaled and the main loop
// will shortly tear down). Any other errno is permanent.
//
// Returns 1 on success (data durable), 0 on failure (tail best-effort
// rolled back, WAL caller should disable w->use and surface the error).
static int
filewrite_commit_durable(File *f, int total)
{
    if (likely(!f->w->durable_sync)) return 1;

    int sr;
    while ((sr = fdatasync(f->fd)) == -1 && errno == EINTR)
        ;
    if (likely(sr != -1)) return 1;

    twarn("durable fdatasync");

    // Best-effort rollback. If ftruncate or the follow-up fdatasync
    // also fail, the tail may still contain the uncommitted bytes; the
    // next readrec on replay will either accept them (kernel flushed
    // the write + the record happened to be fully on-disk) or CRC-fail
    // at the tail (partial record). Both paths are safer than leaving
    // the server's memory claiming "write failed" while disk says
    // "write succeeded", which is the bug this path closes.
    off_t cur;
    if (likely(f->woff_valid)) {
        cur = f->woff;
    } else {
        cur = lseek(f->fd, 0, SEEK_CUR);
        if (cur < 0) { twarn("lseek before durable rollback"); return 0; }
    }
    if (total < 0) {
        // A rollback of a negative staged count would ftruncate the file
        // LONGER than it is, appending a hole that replays as a tail.
        twarnx("durable rollback: negative staged count %d", total);
        return 0;
    }
    if (cur < (off_t)total) {
        twarnx("durable rollback: unexpected offset %lld < total %d",
               (long long)cur, total);
        return 0;
    }
    off_t before = cur - (off_t)total;
    if (ftruncate(f->fd, before) == -1) {
        twarn("ftruncate rollback after durable fdatasync");
        f->woff_valid = 0;
        return 0;
    }
    f->woff = before;
    f->woff_valid = 1;
    if (lseek(f->fd, before, SEEK_SET) < 0) {
        twarn("lseek after durable rollback");
        f->woff_valid = 0;
        // Offset is unknown now, but we still removed the bytes. The
        // caller will filewclose + w->use=0, making further writes
        // impossible, so the misaligned offset is harmless.
    }
    int dr;
    while ((dr = fdatasync(f->fd)) == -1 && errno == EINTR)
        ;
    if (dr == -1) twarn("durable fdatasync after rollback");
    return 0;
}

// file_stage_account applies the staging-side accounting of filewritev:
// `total` bytes leave the reservation (w->resv, f->resv) and become
// uncommitted-but-alive. filewrcommit's commit-fail rollback reverses
// exactly these counters, so staging and rollback must stay in lockstep
// — hence one helper. Per-job (j->walresv, j->walused) adjustments stay
// at the call site.
static inline void
file_stage_account(File *f, int total)
{
    f->uncommitted_bytes += total;
    f->uncommitted_alive += total;
    f->w->resv -= total;
    f->resv -= total;
    f->w->alive += total;
}

// filewritev stages a WAL record: writev_all + accounting, NO fdatasync.
// Accounting is applied immediately on success so each stage looks the
// same to subsequent stages in the same batch; filewrcommit() later
// issues one fdatasync covering every stage and rolls back both the
// tail (ftruncate) and the global counters on failure. f->uncommitted_bytes
// accumulates so the commit path knows how many bytes to reverse.
//
// Returns 1 on writev success (record is in page cache, not yet durable),
// 0 on writev error (WAL caller disables w->use; tail best-effort
// rolled back by writev_all's own rollback path). Per-job counters
// (j->walresv, j->walused) are applied upfront and NOT reversed on
// commit fail — acceptable because a commit fail disables the WAL and
// the per-job slop is reclaimed at job_free (documented on filewrcommit).
__attribute__((hot)) static int
filewritev(File *f, Job *j, struct iovec *iov, int iovcnt)
{
    int total = 0;
    for (int i = 0; i < iovcnt; i++)
        total += iov[i].iov_len;

    if (!writev_all(f, iov, iovcnt, total)) return 0;

    file_stage_account(f, total);
    j->walresv -= total;
    j->walused += total;
    return 1;
}

// filewrcommit — issue one fdatasync covering every filewritev
// stage that landed on f since the last commit. On success, clears the
// uncommitted_bytes counter. On failure, the existing
// filewrite_commit_durable helper ftruncates the tail back by
// uncommitted_bytes and the caller is expected to disable w->use.
//
// Accounting rollback on commit fail:
//   filewritev applies accounting upfront so every stage in a batch
//   looks the same to subsequent stages. If the batch commit fails and
//   the tail is ftruncate'd away, those adjustments must be reversed
//   or w->resv / w->alive drift from what the disk actually holds.
//   w->resv reverts by the full uncommitted_bytes (every stage took
//   its reservation and nothing returned it), but w->alive reverts by
//   uncommitted_alive only: filewrjobshort immediately undoes its own
//   alive contribution (#622 dead space), so reverting the full batch
//   would subtract short-record bytes a second time and drive alive
//   negative. Per-job counters (j->walresv, j->walused) would require
//   a pending-job list on File; left as a known gap since walcommit
//   disables the WAL on fail, after which no further walresv* /
//   walwrite call references those per-job fields. The same gap covers
//   the full-record bytes that an Invalid short record's filermjob
//   already subtracted from alive — they are not restored either.
//
// In durable_sync=0 mode the fdatasync is skipped inside
// filewrite_commit_durable (matches legacy behaviour of filewritev:
// fdatasync only fires under -D); this function still drains the
// uncommitted_* counters so the next batch starts fresh.
int
filewrcommit(File *f)
{
    int total = f->uncommitted_bytes;
    if (total == 0) return 1;  // nothing staged — commit is a no-op
    int r = filewrite_commit_durable(f, total);
    if (!r) {
        // Rollback global counters so they match the post-ftruncate
        // tail. Matches the invariant that after a failed commit every
        // byte the batch claimed to add to the file is gone from both
        // disk AND accounting.
        f->w->resv  += total;
        f->resv     += total;
        f->w->alive -= f->uncommitted_alive;
    }
    // On success or rollback, the counters are drained: success leaves
    // bytes durable, rollback has removed them from the tail. Either way
    // the next batch starts fresh.
    f->uncommitted_bytes = 0;
    f->uncommitted_alive = 0;
    return r;
}


// crc32c_trailer_le serializes the v8 WAL record trailer: final XOR,
// then 4 bytes little-endian. This is the single encoder for the
// on-disk trailer format; the decode mirror is in readrec (stored vs
// computed). testwal2.c re-implements the trailer independently on
// purpose — it is the oracle proving the on-disk bytes — do not
// "deduplicate" it into this helper.
static inline void
crc32c_trailer_le(uint32 crc, unsigned char out[4])
{
    crc ^= WAL_CRC32C_XOR;
    out[0] = (unsigned char)(crc      );
    out[1] = (unsigned char)(crc >>  8);
    out[2] = (unsigned char)(crc >> 16);
    out[3] = (unsigned char)(crc >> 24);
}


__attribute__((hot)) int
filewrjobshort(File *f, Job *j)
{
    int nl = 0; // name len 0 indicates short record

    // CRC32C (v8 trailer) over [nl, j->r], serialized little-endian.
    uint32 crc = WAL_CRC32C_INIT;
    crc = wal_crc32c(crc, &nl,   sizeof nl);
    crc = wal_crc32c(crc, &j->r, sizeof j->r);
    unsigned char crc_bytes[4];
    crc32c_trailer_le(crc, crc_bytes);

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
    // phantom bytes that suppress compaction ratio (#622). Also undo
    // uncommitted_alive: these bytes are no longer counted in w->alive,
    // so the commit-fail rollback must not subtract them a second time
    // (it reverts only uncommitted_alive, never the whole batch).
    int total = sizeof(int) + sizeof(Jobrec) + sizeof crc_bytes;
    j->walused -= total;
    f->w->alive -= total;
    f->uncommitted_alive -= total;

    if (j->r.state == Invalid) {
        if (j->file) {
            filermjob(j->file, j);
        } else if (j->walused > 0) {
            // The binlog that held the full record is already gone, so
            // there is no file to unregister from — but the bytes it
            // accounted for are dead space all the same, and leaving
            // them on the books suppresses the compaction ratio for the
            // rest of the process's life.
            int64 n = j->walused < f->w->alive ? j->walused : f->w->alive;
            f->w->alive -= n;
            j->walused = 0;
        }
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
    unsigned char crc_bytes[4];
    crc32c_trailer_le(crc, crc_bytes);

    struct iovec iov[5] = {
        { .iov_base = &nl,           .iov_len = sizeof nl },
        { .iov_base = j->tube->name, .iov_len = nl },
        { .iov_base = &j->r,         .iov_len = sizeof j->r },
        { .iov_base = j->body,       .iov_len = j->r.body_size },
        { .iov_base = crc_bytes,     .iov_len = sizeof crc_bytes },
    };

    // A full record moves the job to this binlog, so let go of the one
    // it was on — before the write, because filermjob zeroes j->walused
    // and filewritev is about to set it for the new file. Without this
    // the old file keeps a reference it never gets back and walgc can
    // never reap it. (readrec does the same when a full record replays
    // onto a job that already lives on another file.)
    if (j->file && j->file != f)
        filermjob(j->file, j);

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
    // Only ever shorten. A negative free count (an over-committed
    // reservation) would turn "drop the unused tail" into "extend the
    // file with a hole", and the zeroes that hole reads back as look
    // exactly like a fallocate tail to the next replay.
    if (f->free > 0) {
        errno = 0;
        if (ftruncate(f->fd, f->w->filesize - f->free) != 0) {
            twarn("ftruncate");
        }
    } else if (f->free < 0) {
        twarnx("refusing to grow %s on close: free is %d",
               f->path ? f->path : "(unnamed binlog)", f->free);
    }
    // Flush before close when running non-durable: close() drops the
    // last fd for this binlog (rotation in usenext closes the old file
    // through here), so this is the final chance to push its data out of
    // the page cache. Cheap insurance against the power-fail window
    // between the last periodic fsync and rotation. In durable_sync mode
    // filewrcommit has already fdatasynced everything staged
    // (uncommitted_bytes == 0), so a second sync here would be pure
    // overhead. Failure is non-fatal — the file is closed regardless.
    if (!(f->w->durable_sync && f->uncommitted_bytes == 0)) {
        int sr;
        while ((sr = fdatasync(f->fd)) == -1 && errno == EINTR)
            ;
        if (sr == -1)
            twarn("fdatasync before close");
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
    // walscandir only ever rediscovers a name whose suffix is a
    // positive decimal, so a sequence outside that range would name a
    // binlog the writer fills and the reader can never find again.
    if (n < 1) {
        twarnx("refusing to name a binlog with sequence %d", n);
        return 0;
    }
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
    // The node being appended is the end of the list. A stale ->next
    // left over from a previous life would otherwise be spliced in
    // whole, making the list reach files nfile never counted and walgc
    // free files the Wal never adopted.
    f->next = NULL;
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
