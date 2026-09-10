// Angry tests for readfull (file.c) — the byte pump every WAL reader
// stands on. It is static and reachable only through fileread(), so the
// fixtures below are shaped to steer bytes across its 64 KiB refill
// boundary, its EOF verdicts and its EINTR retry.

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
rf_setup(void)
{
    fault_clear_all();
    progname = "testfile_readfull";
}

static void
rf_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
rf_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
rf_create(char *path, int ver)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    rf_put(fd, &ver, sizeof ver);
    return fd;
}

static void
rf_v8(int fd, const char *tube, int namelen, const void *jr,
      const void *body, int bodylen)
{
    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &namelen, sizeof namelen);
    if (namelen > 0) c = wal_crc32c(c, tube, (size_t)namelen);
    c = wal_crc32c(c, jr, sizeof(Jobrec));
    if (bodylen > 0) c = wal_crc32c(c, body, (size_t)bodylen);
    c ^= WAL_CRC32C_XOR;
    unsigned char tr[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };
    rf_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) rf_put(fd, tube, (size_t)namelen);
    rf_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) rf_put(fd, body, (size_t)bodylen);
    rf_put(fd, tr, sizeof tr);
}

static void
rf_tail(int fd)
{
    char z[512];
    memset(z, 0, sizeof z);
    rf_put(fd, z, sizeof z);
}

static Jobrec
rf_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 5;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 3;
    jr.state = state;
    return jr;
}

// Deterministic non-repeating fill: a body of identical bytes would
// survive a reader that duplicated or dropped a chunk at a refill.
static void
rf_fill(char *b, int n, uint32 seed)
{
    for (int i = 0; i < n; i++) {
        seed = seed * 1664525u + 1013904223u;
        b[i] = (char)(seed >> 24);
    }
    if (n >= 2) {
        b[n-2] = '\r';
        b[n-1] = '\n';
    }
}

// Writes `count` identical small records so the NEXT record begins at a
// chosen offset. Loop lives here, never in a test body.
static void
rf_pad_records(int fd, int count)
{
    for (int i = 0; i < count; i++) {
        Jobrec jr = rf_job((uint64)(1000 + i), 2, Ready);
        rf_v8(fd, "d", 1, &jr, "\r\n", 2);
    }
}

static int
rf_replay(char *path, Wal *w, File *f, Job *list)
{
    f->w = w;
    f->path = path;
    f->fd = open(path, O_RDONLY);
    assertf(f->fd >= 0, "setup: open fixture: %s", strerror(errno));
    job_list_reset(list);
    fileadd(f, w);
    w->cur = f;
    int r = fileread(f, list);
    close(f->fd);
    f->fd = -1;
    return r;
}


// A body larger than the 64 KiB read buffer is assembled from several
// refills. Every byte has to land at the offset it was written to; the
// pattern is non-repeating so a duplicated or skipped chunk shows up as
// a body mismatch and not merely as a CRC failure.
void
cttest_readfull_assembles_a_body_that_spans_several_refills(void)
{
    rf_setup();
    job_data_size_limit = 200000;
    char path[512];
    rf_path(path, sizeof path, "bigbody.binlog");

    int n = 100000;
    char *body = malloc((size_t)n);
    assertf(body != NULL, "setup: malloc body");
    rf_fill(body, n, 0xC0FFEEu);

    Jobrec jr = rf_job(11, n, Ready);
    int fd = rf_create(path, Walver);
    rf_v8(fd, "default", 7, &jr, body, n);
    rf_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rf_replay(path, &w, &f, &list);

    Job *j = job_find(11);
    int intact = j && memcmp(j->body, body, (size_t)n) == 0;
    free(body);

    assertf(intact,
            "a %d-byte body must be reassembled byte for byte across the "
            "64 KiB refill boundary", n);
}


// The refill boundary is invisible to small fixtures. This one places
// the buffer edge inside a record's Jobrec, so the struct is delivered
// from two different reads.
void
cttest_readfull_delivers_a_jobrec_split_across_the_buffer_edge(void)
{
    rf_setup();
    char path[512];
    rf_path(path, sizeof path, "straddle.binlog");

    // Filler record: 4 + 1 + 80 + 2 + 4 = 91 bytes each, after the
    // 4-byte header. 720 of them put the next record's Jobrec across
    // offset 65536.
    int fd = rf_create(path, Walver);
    rf_pad_records(fd, 720);
    Jobrec jr = rf_job(22, 5, Ready);
    rf_v8(fd, "default", 7, &jr, "spl\r\n", 5);
    rf_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rf_replay(path, &w, &f, &list);

    Job *j = job_find(22);
    assertf(j && j->r.ttr == 120000000000LL && j->r.pri == 5,
            "a Jobrec delivered from two refills must arrive intact");
}


// A file that ends exactly after its last complete record is a healthy
// binlog: the reader has consumed everything and must say so.
void
cttest_readfull_treats_the_end_of_the_last_record_as_a_clean_stop(void)
{
    rf_setup();
    char path[512];
    rf_path(path, sizeof path, "exactend.binlog");

    Jobrec jr = rf_job(33, 5, Ready);
    int fd = rf_create(path, Walver);
    rf_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    close(fd);                       // no fallocate tail at all

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = rf_replay(path, &w, &f, &list);

    assertf(r == 0,
            "a binlog ending on a record boundary is fully consumed and "
            "must replay without an error verdict, got %d", r);
}


// The mirror of the test above: a file cut mid-body has lost data, and
// reporting that identically to a clean end is the difference between
// "replay finished" and "replay lost your jobs".
void
cttest_readfull_reports_a_body_cut_short_by_end_of_file(void)
{
    rf_setup();
    char path[512];
    rf_path(path, sizeof path, "cutbody.binlog");

    Jobrec whole = rf_job(44, 5, Ready);
    Jobrec cut = rf_job(45, 5, Ready);

    int fd = rf_create(path, Walver);
    rf_v8(fd, "default", 7, &whole, "abc\r\n", 5);
    int nl = 7;
    rf_put(fd, &nl, sizeof nl);
    rf_put(fd, "default", 7);
    rf_put(fd, &cut, sizeof cut);
    rf_put(fd, "ab", 2);            // 2 of 5 body bytes, then EOF
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = rf_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a body cut short by end of file is a partial record and must "
            "be reported, got verdict %d", r);
}


// EINTR is not a failure. A signal landing on the very first refill
// must cost nothing but a retry.
void
cttest_readfull_retries_an_interrupted_refill(void)
{
    rf_setup();
    char path[512];
    rf_path(path, sizeof path, "eintr.binlog");

    Jobrec jr = rf_job(55, 5, Ready);
    int fd = rf_create(path, Walver);
    rf_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    rf_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    f.w = &w;
    f.path = path;
    f.fd = open(path, O_RDONLY);
    assertf(f.fd >= 0, "setup: open fixture: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&f, &w);
    w.cur = &f;

    fault_set(FAULT_READ, 0, EINTR);
    fileread(&f, &list);
    close(f.fd);

    assertf(job_find(55) != NULL,
            "an EINTR on the first refill must be retried, not turned into "
            "a lost binlog");
}


// A hard read error is the opposite: it must never be reported as the
// tidy end of the file.
void
cttest_readfull_reports_a_read_error_rather_than_a_clean_end(void)
{
    rf_setup();
    char path[512];
    rf_path(path, sizeof path, "eio.binlog");

    Jobrec jr = rf_job(66, 5, Ready);
    int fd = rf_create(path, Walver);
    rf_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    rf_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    f.w = &w;
    f.path = path;
    f.fd = open(path, O_RDONLY);
    assertf(f.fd >= 0, "setup: open fixture: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&f, &w);
    w.cur = &f;

    fault_set(FAULT_READ, 0, EIO);
    int r = fileread(&f, &list);
    close(f.fd);

    assertf(r == 1,
            "a failed read must be reported as an error, got verdict %d", r);
}
