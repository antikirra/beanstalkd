// Angry tests for warn_if_not_tail (file.c).
//
// It fires when an all-zero Jobrec is read and decides whether that
// zero region is the fallocate tail or a torn write with live records
// behind it. The unit is static and produces only a diagnostic, so the
// attacks below pin what an operator can actually observe: the verdict
// fileread hands back for each of the three zero-region shapes, and the
// explicit bound on how far the scan is allowed to look.

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
wt_setup(void)
{
    fault_clear_all();
    progname = "testfile_warn_if_not_tail";
}

static void
wt_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
wt_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
wt_create(char *path)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    int ver = Walver;
    wt_put(fd, &ver, sizeof ver);
    return fd;
}

static void
wt_v8(int fd, const char *tube, int namelen, const void *jr,
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
    wt_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) wt_put(fd, tube, (size_t)namelen);
    wt_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) wt_put(fd, body, (size_t)bodylen);
    wt_put(fd, tr, sizeof tr);
}

static Jobrec
wt_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 1;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 4;
    jr.state = state;
    return jr;
}

// An all-zero record: namelen 0 followed by a zeroed Jobrec, exactly
// what a fallocate-zeroed region parses as.
static void
wt_zero_record(int fd)
{
    char z[4 + sizeof(Jobrec)];
    memset(z, 0, sizeof z);
    wt_put(fd, z, sizeof z);
}

// `count` filler records of 101 bytes each: 4 + 7 + 80 + 6 + 4.
static void
wt_pad_records(int fd, int count)
{
    for (int i = 0; i < count; i++) {
        Jobrec jr = wt_job((uint64)(5000 + i), 6, Ready);
        wt_v8(fd, "default", 7, &jr, "abcd\r\n", 6);
    }
}

static void
wt_zeros(int fd, int n)
{
    char z[1024];
    memset(z, 0, sizeof z);
    while (n > 0) {
        int chunk = n < (int)sizeof z ? n : (int)sizeof z;
        wt_put(fd, z, (size_t)chunk);
        n -= chunk;
    }
}

static int
wt_replay(char *path, Wal *w, File *f, Job *list)
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


// The scan has POSITIVELY established that the zero region is not a
// tail: live record bytes follow it inside the very buffer it just
// read. Replay stops there either way, so the only thing that reaches
// the operator is the verdict — and reporting "no errors" for a binlog
// whose tail was just discarded is a failure wearing a legitimate mask.
void
cttest_warn_if_not_tail_reports_a_zero_record_with_live_data_behind_it(void)
{
    wt_setup();
    char path[512];
    wt_path(path, sizeof path, "notatail.binlog");

    Jobrec first = wt_job(11, 5, Ready);
    Jobrec behind = wt_job(12, 5, Ready);

    int fd = wt_create(path);
    wt_v8(fd, "default", 7, &first, "abc\r\n", 5);
    wt_zero_record(fd);
    wt_v8(fd, "default", 7, &behind, "xyz\r\n", 5);
    wt_zeros(fd, 512);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = wt_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a zero record with live records behind it is detected "
            "corruption and must be reported, got verdict %d", r);
}


// The mirror case: a real fallocate tail. Nothing follows it, nothing
// was lost, and the run must stay clean — a scan that cried wolf here
// would make every healthy restart look damaged.
void
cttest_warn_if_not_tail_accepts_a_genuine_fallocate_tail(void)
{
    wt_setup();
    char path[512];
    wt_path(path, sizeof path, "realtail.binlog");

    Jobrec only = wt_job(21, 5, Ready);
    int fd = wt_create(path);
    wt_v8(fd, "default", 7, &only, "abc\r\n", 5);
    wt_zeros(fd, 4096);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = wt_replay(path, &w, &f, &list);

    assertf(r == 0,
            "a binlog ending in its fallocate tail is healthy and must "
            "replay without an error verdict, got %d", r);
}


// Contract (1) bounds the scan to the bytes already buffered and says
// so out loud. Here the zero record ends exactly on the 64 KiB refill
// boundary, so the buffered remainder is empty and the live record that
// follows is out of the diagnostic's reach — by design, and pinned so
// the bound cannot quietly turn into a whole-file scan or an extra read.
void
cttest_warn_if_not_tail_does_not_look_past_the_buffered_remainder(void)
{
    wt_setup();
    char path[512];
    wt_path(path, sizeof path, "beyondwindow.binlog");

    // 4 header + 648 * 101 filler + 84 zero record = exactly 65536.
    int fd = wt_create(path);
    wt_pad_records(fd, 648);
    wt_zero_record(fd);
    Jobrec behind = wt_job(31, 5, Ready);
    wt_v8(fd, "default", 7, &behind, "xyz\r\n", 5);
    wt_zeros(fd, 512);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = wt_replay(path, &w, &f, &list);

    assertf(r == 0,
            "nonzero data past the buffered window is explicitly out of "
            "the diagnostic's scope and must not be reported, got %d", r);
}
