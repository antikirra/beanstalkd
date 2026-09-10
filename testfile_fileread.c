// Angry tests for fileread (file.c) — the version dispatcher and the
// owner of the recovery read buffer.
//
// Its documented promise is narrow ("returns 0 on success, or 1 if any
// errors occurred") but everything downstream of a restart depends on
// it: which reader runs, whether a bad header is reported, and whether
// the File it was handed is still alive when it returns.

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
fr_setup(void)
{
    fault_clear_all();
    progname = "testfile_fileread";
}

static void
fr_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
fr_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
fr_create(char *path)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    return fd;
}

static void
fr_v8(int fd, const char *tube, int namelen, const void *jr,
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
    fr_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) fr_put(fd, tube, (size_t)namelen);
    fr_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) fr_put(fd, body, (size_t)bodylen);
    fr_put(fd, tr, sizeof tr);
}

static void
fr_tail(int fd)
{
    char z[512];
    memset(z, 0, sizeof z);
    fr_put(fd, z, sizeof z);
}

static Jobrec
fr_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 9;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 2;
    jr.state = state;
    return jr;
}

static int
fr_replay(char *path, Wal *w, File *f, Job *list)
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


// An unrecognised header is the one case where the reader knows it
// cannot understand the file. Silence here would let walread report a
// clean start for a binlog it never parsed.
void
cttest_fileread_returns_an_error_for_an_unrecognised_version(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "ver9.binlog");

    int ver = Walver + 1;
    Jobrec jr = fr_job(1, 5, Ready);
    int fd = fr_create(path);
    fr_put(fd, &ver, sizeof ver);
    fr_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    fr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = fr_replay(path, &w, &f, &list);

    assertf(r == 1,
            "version %d is not a format this build can read and must be "
            "reported, got verdict %d", ver, r);
}


// A version the reader does not know must also cost nothing: parsing it
// with the wrong reader would apply records from a layout that no
// longer means what it says.
void
cttest_fileread_applies_no_record_from_an_unrecognised_version(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "ver6.binlog");

    int ver = 6;
    Jobrec jr = fr_job(2, 5, Ready);
    int fd = fr_create(path);
    fr_put(fd, &ver, sizeof ver);
    fr_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    fr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    fr_replay(path, &w, &f, &list);

    assertf(job_find(2) == NULL,
            "no record from a v6 binlog may be applied by a v8/v7 reader");
}


void
cttest_fileread_reports_a_truncated_version_header(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "halfheader.binlog");

    int fd = fr_create(path);
    fr_put(fd, "\x08\x00", 2);       // 2 of the 4 header bytes
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = fr_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a header cut in half is a damaged file, not an empty one, "
            "got verdict %d", r);
}


// The complementary boundary: a file with no bytes at all carries no
// records and no damage, so its verdict is success. Pinned explicitly
// because readfull answers "0" to both questions.
void
cttest_fileread_reports_success_for_an_empty_binlog(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "empty.binlog");

    int fd = fr_create(path);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = fr_replay(path, &w, &f, &list);

    assertf(r == 0,
            "an empty binlog holds no records and no damage, got verdict %d",
            r);
}


// f->rbuf points at a buffer that lives in fileread's own stack frame.
// Leaving it published after the return arms every later warnpos with a
// dangling 64 KiB pointer.
void
cttest_fileread_clears_the_read_buffer_before_returning(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "rbufok.binlog");

    Jobrec jr = fr_job(3, 5, Ready);
    int fd = fr_create(path);
    int ver = Walver;
    fr_put(fd, &ver, sizeof ver);
    fr_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    fr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    fr_replay(path, &w, &f, &list);

    assertf(f.rbuf == NULL,
            "the recovery buffer lives on fileread's stack frame and must "
            "not outlive it");
}


// The same promise on the exit path that skips both readers entirely.
void
cttest_fileread_clears_the_read_buffer_after_an_unknown_version(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "rbufver.binlog");

    int ver = 0;
    int fd = fr_create(path);
    fr_put(fd, &ver, sizeof ver);
    fr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    fr_replay(path, &w, &f, &list);

    assertf(f.rbuf == NULL,
            "the unknown-version exit must clear the recovery buffer too");
}


// Contract (5): one bad record does not entitle the reader to call the
// whole replay clean.
void
cttest_fileread_reports_a_corrupt_record_in_its_verdict(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "onecorrupt.binlog");

    Jobrec good = fr_job(4, 5, Ready);
    Jobrec bad = fr_job(5, 5, Ready);
    int fd = fr_create(path);
    int ver = Walver;
    fr_put(fd, &ver, sizeof ver);
    fr_v8(fd, "default", 7, &good, "abc\r\n", 5);
    fr_v8(fd, "default", 7, &bad, "xyz\r\n", 5);
    fr_tail(fd);
    close(fd);

    int wfd = open(path, O_RDWR);
    assertf(wfd >= 0, "setup: reopen fixture: %s", strerror(errno));
    off_t second_body = 4 + (4 + 7 + 80 + 5 + 4) + 4 + 7 + 80;
    unsigned char flip = 'Q';
    assertf(pwrite(wfd, &flip, 1, second_body) == 1, "setup: pwrite flip");
    close(wfd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = fr_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a checksum-rejected record must reach the caller's verdict, "
            "got %d", r);
}


// Contract (4): fileread holds a ref for the duration of the replay so
// the File cannot be reaped underneath it. A binlog whose jobs were all
// deleted drives its own refcount to zero — the ordinary "compacted
// away" recovery shape — and the reader must still hand back the File
// it was given, registered and intact, before it touches it again.
void
cttest_fileread_must_not_reap_the_file_it_is_still_reading(void)
{
    fr_setup();
    char path[512];
    fr_path(path, sizeof path, "allgone.binlog");

    Jobrec full = fr_job(6, 5, Ready);
    Jobrec del = fr_job(6, 5, Invalid);
    int fd = fr_create(path);
    int ver = Walver;
    fr_put(fd, &ver, sizeof ver);
    fr_v8(fd, "default", 7, &full, "abc\r\n", 5);
    fr_v8(fd, "", 0, &del, NULL, 0);
    fr_tail(fd);
    close(fd);

    Wal w = {0};
    w.dir = ctdir();
    File *f = calloc(1, sizeof(File));
    assertf(f != NULL, "setup: calloc File");
    f->w = &w;
    f->path = strdup(path);
    assertf(f->path != NULL, "setup: strdup path");
    f->fd = open(path, O_RDONLY);
    assertf(f->fd >= 0, "setup: open fixture: %s", strerror(errno));

    Job list = {0};
    job_list_reset(&list);
    fileadd(f, &w);
    int fdcopy = f->fd;
    fileread(f, &list);
    close(fdcopy);

    assertf(w.nfile == 1,
            "fileread must still own the File it was handed when it "
            "returns: it was reaped mid-read and then written to");
}
