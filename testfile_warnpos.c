// Angry tests for warnpos (file.c).
//
// Everything warnpos prints is incidental — the text, the offset, the
// stream, the newline. The one promise a caller depends on is that a
// diagnostic is observationally NEUTRAL for the reader: it must not
// move the file offset (it probes with SEEK_CUR/0) and must not touch
// the read buffer. A binlog containing a legacy truncate marker warns
// and then keeps going, which is the only shape where a warnpos call
// sits between two records and its side effects become visible.

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
wp_setup(void)
{
    fault_clear_all();
    progname = "testfile_warnpos";
}

static void
wp_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
wp_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
wp_create(char *path)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    int ver = Walver;
    wp_put(fd, &ver, sizeof ver);
    return fd;
}

static void
wp_v8(int fd, const char *tube, int namelen, const void *jr,
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
    wp_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) wp_put(fd, tube, (size_t)namelen);
    wp_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) wp_put(fd, body, (size_t)bodylen);
    wp_put(fd, tr, sizeof tr);
}

static void
wp_tail(int fd)
{
    char z[512];
    memset(z, 0, sizeof z);
    wp_put(fd, z, sizeof z);
}

static Jobrec
wp_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 3;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 6;
    jr.state = state;
    return jr;
}

static int
wp_replay(char *path, Wal *w, File *f, Job *list)
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


// A legacy truncate marker is consumed, warned about, and replay
// continues — the only record shape that leaves a warnpos call sitting
// between two records. If the diagnostic moved the descriptor or ate
// buffered bytes, the record behind it would never parse.
void
cttest_warnpos_leaves_the_reader_aligned_for_the_next_record(void)
{
    wp_setup();
    char path[512];
    wp_path(path, sizeof path, "markerthenjob.binlog");

    Jobrec marker = wp_job(900, 2, Invalid);
    Jobrec live = wp_job(41, 5, Ready);

    int fd = wp_create(path);
    wp_v8(fd, "default", 7, &marker, "\r\n", 2);
    wp_v8(fd, "default", 7, &live, "abc\r\n", 5);
    wp_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    wp_replay(path, &w, &f, &list);

    assertf(job_find(41) != NULL,
            "a diagnostic must not disturb the reader: the record written "
            "after the warned-about marker did not replay");
}


// The next rung of the same ladder. "The job came back" survives a
// diagnostic that shifted the position by a whole record and happened
// to resynchronise; the byte count does not.
void
cttest_warnpos_leaves_the_byte_accounting_of_the_next_record_exact(void)
{
    wp_setup();
    char path[512];
    wp_path(path, sizeof path, "markeraccounting.binlog");

    Jobrec marker = wp_job(901, 2, Invalid);
    Jobrec live = wp_job(42, 9, Ready);

    int fd = wp_create(path);
    wp_v8(fd, "default", 7, &marker, "\r\n", 2);
    wp_v8(fd, "tubetube", 8, &live, "efghijk\r\n", 9);
    wp_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    wp_replay(path, &w, &f, &list);

    int64 want = 4 + 8 + (int)sizeof(Jobrec) + 9 + 4;
    assertf(w.alive == want,
            "the record after a warned-about marker must be charged its "
            "exact on-disk size: expected %"PRId64", got %"PRId64,
            want, w.alive);
}
