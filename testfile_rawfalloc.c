// Angry tests for rawfalloc (file.c) — the default disk-space
// allocator behind the `falloc` hook. Its promises are narrow: return 0
// only on success, and on success leave len bytes of readable zeroes.
// Everything the WAL writer computes afterwards (f->free, and with it
// every reservation) is built on that answer being honest.

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
rfa_setup(void)
{
    fault_clear_all();
    progname = "testfile_rawfalloc";
}

static int
rfa_file(char *path, size_t n, const char *name)
{
    int k = snprintf(path, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create file: %s", strerror(errno));
    return fd;
}

static off_t
rfa_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}

static int
rfa_all_zero(const unsigned char *p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (p[i]) return 0;
    return 1;
}


// `falloc` is a mutable file-scope hook. Any test that installs a double
// and returns early leaves every later test in the binary talking to
// that double instead of the real allocator, and the damage is silent.
// This pins the shipped default.
void
cttest_rawfalloc_is_the_default_allocation_hook(void)
{
    rfa_setup();

    assertf(falloc == rawfalloc,
            "the falloc hook must start out pointing at rawfalloc, or a "
            "leaked test double is deciding how binlogs get their space");
}


// "Returns 0 on success" is the whole contract filewopen leans on: a 0
// makes it publish f->free and start writing records. A length it
// cannot possibly have allocated must not come back as success.
void
cttest_rawfalloc_does_not_report_success_for_a_negative_length(void)
{
    rfa_setup();
    char path[512];
    int fd = rfa_file(path, sizeof path, "negfalloc.bin");

    int r = rawfalloc(fd, -1);

    assertf(r != 0,
            "a negative length cannot have been allocated, yet rawfalloc "
            "reported success (%d)", r);

    close(fd);
}


// 4097 straddles the 4 KiB chunking of the write fallback: the last
// chunk is a single byte, which is exactly where `len - i < sizeof buf`
// arithmetic goes wrong.
void
cttest_rawfalloc_allocates_exactly_the_requested_length(void)
{
    rfa_setup();
    char path[512];
    int fd = rfa_file(path, sizeof path, "exactlen.bin");

    int len = 4097;
    int r = rawfalloc(fd, len);
    assertf(r == 0, "setup: rawfalloc must succeed, got errno %d", r);

    assertf(rfa_size(fd) == (off_t)len,
            "the file must hold exactly the requested %d bytes, got %lld",
            len, (long long)rfa_size(fd));

    close(fd);
}


// The space is only useful because it reads back as zeroes: replay
// stops at an all-zero Jobrec, which is what makes the preallocated
// tail a tail at all.
void
cttest_rawfalloc_leaves_the_allocated_space_readable_as_zeroes(void)
{
    rfa_setup();
    char path[512];
    int fd = rfa_file(path, sizeof path, "zerofill.bin");

    int len = 4097;
    int r = rawfalloc(fd, len);
    assertf(r == 0, "setup: rawfalloc must succeed, got errno %d", r);

    unsigned char *buf = malloc((size_t)len);
    assertf(buf != NULL, "setup: malloc");
    memset(buf, 0xCD, (size_t)len);
    assertf(pread(fd, buf, (size_t)len, 0) == (ssize_t)len,
            "setup: pread the allocated space");
    int zeroed = rfa_all_zero(buf, (size_t)len);
    free(buf);

    assertf(zeroed,
            "every byte of the preallocated space must read back as zero, "
            "or replay cannot tell a tail from a record");

    close(fd);
}


// The native path and the write-zero fallback are supposed to be
// interchangeable, but only one of them preserves what a shorter
// existing file already held. filewopen calls this on a path that may
// already exist, so which behaviour ships is observable — pin it.
void
cttest_rawfalloc_preserves_the_bytes_of_a_shorter_existing_file(void)
{
    rfa_setup();
    char path[512];
    int fd = rfa_file(path, sizeof path, "preserve.bin");

    char head[10];
    memset(head, 'A', sizeof head);
    assertf(write(fd, head, sizeof head) == (ssize_t)sizeof head,
            "setup: seed the file");
    assertf(lseek(fd, 0, SEEK_SET) == 0, "setup: rewind to the documented "
            "starting offset");

    int r = rawfalloc(fd, 4096);
    assertf(r == 0, "setup: rawfalloc must succeed, got errno %d", r);

    char back[10];
    memset(back, 0, sizeof back);
    assertf(pread(fd, back, sizeof back, 0) == (ssize_t)sizeof back,
            "setup: pread the seeded bytes");

    assertf(memcmp(back, head, sizeof head) == 0,
            "allocating space must not rewrite the bytes an existing file "
            "already held");

    close(fd);
}


// The write-loop fallback. fallocate returns EOPNOTSUPP on NFS and
// tmpfs, and the loop that stands in for it writes the zeroes itself —
// which leaves the file offset at the END of the preallocation. Two
// promises hang on that: the file really is len bytes long, and the
// caller (filewopen) still stamps its version header at offset 0. The
// header check is the one that matters: a header written wherever the
// fallback happened to leave the offset makes every later replay read
// zeroes and stop.
void
cttest_rawfalloc_fallback_preallocates_and_leaves_a_readable_header(void)
{
    rfa_setup();
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.7301", ctdir());
    assertf(k > 0 && (size_t)k < sizeof path, "setup: path did not fit");
    unlink(path);

    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    w.dir = ctdir();
    w.filesize = 4096;
    f.w = &w;
    f.path = path;
    f.fd = -1;

    fault_clear_all();
    fault_set(FAULT_FALLOCATE, 0, EOPNOTSUPP);
    filewopen(&f);
    assertf(fault_hits(FAULT_FALLOCATE) == 1,
            "setup: the fallback must have been taken (hits=%d)",
            fault_hits(FAULT_FALLOCATE));
    fault_clear_all();

    assertf(f.iswopen, "the binlog must open on the fallback path too");

    struct stat st;
    assertf(stat(path, &st) == 0, "setup: stat the binlog");
    assertf(st.st_size == 4096,
            "the fallback must preallocate the whole file, got %lld bytes",
            (long long)st.st_size);

    int fd = open(path, O_RDONLY);
    assertf(fd >= 0, "setup: reopen the binlog");
    int ver = -1;
    assertf(pread(fd, &ver, sizeof ver, 0) == (ssize_t)sizeof ver,
            "setup: read the header");
    close(fd);
    assertf(ver == Walver,
            "the version header must sit at offset 0 even though the "
            "fallback left the offset at the end of the file, got %d", ver);

    if (f.fd >= 0) close(f.fd);
    unlink(path);
}
