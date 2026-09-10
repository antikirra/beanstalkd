// Angry tests for crc32c_trailer_le (file.c) — the single encoder of
// the v8 on-disk trailer: final XOR, then four bytes little-endian.
//
// The trailer format is a compatibility promise, not an implementation
// detail: a build that changes both the encoder and readrec's decoder
// together stays perfectly self-consistent and silently rejects every
// binlog written before the upgrade. The only oracle that can see that
// is a checksum computed here, from the Castagnoli definition, with no
// help from crc32c.c.

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
tl_setup(void)
{
    fault_clear_all();
    progname = "testfile_crc32c_trailer_le";
}

// Bitwise CRC32C (Castagnoli): reflected polynomial 0x82F63B78, init
// and final XOR 0xFFFFFFFF. Deliberately naive and independent of the
// table/SSE4.2/ACLE implementations under test.
static uint32
tl_crc32c(const unsigned char *p, size_t n)
{
    uint32 c = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; i++) {
        c ^= (uint32)p[i];
        for (int k = 0; k < 8; k++)
            c = (c >> 1) ^ ((c & 1u) ? 0x82F63B78u : 0u);
    }
    return c ^ 0xFFFFFFFFu;
}

static int
tl_binlog(char *path, size_t n, const char *name)
{
    int k = snprintf(path, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create binlog: %s", strerror(errno));
    assertf(fd > 2, "setup: the wrapped syscalls skip fds 0-2");
    int ver = Walver;
    assertf(write(fd, &ver, sizeof ver) == (ssize_t)sizeof ver,
            "setup: version header");
    return fd;
}

static void
tl_wal(Wal *w, File *f, int fd)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 1 << 20;
    w->use = 1;
    w->resv = 1 << 20;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->free = (1 << 20) - 4;
    f->resv = 1 << 20;
    f->refs = 1;
    w->head = f;
    w->tail = f;
    w->cur = f;
}

static Job *
tl_job(Tube *t, uint64 id, int body_size, const void *body)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 81;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    memcpy(j->body, body, (size_t)body_size);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
tl_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}


// The oracle has to be right before it can judge anything. RFC 3720
// Appendix B.4's standard vector pins the polynomial, the reflection
// and both XOR constants of the implementation above.
void
cttest_crc32c_trailer_le_oracle_matches_the_standard_vector(void)
{
    tl_setup();

    uint32 got = tl_crc32c((const unsigned char *)"123456789", 9);

    assertf(got == 0xE3069283u,
            "the test's own CRC32C must be the standard one before it may "
            "judge the writer: expected 0xE3069283, got 0x%08x", got);
}


// The four bytes a real record ends with, against a checksum this file
// computed for itself. A byte-order flip, a missing final XOR, or a
// polynomial swap adopted on BOTH sides of the product at once — the
// mutation that quietly orphans every binlog written before an upgrade
// — has nowhere left to hide.
void
cttest_crc32c_trailer_le_matches_an_independent_checksum_of_the_record(void)
{
    tl_setup();
    char path[512];
    int fd = tl_binlog(path, sizeof path, "trailer.binlog");

    Wal w;
    File f;
    tl_wal(&w, &f, fd);
    Tube *t = make_tube("trailertube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = tl_job(t, 1201, 6, "abcd\r\n");

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");

    int nl = (int)t->name_len;
    unsigned char covered[4 + 32 + sizeof(Jobrec) + 6];
    int p = 0;
    memcpy(covered + p, &nl, sizeof nl);        p += (int)sizeof nl;
    memcpy(covered + p, t->name, (size_t)nl);   p += nl;
    memcpy(covered + p, &j->r, sizeof j->r);    p += (int)sizeof j->r;
    memcpy(covered + p, j->body, 6);            p += 6;

    uint32 c = tl_crc32c(covered, (size_t)p);
    unsigned char want[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };

    unsigned char got[4];
    assertf(pread(fd, got, 4, tl_size(fd) - 4) == 4, "setup: pread trailer");

    assertf(memcmp(got, want, 4) == 0,
            "the on-disk trailer must be the final-XORed CRC32C of the "
            "record, least significant byte first: wrote %02x%02x%02x%02x, "
            "expected %02x%02x%02x%02x",
            got[0], got[1], got[2], got[3],
            want[0], want[1], want[2], want[3]);

    close(fd);
}


// Encoder and decoder must agree on the byte order for the SAME reason
// they must agree on the polynomial. Reversing the trailer on disk is
// the one edit that a decoder reading big-endian would happily accept.
void
cttest_crc32c_trailer_le_a_byte_reversed_trailer_is_rejected_by_the_reader(void)
{
    tl_setup();
    char path[512];
    int fd = tl_binlog(path, sizeof path, "revtrailer.binlog");

    Wal w;
    File f;
    tl_wal(&w, &f, fd);
    Tube *t = make_tube("default");
    assertf(t != NULL, "setup: make_tube");
    Job *j = tl_job(t, 1202, 6, "abcd\r\n");

    assertf(filewrjobfull(&f, j) == 1, "setup: staging must succeed");

    off_t at = tl_size(fd) - 4;
    unsigned char tr[4], rev[4];
    assertf(pread(fd, tr, 4, at) == 4, "setup: pread trailer");
    rev[0] = tr[3]; rev[1] = tr[2]; rev[2] = tr[1]; rev[3] = tr[0];
    assertf(memcmp(tr, rev, 4) != 0,
            "setup: this record's trailer must not be a palindrome");
    assertf(pwrite(fd, rev, 4, at) == 4, "setup: pwrite reversed trailer");
    close(fd);

    Wal rw = {0};
    File rf = {0};
    Job list = {0};
    rf.w = &rw;
    rf.path = path;
    rf.fd = open(path, O_RDONLY);
    assertf(rf.fd >= 0, "setup: reopen binlog: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&rf, &rw);
    rw.cur = &rf;
    int verdict = fileread(&rf, &list);
    close(rf.fd);

    assertf(verdict == 1,
            "a trailer written in the opposite byte order must not verify: "
            "the reader accepted it (verdict %d)", verdict);
}
