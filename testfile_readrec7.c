// Angry tests for readrec7 (file.c) — the v7 (pre-checksum) WAL reader
// kept alive so a legacy binlog is still recoverable during migration.
//
// v7 carries no CRC trailer, so every structural guard inside readrec7
// is load-bearing: a single flipped bit walks straight into it. Each
// fixture is a hand-built v7 binlog; readrec7 is static, so the attacks
// go through fileread(), whose version switch dispatches on the header.

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

enum { R7Ver = 7 };

static void
r7_setup(void)
{
    fault_clear_all();
    progname = "testfile_readrec7";
}

static void
r7_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
r7_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
r7_create(char *path, int ver)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    r7_put(fd, &ver, sizeof ver);
    return fd;
}

// A v7 record: no trailer, everything else identical to v8.
static void
r7_rec(int fd, const char *tube, int namelen, const void *jr,
       const void *body, int bodylen)
{
    r7_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) r7_put(fd, tube, (size_t)namelen);
    r7_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) r7_put(fd, body, (size_t)bodylen);
}

// A v8 record, used only by the cross-reader agreement test.
static void
r7_rec8(int fd, const char *tube, int namelen, const void *jr,
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
    r7_rec(fd, tube, namelen, jr, body, bodylen);
    r7_put(fd, tr, sizeof tr);
}

static void
r7_tail(int fd)
{
    char z[512];
    memset(z, 0, sizeof z);
    r7_put(fd, z, sizeof z);
}

static Jobrec
r7_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 23;
    jr.delay = 0;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 7;
    jr.state = state;
    return jr;
}

static int
r7_replay(char *path, Wal *w, File *f, Job *list)
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


// Rejecting the corrupt short record is already pinned elsewhere; what
// nothing watches is whether the rejection is REPORTED. A v7 binlog
// that loses a job silently and then tells the operator "no errors" is
// the failure mode the verdict exists to prevent.
void
cttest_readrec7_reports_an_error_for_a_corrupt_short_record(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7shortcorrupt.binlog");

    Jobrec full = r7_job(1, 5, Ready);
    Jobrec upd = r7_job(1, 60000, Buried);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &full, "abc\r\n", 5);
    r7_rec(fd, "", 0, &upd, NULL, 0);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = r7_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a short record whose body_size contradicts the live job is "
            "corruption and must be reported, got verdict %d", r);
}


// #714: the marker body has to be consumed so the file position stays
// aligned. The oldest writers emitted a zero-length one; that is the
// exact shape that silently ends replay in the v8 reader.
void
cttest_readrec7_consumes_a_legacy_marker_with_an_empty_body(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7marker0.binlog");

    Jobrec marker = r7_job(41, 0, Invalid);
    Jobrec live = r7_job(42, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &marker, NULL, 0);
    r7_rec(fd, "default", 7, &live, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    assertf(job_find(42) != NULL,
            "a zero-body legacy marker must be consumed so job 42 behind "
            "it still replays");
}


void
cttest_readrec7_consumes_a_legacy_marker_with_the_largest_allowed_body(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7marker64.binlog");

    char mbody[64];
    memset(mbody, 'm', sizeof mbody);
    Jobrec marker = r7_job(43, 64, Invalid);
    Jobrec live = r7_job(44, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &marker, mbody, 64);
    r7_rec(fd, "default", 7, &live, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    assertf(job_find(44) != NULL,
            "a marker body of exactly the 64-byte bound must be consumed "
            "so job 44 behind it still replays");
}


void
cttest_readrec7_rejects_a_legacy_marker_one_byte_past_the_bound(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7marker65.binlog");

    char mbody[65];
    memset(mbody, 'm', sizeof mbody);
    Jobrec marker = r7_job(45, 65, Invalid);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &marker, mbody, 65);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = r7_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a marker body one byte past the bound must be reported as an "
            "error, got verdict %d", r);
}


// job_find(jr.id) is resolved BEFORE the marker is validated, and both
// marker failure paths jump to the shared Error label. A truncate
// cutoff that merely happens to equal a live job's id then costs that
// job its life — for a record that never claimed to be about it.
void
cttest_readrec7_a_malformed_marker_must_not_destroy_a_live_job(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7markercollide.binlog");

    Jobrec live = r7_job(5, 5, Ready);
    char mbody[65];
    memset(mbody, 'm', sizeof mbody);
    Jobrec marker = r7_job(5, 65, Invalid);   // cutoff collides with job 5

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &live, "abc\r\n", 5);
    r7_rec(fd, "default", 7, &marker, mbody, 65);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    assertf(job_find(5) != NULL,
            "a malformed truncate marker whose cutoff equals a live job id "
            "must not destroy job 5, which its own full record recovered");
}


void
cttest_readrec7_an_unknown_state_must_not_destroy_the_recovered_job(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7unknownstate.binlog");

    Jobrec good = r7_job(71, 5, Ready);
    Jobrec odd = r7_job(71, 5, (byte)Copy);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &good, "abc\r\n", 5);
    r7_rec(fd, "default", 7, &odd, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    assertf(job_find(71) != NULL,
            "a record with an unreplayable state must be rejected without "
            "destroying job 71, which an earlier valid record recovered");
}


// A binlog cut off inside a Jobrec is a partial record, not a tidy end
// of file. Contract (1) separates the two, and only the reported
// verdict tells an operator which one happened.
void
cttest_readrec7_reports_a_record_truncated_at_end_of_file(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7truncated.binlog");

    Jobrec full = r7_job(81, 5, Ready);
    Jobrec half = r7_job(82, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &full, "abc\r\n", 5);
    int nl = 7;
    r7_put(fd, &nl, sizeof nl);
    r7_put(fd, "default", 7);
    r7_put(fd, &half, 40);          // half a Jobrec, then EOF
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = r7_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a record truncated mid-Jobrec must be reported as an error, "
            "not as a clean end of replay, got verdict %d", r);
}


void
cttest_readrec7_stops_before_the_records_that_follow_a_zeroed_jobrec(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7zeromid.binlog");

    Jobrec first = r7_job(91, 5, Ready);
    Jobrec zero;
    memset(&zero, 0, sizeof zero);
    Jobrec behind = r7_job(92, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &first, "abc\r\n", 5);
    int nl0 = 0;
    r7_put(fd, &nl0, sizeof nl0);
    r7_put(fd, &zero, sizeof zero);
    r7_rec(fd, "default", 7, &behind, "xyz\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    assertf(job_find(92) == NULL,
            "replay must stop at a zeroed job record; job 92 lives behind "
            "it and must not be resurrected");
}


// Contract (5): a v7 full record contributes exactly the bytes it
// occupies — and a v7 record has no trailer, so it is four bytes
// shorter than the v8 record for the same job.
void
cttest_readrec7_charges_alive_the_exact_on_disk_size_of_a_full_record(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7alive.binlog");

    Jobrec jr = r7_job(101, 5, Ready);
    int fd = r7_create(path, R7Ver);
    r7_rec(fd, "default", 7, &jr, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    r7_replay(path, &w, &f, &list);

    int64 want = 4 + 7 + (int)sizeof(Jobrec) + 5;
    assertf(w.alive == want,
            "a v7 full record carries no trailer: alive must be %"PRId64
            ", got %"PRId64, want, w.alive);
}


// Migration equivalence is the whole reason readrec7 still exists: the
// same logical job written in either format must land in memory as the
// same job. A divergence is invisible to either reader's own tests.
void
cttest_readrec7_replays_a_record_identically_to_the_v8_reader(void)
{
    r7_setup();
    char p7[512], p8[512];
    r7_path(p7, sizeof p7, "cross_v7.binlog");
    r7_path(p8, sizeof p8, "cross_v8.binlog");

    Jobrec a = r7_job(111, 5, Reserved);
    Jobrec b = r7_job(112, 5, Reserved);

    int fd = r7_create(p7, R7Ver);
    r7_rec(fd, "crosstube", 9, &a, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    fd = r7_create(p8, Walver);
    r7_rec8(fd, "crosstube", 9, &b, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w7 = {0};
    File f7 = {0};
    Job l7 = {0};
    r7_replay(p7, &w7, &f7, &l7);

    Wal w8 = {0};
    File f8 = {0};
    Job l8 = {0};
    r7_replay(p8, &w8, &f8, &l8);

    Job *j7 = job_find(111);
    Job *j8 = job_find(112);
    assertf(j7 && j8, "setup: both readers must recover their job");

    Jobrec c7 = j7->r, c8 = j8->r;
    c7.id = 0;
    c8.id = 0;
    assertf(memcmp(&c7, &c8, sizeof c7) == 0,
            "the v7 and v8 readers must land the same logical job in the "
            "same in-memory state");
}


// The name buffer is MAX_TUBE_NAME_LEN bytes and the reader writes a
// terminator at tubename[namelen], so namelen may run to
// MAX_TUBE_NAME_LEN-1 and no further. The record carries that length
// as a plain int off disk, which is exactly where a corrupt file puts
// the value that walks one past the end.
void
cttest_readrec7_a_namelen_at_the_buffer_size_is_refused(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7namelenmax.binlog");

    static char longname[MAX_TUBE_NAME_LEN + 8];
    memset(longname, 'n', sizeof longname);
    Jobrec jr = r7_job(9001, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, longname, MAX_TUBE_NAME_LEN, &jr, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int err = r7_replay(path, &w, &f, &list);

    assertf(err == 1,
            "a namelen of exactly the buffer size must be refused, got "
            "verdict %d", err);
    assertf(job_find(9001) == NULL,
            "no job may be recovered from a record whose name does not fit");
}


// ...and the largest name that DOES fit still replays, so the bound is
// pinned from both sides.
void
cttest_readrec7_the_longest_legal_name_still_replays(void)
{
    r7_setup();
    char path[512];
    r7_path(path, sizeof path, "v7namelenok.binlog");

    static char longname[MAX_TUBE_NAME_LEN];
    memset(longname, 'n', sizeof longname);
    Jobrec jr = r7_job(9002, 5, Ready);

    int fd = r7_create(path, R7Ver);
    r7_rec(fd, longname, MAX_TUBE_NAME_LEN - 1, &jr, "abc\r\n", 5);
    r7_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int err = r7_replay(path, &w, &f, &list);

    assertf(err == 0,
            "a name of MAX_TUBE_NAME_LEN-1 is legal and must replay "
            "cleanly, got verdict %d", err);
    assertf(job_find(9002) != NULL,
            "the job on the longest legal tube name must come back");
}
