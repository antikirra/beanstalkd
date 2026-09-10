// Angry tests for readrec (file.c) — the v8 WAL record reader.
//
// readrec is static, so every attack below drives it through its only
// public door, fileread(), over a hand-built binlog. Fixtures are
// generated at runtime (never committed) inside the runner's own
// per-test temp dir, and every record carries a CRC32C trailer the
// reader must accept, so the attack lands on the guard under test and
// not on the checksum.

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
rr_setup(void)
{
    fault_clear_all();
    progname = "testfile_readrec";
}

static void
rr_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
}

static void
rr_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

static int
rr_create(char *path, int ver)
{
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    rr_put(fd, &ver, sizeof ver);
    return fd;
}

// Appends one v8 record whose CRC32C trailer is correct for the exact
// bytes written, so a rejected record was rejected on its merits.
static void
rr_v8(int fd, const char *tube, int namelen, const void *jr,
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
    rr_put(fd, &namelen, sizeof namelen);
    if (namelen > 0) rr_put(fd, tube, (size_t)namelen);
    rr_put(fd, jr, sizeof(Jobrec));
    if (bodylen > 0) rr_put(fd, body, (size_t)bodylen);
    rr_put(fd, tr, sizeof tr);
}

// The fallocate-zeroed tail every healthy binlog ends with.
static void
rr_tail(int fd)
{
    char z[512];
    memset(z, 0, sizeof z);
    rr_put(fd, z, sizeof z);
}

static Jobrec
rr_job(uint64 id, int32 body_size, byte state)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 17;
    jr.delay = 0;
    jr.ttr = 120000000000LL;
    jr.body_size = body_size;
    jr.created_at = 1;
    jr.state = state;
    return jr;
}

// Runs one binlog through fileread and returns its verdict. w->cur is
// pinned to f so walgc cannot reap a File that lives on the stack.
static int
rr_replay(char *path, Wal *w, File *f, Job *list)
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


// A legacy truncate marker carries namelen > 0 and, on the oldest
// writers, a zero-length body. Contract (10) says such a marker is
// consumed and replay continues. readfull(...,0,...) returns 0 because
// its loop never runs, which readrec cannot tell apart from EOF — so
// the marker silently ends replay and every record behind it is lost.
void
cttest_readrec_marker_with_an_empty_body_must_not_end_replay(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "marker0.binlog");

    Jobrec marker = rr_job(41, 0, Invalid);
    Jobrec live = rr_job(42, 5, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &marker, NULL, 0);
    rr_v8(fd, "default", 7, &live, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(42) != NULL,
            "a legacy truncate marker with a zero-length body must be "
            "consumed, not mistaken for end-of-file: job 42 was written "
            "after it and did not come back");
}


// MAX_TUBE_NAME_LEN-1 is a tube name the writer is willing to emit, so
// the reader has to accept it. Both sides of the bound get their own
// test; a bound checked from one side only is half-checked.
void
cttest_readrec_accepts_a_tube_name_of_the_maximum_length(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "name200.binlog");

    int maxlen = MAX_TUBE_NAME_LEN - 1;
    char name[MAX_TUBE_NAME_LEN];
    memset(name, 'q', (size_t)maxlen);
    name[maxlen] = '\0';

    Jobrec jr = rr_job(200, 4, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, name, maxlen, &jr, "ab\r\n", 4);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(200) != NULL,
            "a tube name of exactly %d bytes is legal and its record must "
            "replay", maxlen);
}


void
cttest_readrec_rejects_a_namelen_one_past_the_maximum(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "name201.binlog");

    int overlen = MAX_TUBE_NAME_LEN;
    char name[MAX_TUBE_NAME_LEN + 1];
    memset(name, 'q', (size_t)overlen);

    Jobrec jr = rr_job(201, 4, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, name, overlen, &jr, "ab\r\n", 4);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = rr_replay(path, &w, &f, &list);

    assertf(r == 1,
            "namelen %d overruns the reader's %d-byte tube buffer and must "
            "be reported as an error, got verdict %d",
            overlen, MAX_TUBE_NAME_LEN, r);
}


// A zeroed Jobrec in the middle of a file is a torn write, not the
// fallocate tail. Contract (7) stops replay there; this pins the
// consequence a caller depends on — the records behind it are gone and
// must not be half-applied.
void
cttest_readrec_stops_before_the_records_that_follow_a_zeroed_jobrec(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "zeromid.binlog");

    Jobrec first = rr_job(51, 5, Ready);
    Jobrec zero;
    memset(&zero, 0, sizeof zero);
    Jobrec behind = rr_job(52, 5, Ready);

    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &first, "abc\r\n", 5);
    int nl0 = 0;
    rr_put(fd, &nl0, sizeof nl0);
    rr_put(fd, &zero, sizeof zero);
    rr_v8(fd, "default", 7, &behind, "xyz\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(52) == NULL,
            "replay must stop at a zeroed job record; job 52 lives behind "
            "it and must not be resurrected");
}


// Contract (4): corrupted data never overwrites valid in-memory state.
// The existing flip tests only ever show a single-record file, so they
// cannot see a bad record destroying a job an earlier good record
// already recovered.
void
cttest_readrec_a_corrupt_record_must_not_overwrite_a_recovered_job(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "corrupt2nd.binlog");

    Jobrec good = rr_job(61, 5, Ready);
    Jobrec bad = rr_job(61, 5, Buried);

    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &good, "abc\r\n", 5);
    // Second record for the same job, written with a trailer taken over
    // a different body — a bit-rotted re-statement of job 61.
    rr_v8(fd, "default", 7, &bad, "zzz\r\n", 5);
    rr_tail(fd);
    close(fd);

    // Corrupt one body byte of the SECOND record only; its trailer no
    // longer matches, the first record's is untouched.
    int wfd = open(path, O_RDWR);
    assertf(wfd >= 0, "setup: reopen fixture: %s", strerror(errno));
    off_t second_body = 4 + (4 + 7 + 80 + 5 + 4) + 4 + 7 + 80;
    unsigned char flip = 'Z';
    assertf(pwrite(wfd, &flip, 1, second_body) == 1, "setup: pwrite flip");
    close(wfd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    Job *j = job_find(61);
    assertf(j && memcmp(j->body, "abc\r\n", 5) == 0,
            "a CRC-rejected record must leave the previously recovered "
            "job 61 and its body untouched");
}


// Jobrec.state is a byte and Copy (5) is a legal in-memory state, so
// values outside the replayable set reach the `default:` arm from any
// bit-rotted or forward-version file. Rejecting the record is contract
// (13); taking the already-recovered job down with it is not.
void
cttest_readrec_an_unknown_state_must_not_destroy_the_recovered_job(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "unknownstate.binlog");

    Jobrec good = rr_job(71, 5, Ready);
    Jobrec odd = rr_job(71, 5, (byte)Copy);

    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &good, "abc\r\n", 5);
    rr_v8(fd, "default", 7, &odd, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(71) != NULL,
            "a record with an unreplayable state must be rejected without "
            "destroying job 71, which an earlier valid record recovered");
}


void
cttest_readrec_accepts_a_body_of_exactly_the_configured_limit(void)
{
    rr_setup();
    job_data_size_limit = 64;
    char path[512];
    rr_path(path, sizeof path, "bodyat.binlog");

    char body[64];
    memset(body, 'b', sizeof body);
    body[62] = '\r';
    body[63] = '\n';

    Jobrec jr = rr_job(81, 64, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &jr, body, 64);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(81) != NULL,
            "a body of exactly job_data_size_limit (%zu) is legal and must "
            "replay", job_data_size_limit);
}


void
cttest_readrec_rejects_a_body_one_byte_past_the_configured_limit(void)
{
    rr_setup();
    job_data_size_limit = 64;
    char path[512];
    rr_path(path, sizeof path, "bodyover.binlog");

    char body[65];
    memset(body, 'b', sizeof body);
    body[63] = '\r';
    body[64] = '\n';

    Jobrec jr = rr_job(82, 65, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &jr, body, 65);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int r = rr_replay(path, &w, &f, &list);

    assertf(r == 1,
            "a body one byte past job_data_size_limit (%zu) must be "
            "reported as an error, got verdict %d", job_data_size_limit, r);
}


// Contract (11): only full records contribute to alive, and by exactly
// the bytes they occupy on disk — 4 + namelen + sizeof(Jobrec) +
// body_size + 4. A checksum can never see an accounting slip.
void
cttest_readrec_charges_alive_the_exact_on_disk_size_of_every_full_record(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "alive.binlog");

    Jobrec a = rr_job(91, 2, Ready);
    Jobrec b = rr_job(92, 5, Ready);
    Jobrec c = rr_job(93, 9, Ready);
    Jobrec upd = rr_job(91, 2, Buried);

    int fd = rr_create(path, Walver);
    rr_v8(fd, "a", 1, &a, "\r\n", 2);
    rr_v8(fd, "bb", 2, &b, "cd\r\n", 5);
    rr_v8(fd, "ccc", 3, &c, "efghijk\r\n", 9);
    rr_v8(fd, "", 0, &upd, NULL, 0);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    int64 want = (4 + 1 + (int)sizeof(Jobrec) + 2 + 4)
               + (4 + 2 + (int)sizeof(Jobrec) + 5 + 4)
               + (4 + 3 + (int)sizeof(Jobrec) + 9 + 4);
    assertf(w.alive == want,
            "alive must be the on-disk byte count of the three full "
            "records and nothing else: expected %"PRId64", got %"PRId64,
            want, w.alive);
}


// An allocator failure while reading a body is a failure, not the end
// of the file: reporting it as a clean stop drops the whole tail of the
// binlog and tells the operator everything was fine.
void
cttest_readrec_reports_an_error_when_the_body_buffer_cannot_be_allocated(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "bodyoom.binlog");

    Jobrec jr = rr_job(101, 5, Ready);
    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    rr_tail(fd);
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

    fault_set(FAULT_MALLOC, 0, ENOMEM);
    int r = fileread(&f, &list);
    close(f.fd);

    assertf(r == 1,
            "an out-of-memory body buffer must be reported as a replay "
            "error, not as end of file, got verdict %d", r);
}


// Contract (8): a job that was reserved when the server died has no
// owner after restart, so it comes back ready.
void
cttest_readrec_replays_a_reserved_record_as_ready(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "reserved.binlog");

    Jobrec jr = rr_job(111, 5, Reserved);
    int fd = rr_create(path, Walver);
    rr_v8(fd, "default", 7, &jr, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    Job *j = job_find(111);
    assertf(j && j->r.state == Ready,
            "a reserved record must replay as ready, got state %d",
            j ? (int)j->r.state : -1);
}


// Contract (9): a short record whose full record lived in a binlog that
// has since been compacted away is skipped and counted as consumed, so
// the records behind it still replay.
void
cttest_readrec_consumes_a_short_record_for_an_unknown_job(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "orphanshort.binlog");

    Jobrec orphan = rr_job(9999, 5, Buried);
    Jobrec live = rr_job(121, 5, Ready);

    int fd = rr_create(path, Walver);
    rr_v8(fd, "", 0, &orphan, NULL, 0);
    rr_v8(fd, "default", 7, &live, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    rr_replay(path, &w, &f, &list);

    assertf(job_find(121) != NULL,
            "a short record for an unknown job must be consumed so replay "
            "reaches job 121 behind it");
}




// A tube name is not just a key: every stats and list reply prints it
// into a YAML document. On the wire the parser only ever accepts
// [A-Za-z0-9-+/;.$_()], so a name can never carry a separator — but a
// name replayed from a binlog used to be taken as-is. A hand-made or
// corrupt file could therefore introduce a tube whose name contains
// CRLF, and `list-tubes` would emit a body the client reads as extra
// entries (upstream #669, reached through the WAL instead of the wire).
void
cttest_readrec_a_tube_name_with_crlf_must_not_reach_the_tube_set(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "crlftube.binlog");

    static const char evil[] = "ok\r\nname: injected";
    int evil_len = (int)sizeof evil - 1;
    Jobrec jr = rr_job(4242, 5, Ready);

    int fd = rr_create(path, Walver);
    rr_v8(fd, evil, evil_len, &jr, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int err = rr_replay(path, &w, &f, &list);

    assertf(err == 1,
            "a record naming an unusable tube is detected corruption and "
            "must be reported, got verdict %d", err);
    assertf(job_find(4242) == NULL,
            "the job must not be recovered onto a tube whose name cannot "
            "be printed");
    assertf(tube_find_name(evil, (size_t)evil_len) == NULL,
            "no tube may exist under a name the wire parser would refuse");
}


// Same bound as the v7 reader (see testfile_readrec7.c): the name
// buffer is MAX_TUBE_NAME_LEN bytes and the reader terminates at
// tubename[namelen], so the largest legal namelen is one less. The
// length comes off disk as a plain int, which is where a corrupt file
// puts the value that writes one past the end.
void
cttest_readrec_a_namelen_at_the_buffer_size_is_refused(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "namelenmax.binlog");

    static char longname[MAX_TUBE_NAME_LEN + 8];
    memset(longname, 'n', sizeof longname);
    Jobrec jr = rr_job(9101, 5, Ready);

    int fd = rr_create(path, Walver);
    rr_v8(fd, longname, MAX_TUBE_NAME_LEN, &jr, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int err = rr_replay(path, &w, &f, &list);

    assertf(err == 1,
            "a namelen of exactly the buffer size must be refused, got "
            "verdict %d", err);
    assertf(job_find(9101) == NULL,
            "no job may be recovered from a record whose name does not fit");
}


// ...and the longest name that does fit still replays.
void
cttest_readrec_the_longest_legal_name_still_replays(void)
{
    rr_setup();
    char path[512];
    rr_path(path, sizeof path, "namelenok.binlog");

    static char longname[MAX_TUBE_NAME_LEN];
    memset(longname, 'n', sizeof longname);
    Jobrec jr = rr_job(9102, 5, Ready);

    int fd = rr_create(path, Walver);
    rr_v8(fd, longname, MAX_TUBE_NAME_LEN - 1, &jr, "abc\r\n", 5);
    rr_tail(fd);
    close(fd);

    Wal w = {0};
    File f = {0};
    Job list = {0};
    int err = rr_replay(path, &w, &f, &list);

    assertf(err == 0,
            "a name of MAX_TUBE_NAME_LEN-1 is legal and must replay "
            "cleanly, got verdict %d", err);
    assertf(job_find(9102) != NULL,
            "the job on the longest legal tube name must come back");
}
