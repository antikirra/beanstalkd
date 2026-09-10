// Angry tests for filewrjobfull (file.c) — the writer of the v8 full
// record. The on-disk layout it emits is a cross-version compatibility
// promise, not an implementation detail: readrec of this build and of
// every future build has to decode exactly these bytes in exactly this
// order.

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
wf_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewrjobfull";
}

static int
wf_binlog(char *path, size_t n, const char *name)
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
wf_wal(Wal *w, File *f, int fd)
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
wf_job(Tube *t, uint64 id, int body_size, const void *body)
{
    Job *j = allocate_job(body_size);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 13;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body_size;
    j->r.state = Ready;
    memcpy(j->body, body, (size_t)body_size);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}

static off_t
wf_size(int fd)
{
    struct stat st;
    assertf(fstat(fd, &st) == 0, "setup: fstat");
    return st.st_size;
}

// Replays the binlog at `path` with a fresh File, after the staged job
// has been unlinked and freed so the reader must rebuild it from the
// bytes alone.
static void
wf_replay(char *path, Wal *w, File *f, Job *list)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    f->w = w;
    f->path = path;
    f->fd = open(path, O_RDONLY);
    assertf(f->fd >= 0, "setup: reopen binlog: %s", strerror(errno));
    job_list_reset(list);
    fileadd(f, w);
    w->cur = f;
    fileread(f, list);
    close(f->fd);
    f->fd = -1;
}


// The whole record, byte for byte, against a buffer this test lays out
// from the documented format. Field order, field widths and the trailer
// all fail here together, and a reordering that keeps the checksum
// self-consistent is caught because the field bytes move too.
void
cttest_filewrjobfull_writes_the_documented_record_layout(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "layout.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    Tube *t = make_tube("layouttube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wf_job(t, 501, 6, "abcd\r\n");

    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    int nl = (int)t->name_len;
    unsigned char want[4 + 32 + sizeof(Jobrec) + 6 + 4];
    int p = 0;
    memcpy(want + p, &nl, sizeof nl);            p += (int)sizeof nl;
    memcpy(want + p, t->name, (size_t)nl);       p += nl;
    memcpy(want + p, &j->r, sizeof j->r);        p += (int)sizeof j->r;
    memcpy(want + p, j->body, 6);                p += 6;

    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, want, (size_t)p);
    c ^= WAL_CRC32C_XOR;
    want[p++] = (unsigned char)(c);
    want[p++] = (unsigned char)(c >> 8);
    want[p++] = (unsigned char)(c >> 16);
    want[p++] = (unsigned char)(c >> 24);

    unsigned char got[sizeof want];
    assertf(pread(fd, got, (size_t)p, 4) == (ssize_t)p, "setup: pread record");

    assertf(memcmp(got, want, (size_t)p) == 0,
            "the staged record must be [namelen][tube][Jobrec][body][crc32c "
            "LE] byte for byte — that layout is what every reader decodes");

    close(fd);
}


// Contract (3): a staged full record makes f the job's home, and f
// holds a reference for as long as the job lives there.
void
cttest_filewrjobfull_registers_the_job_on_the_file_it_wrote(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "register.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    Tube *t = make_tube("regtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wf_job(t, 502, 4, "ab\r\n");

    filewrjobfull(&f, j);

    assertf(j->file == &f,
            "a staged full record must leave the job living in the file "
            "that holds its bytes");

    close(fd);
}


// Contract (5): a failed stage registers nothing. The reference count
// is the half that a resource leak breaks — a job that was never
// written still pinning the binlog against compaction.
void
cttest_filewrjobfull_takes_no_reference_when_the_write_fails(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "failref.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    Tube *t = make_tube("failreftube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wf_job(t, 503, 4, "ab\r\n");

    uint before = f.refs;
    fault_set(FAULT_WRITEV, 0, EIO);
    filewrjobfull(&f, j);

    assertf(f.refs == before,
            "a record that was never written must not pin the binlog: "
            "refs was %u, now %u", before, f.refs);

    close(fd);
}


// The writer is willing to emit a tube name of MAX_TUBE_NAME_LEN-1
// bytes; the reader rejects namelen >= MAX_TUBE_NAME_LEN. The two
// bounds must meet exactly, or a perfectly legal tube name becomes
// unrecoverable the first time the server restarts.
void
cttest_filewrjobfull_a_maximum_length_tube_name_survives_replay(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "maxname.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    size_t maxlen = MAX_TUBE_NAME_LEN - 1;
    char name[MAX_TUBE_NAME_LEN];
    memset(name, 'w', maxlen);
    name[maxlen] = '\0';

    Tube *t = make_tube(name);
    assertf(t != NULL, "setup: make_tube");
    assertf(t->name_len == maxlen, "setup: tube name must not be truncated");
    Job *j = wf_job(t, 504, 4, "ab\r\n");

    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    filermjob(j->file, j);
    job_free(j);
    close(fd);

    Wal rw;
    File rf;
    Job list = {0};
    wf_replay(path, &rw, &rf, &list);

    Job *rj = job_find(504);
    assertf(rj && rj->tube && rj->tube->name_len == maxlen,
            "a %zu-byte tube name the writer emitted must come back out of "
            "the reader", maxlen);
}


// Nothing on the write path may treat a body as a C string: bodies are
// raw bytes and carry NULs, 0xFF and multi-byte sequences.
void
cttest_filewrjobfull_a_body_of_raw_bytes_survives_replay(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "rawbody.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    unsigned char body[12] = {
        0x00, 0xFF, 0x41, 0x00, 0xC3, 0xA9, 0xE2, 0x82, 0xAC, 0x7F,
        '\r', '\n'
    };

    Tube *t = make_tube("rawtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wf_job(t, 505, 12, body);

    int r = filewrjobfull(&f, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);
    filermjob(j->file, j);
    job_free(j);
    close(fd);

    Wal rw;
    File rf;
    Job list = {0};
    wf_replay(path, &rw, &rf, &list);

    Job *rj = job_find(505);
    assertf(rj && memcmp(rj->body, body, sizeof body) == 0,
            "a body carrying NUL and high bytes must round-trip verbatim");
}


// A job belongs to exactly one file at a time — that invariant is what
// makes f->refs a count of live jobs and what lets walgc ever unlink a
// binlog. Re-staging a job into a new file (compaction/migration) must
// release the old file's reference, whether that is filewrjobfull's job
// or an unstated precondition it never documents.
void
cttest_filewrjobfull_moving_a_job_must_release_the_previous_files_reference(void)
{
    wf_setup();
    char patha[512], pathb[512];
    int fda = wf_binlog(patha, sizeof patha, "movefrom.binlog");
    int fdb = wf_binlog(pathb, sizeof pathb, "moveto.binlog");

    Wal w;
    File a, b;
    wf_wal(&w, &a, fda);
    memset(&b, 0, sizeof b);
    b.w = &w;
    b.fd = fdb;
    b.iswopen = 1;
    b.free = (1 << 20) - 4;
    b.resv = 1 << 20;
    b.refs = 1;

    Tube *t = make_tube("movetube");
    assertf(t != NULL, "setup: make_tube");
    Job *j = wf_job(t, 506, 4, "ab\r\n");

    uint before = a.refs;
    fileaddjob(&a, j);
    int r = filewrjobfull(&b, j);
    assertf(r == 1, "setup: staging must succeed, got %d", r);

    assertf(a.refs == before,
            "the job moved to another binlog, so its old file must lose "
            "its reference: refs was %u, still %u — that binlog can never "
            "be reaped again", before, a.refs);

    close(fda);
    close(fdb);
}


// Ties the accounting to the file rather than to another counter: the
// bytes charged to alive are the bytes the binlog actually grew by.
void
cttest_filewrjobfull_charges_alive_exactly_what_the_binlog_grew(void)
{
    wf_setup();
    char path[512];
    int fd = wf_binlog(path, sizeof path, "alivegrow.binlog");

    Wal w;
    File f;
    wf_wal(&w, &f, fd);

    Tube *t = make_tube("growtube");
    assertf(t != NULL, "setup: make_tube");
    Job *j1 = wf_job(t, 507, 4, "ab\r\n");
    Job *j2 = wf_job(t, 508, 9, "efghijk\r\n");
    Job *j3 = wf_job(t, 509, 2, "\r\n");

    off_t before = wf_size(fd);
    int64 alive_before = w.alive;
    assertf(filewrjobfull(&f, j1) == 1, "setup: stage 1");
    assertf(filewrjobfull(&f, j2) == 1, "setup: stage 2");
    assertf(filewrjobfull(&f, j3) == 1, "setup: stage 3");
    off_t grew = wf_size(fd) - before;

    assertf(w.alive - alive_before == (int64)grew,
            "alive must move by exactly the bytes the binlog grew: grew "
            "%lld, alive moved %"PRId64,
            (long long)grew, w.alive - alive_before);

    close(fd);
}
