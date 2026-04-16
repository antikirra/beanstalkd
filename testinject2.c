#include "dat.h"
#include "testinject.h"
#include "ct/ct.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

static void
setup(void)
{
    fault_clear_all();
    progname = "testinject";
}


// --- Memory allocation OOM ---

void
cttest_inject_allocate_job_oom(void)
{
    setup();
    fault_set(FAULT_MALLOC, 0, ENOMEM);
    Job *j = allocate_job(100);
    assertf(!j, "allocate_job must return NULL on malloc failure");
    assertf(fault_hits(FAULT_MALLOC) == 1,
            "malloc fault must fire exactly once, got %d",
            fault_hits(FAULT_MALLOC));
}

void
cttest_inject_job_copy_oom(void)
{
    setup();
    Tube *t = make_tube("copysrc");
    assertf(t, "tube must allocate for test setup");

    Job *orig = make_job(10, 0, 1000000000LL, 8, t);
    assertf(orig, "original job must allocate");
    memcpy(orig->body, "DEADBEEF", 8);
    orig->r.state = Ready;

    fault_set(FAULT_MALLOC, 0, ENOMEM);
    Job *copy = job_copy(orig);
    assertf(!copy, "job_copy must return NULL on malloc failure");

    assertf(orig->r.pri == 10,
            "original pri must survive copy OOM, got %u", orig->r.pri);
    assertf(orig->r.state == Ready,
            "original state must survive, got %d", orig->r.state);
    assertf(orig->r.body_size == 8,
            "original body_size must survive, got %d", orig->r.body_size);
    assertf(memcmp(orig->body, "DEADBEEF", 8) == 0,
            "original body must be intact after copy OOM");
    assertf(orig->tube == t,
            "original tube ref must survive copy OOM");

    job_free(orig);
    tube_dref(t);
}

void
cttest_inject_heapinsert_oom(void)
{
    setup();
    Heap h = {.less = job_pri_less, .setpos = job_setpos};

    // Insert 2 jobs: first triggers realloc(NULL→2), second fits.
    Job *j1 = allocate_job(0);
    assertf(j1, "j1 must allocate");
    j1->r.pri = 1; j1->r.id = 1;
    assertf(heapinsert(&h, j1) == 1, "first heapinsert must succeed");

    Job *j2 = allocate_job(0);
    assertf(j2, "j2 must allocate");
    j2->r.pri = 2; j2->r.id = 2;
    assertf(heapinsert(&h, j2) == 1, "second heapinsert must succeed");

    assertf(h.cap == 2, "heap cap must be 2, got %zu", h.cap);
    assertf(h.len == 2, "heap len must be 2, got %zu", h.len);

    // Third insert needs realloc (len==cap). Inject OOM.
    Job *j3 = allocate_job(0);
    assertf(j3, "j3 must allocate");
    j3->r.pri = 3; j3->r.id = 3;

    fault_set(FAULT_REALLOC, 0, ENOMEM);
    int r = heapinsert(&h, j3);
    assertf(r == 0, "heapinsert must return 0 on realloc OOM, got %d", r);
    assertf(h.len == 2, "heap len must stay 2 after OOM, got %zu", h.len);
    assertf(h.cap == 2, "heap cap must stay 2 after OOM, got %zu", h.cap);
    assertf(h.data[0] == j1, "heap root must still be j1 (pri=1)");

    free(h.data);
    job_free(j1);
    job_free(j2);
    job_free(j3);
}

void
cttest_inject_ms_append_oom(void)
{
    setup();
    Ms m;
    ms_init(&m, NULL, NULL);

    int v1 = 42;
    assertf(ms_append(&m, &v1) == 1, "first ms_append must succeed");
    assertf(m.len == 1, "ms len must be 1, got %zu", m.len);

    // Second append triggers grow (cap=1, len=1). Inject OOM.
    int v2 = 99;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    int r = ms_append(&m, &v2);
    assertf(r == 0, "ms_append must return 0 on realloc OOM, got %d", r);
    assertf(m.len == 1, "ms len must stay 1 after OOM, got %zu", m.len);
    assertf(m.items[0] == &v1, "ms[0] must still point to v1");

    ms_clear(&m);
}

void
cttest_inject_fmtalloc_oom(void)
{
    setup();
    fault_set(FAULT_MALLOC, 0, ENOMEM);
    char *s = fmtalloc("hello %d world %s", 42, "test");
    assertf(!s, "fmtalloc must return NULL on malloc failure");
    assertf(fault_hits(FAULT_MALLOC) == 1, "malloc fault must fire once");
}

void
cttest_inject_zalloc_oom(void)
{
    setup();
    fault_set(FAULT_CALLOC, 0, ENOMEM);
    void *p = zalloc(64);
    assertf(!p, "zalloc must return NULL on calloc failure");
    assertf(fault_hits(FAULT_CALLOC) == 1, "calloc fault must fire once");
}

void
cttest_inject_make_tube_oom(void)
{
    setup();
    fault_set(FAULT_CALLOC, 0, ENOMEM);
    Tube *t = make_tube("oomtube");
    assertf(!t, "make_tube must return NULL on calloc failure");
}

void
cttest_inject_tube_insert_oom(void)
{
    setup();
    // tube_find_or_make → make_and_insert_tube:
    //   make_tube (calloc OK) → ms_append (realloc FAIL)
    // Tests the cleanup path when tube allocates but ms_append fails.
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    Tube *t = tube_find_or_make("insertfail");
    assertf(!t, "tube_find_or_make must return NULL when ms_append OOMs");
    assertf(fault_hits(FAULT_REALLOC) == 1, "realloc fault must fire once");

    Tube *ghost = tube_find_name("insertfail", 10);
    assertf(!ghost, "failed tube must not leak into tube hash");
}


// --- WAL / file I/O failures ---

void
cttest_inject_fileinit_oom(void)
{
    setup();
    Wal w = {.dir = "/tmp"};
    File f = {0};

    fault_set(FAULT_MALLOC, 0, ENOMEM);
    int r = fileinit(&f, &w, 1);
    assertf(r == 0, "fileinit must return 0 when fmtalloc fails, got %d", r);
    assertf(!f.path, "f.path must be NULL after fmtalloc failure");
}

void
cttest_inject_filewopen_open_fail(void)
{
    setup();
    Wal w = {.filesize = 4096};
    File f = {.w = &w, .iswopen = 0, .fd = -1};
    f.path = "/tmp/testinject_open_fail.wal";

    fault_set(FAULT_OPEN, 0, EMFILE);
    filewopen(&f);
    assertf(!f.iswopen,
            "filewopen must not set iswopen when open fails");
}

static int
noop_falloc(int fd, int size)
{
    (void)fd; (void)size;
    return 0;
}

void
cttest_inject_filewopen_write_fail(void)
{
    setup();
    Wal w = {.filesize = 64};
    File f = {.w = &w, .iswopen = 0, .fd = -1};
    f.path = "/tmp/testinject_write_fail.wal";

    FAlloc saved = falloc;
    falloc = noop_falloc;

    fault_set(FAULT_WRITE, 0, EIO);
    filewopen(&f);
    assertf(!f.iswopen,
            "filewopen must not set iswopen when write fails");

    falloc = saved;
}


// --- Recovery: data structures survive OOM and keep working ---

void
cttest_inject_heap_oom_then_success(void)
{
    setup();
    Heap h = {.less = job_pri_less, .setpos = job_setpos};

    Job *j1 = allocate_job(0);
    j1->r.pri = 5; j1->r.id = 1;
    heapinsert(&h, j1);
    Job *j2 = allocate_job(0);
    j2->r.pri = 10; j2->r.id = 2;
    heapinsert(&h, j2);
    // cap=2, len=2.

    // OOM on third insert.
    Job *j3 = allocate_job(0);
    j3->r.pri = 1; j3->r.id = 3;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    assertf(heapinsert(&h, j3) == 0, "must fail on OOM");

    // Remove one to make room, then insert without realloc.
    void *removed = heapremove(&h, 0);
    assertf(removed == j1, "must remove min-pri element (j1, pri=5)");

    assertf(heapinsert(&h, j3) == 1,
            "must succeed when cap > len (no realloc needed)");
    assertf(h.len == 2, "heap must have 2 elements, got %zu", h.len);
    assertf(h.data[0] == j3,
            "j3 (pri=1) must be heap root after insert");

    free(h.data);
    job_free(j1);
    job_free(j2);
    job_free(j3);
}

void
cttest_inject_job_lifecycle_survives_oom(void)
{
    setup();
    Tube *t = make_tube("lifecycle");
    assertf(t, "tube must allocate");

    Job *j = make_job(1, 0, 1000000000LL, 16, t);
    assertf(j, "job must allocate");
    memset(j->body, 'A', 16);
    j->r.state = Ready;

    // Copy fails under OOM.
    fault_set(FAULT_MALLOC, 0, ENOMEM);
    assertf(!job_copy(j), "copy must fail under OOM");

    // Fault auto-cleared. Copy succeeds now.
    Job *copy = job_copy(j);
    assertf(copy, "copy must succeed after fault clears");
    assertf(copy->r.pri == 1, "copy pri must match, got %u", copy->r.pri);
    assertf(memcmp(copy->body, j->body, 16) == 0,
            "copy body must match original");

    // Mutate original, verify copy is independent.
    memset(j->body, 'B', 16);
    assertf(copy->body[0] == 'A',
            "copy must be independent: expected 'A', got '%c'",
            copy->body[0]);

    job_free(copy);
    job_free(j);
    tube_dref(t);
}


// --- Connection allocation OOM ---

void
cttest_inject_make_conn_oom(void)
{
    setup();
    Tube *t = make_tube("default");
    assertf(t, "tube must allocate");

    fault_set(FAULT_CALLOC, 0, ENOMEM);
    Conn *c = make_conn(0, 'r', t, t);
    assertf(!c, "make_conn must return NULL on calloc failure");
}

void
cttest_inject_make_conn_watch_oom(void)
{
    setup();
    Tube *t = make_tube("watchfail");
    assertf(t, "tube must allocate");

    // Conn struct allocates (calloc ok), but ms_append for watch set
    // triggers grow → realloc → OOM. Tests the cleanup path that
    // returns the Conn to the pool or frees it.
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    Conn *c = make_conn(0, 'r', t, t);
    assertf(!c, "make_conn must return NULL when watch append OOMs");

    // TUBE_ASSIGN(c->use, use) is AFTER ms_append — never reached.
    // Tube using_ct must be 0.
    assertf(t->using_ct == 0,
            "tube using_ct must be 0 after failed make_conn, got %u",
            t->using_ct);
    // on_watch_insert callback fires only on successful append.
    assertf(t->watching_ct == 0,
            "tube watching_ct must be 0 after failed make_conn, got %u",
            t->watching_ct);
}


// --- WAL lock path failures ---

void
cttest_inject_waldirlock_malloc_oom(void)
{
    setup();
    Wal w = {.dir = "/tmp"};
    fault_set(FAULT_MALLOC, 0, ENOMEM);
    int r = waldirlock(&w);
    assertf(r == 0, "waldirlock must return 0 when malloc fails, got %d", r);
}

void
cttest_inject_waldirlock_open_fail(void)
{
    setup();
    Wal w = {.dir = "/tmp"};
    fault_set(FAULT_OPEN, 0, EMFILE);
    int r = waldirlock(&w);
    assertf(r == 0, "waldirlock must return 0 when open fails, got %d", r);
}


// --- WAL file close: ftruncate failure ---

void
cttest_inject_filewclose_ftruncate_fail(void)
{
    setup();
    char tmppath[] = "/tmp/testinject_ftrunc.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    Wal w = {.filesize = 4096};
    File f = {0};
    f.w = &w;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 100;   // nonzero triggers ftruncate path
    f.refs = 2;     // prevent walgc when filedecref drops to 1
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;

    fault_set(FAULT_FTRUNCATE, 0, EIO);
    filewclose(&f);
    assertf(!f.iswopen,
            "file must be closed even when ftruncate fails");
    assertf(f.fd == -1,
            "fd must be -1 after close, got %d", f.fd);

    unlink(tmppath);
}


// --- WAL write: writev failure preserves counter invariants ---

void
cttest_inject_filewritev_writev_fail(void)
{
    setup();
    char tmppath[] = "/tmp/testinject_writev.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    // Write WAL version header so fd is positioned for records.
    int ver = Walver;
    ssize_t n = write(fd, &ver, sizeof(ver));
    assertf(n == (ssize_t)sizeof(ver), "version header write must succeed");

    Wal w = {.filesize = 4096, .use = 1, .resv = 200, .alive = 0};
    File f = {0};
    f.w = &w;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 3500;
    f.resv = 200;
    f.refs = 2;
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;

    Tube *t = make_tube("wrfail");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(10);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.state = Ready;
    j->r.body_size = 10;
    j->r.id = 1;
    memset(j->body, 'X', 10);
    j->walresv = 200;
    j->walused = 0;

    // Snapshot counters before fault.
    int64 pre_wal_resv  = w.resv;
    int64 pre_wal_alive = w.alive;
    int   pre_file_resv = f.resv;
    int64 pre_walresv   = j->walresv;
    int64 pre_walused   = j->walused;

    fault_set(FAULT_WRITEV, 0, EIO);
    int r = filewrjobfull(&f, j);
    assertf(r == 0,
            "filewrjobfull must return 0 on writev failure, got %d", r);

    // Counter invariant: all WAL accounting unchanged on failure.
    assertf(w.resv == pre_wal_resv,
            "wal.resv must not change: was %"PRId64", got %"PRId64,
            pre_wal_resv, w.resv);
    assertf(w.alive == pre_wal_alive,
            "wal.alive must not change: was %"PRId64", got %"PRId64,
            pre_wal_alive, w.alive);
    assertf(f.resv == pre_file_resv,
            "file.resv must not change: was %d, got %d",
            pre_file_resv, f.resv);
    assertf(j->walresv == pre_walresv,
            "job.walresv must not change: was %"PRId64", got %"PRId64,
            pre_walresv, j->walresv);
    assertf(j->walused == pre_walused,
            "job.walused must not change: was %"PRId64", got %"PRId64,
            pre_walused, j->walused);

    // Job must NOT be linked to the file on write failure.
    assertf(!j->file,
            "job must not be linked to file after writev failure");

    close(fd);
    unlink(tmppath);
    job_free(j);
}
