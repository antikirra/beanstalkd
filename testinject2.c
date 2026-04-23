#include "dat.h"
#include "testinject.h"
#include "ct/ct.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/stat.h>

// Exposed from prot.c for hostile error-path testing.
extern int kick_buried_job(Server *s, Job *j);
extern int kick_delayed_job(Server *s, Job *j);
extern struct stats global_stat;
extern void prot_init();

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


// --- Kick error-path: walresv balance under ready-heap OOM ---
//
// kick_buried_job reserves WAL space (walresvupdate) for a delete record,
// removes the job from the buried list, then tries to enqueue it on the
// ready heap. If heapinsert() fails (realloc ENOMEM here), the function
// must:
//   1. roll back kick_ct;
//   2. return the reserved bytes via walresvreturn() (w->resv, file.resv,
//      file.free must all return to pre-call values);
//   3. clear j->walresv;
//   4. bury the job again so it is not leaked.
//
// A regression in any of these steps would silently grow w->resv on every
// failed kick — fatal for long-running instances under WAL-full pressure.
void
cttest_inject_kick_buried_heapinsert_oom_walresv(void)
{
    setup();

    Server s = {0};

    // Real file so reserve()/walresvreturn() operate on real counters.
    char tmppath[] = "/tmp/testinject_kickburied.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver),
            "version header write must succeed");

    s.wal.use = 1;
    s.wal.filesize = 4096;
    s.wal.nfile = 1;

    File f = {0};
    f.w = &s.wal;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 4096;
    f.resv = 0;
    f.refs = 2;                  // prevent walgc on filedecref
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;

    s.wal.cur = &f;
    s.wal.tail = &f;
    s.wal.head = &f;

    Tube *t = make_tube("kickburied");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(10);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = 1;
    j->r.body_size = 10;
    memset(j->body, 'X', 10);

    // Place job in Buried state — caller precondition for kick_buried_job.
    job_list_insert(&t->buried, j);
    j->r.state = Buried;
    global_stat.buried_ct++;
    t->stat.buried_ct++;

    // Snapshot every counter we expect to roll back.
    int64  pre_wal_resv      = s.wal.resv;
    int    pre_file_resv     = f.resv;
    int    pre_file_free     = f.free;
    int64  pre_j_walresv     = j->walresv;
    uint32 pre_kick_ct       = j->r.kick_ct;
    int64  pre_buried_global = global_stat.buried_ct;
    int64  pre_buried_tube   = t->stat.buried_ct;

    // Ready heap is fresh (cap==0). The FIRST heapinsert in enqueue_job()
    // WILL call realloc — and that's the only call we arm. Fault disarms
    // after one hit, so the sibling bury_job()'s list insert (no realloc)
    // proceeds normally, letting us observe the rollback cleanly.
    assertf(t->ready.cap == 0,
            "ready heap must start with cap=0 to force realloc");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    int r = kick_buried_job(&s, j);

    assertf(r == 0,
            "kick must fail when ready heapinsert realloc fails, got %d", r);
    assertf(fault_hits(FAULT_REALLOC) == 1,
            "exactly one realloc injection expected, got %d",
            fault_hits(FAULT_REALLOC));

    // Core invariant: WAL reservation balances back to zero net change.
    assertf(s.wal.resv == pre_wal_resv,
            "wal.resv leak: was %" PRId64 " got %" PRId64,
            pre_wal_resv, s.wal.resv);
    assertf(f.resv == pre_file_resv,
            "file.resv leak: was %d got %d", pre_file_resv, f.resv);
    assertf(f.free == pre_file_free,
            "file.free drift: was %d got %d", pre_file_free, f.free);
    assertf(j->walresv == pre_j_walresv,
            "j->walresv leak: was %" PRId64 " got %" PRId64,
            pre_j_walresv, j->walresv);

    // kick_ct must not advance on failure: ++ then -- must net to zero.
    assertf(j->r.kick_ct == pre_kick_ct,
            "kick_ct advanced on failure: was %u got %u",
            pre_kick_ct, j->r.kick_ct);

    // Job must be fully restored to buried state.
    assertf(j->r.state == Buried,
            "job must return to Buried, got state=%d", j->r.state);
    assertf(global_stat.buried_ct == pre_buried_global,
            "global buried_ct drift: was %" PRId64 " got %" PRId64,
            pre_buried_global, global_stat.buried_ct);
    assertf(t->stat.buried_ct == pre_buried_tube,
            "tube buried_ct drift: was %" PRId64 " got %" PRId64,
            pre_buried_tube, t->stat.buried_ct);

    // Ready heap must not retain any reference to the job.
    assertf(t->ready.len == 0,
            "ready heap must be empty after rollback, got len=%zu",
            t->ready.len);

    // Cleanup.
    job_list_remove(j);
    global_stat.buried_ct--;
    t->stat.buried_ct--;
    TUBE_ASSIGN(j->tube, NULL);
    job_free(j);
    close(fd);
    unlink(tmppath);
}


// kick_delayed_job error path: ready-heap OOM forces fallback to the delay
// queue (enqueue_job with update_store=0, no walwrite). The reserved WAL
// space must be returned via walresvreturn(), j->walresv must clear, and
// kick_ct must net to zero (++ then --).
//
// prot_init() is required because the fallback path calls
// delay_tube_update(), which touches the static delay_tube_heap in prot.c;
// without less/setpos set, heapinsert would crash there.
void
cttest_inject_kick_delayed_heapinsert_oom_walresv(void)
{
    setup();
    prot_init();

    Server s = {0};

    char tmppath[] = "/tmp/testinject_kickdelayed.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header write must succeed");

    s.wal.use = 1;
    s.wal.filesize = 4096;
    s.wal.nfile = 1;

    File f = {0};
    f.w = &s.wal;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 4096;
    f.resv = 0;
    f.refs = 2;
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;

    s.wal.cur = &f;
    s.wal.tail = &f;
    s.wal.head = &f;

    Tube *t = make_tube("kickdelayed");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(10);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = 1;
    j->r.body_size = 10;
    j->r.delay = 1000000000LL;
    j->r.deadline_at = 99999999999LL;
    memset(j->body, 'Z', 10);

    // Place job in the tube's delay heap. delay_tube_heap registration
    // is intentionally deferred: remove_delayed_job → delay_tube_update
    // sees t->delay.len==0 && t->in_delay_heap==0 and returns early,
    // then the fallback enqueue_job(..delay..) re-registers t cleanly.
    assertf(heapinsert(&t->delay, j) == 1,
            "setup: delay heapinsert must succeed");
    j->r.state = Delayed;

    int64  pre_wal_resv   = s.wal.resv;
    int    pre_file_resv  = f.resv;
    int    pre_file_free  = f.free;
    int64  pre_j_walresv  = j->walresv;
    uint32 pre_kick_ct    = j->r.kick_ct;

    // First realloc after this point is heapinsert(ready) in enqueue_job.
    assertf(t->ready.cap == 0,
            "ready heap must start at cap=0 to force realloc");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    int r = kick_delayed_job(&s, j);

    assertf(r == 0,
            "kick must fail when ready heapinsert realloc fails, got %d", r);
    assertf(fault_hits(FAULT_REALLOC) == 1,
            "exactly one realloc injection expected, got %d",
            fault_hits(FAULT_REALLOC));

    assertf(s.wal.resv == pre_wal_resv,
            "wal.resv leak: was %" PRId64 " got %" PRId64,
            pre_wal_resv, s.wal.resv);
    assertf(f.resv == pre_file_resv,
            "file.resv leak: was %d got %d", pre_file_resv, f.resv);
    assertf(f.free == pre_file_free,
            "file.free drift: was %d got %d", pre_file_free, f.free);
    assertf(j->walresv == pre_j_walresv,
            "j->walresv leak: was %" PRId64 " got %" PRId64,
            pre_j_walresv, j->walresv);
    assertf(j->r.kick_ct == pre_kick_ct,
            "kick_ct advanced on failure: was %u got %u",
            pre_kick_ct, j->r.kick_ct);

    // Fallback: job re-entered the delay queue. enqueue_job(..delay, 0)
    // succeeds because t->delay retained cap=1 after the heapremove.
    assertf(j->r.state == Delayed,
            "job must return to Delayed, got state=%d", j->r.state);
    assertf(t->delay.len == 1,
            "delay heap must hold the fallback job, got len=%zu",
            t->delay.len);
    assertf(t->ready.len == 0,
            "ready heap must be empty, got len=%zu", t->ready.len);

    // Cleanup for valgrind/ASan mode: fork isolation drops the rest.
    heapremove(&t->delay, 0);
    TUBE_ASSIGN(j->tube, NULL);
    job_free(j);
    close(fd);
    unlink(tmppath);
}


// ─── Durable mode (-D): fdatasync failure must abort walwrite ──
// When -D is active, a failed fdatasync() means the write may not
// survive a crash. walwrite must refuse to claim success: it must
// disable the WAL (w->use=0), close the current file, and return 0
// so upstream reply() sees the failure and bails. A regression that
// silently swallowed the fdatasync error would let beanstalkd ack
// puts that subsequently vanish on crash — the exact invariant -D
// exists to protect.
void
cttest_inject_durable_fdatasync_fail_disables_wal(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_durable.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header must write");

    Wal w = {
        .filesize = 4096,
        .use = 1,
        .durable_sync = 1,   // the code path under test
    };
    File f = {0};
    f.w = &w;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 3500;
    f.resv = 300;
    f.refs = 2;
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;

    w.cur = &f;
    w.tail = &f;
    w.head = &f;
    w.resv = 300;

    Tube *t = make_tube("durable");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(10);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.state = Ready;
    j->r.body_size = 10;
    j->r.id = 1;
    memset(j->body, 'D', 10);
    j->walresv = 300;
    j->walused = 0;

    int64 pre_nrec = w.nrec;

    fault_set(FAULT_FDATASYNC, 0, EIO);
    int r = walwrite(&w, j);

    assertf(r == 0,
            "walwrite must return 0 when durable fdatasync fails, got %d", r);
    assertf(fault_hits(FAULT_FDATASYNC) == 1,
            "fdatasync fault must fire exactly once, got %d",
            fault_hits(FAULT_FDATASYNC));
    assertf(w.use == 0,
            "wal must be disabled (use=0) after fdatasync failure, got %d",
            w.use);
    assertf(!f.iswopen,
            "current file must be closed on fdatasync failure");
    assertf(w.nrec == pre_nrec,
            "nrec must not advance when walwrite fails: was %" PRId64
            " got %" PRId64, pre_nrec, w.nrec);

    close(fd);
    unlink(tmppath);
    job_free(j);
}


// Durable mode success path: fdatasync is called exactly once per
// walwrite when durable_sync=1, and nrec advances on success.
// Guards against a refactor that accidentally skips the sync in the
// fast path.
void
cttest_inject_durable_fdatasync_fires_once_on_success(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_durable_ok.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header must write");

    Wal w = {
        .filesize = 4096,
        .use = 1,
        .durable_sync = 1,
    };
    File f = {0};
    f.w = &w;
    f.fd = fd;
    f.iswopen = 1;
    f.free = 3500;
    f.resv = 300;
    f.refs = 2;
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;
    w.cur = &f;
    w.tail = &f;
    w.head = &f;
    w.resv = 300;

    Tube *t = make_tube("durableok");
    Job *j = allocate_job(10);
    TUBE_ASSIGN(j->tube, t);
    j->r.state = Ready;
    j->r.body_size = 10;
    j->r.id = 2;
    memset(j->body, 'D', 10);
    j->walresv = 300;

    // No fault armed. fault_calls() counts wrapped invocations even
    // when no fault fires — so an incremented count is direct
    // evidence the durable_sync branch reached fdatasync().
    fault_clear_all();              // zero the call counter
    int64 pre_nrec = w.nrec;
    int r = walwrite(&w, j);

    assertf(r == 1, "walwrite must succeed in happy path, got %d", r);
    assertf(w.use == 1, "wal must remain enabled, got use=%d", w.use);
    assertf(w.nrec == pre_nrec + 1,
            "nrec must advance by 1: was %" PRId64 " got %" PRId64,
            pre_nrec, w.nrec);
    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "walwrite in durable mode must invoke fdatasync exactly "
            "once, got %d calls", fault_calls(FAULT_FDATASYNC));
    assertf(fault_hits(FAULT_FDATASYNC) == 0,
            "no fault was armed; unexpected hits: %d",
            fault_hits(FAULT_FDATASYNC));

    close(fd);
    unlink(tmppath);
    job_free(j);
}


// #C1 regression: once w->use=0, walwrite() in durable mode must return 0
// rather than silently ack. The legacy "return 1 when disabled" path was
// valid for best-effort async mode but silently broke the -D "ack ⇒ on
// disk" contract for every subsequent operation in the process.
void
cttest_inject_walwrite_refuses_when_durable_and_wal_disabled(void)
{
    setup();

    Wal w = {
        .use = 0,               // WAL already disabled by a prior failure
        .durable_sync = 1,      // -D mode
    };

    Tube *t = make_tube("cdone");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(4);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.state = Ready;
    j->r.body_size = 4;
    j->r.id = 1;
    memcpy(j->body, "data", 4);

    int r = walwrite(&w, j);
    assertf(r == 0,
            "walwrite under -D with w->use=0 must refuse (return 0), got %d", r);
    assertf(w.use == 0, "WAL must remain disabled, got use=%d", w.use);

    // Non-durable control: same state, wantsync off, durable_sync off
    // — legacy pass-through must still return 1.
    Wal w2 = { .use = 0, .durable_sync = 0 };
    int r2 = walwrite(&w2, j);
    assertf(r2 == 1,
            "walwrite in non-durable mode with w->use=0 must keep the "
            "legacy pass-through (return 1), got %d", r2);

    job_free(j);
    tube_dref(t);
}


// #C1 regression (truncate path): wal_write_truncate must mirror walwrite's
// durable-refusal behavior. A silently successful marker write under -D
// with a disabled WAL would be worse than for put/delete: it looks like
// truncate ran but the cutoff is not on disk and never will be.
void
cttest_inject_wal_write_truncate_refuses_when_durable_and_wal_disabled(void)
{
    setup();

    Wal w = { .use = 0, .durable_sync = 1 };
    Tube *t = make_tube("ctrunc");
    assertf(t, "tube must allocate");

    int r = wal_write_truncate(&w, t, 42);
    assertf(r == 0,
            "wal_write_truncate under -D with w->use=0 must refuse, got %d", r);

    Wal w2 = { .use = 0, .durable_sync = 0 };
    int r2 = wal_write_truncate(&w2, t, 42);
    assertf(r2 == 1,
            "wal_write_truncate non-durable with w->use=0 must pass through, got %d", r2);

    tube_dref(t);
}


// #C1 regression (reserve gate): walresvput / walresvupdate propagate
// through `reserve()`, which was also tightened. Under -D + !use both
// must return 0 so enqueue_incoming_job / bury_job / release see the
// failure and reply with a real error. Under non-durable + !use they
// must keep the legacy "succeeds with 1" pass-through.
//
// This test closes the gap left by the walwrite/wal_write_truncate
// tests above: a hypothetical refactor that undoes the `reserve()`
// guard would let `walresvput` hand out bytes that walwrite then
// refuses to write — the client would get INSERTED reply with no
// matching WAL record, defeating the whole C1 contract.
void
cttest_inject_walresv_refuses_when_durable_and_wal_disabled(void)
{
    setup();

    Tube *t = make_tube("cresv");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(4);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.body_size = 4;
    j->r.id = 1;

    // Durable + disabled: refuse.
    Wal w = { .use = 0, .durable_sync = 1 };
    int r = walresvput(&w, j);
    assertf(r == 0,
            "walresvput under -D with w->use=0 must refuse (return 0), got %d", r);

    int ru = walresvupdate(&w);
    assertf(ru == 0,
            "walresvupdate under -D with w->use=0 must refuse (return 0), got %d", ru);

    // Non-durable + disabled: legacy pass-through. reserve()'s
    // "return 1" survives for async-mode callers whose whole purpose
    // was to tolerate a disabled WAL silently.
    Wal w2 = { .use = 0, .durable_sync = 0 };
    int r2 = walresvput(&w2, j);
    assertf(r2 > 0,
            "walresvput non-durable with w->use=0 must pass through, got %d", r2);

    int ru2 = walresvupdate(&w2);
    assertf(ru2 > 0,
            "walresvupdate non-durable with w->use=0 must pass through, got %d", ru2);

    job_free(j);
    tube_dref(t);
}


// #C2 regression: if writev succeeds but durable fdatasync fails, the bytes
// must be rolled back from the file tail (ftruncate) so replay does NOT
// resurrect a record whose client received INTERNAL_ERROR. Without the
// rollback, server state and on-disk state diverge after the caller
// rolls back its in-memory view — exactly the class of bug the -D
// contract is supposed to prevent.
//
// Drive filewrjobfull directly to observe only the commit-durable ftruncate;
// walwrite's own filewclose() would also ftruncate (to f->filesize - f->free)
// and mask the rollback under test.
void
cttest_inject_durable_fdatasync_fail_rolls_back_tail(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_c2_rollback.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");

    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header must write");

    off_t pre_writev = lseek(fd, 0, SEEK_CUR);
    assertf(pre_writev == (off_t)sizeof(int),
            "offset must sit right after version header");

    Wal w = {
        .filesize = 4096,
        .use = 1,
        .durable_sync = 1,
    };
    File f = {0};
    f.w = &w;
    f.fd = fd;
    f.iswopen = 1;
    // Match what filewopen would produce: header written, rest free.
    f.free = (int)(w.filesize - pre_writev);
    f.resv = 300;
    f.refs = 2;
    f.jlist.fprev = &f.jlist;
    f.jlist.fnext = &f.jlist;
    w.cur = &f;
    w.tail = &f;
    w.head = &f;
    w.resv = 300;

    Tube *t = make_tube("c2");
    assertf(t, "tube must allocate");

    Job *j = allocate_job(10);
    assertf(j, "job must allocate");
    TUBE_ASSIGN(j->tube, t);
    j->r.state = Ready;
    j->r.body_size = 10;
    j->r.id = 7;
    memset(j->body, 'D', 10);
    j->walresv = 300;
    j->walused = 0;

    // Arm the fdatasync fault. writev_all will complete, file will grow,
    // then fdatasync returns EIO; filewrite_commit_durable must ftruncate
    // the tail back to pre_writev before returning 0.
    fault_set(FAULT_FDATASYNC, 0, EIO);
    int r = filewrjobfull(&f, j);

    assertf(r == 0,
            "filewrjobfull must return 0 when durable fdatasync fails, got %d", r);
    assertf(fault_hits(FAULT_FDATASYNC) == 1,
            "fdatasync fault must fire exactly once, got %d",
            fault_hits(FAULT_FDATASYNC));

    // The rollback: file size must be back at pre_writev (just the
    // version header), not pre_writev + writev payload.
    struct stat st;
    assertf(fstat(fd, &st) == 0, "fstat must succeed (fd still open)");
    assertf(st.st_size == pre_writev,
            "durable fdatasync fail must ftruncate the tail: expected "
            "size=%lld, got %lld",
            (long long)pre_writev, (long long)st.st_size);

    // filewritev did NOT apply its accounting on failure, so w->resv
    // and f->resv must be unchanged.
    assertf(w.resv == 300,
            "w.resv must be unchanged on failure, got %" PRId64, w.resv);

    close(fd);
    unlink(tmppath);
    job_free(j);
    tube_dref(t);
}


// ─── Async fsync thread plumbing (#4) ─────────────────────────
// Production path spawned by walsyncstart when wal.wantsync is on,
// but the normal test harness forces syncrate=0 to stay deterministic
// and never exercises the thread. These three tests drive the thread
// directly so a deadlock / race / fd-leak in walsyncstart/stop,
// sync_thread_fn, or the mutex+cond handoff would surface under CI.

// Poll `w->sync_fd < 0` under the mutex. Returns 1 if the handoff
// was drained within max_iterations * 10ms, 0 if it timed out. The
// thread sets sync_fd = -1 the instant it accepts the fd, so this
// polls the "acceptance" edge, not the fdatasync completion.
static int
wait_for_sync_accept(Wal *w, int max_iterations)
{
    for (int i = 0; i < max_iterations; i++) {
        pthread_mutex_lock(&w->sync_mu);
        int done = (w->sync_fd < 0);
        pthread_mutex_unlock(&w->sync_mu);
        if (done) return 1;
        usleep(10000); // 10ms
    }
    return 0;
}

// Spin until the atomic error slot is non-zero OR timeout.
static int
wait_for_sync_err(Wal *w, int max_iterations)
{
    for (int i = 0; i < max_iterations; i++) {
        if (atomic_load_explicit(&w->sync_err, memory_order_relaxed))
            return 1;
        usleep(10000);
    }
    return 0;
}

// #5a — kick_buried_job / kick_delayed_job must propagate the C1
// refusal from walresvupdate. Without this pair of checks, a hostile
// kick in -D + disabled-WAL mode could change job state without WAL
// persistence, ghost-acking the change to the client.
void
cttest_inject_kick_buried_job_refuses_when_durable_and_wal_disabled(void)
{
    setup();

    // Minimal server shell with a disabled durable WAL.
    Server s = {0};
    s.wal.use = 0;
    s.wal.durable_sync = 1;

    Tube *t = make_tube("cdkick");
    assertf(t, "tube must allocate");

    // Buried setup: build a job, bury it by hand (we bypass the
    // normal state machine to isolate the WAL-reservation gate).
    Job *j = make_job(5, 0, 1000000000LL, 4, t);
    assertf(j, "job must allocate");
    memcpy(j->body, "kbry", 4);
    j->r.state = Buried;
    job_list_insert(&t->buried, j);
    t->stat.buried_ct = 1;

    int r = kick_buried_job(&s, j);
    assertf(r == 0,
            "kick_buried_job under -D with w->use=0 must refuse, got %d", r);
    assertf(j->r.state == Buried,
            "job state must remain Buried after refused kick, got %d",
            j->r.state);

    // Delayed branch: same gate, distinct code path.
    Job *jd = make_job(6, 0, 1000000000LL, 4, t);
    assertf(jd, "delayed job must allocate");
    memcpy(jd->body, "kdly", 4);
    jd->r.state = Delayed;
    jd->r.deadline_at = now + 1000000000LL;
    assertf(heapinsert(&t->delay, jd) == 1, "delay heap insert");

    int rd = kick_delayed_job(&s, jd);
    assertf(rd == 0,
            "kick_delayed_job under -D with w->use=0 must refuse, got %d", rd);
    assertf(jd->r.state == Delayed,
            "delayed job state must remain Delayed, got %d", jd->r.state);

    // Cleanup.
    job_list_remove(j);
    heapremove(&t->delay, jd->heap_index);
    job_free(j);
    job_free(jd);
    tube_dref(t);
}


// #4a — full thread lifecycle: start → handoff fd → thread drains →
// stop → join. No errors expected; sync_err must remain 0 at the end.
void
cttest_walsync_thread_roundtrip(void)
{
    setup();

    char tmppath[] = "/tmp/testsync_rt.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");
    ssize_t nw = write(fd, "x", 1);
    assertf(nw == 1, "write must produce 1 byte");

    Wal w = {0};
    w.use = 1;

    walsyncstart(&w);
    assertf(w.sync_on == 1,
            "walsyncstart must set sync_on, got %d", w.sync_on);

    // Hand off a dup'd fd the same way walsync() would.
    int dupfd = dup(fd);
    assertf(dupfd >= 0, "dup must succeed");
    pthread_mutex_lock(&w.sync_mu);
    w.sync_fd = dupfd;
    pthread_cond_signal(&w.sync_cond);
    pthread_mutex_unlock(&w.sync_mu);

    assertf(wait_for_sync_accept(&w, 200),
            "sync thread must accept handoff within 2s");

    int err = atomic_load_explicit(&w.sync_err, memory_order_relaxed);
    assertf(err == 0,
            "no fsync error expected on roundtrip, got errno=%d", err);

    walsyncstop(&w);
    assertf(w.sync_on == 0,
            "walsyncstop must clear sync_on, got %d", w.sync_on);

    close(fd);
    unlink(tmppath);
}


// #4b — async fdatasync failure must surface via atomic sync_err. This
// is the load-bearing contract for the main-thread walsync() caller:
// without it, a silently-dropped fsync under async mode would look
// like a successful durable write to the operator.
void
cttest_walsync_thread_error_surface(void)
{
    setup();

    char tmppath[] = "/tmp/testsync_err.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");
    ssize_t nw = write(fd, "x", 1);
    assertf(nw == 1, "write must produce 1 byte");

    Wal w = {0};
    w.use = 1;

    walsyncstart(&w);

    // Arm one fdatasync failure. The thread fires it on the handoff
    // we queue below; fault_fire auto-disarms after one hit.
    fault_set(FAULT_FDATASYNC, 0, EIO);

    int dupfd = dup(fd);
    pthread_mutex_lock(&w.sync_mu);
    w.sync_fd = dupfd;
    pthread_cond_signal(&w.sync_cond);
    pthread_mutex_unlock(&w.sync_mu);

    assertf(wait_for_sync_accept(&w, 200),
            "sync thread must accept handoff within 2s");
    assertf(wait_for_sync_err(&w, 200),
            "sync thread must propagate fdatasync error within 2s");

    int err = atomic_load_explicit(&w.sync_err, memory_order_relaxed);
    assertf(err == EIO,
            "sync_err must surface EIO from fdatasync fault, got %d", err);
    assertf(fault_hits(FAULT_FDATASYNC) == 1,
            "FAULT_FDATASYNC must fire exactly once, got %d",
            fault_hits(FAULT_FDATASYNC));

    walsyncstop(&w);

    close(fd);
    unlink(tmppath);
}


// NOTE: a third test for the "busy slot persists without cond_signal"
// invariant was considered, but POSIX allows spurious wake-ups from
// pthread_cond_wait; relying on their absence makes the test flaky.
// The busy-fallback logic in walsync() / dirsync() is `static` and not
// directly reachable from unit tests anyway; its discipline is locked
// by the roundtrip and error-surface tests above plus E2E coverage via
// `cttest_wal_*` whenever an operator runs the server with -b.
