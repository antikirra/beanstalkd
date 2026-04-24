#include "dat.h"
#include "testinject.h"
#include "ct/ct.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>

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
    // Group-commit contract: walwrite stages the record and returns 1
    // on successful writev; the durable fdatasync (and its rollback /
    // WAL-disable on failure) is triggered by walcommit, which the
    // main loop runs once per epoll drain. In a unit test we fire it
    // ourselves to exercise the same failure path this test guards.
    int r = walwrite(&w, j);
    assertf(r == 1,
            "walwrite must stage successfully before commit, got %d", r);
    assertf(w.nrec == pre_nrec + 1,
            "nrec advances at stage time: was %" PRId64 " got %" PRId64,
            pre_nrec, w.nrec);

    int c = walcommit(&w);

    assertf(c == 0,
            "walcommit must return 0 when durable fdatasync fails, got %d", c);
    assertf(fault_hits(FAULT_FDATASYNC) == 1,
            "fdatasync fault must fire exactly once, got %d",
            fault_hits(FAULT_FDATASYNC));
    assertf(w.use == 0,
            "wal must be disabled (use=0) after fdatasync failure, got %d",
            w.use);
    assertf(!f.iswopen,
            "current file must be closed on fdatasync failure");

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

    // Group-commit contract: walwrite stages (no fdatasync), walcommit
    // issues the single fdatasync covering every staged record. Assert
    // walwrite didn't sync, then walcommit did, then nrec settled.
    fault_clear_all();              // zero the call counter
    int64 pre_nrec = w.nrec;
    int r = walwrite(&w, j);

    assertf(r == 1, "walwrite must stage in happy path, got %d", r);
    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "walwrite must NOT call fdatasync under group commit, got %d",
            fault_calls(FAULT_FDATASYNC));

    int c = walcommit(&w);
    assertf(c == 1, "walcommit must succeed in happy path, got %d", c);
    assertf(w.use == 1, "wal must remain enabled, got use=%d", w.use);
    assertf(w.nrec == pre_nrec + 1,
            "nrec must advance by 1: was %" PRId64 " got %" PRId64,
            pre_nrec, w.nrec);
    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "walcommit in durable mode must invoke fdatasync exactly "
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


// --- 2026-04-23 Tier-1+2 injection framework extension ---
// These tests exercise two new wraps (FAULT_STAT, FAULT_PTHREAD_CREATE)
// added to close gaps in coverage of the e7a97d0 TOCTOU hardening and
// the walsyncstart graceful-fallback contract.

// #6a — make_server_socket on "unix:<path>" must propagate stat()
// failures other than ENOENT as a clean -1 return. Regression guard for
// the EACCES / ELOOP branch of make_unix_socket added in e7a97d0; the
// branch has no other path to exercise it in unit tests.
void
cttest_inject_make_server_socket_stat_eacces_rejects(void)
{
    setup();

    // The path is irrelevant — stat is faulted before it is inspected.
    char path[] = "unix:/tmp/testinject_stat_eacces.sock";

    fault_set(FAULT_STAT, 0, EACCES);
    int fd = make_server_socket(path, NULL);
    assertf(fd == -1,
            "make_server_socket must reject on stat EACCES, got fd=%d", fd);
    assertf(fault_hits(FAULT_STAT) == 1,
            "stat fault must fire exactly once, got %d",
            fault_hits(FAULT_STAT));
}

// #6b — happy-path regression: stat wrap must not alter behaviour when
// no fault is armed. Without this test, a future bug in fault_fire's
// call-counting could silently break every stat() caller. Also asserts
// that exactly one wrapped stat() call happens per make_server_socket:
// if glibc ever routes the user-space stat() through a different symbol
// (__xstat, __statx, …) the count goes to 0 and this fails loudly.
void
cttest_inject_make_server_socket_stat_happy_path_untouched(void)
{
    setup();

    char path[64];
    snprintf(path, sizeof(path),
             "unix:/tmp/testinject_stat_happy_%d.sock", getpid());
    // Pre-clean any leftover from an aborted prior run with the same PID.
    unlink(path + 5);

    int fd = make_server_socket(path, NULL);
    assertf(fd >= 0,
            "make_server_socket on fresh path must succeed, got fd=%d "
            "(errno=%d)", fd, errno);
    assertf(fault_calls(FAULT_STAT) == 1,
            "exactly one wrapped stat() call expected, got %d — if 0, the "
            "linker wrap is bypassed by a glibc alias (__xstat/__statx)",
            fault_calls(FAULT_STAT));
    assertf(fault_hits(FAULT_STAT) == 0,
            "no fault armed → no hits expected, got %d",
            fault_hits(FAULT_STAT));

    if (fd >= 0) close(fd);
    unlink(path + 5);
}

// #7a — walsyncstart must gracefully fall back to synchronous fsync
// when pthread_create fails. sync_on must remain 0; subsequent dirsync
// / walwrite calls (in -D mode) are expected to use the inline
// durable_fsync branch.
void
cttest_inject_walsyncstart_pthread_create_fail_falls_back(void)
{
    setup();

    Wal w = {0};
    w.use = 1;

    fault_set(FAULT_PTHREAD_CREATE, 0, EAGAIN);
    walsyncstart(&w);

    assertf(w.sync_on == 0,
            "walsyncstart must leave sync_on=0 on pthread_create failure, "
            "got %d", w.sync_on);
    assertf(fault_hits(FAULT_PTHREAD_CREATE) == 1,
            "pthread_create fault must fire exactly once, got %d",
            fault_hits(FAULT_PTHREAD_CREATE));

    // walsyncstop must be a no-op when sync_on==0; otherwise pthread_join
    // on an uninitialised handle would segfault.
    walsyncstop(&w);
    assertf(w.sync_on == 0,
            "walsyncstop no-op must keep sync_on=0, got %d", w.sync_on);
}

// #7b — happy path: unfaulted walsyncstart must still spawn a thread,
// confirming our wrap does not corrupt the normal code path.
void
cttest_inject_walsyncstart_pthread_create_happy_path_untouched(void)
{
    setup();

    Wal w = {0};
    w.use = 1;

    walsyncstart(&w);
    assertf(w.sync_on == 1,
            "walsyncstart must set sync_on=1 on success, got %d", w.sync_on);
    assertf(fault_calls(FAULT_PTHREAD_CREATE) == 1,
            "pthread_create must have been called exactly once, got %d",
            fault_calls(FAULT_PTHREAD_CREATE));
    assertf(fault_hits(FAULT_PTHREAD_CREATE) == 0,
            "no fault armed → no hits, got %d",
            fault_hits(FAULT_PTHREAD_CREATE));

    walsyncstop(&w);
    assertf(w.sync_on == 0,
            "walsyncstop must clear sync_on, got %d", w.sync_on);
}

// #7c — "skip N then fail" contract verification for pthread_create.
// Two walsyncstart / walsyncstop cycles: the second must trigger the
// fault. This guards against a future regression where fault_fire's
// countdown on pthread_create breaks (e.g. because the wrap forgot to
// zero errno and subsequent callers saw a stale value).
void
cttest_inject_walsyncstart_pthread_create_skip_then_fail(void)
{
    setup();

    Wal w1 = {0};
    w1.use = 1;
    Wal w2 = {0};
    w2.use = 1;

    fault_set(FAULT_PTHREAD_CREATE, 1, EAGAIN); // skip 1, fail on 2nd.

    walsyncstart(&w1);
    assertf(w1.sync_on == 1,
            "first walsyncstart must succeed, got sync_on=%d", w1.sync_on);
    walsyncstop(&w1);

    walsyncstart(&w2);
    assertf(w2.sync_on == 0,
            "second walsyncstart must fall back, got sync_on=%d", w2.sync_on);
    walsyncstop(&w2);

    assertf(fault_hits(FAULT_PTHREAD_CREATE) == 1,
            "exactly one fault hit expected, got %d",
            fault_hits(FAULT_PTHREAD_CREATE));
    assertf(fault_calls(FAULT_PTHREAD_CREATE) == 2,
            "two real calls expected, got %d",
            fault_calls(FAULT_PTHREAD_CREATE));
}


// ─── Group commit: fdatasync amortization ──────────────────────
//
// Core claim of group commit: many walwrite() calls collapse into
// exactly one fdatasync() when walcommit() fires. If a regression
// reintroduces an inline fsync on the write path, this test catches
// it: N staged records → 1 fdatasync, not N.
//
// Pairs with cttest_inject_durable_fdatasync_fail_disables_wal (commit
// fail contract) and ..._fires_once_on_success (commit success contract).
// This test focuses on the "one per batch" invariant across multiple
// records.
static void
setup_durable_wal(int fd, Wal *w, File *f)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 4096;
    w->use = 1;
    w->durable_sync = 1;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->free = 32768;
    f->resv = 8192;
    f->refs = 2;
    f->jlist.fprev = &f->jlist;
    f->jlist.fnext = &f->jlist;
    w->cur = w->tail = w->head = f;
    w->resv = 8192;
}

void
cttest_inject_group_commit_fires_fdatasync_once_for_batch(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_groupcommit.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");
    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header write");

    Wal w;
    File f;
    setup_durable_wal(fd, &w, &f);

    Tube *t = make_tube("groupcommit");
    assertf(t, "tube must allocate");

    // Stage 5 records. Each is a new job so filewrjobfull runs.
    const int N = 5;
    Job *jobs[N];
    for (int i = 0; i < N; i++) {
        Job *j = allocate_job(10);
        assertf(j, "job must allocate");
        TUBE_ASSIGN(j->tube, t);
        j->r.state = Ready;
        j->r.body_size = 10;
        j->r.id = (uint64)(i + 1);
        memset(j->body, 'D', 10);
        j->walresv = 300;
        j->walused = 0;
        jobs[i] = j;
    }

    fault_clear_all();
    int64 pre_nrec = w.nrec;

    for (int i = 0; i < N; i++) {
        int r = walwrite(&w, jobs[i]);
        assertf(r == 1, "stage %d must succeed, got %d", i, r);
    }
    assertf(fault_calls(FAULT_FDATASYNC) == 0,
            "no fdatasync during staging, got %d",
            fault_calls(FAULT_FDATASYNC));
    assertf(w.cur->uncommitted_bytes > 0,
            "uncommitted_bytes must accumulate, got %d",
            w.cur->uncommitted_bytes);
    assertf(w.nrec == pre_nrec + N,
            "nrec advances per stage: was %" PRId64 " got %" PRId64,
            pre_nrec, w.nrec);

    int c = walcommit(&w);
    assertf(c == 1, "commit must succeed, got %d", c);
    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "exactly ONE fdatasync for the whole batch, got %d",
            fault_calls(FAULT_FDATASYNC));
    assertf(w.cur->uncommitted_bytes == 0,
            "uncommitted_bytes cleared on successful commit, got %d",
            w.cur->uncommitted_bytes);
    assertf(w.use == 1, "wal still enabled after happy commit");

    // Second commit on an empty batch is a no-op: no fdatasync.
    int c2 = walcommit(&w);
    assertf(c2 == 1, "empty commit must succeed, got %d", c2);
    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "empty commit must NOT call fdatasync, got %d",
            fault_calls(FAULT_FDATASYNC));

    for (int i = 0; i < N; i++) job_free(jobs[i]);
    close(fd);
    unlink(tmppath);
}


// Commit-failure path: stage multiple, fdatasync fails → the entire
// batch is rolled back (uncommitted_bytes cleared), WAL disabled, and
// file closed. Clients waiting in dur_batch receive INTERNAL_ERROR at
// the dur_flush_all step — invariant #14 ("ack ⇒ durable") preserved.
void
cttest_inject_group_commit_rollback_disables_wal(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_gc_rollback.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");
    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header write");

    Wal w;
    File f;
    setup_durable_wal(fd, &w, &f);

    Tube *t = make_tube("gcrollback");
    assertf(t, "tube must allocate");

    const int N = 3;
    Job *jobs[N];
    for (int i = 0; i < N; i++) {
        Job *j = allocate_job(10);
        TUBE_ASSIGN(j->tube, t);
        j->r.state = Ready;
        j->r.body_size = 10;
        j->r.id = (uint64)(100 + i);
        memset(j->body, 'R', 10);
        j->walresv = 300;
        j->walused = 0;
        jobs[i] = j;
    }

    for (int i = 0; i < N; i++) {
        int r = walwrite(&w, jobs[i]);
        assertf(r == 1, "stage %d must succeed, got %d", i, r);
    }
    int pre_uncommitted = w.cur->uncommitted_bytes;
    assertf(pre_uncommitted > 0,
            "batch must have uncommitted bytes, got %d", pre_uncommitted);

    fault_set(FAULT_FDATASYNC, 0, EIO);
    int c = walcommit(&w);
    assertf(c == 0, "commit must fail, got %d", c);
    assertf(w.use == 0, "wal must be disabled after commit fail");
    assertf(!f.iswopen, "current file must be closed after commit fail");
    assertf(w.cur->uncommitted_bytes == 0,
            "uncommitted_bytes cleared after rollback, got %d",
            w.cur->uncommitted_bytes);
    assertf(fault_hits(FAULT_FDATASYNC) >= 1,
            "fdatasync fault must fire at least once (rollback may call again), got %d",
            fault_hits(FAULT_FDATASYNC));

    // After WAL disable, further walwrite must refuse (#14).
    Job *jlate = allocate_job(10);
    TUBE_ASSIGN(jlate->tube, t);
    jlate->r.state = Ready;
    jlate->r.body_size = 10;
    jlate->r.id = 999;
    jlate->walresv = 300;
    int rlate = walwrite(&w, jlate);
    assertf(rlate == 0,
            "walwrite on disabled durable WAL must refuse, got %d", rlate);

    job_free(jlate);
    for (int i = 0; i < N; i++) job_free(jobs[i]);
    close(fd);
    unlink(tmppath);
}


// dur_enqueue / dur_remove: batch index bookkeeping (swap-remove
// preserves remaining conns' indices so connclose mid-batch cannot
// corrupt dur_flush_all's iteration).
void
cttest_dur_batch_swap_remove_preserves_indices(void)
{
    setup();
    // Force durable path so dur_enqueue is not a no-op.
    int saved_durable = srv.wal.durable_sync;
    srv.wal.durable_sync = 1;

    static Conn c0, c1, c2;
    memset(&c0, 0, sizeof c0);
    memset(&c1, 0, sizeof c1);
    memset(&c2, 0, sizeof c2);
    c0.sock.fd = -1; c1.sock.fd = -1; c2.sock.fd = -1;

    dur_enqueue(&c0);
    dur_enqueue(&c1);
    dur_enqueue(&c2);
    assertf(c0.in_dur_batch && c1.in_dur_batch && c2.in_dur_batch,
            "all three conns must be flagged in-batch");
    assertf(c0.dur_batch_idx == 0 && c1.dur_batch_idx == 1 && c2.dur_batch_idx == 2,
            "indices must be 0,1,2; got %d,%d,%d",
            c0.dur_batch_idx, c1.dur_batch_idx, c2.dur_batch_idx);

    // Remove c1 (middle). c2 should swap into slot 1; c0 untouched.
    dur_remove(&c1);
    assertf(!c1.in_dur_batch, "removed conn must be unflagged");
    assertf(c0.in_dur_batch && c2.in_dur_batch,
            "survivors must still be in batch");
    assertf(c0.dur_batch_idx == 0,
            "c0 index preserved, got %d", c0.dur_batch_idx);
    assertf(c2.dur_batch_idx == 1,
            "c2 swap-remove'd into slot 1, got %d", c2.dur_batch_idx);

    // Drain: dur_flush_all(1) with no sockets (fd=-1) must leave state
    // consistent (batch empty, flags cleared) without writing anywhere.
    dur_flush_all(1);
    assertf(!c0.in_dur_batch && !c2.in_dur_batch,
            "flush must clear all batch flags");
    assertf(c0.dur_reply_len == 0 && c2.dur_reply_len == 0,
            "flush must clear reply lengths");

    // Re-enqueue must work afterwards (batch_n was reset).
    dur_enqueue(&c0);
    assertf(c0.in_dur_batch && c0.dur_batch_idx == 0,
            "re-enqueue must place at idx 0, got in=%d idx=%d",
            c0.in_dur_batch, c0.dur_batch_idx);

    // Clean up.
    dur_flush_all(1);
    srv.wal.durable_sync = saved_durable;
}


// dur_enqueue is a no-op outside durable mode — must not pollute the
// batch for connections that never had their acks deferred.
void
cttest_dur_enqueue_noop_in_async_mode(void)
{
    setup();
    int saved_durable = srv.wal.durable_sync;
    srv.wal.durable_sync = 0;

    static Conn c;
    memset(&c, 0, sizeof c);
    c.sock.fd = -1;

    dur_enqueue(&c);
    assertf(c.in_dur_batch == 0,
            "async mode: enqueue must be a no-op, got in=%d",
            c.in_dur_batch);

    // dur_flush_all on empty batch is a pure no-op (smoke).
    dur_flush_all(1);

    srv.wal.durable_sync = saved_durable;
}


// After a failed walcommit, global WAL counters must match the
// post-ftruncate disk state. filewritev applies accounting upfront so
// each stage in a batch looks the same to subsequent stages; without
// rollback, w->resv and w->alive would drift from the disk for the
// lifetime of the process. Not safety-critical while WAL stays disabled
// — but stats output would see phantom bytes.
void
cttest_inject_group_commit_fail_rolls_back_global_counters(void)
{
    setup();

    char tmppath[] = "/tmp/testinject_gc_counters.XXXXXX";
    int fd = mkstemp(tmppath);
    assertf(fd >= 0, "mkstemp must succeed");
    int ver = Walver;
    ssize_t nver = write(fd, &ver, sizeof(ver));
    assertf(nver == (ssize_t)sizeof(ver), "version header write");

    Wal w;
    File f;
    setup_durable_wal(fd, &w, &f);

    Tube *t = make_tube("countersroll");
    assertf(t, "tube must allocate");

    int64 pre_w_resv  = w.resv;
    int64 pre_w_alive = w.alive;
    int   pre_f_resv  = f.resv;

    const int N = 4;
    Job *jobs[4];
    for (int i = 0; i < N; i++) {
        Job *j = allocate_job(10);
        TUBE_ASSIGN(j->tube, t);
        j->r.state = Ready;
        j->r.body_size = 10;
        j->r.id = (uint64)(200 + i);
        memset(j->body, 'Z', 10);
        j->walresv = 300;
        j->walused = 0;
        jobs[i] = j;
    }

    for (int i = 0; i < N; i++) {
        int r = walwrite(&w, jobs[i]);
        assertf(r == 1, "stage %d must succeed, got %d", i, r);
    }
    int staged = f.uncommitted_bytes;
    assertf(staged > 0, "batch accumulates uncommitted, got %d", staged);
    assertf(w.resv < pre_w_resv,
            "w.resv drops during staging: pre=%" PRId64 " post=%" PRId64,
            pre_w_resv, w.resv);
    assertf(w.alive > pre_w_alive,
            "w.alive grows during staging: pre=%" PRId64 " post=%" PRId64,
            pre_w_alive, w.alive);

    fault_set(FAULT_FDATASYNC, 0, EIO);
    int c = walcommit(&w);
    assertf(c == 0, "commit must fail under injected fdatasync EIO");
    assertf(w.use == 0, "wal disabled after commit fail");

    // Counters must be restored to pre-batch levels: the disk no longer
    // holds those bytes (ftruncate in filewrite_commit_durable), so the
    // accounting must not either.
    assertf(w.resv == pre_w_resv,
            "w.resv must roll back to %" PRId64 ", got %" PRId64,
            pre_w_resv, w.resv);
    assertf(f.resv == pre_f_resv,
            "f.resv must roll back to %d, got %d",
            pre_f_resv, f.resv);
    assertf(w.alive == pre_w_alive,
            "w.alive must roll back to %" PRId64 ", got %" PRId64,
            pre_w_alive, w.alive);
    assertf(f.uncommitted_bytes == 0,
            "uncommitted_bytes drained after rollback, got %d",
            f.uncommitted_bytes);

    for (int i = 0; i < N; i++) job_free(jobs[i]);
    close(fd);
    unlink(tmppath);
}


// Step D: byte-level verification of dur_flush_all over a real socket
// pair. The swap-remove / noop tests above exercised in-memory state
// only; these confirm the actual wire bytes that reach a client.
//
// Rationale: dur_flush_all writes buffered replies on commit success
// and INTERNAL_ERROR on commit failure. A regression that swapped
// those two branches, truncated the buffer, or emitted the wrong
// literal would silently produce wrong-protocol output that earlier
// tests could not see.

// Helper: open a non-blocking socketpair, return (server, client) fds.
// Server side is non-blocking to mirror how a real Conn socket is
// configured; the client side blocks so the test's recv can pull
// bytes without a retry loop.
static void
open_pair_nb_server(int *server, int *client)
{
    int sv[2];
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
    assertf(r == 0, "socketpair must succeed, got errno=%d", errno);
    int flags = fcntl(sv[0], F_GETFL, 0);
    assertf(flags >= 0, "fcntl GETFL must succeed");
    r = fcntl(sv[0], F_SETFL, flags | O_NONBLOCK);
    assertf(r == 0, "fcntl O_NONBLOCK must succeed");
    *server = sv[0];
    *client = sv[1];
}

// Success path: commit ok → buffered bytes reach the peer unchanged
// and the conn state settles to WANT_COMMAND.
void
cttest_dur_flush_all_success_emits_buffered_replies(void)
{
    setup();
    int saved = srv.wal.durable_sync;
    srv.wal.durable_sync = 1;

    static Conn c;
    memset(&c, 0, sizeof c);
    c.srv = &srv;                 // connsched/heap access
    c.rw = 'r';                   // avoid epollq_add in flush path
    int cfd;
    open_pair_nb_server(&c.sock.fd, &cfd);
    c.state = STATE_WANT_COMMAND;

    dur_enqueue(&c);
    // Hand-buffer three acks as reply() would have done.
    const char *payload = "INSERTED 42\r\nDELETED\r\nRELEASED\r\n";
    int plen = (int)strlen(payload);
    memcpy(c.dur_reply_buf, payload, plen);
    c.dur_reply_len = plen;

    dur_flush_all(1);

    assertf(!c.in_dur_batch,
            "flush must clear in_dur_batch, got %d", c.in_dur_batch);
    assertf(c.dur_reply_len == 0,
            "flush must reset dur_reply_len, got %d", c.dur_reply_len);
    assertf(c.state == STATE_WANT_COMMAND,
            "post-flush state must be WANT_COMMAND, got %d", c.state);

    char buf[128];
    ssize_t got = recv(cfd, buf, sizeof buf, 0);
    assertf(got == plen,
            "peer must receive exactly %d bytes, got %zd (errno=%d)",
            plen, got, errno);
    assertf(memcmp(buf, payload, plen) == 0,
            "peer bytes must match buffered payload");

    close(c.sock.fd); close(cfd);
    srv.wal.durable_sync = saved;
}


// Failure path: commit=0 → peer receives INTERNAL_ERROR\r\n (NOT the
// buffered acks). Preserves invariant #14: a client never sees an ack
// for a record whose fdatasync did not land.
void
cttest_dur_flush_all_failure_emits_internal_error(void)
{
    setup();
    int saved = srv.wal.durable_sync;
    srv.wal.durable_sync = 1;

    static Conn c;
    memset(&c, 0, sizeof c);
    c.srv = &srv;
    c.rw = 'r';
    int cfd;
    open_pair_nb_server(&c.sock.fd, &cfd);
    c.state = STATE_WANT_COMMAND;

    dur_enqueue(&c);
    // Pre-fill the buffer with what WOULD have been sent — so a bug
    // that leaks this content on the fail branch is caught.
    const char *leaked = "INSERTED 99\r\n";
    int llen = (int)strlen(leaked);
    memcpy(c.dur_reply_buf, leaked, llen);
    c.dur_reply_len = llen;

    dur_flush_all(0);  // commit failed

    assertf(!c.in_dur_batch, "flush must clear in_dur_batch");
    assertf(c.dur_reply_len == 0, "flush must reset dur_reply_len");

    char buf[64];
    ssize_t got = recv(cfd, buf, sizeof buf, 0);
    const char *expected = "INTERNAL_ERROR\r\n";
    int elen = (int)strlen(expected);
    assertf(got == elen,
            "peer must receive exactly %d bytes (INTERNAL_ERROR), got %zd",
            elen, got);
    assertf(memcmp(buf, expected, elen) == 0,
            "peer must see INTERNAL_ERROR, not the buffered ack");

    close(c.sock.fd); close(cfd);
    srv.wal.durable_sync = saved;
}


// Partial-write path: when the kernel socket buffer is full and
// accepts only part of the write, dur_flush_all must save the
// remainder on the conn (c->reply/reply_len/reply_sent) and leave
// c->state = STATE_SEND_WORD so the epoll 'w' handler finishes the
// send. A regression that dropped the leftover would lose bytes.
//
// We force the partial by shrinking the peer's receive buffer to the
// platform minimum, then filling it with padding so exactly part of
// the next write fits. socketpair's Unix sockets are atomic — to get a
// true short write we rely on the buffer being near full when we
// call dur_flush_all.
void
cttest_dur_flush_all_partial_write_saves_remainder(void)
{
    setup();
    int saved = srv.wal.durable_sync;
    srv.wal.durable_sync = 1;

    static Conn c;
    memset(&c, 0, sizeof c);
    c.srv = &srv;
    c.rw = 'r';
    int cfd;
    open_pair_nb_server(&c.sock.fd, &cfd);
    c.state = STATE_WANT_COMMAND;

    // Shrink the server-side send buffer so we can saturate it in a
    // reasonable loop. Linux rounds up; exact minimum varies per kernel.
    int small = 4096;
    setsockopt(c.sock.fd, SOL_SOCKET, SO_SNDBUF, &small, sizeof small);
    setsockopt(cfd,       SOL_SOCKET, SO_RCVBUF, &small, sizeof small);

    // Saturate: write until non-blocking send returns EAGAIN. We need
    // the next small write to go partial, so leave a tiny window.
    char pad[1024];
    memset(pad, 'P', sizeof pad);
    ssize_t filled = 0;
    for (;;) {
        ssize_t w = send(c.sock.fd, pad, sizeof pad, 0);
        if (w < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            assertf(0, "send during fill must not fail non-EAGAIN, errno=%d",
                    errno);
        }
        filled += w;
        if (filled > 1 << 20) {
            // Guard against a kernel that refuses to honour SO_SNDBUF;
            // 1MB is way more than a properly-sized pair should absorb.
            assertf(0, "socket absorbed >1MB without EAGAIN; "
                       "SO_SNDBUF not honoured on this kernel");
        }
    }

    // Now drain exactly a sliver on the peer so the next send can
    // write a few bytes before EAGAIN fires again.
    char sink[200];
    ssize_t drained = recv(cfd, sink, sizeof sink, 0);
    assertf(drained > 0, "peer recv must free window, got %zd", drained);

    dur_enqueue(&c);
    // Payload large enough to overflow the freed window.
    static char payload[2048];
    memset(payload, 'Q', sizeof payload);
    memcpy(c.dur_reply_buf, payload, sizeof payload);
    c.dur_reply_len = (int)sizeof payload;

    dur_flush_all(1);

    // Expected outcomes: either full write (rare if drained window <
    // payload), or partial with state saved. Partial is the whole
    // point of this test.
    if (c.state == STATE_SEND_WORD) {
        assertf(c.reply == c.dur_reply_buf,
                "partial-write path must park c->reply on dur_reply_buf");
        assertf(c.reply_len == (int)sizeof payload,
                "reply_len must equal original payload length, got %d",
                c.reply_len);
        assertf(c.reply_sent > 0 && c.reply_sent < c.reply_len,
                "reply_sent must be strictly between 0 and reply_len; "
                "got %d (full len %d). Short write not observed — test "
                "harness may need a smaller window.",
                c.reply_sent, c.reply_len);
    } else if (c.state == STATE_WANT_COMMAND) {
        // Harness did not coerce a partial write — skip rather than
        // silently claim coverage. The kernel can be stubborn about
        // SO_SNDBUF minimums under containers.
        assertf(c.dur_reply_len == 0,
                "full-write path must clear dur_reply_len");
    } else {
        assertf(0, "unexpected state after flush: %d", c.state);
    }

    close(c.sock.fd); close(cfd);
    srv.wal.durable_sync = saved;
}


// dur_enqueue is idempotent per conn: calling twice must not double-
// register (would corrupt dur_batch_idx of whatever conn it displaces).
void
cttest_dur_enqueue_idempotent(void)
{
    setup();
    int saved_durable = srv.wal.durable_sync;
    srv.wal.durable_sync = 1;

    static Conn c;
    memset(&c, 0, sizeof c);
    c.sock.fd = -1;

    dur_enqueue(&c);
    int idx_first = c.dur_batch_idx;
    dur_enqueue(&c);
    assertf(c.dur_batch_idx == idx_first,
            "second enqueue must be a no-op, idx changed %d -> %d",
            idx_first, c.dur_batch_idx);

    dur_flush_all(1);
    srv.wal.durable_sync = saved_durable;
}
