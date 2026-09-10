#include "dat.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "ct/ct.h"


// assert_job_positions checks the promise job_setpos exists for: every
// live element records the slot it actually occupies. prot.c
// (remove_ready_job / remove_delayed_job) calls heapremove(h,
// j->heap_index) with no further validation, so a stale or truncated
// index silently removes an innocent job and reports success.
static void
assert_job_positions(Heap *h)
{
    size_t i;
    for (i = 0; i < h->len; i++) {
        Job *j = (Job *)h->data[i];
        assertf(j->heap_index == i,
                "job %"PRIu64" sits in slot %zu but records heap_index %zu; "
                "prot.c removes jobs by that cached index",
                j->r.id, i, j->heap_index);
    }
}


void
cttest_heap_insert_one()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j = make_job(1, 0, 1, 0, 0);
    assertf(j, "allocate job");

    heapinsert(&h, j);
    assertf(h.len == 1, "h should contain one item.");
    assertf(j->heap_index == 0, "should match");

    assert(heapremove(&h, 0));
    job_free(j);
    free(h.data);
}

void
cttest_heap_insert_and_remove_one()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j1 = make_job(1, 0, 1, 0, 0);
    assertf(j1, "allocate job");

    size_t stale = 4242;
    j1->heap_index = stale;

    int r = heapinsert(&h, j1);
    assertf(r, "insert should succeed");
    assertf(j1->heap_index < h.len,
            "heapinsert must record a live slot, not leave the stale index %zu "
            "(len %zu)", stale, h.len);
    assertf(h.data[j1->heap_index] == j1,
            "the job must be exactly where it claims to be: slot %zu holds %p, want %p",
            j1->heap_index, h.data[j1->heap_index], (void *)j1);

    Job *got = heapremove(&h, 0);
    assertf(got == j1, "j1 should come back out");
    assertf(h.len == 0, "h should be empty.");

    j1->heap_index = stale;
    r = heapinsert(&h, j1);
    assertf(r, "re-insert should succeed");
    got = heapremove(&h, j1->heap_index);
    assertf(got == j1,
            "re-insert must overwrite the stale index the removal left behind "
            "(heap.c leans on exactly that): removing at the recorded index gave "
            "%p, want %p", (void *)got, (void *)j1);
    assertf(h.len == 0, "the heap must be empty after the second removal");

    free(h.data);
    job_free(j1);
}

void
cttest_heap_priority()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    Job *j, *j1, *j2, *j3;

    j1 = make_job(1, 0, 1, 0, 0);
    j2 = make_job(2, 0, 1, 0, 0);
    j3 = make_job(3, 0, 1, 0, 0);
    assertf(j1, "allocate job");
    assertf(j2, "allocate job");
    assertf(j3, "allocate job");

    j1->heap_index = 91;
    j2->heap_index = 92;
    j3->heap_index = 93;

    int r = heapinsert(&h, j2);
    assertf(r, "insert should succeed");
    assertf(j2->heap_index == 0,
            "should match: the only element must record slot 0, records %zu",
            j2->heap_index);
    assert_job_positions(&h);

    r = heapinsert(&h, j3);
    assertf(r, "insert should succeed");
    assertf(j2->heap_index == 0, "should match: j2 stays at the root, records %zu",
            j2->heap_index);
    assertf(j3->heap_index == 1, "should match: j3 lands in slot 1, records %zu",
            j3->heap_index);
    assert_job_positions(&h);

    r = heapinsert(&h, j1);
    assertf(r, "insert should succeed");
    assertf(j1->heap_index == 0, "should match: j1 is the new root, records %zu",
            j1->heap_index);
    assertf(j2->heap_index == 2, "should match: j2 was pushed down, records %zu",
            j2->heap_index);
    assertf(j3->heap_index == 1, "should match: j3 stays put, records %zu",
            j3->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j1, "j1 should come out first.");
    assertf(j2->heap_index == 0, "should match: j2 became the root, records %zu",
            j2->heap_index);
    assertf(j3->heap_index == 1, "should match: j3 stays put, records %zu",
            j3->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j2, "j2 should come out second.");
    assertf(j3->heap_index == 0, "should match: j3 became the root, records %zu",
            j3->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j3, "j3 should come out third.");

    /* deeper than the first level: slots 3..DEEP-1 must be recorded too,
     * and the priority order must still hold on the way out */
    enum { DEEP = 12 };
    Job *many[DEEP], *deep, *got;
    uint last_pri;
    int i;

    for (i = 0; i < DEEP; i++) {
        many[i] = make_job((uint32)(DEEP - i), 0, 1, 0, 0);
        assertf(many[i], "allocate job %d", i);
        many[i]->heap_index = 999;
        assertf(heapinsert(&h, many[i]), "insert %d should succeed", i);
        assert_job_positions(&h);
    }
    assertf(h.len == (size_t)DEEP, "the heap must hold %d jobs, got %zu",
            DEEP, h.len);

    deep = (Job *)h.data[DEEP - 1];
    got = heapremove(&h, deep->heap_index);
    assertf(got == deep,
            "removing at the index the heap recorded must return that job: got %p, want %p",
            (void *)got, (void *)deep);
    job_free(got);
    assert_job_positions(&h);

    last_pri = 0;
    while (h.len) {
        j = heapremove(&h, 0);
        assertf(j != NULL, "every remaining job must come back out");
        assertf(j->r.pri >= last_pri,
                "priority order must hold on the drain: pri %u came after pri %u",
                j->r.pri, last_pri);
        last_pri = j->r.pri;
        job_free(j);
    }

    free(h.data);
    job_free(j1);
    job_free(j2);
    job_free(j3);
}

void
cttest_heap_fifo_property()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    Job *j, *j3a, *j3b, *j3c;

    j3a = make_job(3, 0, 1, 0, 0);
    j3b = make_job(3, 0, 1, 0, 0);
    j3c = make_job(3, 0, 1, 0, 0);
    assertf(j3a, "allocate job");
    assertf(j3b, "allocate job");
    assertf(j3c, "allocate job");

    j3a->heap_index = 71;
    j3b->heap_index = 72;
    j3c->heap_index = 73;

    int r = heapinsert(&h, j3a);
    assertf(r, "insert should succeed");
    assertf(h.data[0] == j3a, "j3a should be in pos 0");
    assertf(j3a->heap_index == 0, "should match: j3a records %zu, want slot 0",
            j3a->heap_index);
    assert_job_positions(&h);

    r = heapinsert(&h, j3b);
    assertf(r, "insert should succeed");
    assertf(h.data[1] == j3b, "j3b should be in pos 1");
    assertf(j3a->heap_index == 0, "should match: j3a records %zu, want slot 0",
            j3a->heap_index);
    assertf(j3b->heap_index == 1, "should match: j3b records %zu, want slot 1",
            j3b->heap_index);
    assert_job_positions(&h);

    r = heapinsert(&h, j3c);
    assertf(r, "insert should succeed");
    assertf(h.data[2] == j3c, "j3c should be in pos 2");
    assertf(j3a->heap_index == 0, "should match: j3a records %zu, want slot 0",
            j3a->heap_index);
    assertf(j3b->heap_index == 1, "should match: j3b records %zu, want slot 1",
            j3b->heap_index);
    assertf(j3c->heap_index == 2, "should match: j3c records %zu, want slot 2",
            j3c->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j3a, "j3a should come out first.");
    assertf(j3b->heap_index == 0, "should match: j3b records %zu, want slot 0",
            j3b->heap_index);
    assertf(j3c->heap_index == 1, "should match: j3c records %zu, want slot 1",
            j3c->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j3b, "j3b should come out second.");
    assertf(j3c->heap_index == 0, "should match: j3c records %zu, want slot 0",
            j3c->heap_index);
    assert_job_positions(&h);

    j = heapremove(&h, 0);
    assertf(j == j3c, "j3c should come out third.");

    /* FIFO must hold past the first level of the 4-ary tree, where a
     * position mutation that only bites for slots >= 3 would hide */
    enum { QN = 8 };
    Job *q[QN], *tail, *got;
    uint64 last_id;
    int i, seen;

    for (i = 0; i < QN; i++) {
        q[i] = make_job(3, 0, 1, 0, 0);
        assertf(q[i], "allocate job %d", i);
        q[i]->heap_index = 777;
        assertf(heapinsert(&h, q[i]), "insert %d should succeed", i);
        assert_job_positions(&h);
    }

    tail = (Job *)h.data[QN - 1];
    got = heapremove(&h, tail->heap_index);
    assertf(got == tail,
            "removing at the index the heap recorded must return that job: got %p, want %p",
            (void *)got, (void *)tail);
    assert_job_positions(&h);

    last_id = 0;
    seen = 0;
    while (h.len) {
        j = heapremove(&h, 0);
        assertf(j != NULL, "every remaining job must come back out");
        assertf(j->r.id > last_id,
                "equal priorities must drain oldest first: id %"PRIu64" came after id %"PRIu64,
                j->r.id, last_id);
        last_id = j->r.id;
        seen++;
        job_free(j);
    }
    assertf(seen == QN - 1,
            "the heap must give back every job it still held: %d of %d", seen, QN - 1);
    job_free(tail);

    free(h.data);
    job_free(j3a);
    job_free(j3b);
    job_free(j3c);
}

void
cttest_heap_many_jobs()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int n = 20;
    Job *j, *probe, *got;

    srand(4321);   /* fixed seed: a red must name a reproducible sample */

    int i;
    for (i = 0; i < n; i++) {
        j = make_job(1 + rand() % 8192, 0, 1, 0, 0);
        assertf(j, "allocation");
        j->heap_index = 555;
        int r = heapinsert(&h, j);
        assertf(r, "heapinsert");
    }
    assertf(h.len == (size_t)n, "the heap must hold all %d jobs, got %zu", n, h.len);
    assert_job_positions(&h);

    probe = make_job(4096, 0, 1, 0, 0);
    assertf(probe, "allocation");
    probe->heap_index = 555;
    assertf(heapinsert(&h, probe), "heapinsert");
    assert_job_positions(&h);

    got = heapremove(&h, probe->heap_index);
    assertf(got == probe,
            "removing at the index the heap recorded must return that job: got %p, want %p",
            (void *)got, (void *)probe);
    job_free(probe);
    assert_job_positions(&h);

    uint last_pri = 0;
    uint64 last_id = 0;
    for (i = 0; i < n; i++) {
        j = heapremove(&h, 0);
        assert(j);
        assertf(j->r.pri > last_pri || (j->r.pri == last_pri && j->r.id > last_id),
                "should come out in order: pri %u id %"PRIu64" came after pri %u id %"PRIu64,
                j->r.pri, j->r.id, last_pri, last_id);
        last_pri = j->r.pri;
        last_id = j->r.id;
        job_free(j);
    }
    assertf(h.len == 0, "the heap must be empty after %d removals, len is %zu", n, h.len);
    free(h.data);
}

typedef struct { int64 key; size_t pos; } HeapTestItem;

static int
heap_test_less(void *a, void *b)
{
    return ((HeapTestItem *)a)->key < ((HeapTestItem *)b)->key;
}

static void
heap_test_setpos(void *x, size_t i)
{
    ((HeapTestItem *)x)->pos = i;
}

static void
heap_test_assert_valid(Heap *h)
{
    size_t i;
    for (i = 0; i < h->len; i++) {
        HeapTestItem *it = h->data[i];
        assertf(it->pos == i, "pos hint must match slot");
        if (i > 0) {
            HeapTestItem *p = h->data[(i - 1) >> 2];
            assertf(p->key <= it->key, "min-heap invariant violated");
        }
    }
}

// Regression test for upstream beanstalkd PR #670: removing the last
// element when its key equals its parent's must not leave the removed
// element in the live heap (upstream used k <= h->len, causing a
// use-after-free once the caller freed the returned element).
void
cttest_heap_remove_last_equal()
{
    Heap h = {
        .less = heap_test_less,
        .setpos = heap_test_setpos,
    };

    HeapTestItem a = { .key = 50 };
    HeapTestItem b = { .key = 100 };
    HeapTestItem c = { .key = 100 };
    HeapTestItem d = { .key = 100 };

    assertf(heapinsert(&h, &a), "insert a must succeed");
    assertf(heapinsert(&h, &b), "insert b must succeed");
    assertf(heapinsert(&h, &c), "insert c must succeed");
    assertf(heapinsert(&h, &d), "insert d must succeed");
    assertf(h.len == 4, "four inserts must give len 4, got %zu", h.len);
    heap_test_assert_valid(&h);

    void *expect = h.data[h.len - 1];
    void *got = heapremove(&h, h.len - 1);
    assertf(got == &d, "last element should come back");
    assertf(got == expect,
            "heapremove(h, k) must return exactly the element that sat in slot k: "
            "got %p, want %p", got, expect);

    size_t i;
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    assertf(h.len == 3, "len should drop to 3 after one removal");
    heap_test_assert_valid(&h);

    /* remove the remaining elements in an order that stresses equal
     * keys, checking ownership and the invariant after each removal */
    got = heapremove(&h, 0);
    assertf(got == &a, "min should come out first");
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    heap_test_assert_valid(&h);

    expect = h.data[h.len - 1];
    got = heapremove(&h, h.len - 1);
    assertf(got == expect,
            "an equal-key item should come back — the one in the slot asked for: "
            "got %p, want %p", got, expect);
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    heap_test_assert_valid(&h);

    expect = h.data[0];
    got = heapremove(&h, 0);
    assertf(got == expect,
            "the last equal-key item should come back — the one in slot 0: "
            "got %p, want %p", got, expect);
    assertf(h.len == 0, "heap should be empty");

    /* Same key everywhere: in a binary heap this is the exact setup
     * whose unconditional siftdown dragged the removed element back
     * into the live heap. Here every tail removal has a parent of equal
     * key — slot 0 for len 2..5, slot 1 for len 6..8 — so the guard is
     * exercised at both levels of the 4-ary tree. */
    HeapTestItem items[8];
    int j;
    for (j = 0; j < 8; j++) {
        items[j].key = 100;
        assertf(heapinsert(&h, &items[j]), "insert %d must succeed", j);
    }
    assertf(h.len == 8, "eight inserts must give len 8, got %zu", h.len);
    heap_test_assert_valid(&h);
    while (h.len > 0) {
        expect = h.data[h.len - 1];
        got = heapremove(&h, h.len - 1);
        assertf(got == expect,
                "the tail removal must return the tail element: got %p, want %p",
                got, expect);
        for (i = 0; i < h.len; i++) {
            assertf(h.data[i] != got, "removed item must not remain in the heap");
        }
        heap_test_assert_valid(&h);
    }

    free(h.data);

    /* The same tail-removal scenario driven through the callbacks the
     * server actually installs, so this file's position invariant is
     * spent on a real Job and not only on the test double. */
    Heap jh = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    Job *jobs[8];
    for (j = 0; j < 8; j++) {
        jobs[j] = make_job(100, 0, 1, 0, 0);
        assertf(jobs[j], "allocate job %d", j);
        jobs[j]->heap_index = 6161;
        assertf(heapinsert(&jh, jobs[j]), "insert job %d must succeed", j);
    }
    assert_job_positions(&jh);
    while (jh.len > 0) {
        void *want = jh.data[jh.len - 1];
        got = heapremove(&jh, jh.len - 1);
        assertf(got == want,
                "the tail job must come back out: got %p, want %p", got, want);
        assert_job_positions(&jh);
    }
    for (j = 0; j < 8; j++) {
        job_free(jobs[j]);
    }
    free(jh.data);
}

void
cttest_heap_remove_k()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int n = 50;
    const int mid = 25;

    srand(42);   /* fixed seed: a red must name a reproducible sample */

    int c, i;
    for (c = 0; c < 50; c++) {
        for (i = 0; i < n; i++) {
            Job *j = make_job(1 + rand() % 8192, 0, 1, 0, 0);
            assertf(j, "allocation");
            j->heap_index = 4242;
            int r = heapinsert(&h, j);
            assertf(r, "heapinsert");
        }
        assert_job_positions(&h);

        /* the production removal path: prot.c removes by the index the
         * heap itself recorded, never by a literal */
        Job *probe = make_job(1 + rand() % 8192, 0, 1, 0, 0);
        assertf(probe, "allocation");
        probe->heap_index = 4242;
        assertf(heapinsert(&h, probe), "heapinsert");
        Job *bycached = heapremove(&h, probe->heap_index);
        assertf(bycached == probe,
                "round %d: removing at the recorded index must return that job: "
                "got %p, want %p", c, (void *)bycached, (void *)probe);
        job_free(probe);

        /* remove one from the middle */
        Job *j0 = heapremove(&h, mid);
        assertf(j0, "j0 should not be NULL");
        job_free(j0);
        assert_job_positions(&h);

        /* now make sure the rest are still a valid heap */
        uint last_pri = 0;
        uint64 last_id = 0;
        for (i = 1; i < n; i++) {
            Job *j = heapremove(&h, 0);
            assertf(j, "j should not be NULL");
            assertf(j->r.pri > last_pri || (j->r.pri == last_pri && j->r.id > last_id),
                    "should come out in order: round %d, pri %u id %"PRIu64
                    " came after pri %u id %"PRIu64,
                    c, j->r.pri, j->r.id, last_pri, last_id);
            last_pri = j->r.pri;
            last_id = j->r.id;
            job_free(j);
        }
        assertf(h.len == 0, "round %d must drain the heap, len is %zu", c, h.len);
    }
    free(h.data);
}

void
ctbench_heap_insert(int n)
{
    Job **j = calloc(n, sizeof *j);
    assertf(j != NULL, "calloc of %d job slots must succeed", n);
    int i;
    for (i = 0; i < n; i++) {
        j[i] = make_job(1, 0, 1, 0, 0);
        assert(j[i]);
        j[i]->r.pri = -j[i]->r.id;
        j[i]->heap_index = 8181;
    }
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    ctresettimer();
    for (i = 0; i < n; i++) {
        heapinsert(&h, j[i]);
    }
    ctstoptimer();

    assertf(h.len == (size_t)n,
            "every insert must land in the heap: len %zu, want %d", h.len, n);
    assert_job_positions(&h);

    for (i = 0; i < n; i++)
        job_free(heapremove(&h, 0));
    free(h.data);
    free(j);
}

void
ctbench_heap_remove(int n)
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    int i;
    for (i = 0; i < n; i++) {
        Job *j = make_job(1, 0, 1, 0, 0);
        assertf(j, "allocate job");
        j->heap_index = 9191;
        assertf(heapinsert(&h, j), "insert %d must succeed", i);
    }
    assertf(n > 0, "n must be positive");
    assertf(h.len == (size_t)n,
            "every insert must land in the heap: len %zu, want %d", h.len, n);
    assert_job_positions(&h);
    Job **jj = calloc((unsigned)n, sizeof(Job *)); // temp storage to deallocate jobs later
    assertf(jj != NULL, "calloc of %d job slots must succeed", n);

    ctresettimer();
    for (i = 0; i < n; i++) {
        jj[i] = (Job *)heapremove(&h, 0);
    }
    ctstoptimer();

    assertf(h.len == 0, "the heap must be empty after %d removals, len is %zu", n, h.len);
    assertf(jj[0] != NULL, "the first removal must return a job");
    for (i = 1; i < n; i++) {
        assertf(jj[i] != NULL && jj[i]->r.id > jj[i - 1]->r.id,
                "equal priorities must drain oldest first: removal %d gave id "
                "%"PRIu64" after id %"PRIu64,
                i, jj[i] ? jj[i]->r.id : 0, jj[i - 1]->r.id);
    }

    free(h.data);
    for (i = 0; i < n; i++)
        job_free(jj[i]);
    free(jj);
}


// Removing the LAST element must leave the heap untouched — not even a
// self-assignment, because the slot write is followed by
// setpos(removed, k) and a sift over an index that is no longer part of
// the heap. The caller owns the returned element and is free to release
// it the moment it has it; a heap that wrote through it after handing
// it back is the shape upstream's #670 took. `h->data[i] != got` cannot
// see this: the element sits at data[len], one past what that loop
// walks. Its recorded position can.
void
cttest_heap_remove_last_writes_nothing_through_the_returned_element(void)
{
    Heap h = {
        .less = heap_test_less,
        .setpos = heap_test_setpos,
    };

    HeapTestItem a = { .key = 10 };
    HeapTestItem b = { .key = 20 };
    HeapTestItem c = { .key = 20 };
    assertf(heapinsert(&h, &a), "setup: insert a");
    assertf(heapinsert(&h, &b), "setup: insert b");
    assertf(heapinsert(&h, &c), "setup: insert c");
    heap_test_assert_valid(&h);

    size_t last = h.len - 1;
    HeapTestItem *doomed = h.data[last];
    size_t pos_before = doomed->pos;
    doomed->pos = (size_t)-1;          // poison: any write shows up

    void *got = heapremove(&h, last);
    assertf(got == doomed, "setup: the last slot's element must come back");

    assertf(doomed->pos == (size_t)-1,
            "heapremove must not write a position through an element it "
            "has just handed back (pos was %zu before, %zu after)",
            pos_before, doomed->pos);
    assertf(h.len == 2, "the heap must have shrunk by one, got %zu", h.len);
    heap_test_assert_valid(&h);

    // The survivors keep positions that address them.
    for (size_t i = 0; i < h.len; i++) {
        HeapTestItem *it = h.data[i];
        assertf(it->pos == i,
                "element %zu records position %zu", i, it->pos);
    }

    free(h.data);
}
