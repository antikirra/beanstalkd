#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

void
cttest_heap_remove_empty()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    void *got = heapremove(&h, 0);
    assertf(got == NULL, "remove from empty heap must return NULL");
}

void
cttest_heap_remove_out_of_bounds()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j = make_job(1, 0, 1, 0, 0);
    heapinsert(&h, j);

    void *got = heapremove(&h, 999);
    assertf(got == NULL, "out-of-bounds remove must return NULL");

    got = heapremove(&h, 0);
    assertf(got == j, "valid remove must work");

    free(h.data);
    job_free(j);
}

void
cttest_heap_remove_last()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *a = make_job(1, 0, 1, 0, 0);
    Job *b = make_job(2, 0, 1, 0, 0);
    Job *c = make_job(3, 0, 1, 0, 0);
    heapinsert(&h, a);
    heapinsert(&h, b);
    heapinsert(&h, c);

    /* remove last element by index */
    Job *got = heapremove(&h, h.len - 1);
    assertf(got != NULL, "removing last must succeed");
    assertf(h.len == 2, "heap must have 2 elements");

    /* remaining must still be valid heap */
    Job *first = heapremove(&h, 0);
    Job *second = heapremove(&h, 0);
    assertf(first->r.pri <= second->r.pri, "heap order must hold");

    free(h.data);
    job_free(a);
    job_free(b);
    job_free(c);
}

void
cttest_heap_delay_ordering()
{
    Heap h = {
        .less = job_delay_less,
        .setpos = job_setpos,
    };

    Job *a = make_job(1, 0, 1, 0, 0);
    Job *b = make_job(1, 0, 1, 0, 0);
    Job *c = make_job(1, 0, 1, 0, 0);
    a->r.deadline_at = 3000;
    b->r.deadline_at = 1000;
    c->r.deadline_at = 2000;

    heapinsert(&h, a);
    heapinsert(&h, b);
    heapinsert(&h, c);

    Job *first = heapremove(&h, 0);
    Job *second = heapremove(&h, 0);
    Job *third = heapremove(&h, 0);

    assertf(first == b, "earliest deadline must come first");
    assertf(second == c, "middle deadline second");
    assertf(third == a, "latest deadline last");

    free(h.data);
    job_free(a);
    job_free(b);
    job_free(c);
}


// ─── 4-ary heap invariant: every parent <= its 4 children ──────
// Walks every node after N inserts and verifies the heap property
// at every edge (k → 4k+1..4k+4). A binary-heap regression in
// heap_parent / child indices would fail this on N>4.
static int
verify_heap_invariant(Heap *h)
{
    for (size_t k = 0; k < h->len; k++) {
        size_t c = (k << 2) + 1;
        for (size_t i = 0; i < 4 && c + i < h->len; i++) {
            if (h->less(h->data[c + i], h->data[k]))
                return 0;  // child beats parent — invariant broken
        }
    }
    return 1;
}

void
cttest_heap_4ary_random_stress()
{
    enum { N = 10000 };
    srand(42);  // determinism per manifesto rule #8

    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job **jobs = calloc(N, sizeof *jobs);
    assertf(jobs, "calloc must succeed");

    // Insert N jobs with random priorities.
    for (int i = 0; i < N; i++) {
        jobs[i] = make_job((uint32_t)rand() & 0xFFFF, 0, 1, 0, 0);
        assertf(jobs[i], "make_job must succeed");
        assertf(heapinsert(&h, jobs[i]) == 1, "heapinsert must succeed");
    }

    assertf(verify_heap_invariant(&h),
            "4-ary heap invariant violated after %d inserts", N);

    // Extract all — must come out in non-decreasing priority.
    uint32 prev = 0;
    for (int i = 0; i < N; i++) {
        Job *j = heapremove(&h, 0);
        assertf(j, "heapremove must succeed at i=%d", i);
        assertf(j->r.pri >= prev,
                "heap order broken at i=%d: %u < previous %u",
                i, j->r.pri, prev);
        prev = j->r.pri;
        job_free(j);
    }

    free(h.data);
    free(jobs);
}

// ─── Interior removal must preserve the heap invariant ─────────
// heapremove(h, k) for arbitrary k is used by remove_buried_job,
// remove_delayed_job, and delay_tube_update. A 4-ary siftup bug
// (missed child, wrong index arithmetic) surfaces only when the
// replacement slot has 4 children instead of 2.
void
cttest_heap_4ary_interior_remove_invariant()
{
    enum { N = 500 };
    srand(42);

    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job **jobs = calloc(N, sizeof *jobs);
    assertf(jobs, "calloc must succeed");

    for (int i = 0; i < N; i++) {
        jobs[i] = make_job((uint32_t)rand() & 0x3FF, 0, 1, 0, 0);
        heapinsert(&h, jobs[i]);
    }

    // Phase 1: target nodes with 4 children (k where 4k+4 < len).
    // Exercises the full 4-way siftup: smallest-of-4 selection.
    for (int pass = 0; pass < 50; pass++) {
        if (h.len < 10) break;
        size_t k = (size_t)(rand() % (int)(h.len / 4));
        Job *j = heapremove(&h, k);
        assertf(j, "interior remove must return job at k=%zu", k);
        assertf(verify_heap_invariant(&h),
                "invariant broken after interior remove at k=%zu "
                "(pass %d, len=%zu)", k, pass, h.len);
        job_free(j);
    }

    // Phase 2: remove at ANY index including leaves and near-leaves
    // (k where fewer than 4 children exist). This exercises the
    // boundary conditions: 1, 2, 3 children and pure-leaf case
    // (c0 >= n, early-exit in siftup).
    for (int pass = 0; pass < 100; pass++) {
        if (h.len == 0) break;
        size_t k = (size_t)(rand() % (int)h.len);
        Job *j = heapremove(&h, k);
        assertf(j, "arbitrary remove must return job at k=%zu", k);
        assertf(verify_heap_invariant(&h),
                "invariant broken after arbitrary remove at k=%zu "
                "(pass %d, len=%zu)", k, pass, h.len);
        job_free(j);
    }

    // Drain.
    while (h.len) {
        Job *j = heapremove(&h, 0);
        job_free(j);
    }

    free(h.data);
    free(jobs);
}

// ─── Duplicate priorities: stable in the sense that heap order
// is consistent; no crash, every element retrievable exactly once.
void
cttest_heap_4ary_all_equal_keys()
{
    enum { N = 1000 };
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job **jobs = calloc(N, sizeof *jobs);
    for (int i = 0; i < N; i++) {
        jobs[i] = make_job(42, 0, 1, 0, 0);  // all same priority
        heapinsert(&h, jobs[i]);
    }

    assertf(h.len == N, "heap must hold N=%d, got %zu", N, h.len);
    assertf(verify_heap_invariant(&h),
            "invariant must hold with duplicate keys");

    int seen = 0;
    while (h.len) {
        Job *j = heapremove(&h, 0);
        assertf(j, "heapremove must succeed");
        assertf(j->r.pri == 42, "priority must be 42, got %u", j->r.pri);
        seen++;
        job_free(j);
    }
    assertf(seen == N, "must retrieve all N elements, got %d", seen);

    free(h.data);
    free(jobs);
}
