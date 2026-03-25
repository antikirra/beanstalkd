#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static Tube *stube;

void
cttest_heap_stress_10k_deterministic()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int N = 10000;
    int i;
    srand(42); /* deterministic */

    for (i = 0; i < N; i++) {
        Job *j = make_job(rand() % 65536, 0, 1, 0, 0);
        assertf(j, "alloc %d", i);
        assertf(heapinsert(&h, j), "insert %d", i);
    }
    assertf((int)h.len == N, "heap must have %d items", N);

    uint32 last_pri = 0;
    uint64 last_id = 0;
    for (i = 0; i < N; i++) {
        Job *j = heapremove(&h, 0);
        assertf(j, "remove %d", i);
        /* verify strict ordering: by priority, then by id for ties */
        if (j->r.pri == last_pri) {
            assertf(j->r.id > last_id, "tie-break by id failed at %d: id %llu <= %llu",
                    i, (unsigned long long)j->r.id, (unsigned long long)last_id);
        } else {
            assertf(j->r.pri > last_pri, "priority order failed at %d", i);
        }
        last_pri = j->r.pri;
        last_id = j->r.id;
        job_free(j);
    }
    assertf(h.len == 0, "heap must be empty");
    free(h.data);
}

void
cttest_tube_stress_with_collision()
{
    ms_init(&tubes, NULL, NULL);
    const int N = 1000;
    int i;
    char name[32];

    for (i = 0; i < N; i++) {
        snprintf(name, sizeof name, "s-%d", i);
        Tube *t = tube_find_or_make(name);
        assertf(t, "tube %d must exist", i);
        tube_iref(t);
    }
    assertf((int)tubes.len == N, "must have %d tubes", N);

    /* verify hash finds every single one */
    for (i = 0; i < N; i++) {
        snprintf(name, sizeof name, "s-%d", i);
        Tube *t = tube_find_name(name);
        assertf(t, "hash must find s-%d", i);
        assertf(strcmp(t->name, name) == 0, "name mismatch for %d", i);
    }

    /* remove every OTHER tube — stress collision chain removal */
    for (i = 0; i < N; i += 2) {
        snprintf(name, sizeof name, "s-%d", i);
        Tube *t = tube_find_name(name);
        assertf(t, "must find before remove");
        tube_dref(t);
    }

    /* verify odd tubes still findable */
    for (i = 1; i < N; i += 2) {
        snprintf(name, sizeof name, "s-%d", i);
        assertf(tube_find_name(name), "odd tube s-%d must survive", i);
    }

    /* verify even tubes gone */
    for (i = 0; i < N; i += 2) {
        snprintf(name, sizeof name, "s-%d", i);
        assertf(tube_find_name(name) == NULL, "even tube s-%d must be gone", i);
    }

    /* cleanup remaining */
    for (i = 1; i < N; i += 2) {
        snprintf(name, sizeof name, "s-%d", i);
        tube_dref(tube_find_name(name));
    }
    ms_clear(&tubes);
}

void
cttest_job_hash_churn()
{
    TUBE_ASSIGN(stube, make_tube("churn"));
    const int N = 5000;
    int i;

    Job **jobs = calloc(N, sizeof(Job*));
    for (i = 0; i < N; i++) {
        jobs[i] = make_job(i, 0, 1, 0, stube);
        assertf(jobs[i], "alloc job %d", i);
    }
    assertf(get_all_jobs_used() >= (size_t)N, "must have >= %d jobs", N);

    /* every job must be findable */
    for (i = 0; i < N; i++) {
        Job *found = job_find(jobs[i]->r.id);
        assertf(found == jobs[i], "must find exact job %d", i);
    }

    /* free all and verify gone */
    for (i = 0; i < N; i++) {
        uint64 id = jobs[i]->r.id;
        job_free(jobs[i]);
        assertf(job_find(id) == NULL, "freed job %d must be gone", i);
    }
    free(jobs);
}

void
cttest_job_copy_independence()
{
    TUBE_ASSIGN(stube, make_tube("copytest"));
    Job *j = make_job(1, 0, 1000000000, 8, stube);
    memcpy(j->body, "original", 8);
    j->r.state = Ready;

    Job *c = job_copy(j);
    assertf(c, "copy must succeed");

    /* mutate original */
    j->r.pri = 999;
    memcpy(j->body, "MODIFIED", 8);
    j->r.state = Buried;

    /* copy must be UNTOUCHED */
    assertf(c->r.pri == 1, "copy pri must be original value, got %u", c->r.pri);
    assertf(memcmp(c->body, "original", 8) == 0, "copy body must be original");
    assertf(c->r.state == Copy, "copy state must remain Copy");

    job_free(c);
    job_free(j);
}

void
cttest_heap_remove_preserves_order()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int N = 50;
    int pos, i;
    srand(123);

    for (pos = 0; pos < N; pos++) {
        /* fresh heap each iteration */
        h.len = 0;
        h.cap = 0;
        free(h.data);
        h.data = NULL;

        for (i = 0; i < N; i++) {
            Job *j = make_job(rand() % 500, 0, 1, 0, 0);
            heapinsert(&h, j);
        }

        /* remove from position pos */
        Job *removed = heapremove(&h, pos);
        assertf(removed, "remove at %d must work", pos);
        job_free(removed);

        /* VERIFY COMPLETE HEAP ORDER of remaining */
        uint32 last_pri = 0;
        for (i = 0; i < N - 1; i++) {
            Job *j = heapremove(&h, 0);
            assertf(j, "get %d after removing pos %d", i, pos);
            assertf(j->r.pri >= last_pri,
                    "order broken: pos=%d i=%d pri=%u < last=%u", pos, i, j->r.pri, last_pri);
            last_pri = j->r.pri;
            job_free(j);
        }
        assertf(h.len == 0, "heap must be empty after pos=%d", pos);
    }
    free(h.data);
}

void
cttest_heap_stability_equal_priority()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int N = 100;
    int i;

    /* all jobs have SAME priority — order must be by ID */
    for (i = 0; i < N; i++) {
        Job *j = make_job(42, 0, 1, 0, 0);
        heapinsert(&h, j);
    }

    uint64 last_id = 0;
    for (i = 0; i < N; i++) {
        Job *j = heapremove(&h, 0);
        assertf(j->r.pri == 42, "priority must be 42");
        assertf(j->r.id > last_id, "equal-pri jobs must come out in ID order: %llu <= %llu",
                (unsigned long long)j->r.id, (unsigned long long)last_id);
        last_id = j->r.id;
        job_free(j);
    }
    free(h.data);
}
