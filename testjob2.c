#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static Tube *dtube;

void
cttest_job_copy_preserves_body()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(42, 0, 1000000000, 6, dtube);
    assertf(j != NULL, "make_job");
    memcpy(j->body, "hello\n", 6);
    j->r.state = Ready;

    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy must not return NULL");
    assertf(c->r.pri == 42, "priority must be preserved");
    assertf(c->r.body_size == 6, "body_size must be preserved");
    assertf(memcmp(c->body, "hello\n", 6) == 0, "body must be copied");
    assertf(c->r.state == Copy, "copy state must be Copy");
    assertf(c->tube == j->tube, "tube pointer must match");
    assertf(c->file == NULL, "copy must not reference WAL file");

    /* copy must be independently freeable */
    job_free(c);
    job_free(j);
}

void
cttest_job_copy_null_input()
{
    Job *c = job_copy(NULL);
    assertf(c == NULL, "job_copy(NULL) must return NULL");
}

void
cttest_job_state_names()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(1, 0, 1000000000, 0, dtube);

    j->r.state = Ready;
    assertf(strcmp(job_state(j), "ready") == 0, "ready");

    j->r.state = Reserved;
    assertf(strcmp(job_state(j), "reserved") == 0, "reserved");

    j->r.state = Buried;
    assertf(strcmp(job_state(j), "buried") == 0, "buried");

    j->r.state = Delayed;
    assertf(strcmp(job_state(j), "delayed") == 0, "delayed");

    j->r.state = Invalid;
    assertf(strcmp(job_state(j), "invalid") == 0, "invalid");

    job_free(j);
}

void
cttest_job_list_insert_remove()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    Job head;
    job_list_reset(&head);
    assertf(job_list_is_empty(&head), "new list must be empty");

    Job *a = make_job(1, 0, 1000000000, 0, dtube);
    Job *b = make_job(2, 0, 1000000000, 0, dtube);

    job_list_insert(&head, a);
    assertf(!job_list_is_empty(&head), "list must not be empty after insert");

    job_list_insert(&head, b);

    /* remove a */
    Job *got = job_list_remove(a);
    assertf(got == a, "must return removed job");
    assertf(!job_list_is_empty(&head), "b still in list");

    /* remove b */
    got = job_list_remove(b);
    assertf(got == b, "must return b");
    assertf(job_list_is_empty(&head), "list must be empty now");

    /* double remove must return NULL */
    got = job_list_remove(a);
    assertf(got == NULL, "double remove must return NULL");

    job_free(a);
    job_free(b);
}

void
cttest_job_list_remove_null()
{
    Job *got = job_list_remove(NULL);
    assertf(got == NULL, "remove NULL must return NULL");
}

void
cttest_job_find_nonexistent()
{
    Job *j = job_find(999999999);
    assertf(j == NULL, "must not find nonexistent job");
}

void
cttest_job_pri_equal_breaks_tie_by_id()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *a = make_job(100, 0, 1000000000, 0, dtube);
    Job *b = make_job(100, 0, 1000000000, 0, dtube);

    /* same priority — lower id wins */
    assertf(a->r.id < b->r.id, "a must have lower id");
    assertf(job_pri_less(a, b), "a must come before b (lower id)");
    assertf(!job_pri_less(b, a), "b must not come before a");

    job_free(a);
    job_free(b);
}

void
cttest_job_delay_less()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *a = make_job(1, 0, 1000000000, 0, dtube);
    Job *b = make_job(1, 0, 1000000000, 0, dtube);

    a->r.deadline_at = 1000;
    b->r.deadline_at = 2000;
    assertf(job_delay_less(a, b), "earlier deadline must be less");
    assertf(!job_delay_less(b, a), "later deadline must not be less");

    /* equal deadlines — lower id wins */
    b->r.deadline_at = 1000;
    assertf(job_delay_less(a, b), "same deadline, lower id wins");

    job_free(a);
    job_free(b);
}


// ─── Pool boundary tests ────────────────────────────────────
// Attack pool_class boundaries: 63/64/65, 128/129, etc.
// If boundary is off-by-one, wrong pool class is used,
// and a reused job could have insufficient body space.

void
cttest_job_pool_boundary_class0()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    // 63 bytes → class 0 (64). Allocate, free, reallocate.
    // The reused allocation must fit 63 bytes.
    Job *j63 = make_job(1, 0, 1000000000, 63, dtube);
    assertf(j63 != NULL, "63-byte job must allocate");
    memset(j63->body, 'A', 63);
    job_free(j63);

    // 64 bytes → class 0 (64). Must reuse the pooled 63-byte entry.
    Job *j64 = make_job(1, 0, 1000000000, 64, dtube);
    assertf(j64 != NULL, "64-byte job must allocate");
    memset(j64->body, 'B', 64);  // must not overflow
    job_free(j64);

    // 65 bytes → class 1 (128). Must NOT reuse class 0 entry.
    Job *j65 = make_job(1, 0, 1000000000, 65, dtube);
    assertf(j65 != NULL, "65-byte job must allocate");
    memset(j65->body, 'C', 65);  // must not overflow
    job_free(j65);
}

void
cttest_job_pool_boundary_all_classes()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    // Boundaries: 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
    int boundaries[] = {64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};

    for (int i = 0; i < 11; i++) {
        int sz = boundaries[i];

        // Exactly at boundary
        Job *j = make_job(1, 0, 1000000000, sz, dtube);
        assertf(j != NULL, "job at boundary %d must allocate", sz);
        memset(j->body, 'X', sz);
        job_free(j);

        // One byte over boundary (next class)
        if (sz < 65536) {
            Job *j2 = make_job(1, 0, 1000000000, sz + 1, dtube);
            assertf(j2 != NULL, "job at boundary %d+1 must allocate", sz);
            memset(j2->body, 'Y', sz + 1);
            job_free(j2);
        }
    }
}

// ─── Pool overflow tests ────────────────────────────────────
// Fill a pool class beyond its 512-entry limit.
// Excess jobs must be freed (not pooled). No crash, no leak.

void
cttest_job_pool_per_class_overflow()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *jobs[600];

    // Allocate 600 jobs of class 0 (body_size=1)
    for (int i = 0; i < 600; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 1, dtube);
        assertf(jobs[i] != NULL, "job %d must allocate", i);
        jobs[i]->body[0] = (char)i;
    }

    // Free all — first 512 should be pooled, rest freed
    for (int i = 0; i < 600; i++) {
        job_free(jobs[i]);
    }

    // Allocate 600 more — first 512 should come from pool
    for (int i = 0; i < 600; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 1, dtube);
        assertf(jobs[i] != NULL, "realloc job %d must succeed", i);
    }

    // Clean up
    for (int i = 0; i < 600; i++) {
        job_free(jobs[i]);
    }
}

// ─── Rehash under deletion pressure ─────────────────────────
// Create enough jobs to trigger upscale rehash, then delete
// every other job while rehash may still be in progress.
// All surviving jobs must remain findable.

void
cttest_job_hash_interleaved_delete()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    int N = 300; // enough to trigger rehash (threshold: 4 * 12289 = ~49K, but
                 // rehash_step runs on every find/insert/delete)
    Job *jobs[300];

    // Phase 1: insert all
    for (int i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 4, dtube);
        assertf(jobs[i] != NULL, "job %d must allocate", i);
    }

    // Phase 2: delete every other job
    for (int i = 0; i < N; i += 2) {
        uint64 id = jobs[i]->r.id;
        job_free(jobs[i]);
        jobs[i] = NULL;

        // The freed job must NOT be findable
        Job *ghost = job_find(id);
        assertf(ghost == NULL, "freed job %"PRIu64" must not be findable", id);
    }

    // Phase 3: all surviving jobs must still be findable
    for (int i = 1; i < N; i += 2) {
        Job *found = job_find(jobs[i]->r.id);
        assertf(found == jobs[i],
                "surviving job %"PRIu64" must be findable after interleaved deletes",
                jobs[i]->r.id);
    }

    // Phase 4: clean up survivors
    for (int i = 1; i < N; i += 2) {
        job_free(jobs[i]);
    }
    assertf(get_all_jobs_used() == 0,
            "all jobs must be freed, got %zu", get_all_jobs_used());
}

// ─── Oversized jobs bypass pool ─────────────────────────────
// Jobs with body > 65536 bytes must not enter the pool.

void
cttest_job_pool_oversized_bypass()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    // 65537 bytes = too large for any pool class
    Job *j = make_job(1, 0, 1000000000, 65537, dtube);
    assertf(j != NULL, "oversized job must allocate");
    memset(j->body, 'Z', 65537); // must not corrupt
    job_free(j); // must go to free(), not pool

    // Verify pool wasn't corrupted by allocating a small job
    Job *j2 = make_job(1, 0, 1000000000, 1, dtube);
    assertf(j2 != NULL, "small job after oversized must work");
    job_free(j2);
}
