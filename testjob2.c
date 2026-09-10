#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <inttypes.h>
#include <malloc.h>
#include <stdlib.h>
#include <string.h>

static Tube *dtube;

void
cttest_job_copy_preserves_body()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(42, 0, 1000000000, 6, dtube);
    assertf(j != NULL, "make_job");
    assertf(job_list_is_empty(j),
            "a job must be born as its own singleton list, not with stale "
            "prev/next from a pooled slab (bug #22)");
    memcpy(j->body, "hello\n", 6);
    j->r.state = Ready;

    uint refs_before = dtube->refs;
    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy must not return NULL");
    assertf(c->r.pri == 42, "priority must be preserved");
    assertf(c->r.body_size == 6, "body_size must be preserved");
    assertf(memcmp(c->body, "hello\n", 6) == 0, "body must be copied");
    assertf(c->r.state == Copy, "copy state must be Copy");
    assertf(c->tube == j->tube, "tube pointer must match");
    assertf(c->file == NULL, "copy must not reference WAL file");
    assertf(job_list_is_empty(c),
            "a copy must not dangle prev/next into the source's list (bug #22)");
    assertf(dtube->refs == refs_before + 1,
            "a copy must take its own tube reference: refs %u -> %u",
            refs_before, dtube->refs);

    /* copy must be independently freeable */
    job_free(c);
    assertf(dtube->refs == refs_before,
            "freeing a copy must give the tube reference back: refs %u -> %u",
            refs_before, dtube->refs);
    job_free(j);
}

void
cttest_job_copy_null_input()
{
    Job *c = job_copy(NULL);
    assertf(c == NULL, "job_copy(NULL) must return NULL");
}

// #J1 regression: job_copy must null every pointer field, including
// reserver. Previously reserver was left uninitialised (malloc, not
// calloc, and no explicit assignment) — a latent clone of bug #22.
// Safe today only because is_job_reserved_by_conn gates on Reserved,
// but a future reader without that gate would dereference garbage.
void
cttest_job_copy_reserver_is_null()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *j = make_job(7, 0, 1000000000, 4, dtube);
    assertf(j != NULL, "make_job");
    memcpy(j->body, "xxxx", 4);
    // Simulate the state right before reply_job: a reserved job with
    // a non-NULL reserver pointer. The copy must not inherit it.
    j->r.state = Reserved;
    j->reserver = (void *)0xDEADBEEFCAFEBABEULL;

    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy must not return NULL");
    assertf(c->reserver == NULL,
            "job_copy must null reserver (was %p)", (void *)c->reserver);
    assertf(c->r.state == Copy, "copy state must be Copy");

    // Scrub source pointer before freeing so any accidental
    // later use hits a predictable invalid address, not reserver.
    j->reserver = NULL;
    job_free(c);
    job_free(j);
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

    // Copy is an internal bookkeeping state, not one of the four states
    // protocol.txt lets a client see. Reporting it as any of those would
    // put a lie on the wire.
    j->r.state = Copy;
    assertf(strcmp(job_state(j), "invalid") == 0,
            "the internal Copy state must never be reported as a wire state, got \"%s\"",
            job_state(j));

    j->r.state = (byte)(Copy + 42);
    assertf(strcmp(job_state(j), "invalid") == 0,
            "an out-of-enum state must be reported as invalid, got \"%s\"",
            job_state(j));

    job_free(j);
}

void
cttest_job_list_insert_remove()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    Job *head = make_job(9, 0, 1000000000, 0, dtube);
    assertf(head != NULL, "make_job");
    job_list_reset(head);
    assertf(job_list_is_empty(head), "new list must be empty");

    Job *a = make_job(1, 0, 1000000000, 0, dtube);
    Job *b = make_job(2, 0, 1000000000, 0, dtube);
    Job *c = make_job(3, 0, 1000000000, 0, dtube);

    job_list_insert(head, a);
    assertf(!job_list_is_empty(head), "list must not be empty after insert");

    job_list_insert(head, b);
    job_list_insert(head, c);

    // Order is the contract, not an accident: a conn's reserved jobs are
    // walked front to back to find the next deadline, so insert must
    // APPEND. A prepend silently turns that FIFO into a LIFO.
    assertf(head->next == a && a->next == b && b->next == c && c->next == head,
            "insert must append in call order a,b,c");
    assertf(head->prev == c && c->prev == b && b->prev == a && a->prev == head,
            "the backward chain must mirror the forward one");

    // Re-inserting a job that is already linked must be refused, not
    // allowed to splice the list into a cycle that loses c.
    job_list_insert(head, b);
    assertf(head->next == a && a->next == b && b->next == c && c->next == head,
            "re-inserting a linked job must leave the list untouched");

    /* remove from the middle */
    Job *got = job_list_remove(b);
    assertf(got == b, "must return removed job");
    assertf(head->next == a && a->next == c && c->next == head && c->prev == a,
            "removing the middle must splice its neighbours together");
    assertf(job_list_is_empty(b), "a removed job must become a singleton again");

    /* remove a */
    got = job_list_remove(a);
    assertf(got == a, "must return removed job");
    assertf(!job_list_is_empty(head), "c still in list");

    /* remove c */
    got = job_list_remove(c);
    assertf(got == c, "must return c");
    assertf(job_list_is_empty(head), "list must be empty now");

    /* double remove must return NULL */
    got = job_list_remove(a);
    assertf(got == NULL, "double remove must return NULL");

    job_free(a);
    job_free(b);
    job_free(c);
    job_free(head);
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

    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *live = make_job(1, 0, 1000000000, 4, dtube);
    assertf(live != NULL, "make_job");
    assertf(job_find(live->r.id) == live,
            "a stored job must be findable by its id");

    // An empty table makes "not found" true by construction, so probe an
    // id that lands in the SAME bucket as a live job (cap is primes[0]
    // until the first rehash). The bucket walk has to compare ids, not
    // just hand back the head of the chain.
    uint64 colliding = live->r.id + primes[0];
    assertf(job_find(colliding) == NULL,
            "id %"PRIu64" shares the bucket of live job %"PRIu64" and must not be found",
            colliding, live->r.id);

    assertf(job_find(0) == NULL, "id 0 is never issued and must not be found");

    uint64 gone = live->r.id;
    job_free(live);
    assertf(job_find(gone) == NULL,
            "a freed job %"PRIu64" must not stay findable", gone);
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

    // Priority outranks age. The urgent job is created LAST so its id is
    // the higher one: only the pri comparison can put it first, and a
    // comparator that fell back to id order would answer backwards.
    Job *urgent = make_job(5, 0, 1000000000, 0, dtube);
    assertf(urgent->r.id > b->r.id, "urgent must be the youngest job");
    assertf(job_pri_less(urgent, b),
            "pri 5 must come before pri 100 despite the higher id");
    assertf(!job_pri_less(b, urgent),
            "pri 100 must not come before pri 5");

    // Both ends of the uint32 priority range, again with the youngest
    // job holding the most urgent priority.
    Job *lax = make_job(4294967295u, 0, 1000000000, 0, dtube);
    Job *top = make_job(0, 0, 1000000000, 0, dtube);
    assertf(job_pri_less(top, lax),
            "pri 0 must come before pri 4294967295 despite the higher id");
    assertf(!job_pri_less(lax, top),
            "pri 4294967295 must not come before pri 0");

    job_free(a);
    job_free(b);
    job_free(urgent);
    job_free(lax);
    job_free(top);
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

    // ...and the pair must be ordered ONE way. A comparator that answers
    // "less" in both directions is not a strict weak ordering, and the
    // delay heap silently corrupts on it.
    assertf(!job_delay_less(b, a),
            "with equal deadlines only the lower id may compare less");
    assertf(!job_delay_less(a, a),
            "a job must never compare less than itself");

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
    job_pool_drain();
    size_t bytes; int count;

    // A class holds STORED sizes (user body + the \r\n trailer room), so
    // class 0 runs to 64 + POOL_PAD = 66, not to 64. The accounting is
    // what makes the fit provable without a sanitizer: what job_free
    // CHARGES for a slab and what allocate_job CREDITS when it hands the
    // same slab back must be the same number, or the slab it handed back
    // is not the size it was charged as.
    Job *j63 = make_job(1, 0, 1000000000, 63, dtube);
    assertf(j63 != NULL, "63-byte job must allocate");
    memset(j63->body, 'A', 63);
    job_free(j63);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1 && bytes > 0,
            "a 63-byte body must be pooled in class 0: count=%d bytes=%zu",
            count, bytes);

    // 64 bytes → still class 0. Must reuse the pooled entry and give the
    // exact charge back.
    Job *j64 = make_job(1, 0, 1000000000, 64, dtube);
    assertf(j64 != NULL, "64-byte job must allocate");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "class-0 reuse must credit back exactly what pooling charged: "
            "count=%d bytes=%zu", count, bytes);
    memset(j64->body, 'B', 64);  // must not overflow
    job_free(j64);

    // 65 bytes → still class 0 (65 <= 64 + POOL_PAD): the pooled slab is
    // the one it must get, and the counters must balance again.
    Job *j65 = make_job(1, 0, 1000000000, 65, dtube);
    assertf(j65 != NULL, "65-byte job must allocate");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "65 stored bytes still fit class 0 and must reuse its slab: "
            "count=%d bytes=%zu", count, bytes);
    memset(j65->body, 'C', 65);  // must not overflow
    job_free(j65);

    // 66 bytes is the class-0 ceiling: the last size the pooled slab may
    // serve, and every byte of it must be backed by that slab.
    Job *j66 = make_job(1, 0, 1000000000, 66, dtube);
    assertf(j66 != NULL, "66-byte job must allocate");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "the class-0 ceiling must reuse the pooled slab: count=%d bytes=%zu",
            count, bytes);
    assertf(malloc_usable_size(j66) >= sizeof(Job) + 66,
            "the class-0 slab must really hold %zu bytes, usable=%zu",
            sizeof(Job) + (size_t)66, malloc_usable_size(j66));
    memset(j66->body, 'D', 66);
    job_free(j66);

    // 67 is the first size class 0 cannot serve. Handing it the 66-byte
    // slab would overflow the heap on the trailer write.
    Job *j67 = make_job(1, 0, 1000000000, 67, dtube);
    assertf(j67 != NULL, "67-byte job must allocate");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1,
            "67 stored bytes must not consume the pooled class-0 slab: count=%d",
            count);
    assertf(malloc_usable_size(j67) >= sizeof(Job) + 67,
            "a 67-byte body needs a slab of at least %zu bytes, usable=%zu",
            sizeof(Job) + (size_t)67, malloc_usable_size(j67));
    memset(j67->body, 'E', 67);
    job_free(j67);
    job_pool_drain();
}

void
cttest_job_pool_boundary_all_classes()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();
    // Boundaries: 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
    int boundaries[] = {64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536};
    size_t nbound = sizeof boundaries / sizeof boundaries[0];
    // The class a size lands in is decided on the STORED size — the user
    // body plus the POOL_PAD room every caller uses for the \r\n trailer
    // — so a class ends at 2^k + POOL_PAD, and 2^k + 1 is still the SAME
    // class. These are the real ceilings, and the first size past each of
    // them must not be served that class's slab.
    int ceilings[] = {66, 130, 258, 514, 1026, 2050, 4098, 8194, 16386, 32770, 65538};
    size_t nceil = sizeof ceilings / sizeof ceilings[0];
    size_t bytes; int count;

    for (size_t i = 0; i < nbound; i++) {
        int sz = boundaries[i];

        // Exactly at boundary
        Job *j = make_job(1, 0, 1000000000, sz, dtube);
        assertf(j != NULL, "job at boundary %d must allocate", sz);
        memset(j->body, 'X', sz);
        job_free(j);

        // The same size must come back out of the pool, and the bytes
        // credited on reuse must be the bytes charged on release — a
        // mismatch means the slab is not the size the pool thinks it is.
        Job *again = make_job(1, 0, 1000000000, sz, dtube);
        assertf(again != NULL, "reuse at boundary %d must allocate", sz);
        get_job_pool_stats(&bytes, &count);
        assertf(count == 0 && bytes == 0,
                "reuse at boundary %d must balance the pool back to empty: "
                "count=%d bytes=%zu", sz, count, bytes);
        memset(again->body, 'X', sz);
        job_free(again);

        // One byte over boundary (next class)
        if (sz < 65536) {
            Job *j2 = make_job(1, 0, 1000000000, sz + 1, dtube);
            assertf(j2 != NULL, "job at boundary %d+1 must allocate", sz);
            memset(j2->body, 'Y', sz + 1);
            job_free(j2);
        }
        job_pool_drain();
    }

    for (size_t i = 0; i < nceil; i++) {
        int top = ceilings[i];

        Job *j = make_job(1, 0, 1000000000, top, dtube);
        assertf(j != NULL, "job at class ceiling %d must allocate", top);
        assertf(malloc_usable_size(j) >= sizeof(Job) + (size_t)top,
                "the slab for %d bytes must really hold %zu bytes, usable=%zu",
                top, sizeof(Job) + (size_t)top, malloc_usable_size(j));
        memset(j->body, 'C', top);
        job_free(j);

        Job *over = make_job(1, 0, 1000000000, top + 1, dtube);
        assertf(over != NULL, "job at %d must allocate", top + 1);
        get_job_pool_stats(&bytes, &count);
        assertf(count == 1,
                "%d bytes must not be handed the slab pooled for the %d-byte "
                "class: count=%d", top + 1, top, count);
        assertf(malloc_usable_size(over) >= sizeof(Job) + (size_t)(top + 1),
                "the slab for %d bytes must really hold %zu bytes, usable=%zu",
                top + 1, sizeof(Job) + (size_t)(top + 1), malloc_usable_size(over));
        memset(over->body, 'D', top + 1);
        job_free(over);
        job_pool_drain();
    }
}

// ─── Pool overflow tests ────────────────────────────────────
// Fill a pool class beyond its 512-entry limit.
// Excess jobs must be freed (not pooled). No crash, no leak.

void
cttest_job_pool_per_class_overflow()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();
    Job *jobs[600];
    size_t bytes; int count;

    // Allocate 600 jobs of class 0 (body_size=1)
    for (int i = 0; i < 600; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 1, dtube);
        assertf(jobs[i] != NULL, "job %d must allocate", i);
        jobs[i]->body[0] = (char)i;
    }
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "live jobs must not sit in the free list: count=%d bytes=%zu",
            count, bytes);

    // Free all — first 512 should be pooled, rest freed
    for (int i = 0; i < 600; i++) {
        job_free(jobs[i]);
    }

    // The 512-entry cap is the only thing bounding a single size class:
    // without it a burst of small jobs is cached forever instead of going
    // back to the allocator.
    get_job_pool_stats(&bytes, &count);
    assertf(count == 512,
            "one size class may pool at most 512 entries; the other 88 must go "
            "back to free(3), got %d pooled", count);

    // Allocate 600 more — first 512 should come from pool
    for (int i = 0; i < 512; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 1, dtube);
        assertf(jobs[i] != NULL, "realloc job %d must succeed", i);
    }
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "512 reuses must drain the class and balance its accounting: "
            "count=%d bytes=%zu", count, bytes);

    for (int i = 512; i < 600; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 1, dtube);
        assertf(jobs[i] != NULL, "realloc job %d must succeed", i);
    }

    // Clean up
    for (int i = 0; i < 600; i++) {
        job_free(jobs[i]);
    }
    get_job_pool_stats(&bytes, &count);
    assertf(count == 512,
            "the cap must still hold on the second fill, got %d pooled", count);
    job_pool_drain();
}

// ─── Rehash under deletion pressure ─────────────────────────
// Create enough jobs to trigger upscale rehash, then delete
// every other job while rehash may still be in progress.
// All surviving jobs must remain findable.

void
cttest_job_hash_interleaved_delete()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    // store_job upscales when used > (cap << 1), and cap starts at
    // primes[0] == 12289 — so a rehash only ever begins past 24578 jobs.
    // Below that the incremental-migration machinery, and job_find's
    // second lookup in the old table, are never entered at all.
    int N = (int)(primes[0] * 2) + 64;
    static Job *jobs[24642];
    assertf((size_t)N <= sizeof jobs / sizeof jobs[0],
            "fixture too small for the %d jobs the upscale threshold needs", N);

    // Phase 1: insert all
    for (int i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 4, dtube);
        assertf(jobs[i] != NULL, "job %d must allocate", i);
    }
    assertf(get_all_jobs_used() == (size_t)N,
            "every stored job must be accounted, want %d got %zu",
            N, get_all_jobs_used());

    // Phase 1b: the migration walks the old buckets in order, 16 per
    // operation, so a job in the LAST old bucket is still sitting in the
    // old table right now. Finding it can only work through job_find's
    // second lookup — the branch a small fixture never reaches.
    Job *unmigrated = jobs[primes[0] - 2];
    assertf(unmigrated->r.id % primes[0] == primes[0] - 1,
            "fixture assumption: job %"PRIu64" must sit in the last old bucket",
            unmigrated->r.id);
    assertf(job_find(unmigrated->r.id) == unmigrated,
            "a job still living in the old table must be found through the "
            "rehash fallback, not reported missing");

    // ...and nothing else may be lost while the table is split in two.
    for (int i = 0; i < N; i++) {
        Job *found = job_find(jobs[i]->r.id);
        assertf(found == jobs[i],
                "job %"PRIu64" must stay findable while the table is migrating",
                jobs[i]->r.id);
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
    assertf(get_all_jobs_used() == (size_t)(N / 2),
            "half the jobs must remain accounted, want %d got %zu",
            N / 2, get_all_jobs_used());

    // Phase 3: all surviving jobs must still be findable
    for (int i = 1; i < N; i += 2) {
        Job *found = job_find(jobs[i]->r.id);
        assertf(found == jobs[i],
                "surviving job %"PRIu64" must be findable after interleaved deletes",
                jobs[i]->r.id);
    }

    // Phase 3b: jobs inserted DURING the migration land in the new table
    // and must be findable immediately, without disturbing the old ones.
    Job *fresh = make_job(1, 0, 1000000000, 4, dtube);
    assertf(fresh != NULL, "insert during rehash must allocate");
    assertf(job_find(fresh->r.id) == fresh,
            "a job inserted during a rehash must be findable at once");
    assertf(job_find(jobs[1]->r.id) == jobs[1],
            "inserting during a rehash must not lose the jobs already stored");
    job_free(fresh);

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
    job_pool_drain();
    size_t bytes; int count;

    // 65537 is NOT oversized: the top class is measured on the stored
    // size and runs to 65536 + POOL_PAD, so this one is still pooled.
    Job *j = make_job(1, 0, 1000000000, 65537, dtube);
    assertf(j != NULL, "oversized job must allocate");
    memset(j->body, 'Z', 65537); // must not corrupt
    job_free(j);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1,
            "65537 stored bytes still fits the top class and must be pooled, got %d",
            count);
    job_pool_drain();

    // The real cutoff. A body past every class must go to free(3): pooling
    // it would put a slab on a free list whose class promises more room
    // than the slab has, and the next allocation of that class overflows
    // the heap.
    Job *huge = make_job(1, 0, 1000000000, 65539, dtube);
    assertf(huge != NULL, "a body past the top class must still allocate");
    assertf(malloc_usable_size(huge) >= sizeof(Job) + (size_t)65539,
            "an unpooled body must get its own exact slab, usable=%zu",
            malloc_usable_size(huge));
    memset(huge->body, 'Z', 65539);
    job_free(huge);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "a body past the top class must not enter the pool: count=%d bytes=%zu",
            count, bytes);

    // The top class itself must still pool, and its slab must really
    // cover the size it claims.
    Job *top = make_job(1, 0, 1000000000, 65538, dtube);
    assertf(top != NULL, "top-class job must allocate");
    assertf(malloc_usable_size(top) >= sizeof(Job) + (size_t)65538,
            "the top class slab must hold %zu bytes, usable=%zu",
            sizeof(Job) + (size_t)65538, malloc_usable_size(top));
    memset(top->body, 'T', 65538);
    job_free(top);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "the top class must still be poolable, got %d", count);
    job_pool_drain();

    // Verify pool wasn't corrupted by allocating a small job
    Job *j2 = make_job(1, 0, 1000000000, 1, dtube);
    assertf(j2 != NULL, "small job after oversized must work");
    assertf(malloc_usable_size(j2) >= sizeof(Job) + (size_t)66,
            "every job's slab must cover its whole size class (%zu bytes here), "
            "usable=%zu", sizeof(Job) + (size_t)66, malloc_usable_size(j2));
    job_free(j2);
    job_pool_drain();
}

// ─── Pool drain returns memory to glibc ─────────────────────
// job_pool_drain() frees every pooled entry and zeroes the size-class
// accounting so the periodic malloc_trim(0) can reclaim the pages.
// Hostile angles: counter balance (#2), idempotence on an empty pool,
// re-pool balance afterward, and proof that LIVE jobs in the hash table
// are never touched by a drain.

void
cttest_job_pool_drain_balances_counters()
{
    TUBE_ASSIGN(dtube, make_tube("default"));

    // Start cold. (Tests are fork-isolated, so the pool is already empty;
    // the explicit drain documents intent and must be a safe no-op.)
    job_pool_drain();
    size_t bytes; int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "pool must be empty after initial drain: count=%d bytes=%zu", count, bytes);

    // Draining an already-empty pool stays empty (idempotent, no double-free).
    job_pool_drain();
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0, "double drain must stay empty");

    // Fill five distinct size classes, then return them to the pool.
    int sizes[] = {1, 100, 1000, 9000, 60000};
    for (int i = 0; i < 5; i++) {
        Job *j = make_job(1, 0, 1000000000, sizes[i], dtube);
        assertf(j != NULL, "alloc size %d must succeed", sizes[i]);
        job_free(j);
    }
    get_job_pool_stats(&bytes, &count);
    assertf(count == 5, "five freed jobs must be pooled, got %d", count);
    assertf(bytes > 0, "pool_mem must be positive after pooling, got %zu", bytes);

    // The drain under test: every entry freed, accounting back to zero.
    job_pool_drain();
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0, "drain must zero pooled count, got %d", count);
    assertf(bytes == 0, "drain must zero pool_mem, got %zu", bytes);

    // Pool is usable again: one size-100 job lands in class 1 (130-byte slab);
    // the re-pooled entry's accounting must balance exactly.
    Job *j = make_job(1, 0, 1000000000, 100, dtube);
    assertf(j != NULL, "post-drain alloc must succeed");
    job_free(j);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1 && bytes == sizeof(Job) + 130,
            "post-drain re-pool must balance: count=%d bytes=%zu", count, bytes);
    job_pool_drain();

    // Hostile sizes come through the same door. A negative body size is
    // not a size at all: it must be refused, never turned into
    // malloc(sizeof(Job) + (size_t)-1), which wraps to an under-allocated
    // job whose body points past the end of its own slab.
    assertf(allocate_job(-1) == NULL,
            "a negative body size must be refused outright");
    assertf(allocate_job(INT32_MIN) == NULL,
            "the most negative body size must be refused outright");

    // Zero is a legal smallest body, and the job must come back usable.
    Job *zero = allocate_job(0);
    assertf(zero != NULL, "a zero-length body must still allocate");
    assertf(zero->r.body_size == 0,
            "body_size must be the size asked for, got %d", zero->r.body_size);
    assertf(zero->body == (char *)zero + sizeof(Job),
            "the body must point just past the Job header");
    zero->r.state = Copy;
    job_free(zero);
    job_pool_drain();
}

void
cttest_job_pool_drain_spares_live_jobs()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();

    // Live jobs live in the hash table, NOT the free-list pool. A drain must
    // touch only pooled (already-freed) entries, never a live allocation.
    Job *live[8];
    for (int i = 0; i < 8; i++) {
        live[i] = make_job(1, 0, 1000000000, 200, dtube);
        assertf(live[i] != NULL, "live job %d must allocate", i);
    }

    // Populate the pool with unrelated freed jobs of the same class.
    // Allocate all four before freeing — freeing one then re-allocating the
    // same class would just reuse it from the pool and never accumulate.
    Job *tmp[4];
    for (int i = 0; i < 4; i++) {
        tmp[i] = make_job(1, 0, 1000000000, 200, dtube);
        assertf(tmp[i] != NULL, "tmp job %d must allocate", i);
    }
    uint refs_held = dtube->refs;
    for (int i = 0; i < 4; i++) job_free(tmp[i]);
    size_t bytes; int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 4, "expected 4 pooled jobs, got %d", count);

    // A pooled job is a released job: it must have handed its tube
    // reference back, or every drained tube leaks one reference and
    // tube_free never fires.
    assertf(dtube->refs == refs_held - 4,
            "freeing 4 jobs must release 4 tube references: refs %u -> %u",
            refs_held, dtube->refs);

    job_pool_drain();

    // Every live job is still findable and unchanged after the drain.
    for (int i = 0; i < 8; i++) {
        Job *f = job_find(live[i]->r.id);
        assertf(f == live[i],
                "live job %"PRIu64" must survive drain", live[i]->r.id);
    }
    assertf(get_all_jobs_used() == 8,
            "a drain must not unaccount the 8 live jobs, got %zu",
            get_all_jobs_used());
    assertf(dtube->refs >= 8,
            "the 8 live jobs must still hold their tube references, refs=%u",
            dtube->refs);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0, "pool must be empty after drain");

    for (int i = 0; i < 8; i++) job_free(live[i]);
    assertf(get_all_jobs_used() == 0, "no live jobs must remain after free");
    job_pool_drain();
}

// ─── Copy jobs and the size-class pool ──────────────────────
// Historically job_free sent every Copy job to free(3) while do_stats /
// do_list_tubes POPPED warm slabs from the pool for their Copy jobs —
// monitoring traffic permanently drained the pool that PUT traffic
// filled. job_copy now allocates via allocate_job (class-rounded slab)
// and job_free pools copies. These tests fail on the old code and go
// ASan-red on any half-applied fix (pooling copies while job_copy still
// raw-mallocs is a heap overflow).

void
cttest_job_copy_free_returns_to_pool()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();

    Job *j = make_job(1, 0, 1000000000, 100, dtube);
    assertf(j != NULL, "make_job");
    memset(j->body, 'a', 100);

    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy");
    assertf(memcmp(c->body, j->body, 100) == 0, "body must be copied");
    job_free(c);

    // Old code: the copy goes to free(3), pool stays empty — this is
    // the assert that kills the always-free behavior.
    size_t bytes; int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "freed copy must be pooled, got %d", count);
    assertf(bytes == sizeof(Job) + 130,
            "pooled copy must account a class-1 slab: bytes=%zu", bytes);

    // The pooled copy must be reusable by a same-class allocation.
    Job *r = make_job(1, 0, 1000000000, 102, dtube);
    assertf(r != NULL, "post-copy alloc");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "class-1 alloc must pop the pooled copy: count=%d bytes=%zu",
            count, bytes);

    job_free(r);
    job_free(j);
    job_pool_drain();
}

void
cttest_job_copy_pooled_slab_is_class_rounded()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();

    // Copy of a 100-byte body. With a raw-malloc job_copy the real slab
    // would be sizeof(Job)+100; pooled as class 1 it would CLAIM a
    // 130-byte slab.
    Job *j = make_job(1, 0, 1000000000, 100, dtube);
    assertf(j != NULL, "make_job");
    memset(j->body, 'b', 100);
    Job *c = job_copy(j);
    assertf(c != NULL, "job_copy");

    // The claim under test, made provable without a sanitizer: the copy's
    // REAL allocation must already cover the class-1 slab that job_free
    // is about to pool it as. A raw-malloc job_copy allocates
    // sizeof(Job)+100 here, which cannot cover sizeof(Job)+130.
    assertf(malloc_usable_size(c) >= sizeof(Job) + 130,
            "a copy pooled as a class-1 slab must really own %zu bytes, usable=%zu",
            sizeof(Job) + (size_t)130, malloc_usable_size(c));

    job_free(c);

    size_t bytes; int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "copy must be pooled, got %d", count);

    // Pop it at the class-1 ceiling (body_size 130 = user 128 + \r\n
    // pad) and write every claimed byte. Under ASan this goes red
    // against any fix that pools copies without class-rounding them.
    Job *r = make_job(1, 0, 1000000000, 130, dtube);
    assertf(r != NULL, "boundary alloc");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0,
            "boundary alloc must reuse the pooled copy, got %d pooled", count);
    memset(r->body, 'x', 130);

    job_free(r);
    job_free(j);
    job_pool_drain();
}

void
cttest_stats_copy_shrunken_body_pools_safely()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    job_pool_drain();

    // Mimic do_stats: allocate STATS_BUF_SIZE (4096), mark Copy, then
    // SHRINK r.body_size to the formatted length before job_free. The
    // pooling branch must classify by the shrunken size, and the
    // (larger) real slab must cover the claimed class.
    Job *s = allocate_job(4096);
    assertf(s != NULL, "allocate_job(4096)");
    s->r.state = Copy;
    memset(s->body, 's', 900);
    s->r.body_size = 900;
    job_free(s);

    size_t bytes; int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "shrunken copy must be pooled, got %d", count);
    assertf(bytes == sizeof(Job) + 1026,
            "accounting must follow the shrunken class (1026 slab): bytes=%zu",
            bytes);

    // Pop from the shrunken class and write the full claimed slab —
    // proves the real 4098-byte slab backs the claimed 1026 bytes.
    Job *r = make_job(1, 0, 1000000000, 1026, dtube);
    assertf(r != NULL, "class-4 alloc");
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "class-4 alloc must pop the pooled entry: count=%d bytes=%zu",
            count, bytes);
    memset(r->body, 'y', 1026);

    job_free(r);
    job_pool_drain();
}


// The id counter must never reach 0, because readrec reads a zero id as
// the start of the fallocate tail: a job carrying it would end every
// later replay at its own record, losing everything behind it. The
// counter advances to id+1 for every replayed id, so the guard has to
// live on the way in — the reader refuses UINT64_MAX (see readrec) and
// the largest replayable id is therefore UINT64_MAX-1.
void
cttest_job_id_counter_stays_nonzero_at_the_top_of_the_id_space(void)
{
    Tube *t = make_tube("idwrap");
    assertf(t != NULL, "setup: make_tube");
    tube_iref(t);

    Job *replayed = make_job_with_id(1, 0, 1000000000, 2, t, UINT64_MAX - 1);
    assertf(replayed != NULL, "setup: the replayed job must allocate");
    assertf(replayed->r.id == UINT64_MAX - 1,
            "setup: the replayed id must be kept, got %" PRIu64,
            replayed->r.id);

    Job *fresh = make_job(1, 0, 1000000000, 2, t);
    assertf(fresh != NULL, "setup: the fresh job must allocate");

    assertf(fresh->r.id != 0,
            "a job id of 0 is indistinguishable from the zero tail a "
            "binlog ends with, so the counter must never produce one");
    assertf(fresh->r.id != replayed->r.id,
            "two live jobs must not share an id (%" PRIu64 ")",
            fresh->r.id);

    job_free(fresh);
    job_free(replayed);
    tube_dref(t);
}
