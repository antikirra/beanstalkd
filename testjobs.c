#include "ct/ct.h"
#include "dat.h"
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

static Tube *default_tube;

void
cttest_job_creation()
{
    Job *j, *k, *found;
    uint refs_before;
    uint64 prev_id;

    TUBE_ASSIGN(default_tube, make_tube("default"));
    assertf(get_all_jobs_used() == 0,
            "the job index must start empty, count is %zu", get_all_jobs_used());

    refs_before = default_tube->refs;
    j = make_job(1, 0, 1, 0, default_tube);
    assertf(j->r.pri == 1, "priority should match: asked 1, got %u", j->r.pri);
    assertf(j->r.id != 0,
            "a created job must be issued a real id; id 0 is never handed out");

    found = job_find(j->r.id);
    assertf(found == j,
            "make_job must STORE the job it creates: job_find(%"PRIu64") gave %p, want %p",
            j->r.id, (void *)found, (void *)j);
    assertf(get_all_jobs_used() == 1,
            "one created job must count as 1 in the index, count is %zu",
            get_all_jobs_used());
    assertf(j->tube == default_tube,
            "a job must belong to the tube it was created in");
    assertf(default_tube->refs == refs_before + 1,
            "creation must take a tube reference: refs %u -> %u",
            refs_before, default_tube->refs);

    prev_id = j->r.id;
    k = make_job(7, 500, 900, 5, default_tube);
    assertf(k != NULL, "the second job must allocate");
    assertf(k->r.delay == 500 && k->r.ttr == 900,
            "delay and ttr must reach the job unswapped: delay %"PRId64" ttr %"PRId64,
            k->r.delay, k->r.ttr);
    assertf(k->r.body_size == 5,
            "body_size must be recorded as asked: got %d, want 5", k->r.body_size);
    assertf(k->r.id > prev_id,
            "ids must be issued increasing: %"PRIu64" followed %"PRIu64,
            k->r.id, prev_id);
    assertf(get_all_jobs_used() == 2,
            "two created jobs must count as 2, count is %zu", get_all_jobs_used());

    assertf(make_job(1, 0, 1, -1, default_tube) == NULL,
            "a negative body size must be refused, never wrapped into an under-allocation");
    assertf(get_all_jobs_used() == 2,
            "a refused creation must not touch the index, count is %zu",
            get_all_jobs_used());

    job_free(k);
    job_free(j);
}

void
cttest_job_cmp_pris()
{
    Job *a, *b;

    TUBE_ASSIGN(default_tube, make_tube("default"));
    a = make_job(1, 0, 1, 0, default_tube);
    b = make_job(1 << 27, 0, 1, 0, default_tube);

    assertf(job_pri_less(a, b), "should be less: pri %u must precede pri %u",
            a->r.pri, b->r.pri);
    assertf(!job_pri_less(b, a),
            "the order must be antisymmetric: pri %u must not precede pri %u",
            b->r.pri, a->r.pri);
    assertf(!job_pri_less(a, a),
            "a job must never precede itself; the ready heap needs a strict order "
            "(pri %u, id %"PRIu64")", a->r.pri, a->r.id);
    assertf(!job_pri_less(b, b),
            "the same holds at the other end of the range (pri %u, id %"PRIu64")",
            b->r.pri, b->r.id);

    job_free(a);
    job_free(b);
}

void
cttest_job_cmp_ids()
{
    Job *a, *b;

    TUBE_ASSIGN(default_tube, make_tube("default"));
    a = make_job(1, 0, 1, 0, default_tube);
    b = make_job(1, 0, 1, 0, default_tube);

    b->r.id <<= 49;
    assertf(job_pri_less(a, b),
            "should be less: with equal pri, id %"PRIu64" must precede id %"PRIu64,
            a->r.id, b->r.id);
    assertf(!job_pri_less(b, a),
            "the id tiebreak must be antisymmetric: id %"PRIu64" must not precede id %"PRIu64,
            b->r.id, a->r.id);
    assertf(!job_pri_less(a, a),
            "equal pri and equal id must not compare less: job %"PRIu64
            " may not precede itself", a->r.id);
    assertf(!job_pri_less(b, b),
            "the same holds for a far-apart id: job %"PRIu64" may not precede itself",
            b->r.id);
}

void
cttest_job_large_pris()
{
    Job *a, *b;

    TUBE_ASSIGN(default_tube, make_tube("default"));
    a = make_job(1, 0, 1, 0, default_tube);
    b = make_job(-5, 0, 1, 0, default_tube);

    assertf(job_pri_less(a, b), "should be less");

    a = make_job(-5, 0, 1, 0, default_tube);
    b = make_job(1, 0, 1, 0, default_tube);

    assertf(!job_pri_less(a, b), "should not be less");
}

void
cttest_job_hash_free()
{
    Job *j, *neighbour;
    uint64 jid = 83;
    uint64 nid = jid + primes[0];   /* same bucket: the table is primes[0] wide here */

    TUBE_ASSIGN(default_tube, make_tube("default"));
    j = make_job_with_id(0, 0, 1, 0, default_tube, jid);
    assertf(j->r.id == jid,
            "make_job_with_id must use the id it was handed: asked %"PRIu64", got %"PRIu64,
            jid, j->r.id);
    assertf(job_find(jid) == j,
            "job %"PRIu64" must be IN the index before the free, or its later absence "
            "proves nothing", jid);

    neighbour = make_job_with_id(0, 0, 1, 0, default_tube, nid);
    assertf(job_find(nid) == neighbour,
            "the bucket neighbour %"PRIu64" must be indexed too", nid);
    assertf(get_all_jobs_used() == 2,
            "two stored jobs must count as 2, count is %zu", get_all_jobs_used());

    job_free(NULL);
    assertf(get_all_jobs_used() == 2,
            "job_free(NULL) must be a no-op, count is %zu", get_all_jobs_used());

    job_free(j);

    assertf(!job_find(jid), "job should be missing");
    assertf(job_find(nid) == neighbour,
            "freeing job %"PRIu64" must not unlink its bucket neighbour %"PRIu64,
            jid, nid);
    assertf(get_all_jobs_used() == 1,
            "one free must drop the count by exactly one: count is %zu, want 1",
            get_all_jobs_used());

    job_free(neighbour);
    assertf(get_all_jobs_used() == 0,
            "the index must be empty once both jobs are freed, count is %zu",
            get_all_jobs_used());
}

void
cttest_job_hash_free_next()
{
    Job *a, *b;
    uint64 aid = 97, bid = aid + primes[0];  /* 12386 while the table is primes[0] wide */

    TUBE_ASSIGN(default_tube, make_tube("default"));
    b = make_job_with_id(0, 0, 1, 0, default_tube, bid);
    a = make_job_with_id(0, 0, 1, 0, default_tube, aid);

    assertf(a->ht_next == b, "b should be chained to a");
    assertf(job_find(aid) == a && job_find(bid) == b,
            "both colliding jobs (%"PRIu64", %"PRIu64") must be findable before the free",
            aid, bid);
    assertf(get_all_jobs_used() == 2,
            "two colliding jobs must count as 2, count is %zu", get_all_jobs_used());

    job_free(b);

    assertf(a->ht_next == NULL, "b must be unlinked from a's chain");
    assertf(job_find(bid) == NULL,
            "the freed job %"PRIu64" must be gone from its bucket", bid);
    assertf(job_find(aid) == a,
            "unlinking the tail must leave the head of the bucket findable: job %"PRIu64,
            aid);
    assertf(get_all_jobs_used() == 1,
            "freeing one of two jobs must leave the count at 1, count is %zu",
            get_all_jobs_used());

    job_free(a);
    assertf(get_all_jobs_used() == 0,
            "the index must be empty once both jobs are freed, count is %zu",
            get_all_jobs_used());
}

void
cttest_job_all_jobs_used()
{
    Job *j, *x, *live, *copy, *twin;
    uint64 live_id = 4242;

    TUBE_ASSIGN(default_tube, make_tube("default"));
    assertf(get_all_jobs_used() == 0,
            "the index must start empty, count is %zu", get_all_jobs_used());

    j = make_job(0, 0, 1, 0, default_tube);
    assertf(get_all_jobs_used() == 1,
            "should match: make_job must index its job, count is %zu, want 1",
            get_all_jobs_used());

    x = allocate_job(10);
    assertf(get_all_jobs_used() == 1,
            "should match: allocate_job must not index its job, count is %zu, want 1",
            get_all_jobs_used());

    job_free(x);
    assertf(get_all_jobs_used() == 1,
            "should match: freeing a never-indexed job must not decrement, "
            "count is %zu, want 1", get_all_jobs_used());

    job_free(j);
    assertf(get_all_jobs_used() == 0,
            "should match: freeing the indexed job must empty the index, count is %zu",
            get_all_jobs_used());

    live = make_job_with_id(1, 0, 1, 0, default_tube, live_id);
    assertf(live != NULL, "the live job must allocate");
    assertf(get_all_jobs_used() == 1,
            "the live job must be indexed, count is %zu, want 1", get_all_jobs_used());

    copy = job_copy(live);
    assertf(copy != NULL, "job_copy must not return NULL");
    assertf(get_all_jobs_used() == 1,
            "a copy must stay out of the index, count is %zu, want 1",
            get_all_jobs_used());

    job_free(copy);
    assertf(get_all_jobs_used() == 1,
            "freeing a copy must not decrement the index, count is %zu, want 1",
            get_all_jobs_used());

    twin = allocate_job(0);
    assertf(twin != NULL, "the twin must allocate");
    twin->r.id = live_id;   /* never stored, but hashes to the live job's bucket AND key */
    job_free(twin);
    assertf(job_find(live_id) == live,
            "freeing an unstored job carrying a live job's id must not unlink job "
            "%"PRIu64": the bucket walk matches by pointer, not by key", live_id);
    assertf(get_all_jobs_used() == 1,
            "that free must not decrement either, count is %zu, want 1",
            get_all_jobs_used());

    job_free(live);
    assertf(get_all_jobs_used() == 0,
            "every job freed, so the index must be empty, count is %zu",
            get_all_jobs_used());
}

void
cttest_job_100_000_jobs()
{
    int i;
    const int n = 100000;
    Job *first, *j, *after;
    uint64 base;

    TUBE_ASSIGN(default_tube, make_tube("default"));

    first = make_job(0, 0, 1, 0, default_tube);
    assertf(first != NULL, "the first job must allocate");
    base = first->r.id;

    for (i = 1; i < n; i++) {
        assertf(make_job(0, 0, 1, 0, default_tube) != NULL,
                "job %d of %d must allocate", i, n);
    }
    assertf(get_all_jobs_used() == (size_t)n,
            "should match: %d stored jobs must count as %d, count is %zu",
            n, n, get_all_jobs_used());
    assertf(job_find(base) == first,
            "job %"PRIu64" must survive every rehash upscale and stay findable",
            base);

    for (i = 0; i < n; i++) {
        uint64 id = base + (uint64)i;
        j = job_find(id);
        assertf(j != NULL && j->r.id == id,
                "job %"PRIu64" must stay findable in both tables while the hash rehashes",
                id);
        job_free(j);
        assertf(get_all_jobs_used() == (size_t)(n - 1 - i),
                "each free must drop the count by exactly one: after %d frees "
                "the count is %zu, want %d", i + 1, get_all_jobs_used(), n - 1 - i);
    }
    assertf(get_all_jobs_used() == 0,
            "should match: the index must be empty again, count is %zu",
            get_all_jobs_used());

    after = make_job(0, 0, 1, 0, default_tube);
    assertf(after != NULL, "a job must still allocate after the downscale");
    assertf(job_find(after->r.id) == after,
            "the shrunken table must still index new jobs: job %"PRIu64" is not findable",
            after->r.id);
    job_free(after);
}

void
ctbench_job_make(int n)
{
    int i;
    Job **j = calloc(n, sizeof *j);
    assertf(j != NULL, "calloc of %d job slots must succeed", n);
    TUBE_ASSIGN(default_tube, make_tube("default"));

    ctresettimer();
    for (i = 0; i < n; i++) {
        j[i] = make_job(0, 0, 1, 0, default_tube);
    }
    ctstoptimer();

    assertf(get_all_jobs_used() == (size_t)n,
            "every made job must be indexed: count is %zu, want %d",
            get_all_jobs_used(), n);
    for (i = 0; i < n; i++) {
        assertf(j[i] != NULL && j[i]->r.id != 0,
                "job %d must be a real job with an issued id", i);
        job_free(j[i]);
    }
    assertf(get_all_jobs_used() == 0,
            "freeing every job must empty the index: count is %zu", get_all_jobs_used());
    free(j);
}

void
ctbench_job_free(int n)
{
    int i;
    Job **j = calloc(n, sizeof *j);
    assertf(j != NULL, "calloc of %d job slots must succeed", n);
    TUBE_ASSIGN(default_tube, make_tube("default"));
    for (i = 0; i < n; i++) {
        j[i] = make_job(0, 0, 1, 0, default_tube);
        assertf(j[i] != NULL, "job %d must allocate before the timed run", i);
    }
    assertf(get_all_jobs_used() == (size_t)n,
            "all %d jobs must be indexed before the timed run, count is %zu",
            n, get_all_jobs_used());

    ctresettimer();
    for (i = 0; i < n; i++) {
        job_free(j[i]);
    }
    ctstoptimer();

    assertf(get_all_jobs_used() == 0,
            "every free must unindex its job: count is %zu, want 0", get_all_jobs_used());
    free(j);
}
