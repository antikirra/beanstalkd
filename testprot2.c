#include "ct/ct.h"
#include "dat.h"
#include "testinject.h"
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* Defined in prot.c, exposed here for hostile unit tests that need to
 * observe or seed the global stats counters directly. */
extern struct stats global_stat;

/*
 * Hostile tests for Phase 6 optimizations:
 * - delayed_ct global counter
 * - delay_tube_heap (global tube delay heap)
 * - soonest_delayed_job() O(1) via global heap
 * - delay_tube_update OOM resilience
 * - now cache correctness
 *
 * These tests are HOSTILE: they mutate state, force edge cases,
 * and verify counters across all code paths.
 */

/* Access to prot.c internals needed for testing.
 * These are extern or accessible via dat.h. */

/* The period prottick returns when it has found no work at all. */
#define PROTTICK_IDLE_PERIOD 0x34630B8A000LL

/* Registers delayed jobs the way a live `put <pri> <delay> ...` does.
 * prot_replay is the only entry point outside prot.c that reaches
 * enqueue_job's delayed branch, so it is also the only way a test can
 * move delayed_ct, the per-tube delay heap AND the global
 * delay_tube_heap together. A hand-rolled heapinsert moves the first
 * two and silently skips the third — which is exactly why every delay
 * test below used to be blind to delay_tube_update. */
static void
replay_delayed(Server *s, Job **jobs, int n)
{
    Job list;
    job_list_reset(&list);
    for (int i = 0; i < n; i++) {
        jobs[i]->r.state = Delayed;
        job_list_insert(&list, jobs[i]);
    }
    assertf(prot_replay(s, &list) == 1,
            "prot_replay must accept %d delayed jobs", n);
}

/* prottick's return value is the only public window onto
 * soonest_delayed_job(): it must name the earliest deadline registered
 * anywhere. Bracketed by clock samples taken around the call, so the
 * check carries no slack beyond the measurement itself. */
static void
assert_tick_wakes_at(Server *s, int64 deadline, const char *ctx)
{
    int64 t0 = nanoseconds();
    int64 p = prottick(s);
    int64 t1 = nanoseconds();
    assertf(p <= deadline - t0 && p >= deadline - t1,
            "%s: prottick must wake for the soonest delayed job "
            "(deadline %" PRId64 "); period %" PRId64
            " is outside [%" PRId64 ", %" PRId64 "]",
            ctx, deadline, p, deadline - t1, deadline - t0);
}

/* Number of distinct hashes over n names sharing `prefix`. A hash that
 * stops reading after the first 8 or 16 bytes collapses them all. */
static int
distinct_tail_hashes(const char *prefix, int n)
{
    uint seen[128];
    int distinct = 0;
    for (int i = 0; i < n && i < 128; i++) {
        char name[80];
        snprintf(name, sizeof name, "%s%03d", prefix, i);
        seen[i] = tube_name_hash(name);
        int dup = 0;
        for (int k = 0; k < i; k++)
            dup |= (seen[k] == seen[i]);
        distinct += !dup;
    }
    return distinct;
}

/* --- delayed_ct counter correctness --- */

void
cttest_delayed_ct_starts_zero()
{
    /* Fresh state: prot_init sets up tubes, no delayed jobs exist. */
    prot_init();
    /* After prot_init, delayed_ct must be 0 because no jobs exist. */
    /* We test this indirectly: get_delayed_job_ct is exposed via stats,
     * but we can verify by creating zero delayed jobs. */

    Tube *t = tube_find_or_make("delay-test");
    tube_iref(t);

    assertf(t->delay.len == 0, "fresh tube must have no delayed jobs");

    /* The only public window on the counter: with nothing delayed
     * anywhere, prottick must find no delayed work and park for its
     * full idle period. A tube pre-registered in the global delay heap
     * by prot_init, or a residual delayed job, shortens it. */
    Tube *fresh = tube_find_or_make("delay-test-fresh");
    tube_iref(fresh);
    assertf(fresh->in_delay_heap == 0,
            "a tube with no delayed job must not sit in the global delay heap");

    mem_trim_rate = 0;
    Server s = {0};
    int64 idle = prottick(&s);
    assertf(idle == PROTTICK_IDLE_PERIOD,
            "with nothing delayed prottick must park for the full idle "
            "period, got %" PRId64, idle);

    tube_dref(fresh);
    tube_dref(t);
}

void
cttest_delayed_ct_enqueue_and_remove()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("dct-er");
    tube_iref(t);

    /* Create jobs and put them directly into delay heap to test the counter.
     * We use the delay heap directly because enqueue_job requires a Server. */
    const int N = 5;
    Job *jobs[5];
    int i;
    for (i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1000000000, 0, t);
        assertf(jobs[i], "alloc job %d", i);
        /* Simulate delayed state: insert into tube's delay heap */
        jobs[i]->r.deadline_at = now + (i + 1) * 1000000000LL;
        int r = heapinsert(&t->delay, jobs[i]);
        assertf(r, "heapinsert %d", i);
        jobs[i]->r.state = Delayed;
    }

    assertf(t->delay.len == (size_t)N,
            "tube must have %d delayed jobs, got %zu", N, t->delay.len);

    /* Remove each job from delay heap */
    for (i = 0; i < N; i++) {
        heapremove(&t->delay, jobs[i]->heap_index);
        jobs[i]->r.state = Ready; /* so job_free doesn't complain */
    }

    assertf(t->delay.len == 0,
            "tube must have 0 delayed jobs after removal, got %zu", t->delay.len);

    /* Everything above moves heap.c's bookkeeping only. Re-run the same
     * tube through the production writer so delayed_ct and the global
     * delay_tube_heap actually move: enqueue_job must register the tube
     * (delay_tube_update) or the job is invisible to prottick forever. */
    mem_trim_rate = 0;
    Server s = {0};
    Job *live[3];
    int64 base = nanoseconds();
    for (i = 0; i < 3; i++) {
        live[i] = make_job(1, 30000000000LL, 1000000000, 0, t);
        assertf(live[i], "live job %d must allocate", i);
        live[i]->r.deadline_at = base + (int64)(30 + i * 10) * 1000000000LL;
    }
    replay_delayed(&s, live, 3);

    assertf(t->in_delay_heap == 1,
            "enqueue_job must register the tube in the global delay heap");
    assert_tick_wakes_at(&s, live[0]->r.deadline_at, "delayed enqueue");

    for (i = 0; i < 3; i++) {
        heapremove(&t->delay, live[i]->heap_index);
        job_free(live[i]);
    }

    for (i = 0; i < N; i++)
        job_free(jobs[i]);

    tube_dref(t);
}

/* --- delay_tube_heap: global heap tracks tubes correctly --- */

void
cttest_delay_tube_heap_single_tube()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("dth-single");
    tube_iref(t);

    /* Fresh tube must NOT be in global delay heap */
    assertf(t->in_delay_heap == 0,
            "fresh tube must not be in delay heap");

    /* Add a delayed job */
    Job *j = make_job(1, 0, 1000000000, 0, t);
    assertf(j, "alloc job");
    j->r.deadline_at = now + 5000000000LL; /* 5 seconds from now */
    int r = heapinsert(&t->delay, j);
    assertf(r, "heapinsert");
    j->r.state = Delayed;

    /* Manually call delay_tube_update — in production, enqueue_job does this */
    /* We need access to the static function. Instead, verify the tube state
     * is consistent by checking the tube's delay heap. */

    assertf(t->delay.len == 1, "tube must have 1 delayed job");

    /* The promise this test's name makes, finally checked: once a tube
     * holds a delayed job it must be VISIBLE in the global delay heap,
     * or soonest_delayed_job never sees it and prottick sleeps straight
     * past the deadline. Registration lives inside enqueue_job, so it
     * has to be driven through prot_replay, not a hand heapinsert. */
    mem_trim_rate = 0;
    Server s = {0};
    Tube *live = tube_find_or_make("dth-single-live");
    tube_iref(live);
    Job *lj = make_job(1, 30000000000LL, 1000000000, 0, live);
    assertf(lj, "live delayed job must allocate");
    lj->r.deadline_at = nanoseconds() + 30000000000LL;
    replay_delayed(&s, &lj, 1);

    assertf(live->in_delay_heap == 1,
            "a tube holding a delayed job must be in the global delay heap");
    assert_tick_wakes_at(&s, lj->r.deadline_at, "single tube");

    heapremove(&live->delay, lj->heap_index);
    job_free(lj);
    tube_dref(live);

    /* Clean up */
    heapremove(&t->delay, j->heap_index);
    job_free(j);
    tube_dref(t);
}

void
cttest_delay_tube_heap_ordering_multi_tube()
{
    now = nanoseconds();
    prot_init();

    /* Create 3 tubes with delayed jobs at different deadlines */
    Tube *t1 = tube_find_or_make("dth-early");
    Tube *t2 = tube_find_or_make("dth-mid");
    Tube *t3 = tube_find_or_make("dth-late");
    tube_iref(t1);
    tube_iref(t2);
    tube_iref(t3);

    Job *j1 = make_job(1, 0, 1000000000, 0, t1);
    Job *j2 = make_job(1, 0, 1000000000, 0, t2);
    Job *j3 = make_job(1, 0, 1000000000, 0, t3);

    /* j1 has the EARLIEST deadline, j3 the LATEST */
    j1->r.deadline_at = now + 1000000000LL;  /* 1s */
    j2->r.deadline_at = now + 3000000000LL;  /* 3s */
    j3->r.deadline_at = now + 5000000000LL;  /* 5s */

    /* Insert in REVERSE order to test heap ordering */
    heapinsert(&t3->delay, j3); j3->r.state = Delayed;
    heapinsert(&t2->delay, j2); j2->r.state = Delayed;
    heapinsert(&t1->delay, j1); j1->r.state = Delayed;

    /* Verify each tube's delay heap is correct */
    assertf(t1->delay.len == 1 && t1->delay.data[0] == j1,
            "t1 must have j1 at top");
    assertf(t2->delay.len == 1 && t2->delay.data[0] == j2,
            "t2 must have j2 at top");
    assertf(t3->delay.len == 1 && t3->delay.data[0] == j3,
            "t3 must have j3 at top");

    /* CROSS-tube ordering — the thing this test is named for, and the
     * thing the three per-tube checks above cannot see. Three tubes
     * registered through the production path with soonest deadlines of
     * 50s, 30s and 10s, handed over latest-first so the global heap has
     * to sift. prottick must wake for the 10s tube; inverting
     * tube_delay_less parks it on the 50s one instead. */
    mem_trim_rate = 0;
    Server s = {0};
    Tube *g1 = tube_find_or_make("dth-g-late");
    Tube *g2 = tube_find_or_make("dth-g-mid");
    Tube *g3 = tube_find_or_make("dth-g-early");
    assertf(g1 && g2 && g3, "cross-tube tubes must allocate");
    tube_iref(g1);
    tube_iref(g2);
    tube_iref(g3);

    int64 base = nanoseconds();
    Job *gj[3];
    gj[0] = make_job(1, 50000000000LL, 1000000000, 0, g1);
    gj[1] = make_job(1, 30000000000LL, 1000000000, 0, g2);
    gj[2] = make_job(1, 10000000000LL, 1000000000, 0, g3);
    assertf(gj[0] && gj[1] && gj[2], "cross-tube jobs must allocate");
    gj[0]->r.deadline_at = base + 50000000000LL;
    gj[1]->r.deadline_at = base + 30000000000LL;
    gj[2]->r.deadline_at = base + 10000000000LL;
    replay_delayed(&s, gj, 3);

    assert_tick_wakes_at(&s, gj[2]->r.deadline_at, "cross-tube ordering");

    heapremove(&g1->delay, gj[0]->heap_index);
    heapremove(&g2->delay, gj[1]->heap_index);
    heapremove(&g3->delay, gj[2]->heap_index);
    job_free(gj[0]);
    job_free(gj[1]);
    job_free(gj[2]);
    tube_dref(g1);
    tube_dref(g2);
    tube_dref(g3);

    /* Clean up */
    heapremove(&t1->delay, j1->heap_index);
    heapremove(&t2->delay, j2->heap_index);
    heapremove(&t3->delay, j3->heap_index);
    job_free(j1);
    job_free(j2);
    job_free(j3);
    tube_dref(t1);
    tube_dref(t2);
    tube_dref(t3);
}

/* --- now cache: allocate_job uses cached value --- */

void
cttest_now_cache_used_by_allocate_job()
{
    /* Set now to a known value */
    now = 42000000000LL; /* 42 seconds */

    Job *j = allocate_job(10);
    assertf(j, "must allocate");
    assertf(j->r.created_at == 42000000000LL,
            "created_at must use cached now=42s, got %lld",
            (long long)j->r.created_at);

    /* A single fixed 42s probe cannot separate "reads the cache" from
     * "returns 42s" or from "now + K": take a second sample at a
     * different cached value. */
    int64 second = 7000000000LL; /* 7 seconds */
    now = second;
    Job *j2 = allocate_job(10);
    assertf(j2, "second job must allocate");
    assertf(j2->r.created_at == second,
            "created_at must track the cache on every call, want %lld "
            "got %lld", (long long)second, (long long)j2->r.created_at);
    free(j2);

    free(j);
}

void
cttest_now_cache_fallback_when_zero()
{
    /* When now=0 (before prot_init), must fall back to nanoseconds() */
    int64 before = nanoseconds();
    now = 0;

    Job *j = allocate_job(10);
    int64 after = nanoseconds();
    assertf(j, "must allocate");
    assertf(j->r.created_at > 0,
            "created_at must be > 0 even when now=0 (fallback), got %lld",
            (long long)j->r.created_at);
    /* ">0" is satisfied by created_at=1 or by any constant. The promise
     * is a REAL wall-clock stamp, so bracket it with the clock samples
     * taken either side of the call. */
    assertf(j->r.created_at >= before && j->r.created_at <= after,
            "the now=0 fallback must stamp the live clock, not a sentinel: "
            "created_at %lld outside [%lld, %lld]",
            (long long)j->r.created_at, (long long)before, (long long)after);

    /* Restore now for other tests */
    now = nanoseconds();
    free(j);
}

/* --- delay heap: tube in_delay_heap flag integrity --- */

void
cttest_tube_delay_heap_flag_init()
{
    Tube *t = make_tube("flag-test");
    tube_iref(t);
    assertf(t->in_delay_heap == 0,
            "fresh tube: in_delay_heap must be 0, got %d", t->in_delay_heap);
    assertf(t->delay_heap_index == 0,
            "fresh tube: delay_heap_index must be 0");

    /* Both fields above are zero because Tube comes from zalloc, so the
     * flag was never shown to TRACK anything. Register a tube for real,
     * kick its only delayed job (delay_tube_update's removal branch),
     * and require the flag back down — a stale 1 sends the tube's NEXT
     * delayed job through the in-place resift branch at an index it no
     * longer occupies, and the tube stops being visible to
     * soonest_delayed_job for good. */
    now = nanoseconds();
    prot_init();
    mem_trim_rate = 0;
    Server s = {0};

    Tube *lt = tube_find_or_make("flag-live");
    tube_iref(lt);
    Job *j1 = make_job(1, 20000000000LL, 1000000000, 0, lt);
    assertf(j1, "first delayed job must allocate");
    j1->r.deadline_at = nanoseconds() + 20000000000LL;
    replay_delayed(&s, &j1, 1);
    assertf(lt->in_delay_heap == 1,
            "tube must enter the delay heap when it gains a delayed job");

    assertf(kick_delayed_job(&s, j1) == 1,
            "kick must promote the tube's only delayed job");
    assertf(lt->in_delay_heap == 0,
            "tube must leave the delay heap when its last delayed job goes, "
            "got in_delay_heap=%d", lt->in_delay_heap);

    Job *j2 = make_job(1, 15000000000LL, 1000000000, 0, lt);
    assertf(j2, "second delayed job must allocate");
    j2->r.deadline_at = nanoseconds() + 15000000000LL;
    replay_delayed(&s, &j2, 1);
    assert_tick_wakes_at(&s, j2->r.deadline_at, "re-registration after kick");

    heapremove(&lt->ready, j1->heap_index);
    heapremove(&lt->delay, j2->heap_index);
    job_free(j1);
    job_free(j2);
    tube_dref(lt);

    tube_dref(t);
}

/* --- stress: many tubes with delayed jobs, all correctly tracked --- */

void
cttest_delay_tubes_stress_100()
{
    now = nanoseconds();
    prot_init();

    const int N = 100;
    Tube *tbs[100];
    Job *jbs[100];
    int i;

    srand(42); /* deterministic */

    for (i = 0; i < N; i++) {
        char name[32];
        snprintf(name, sizeof(name), "stress-delay-%d", i);
        tbs[i] = tube_find_or_make(name);
        tube_iref(tbs[i]);

        jbs[i] = make_job(1, 0, 1000000000, 0, tbs[i]);
        assertf(jbs[i], "alloc job %d", i);
        /* Random deadlines spread over 100 seconds */
        jbs[i]->r.deadline_at = now + (rand() % 100 + 1) * 1000000000LL;
        int r = heapinsert(&tbs[i]->delay, jbs[i]);
        assertf(r, "heapinsert %d", i);
        jbs[i]->r.state = Delayed;

        assertf(tbs[i]->delay.len == 1,
                "tube %d must have exactly 1 delayed job", i);
    }

    /* Find the actual soonest by linear scan (reference implementation) */
    int64 min_deadline = INT64_MAX;
    int min_idx = -1;
    for (i = 0; i < N; i++) {
        if (jbs[i]->r.deadline_at < min_deadline) {
            min_deadline = jbs[i]->r.deadline_at;
            min_idx = i;
        }
    }
    assertf(min_idx >= 0, "must find a minimum");

    /* The soonest job (by linear scan) must be in the tube
     * that has the earliest deadline among all tubes */
    Job *soonest = jbs[min_idx];
    assertf(soonest->r.deadline_at == min_deadline,
            "soonest job deadline must match minimum");

    /* Remove all in a random-ish order (odd first, then even) */
    for (i = 1; i < N; i += 2) {
        heapremove(&tbs[i]->delay, jbs[i]->heap_index);
        jbs[i]->r.state = Ready;
    }
    for (i = 0; i < N; i += 2) {
        heapremove(&tbs[i]->delay, jbs[i]->heap_index);
        jbs[i]->r.state = Ready;
    }

    /* All tubes must have empty delay heaps */
    for (i = 0; i < N; i++) {
        assertf(tbs[i]->delay.len == 0,
                "tube %d delay heap must be empty after removal, got %zu",
                i, tbs[i]->delay.len);
    }

    /* The linear scan above was an oracle nobody consulted. Register the
     * same 100 tubes through the production writer and hold
     * soonest_delayed_job to that oracle: returning NULL, or the LATEST
     * tube instead of the earliest, must show up in prottick's period. */
    mem_trim_rate = 0;
    Server s = {0};
    Job *live[100];
    int64 base = nanoseconds();
    int64 live_min = INT64_MAX;
    srand(7);
    for (i = 0; i < N; i++) {
        live[i] = make_job(1, 10000000000LL, 1000000000, 0, tbs[i]);
        assertf(live[i], "live job %d must allocate", i);
        live[i]->r.deadline_at =
            base + (int64)(rand() % 100 + 10) * 1000000000LL;
        if (live[i]->r.deadline_at < live_min)
            live_min = live[i]->r.deadline_at;
    }
    replay_delayed(&s, live, N);
    assert_tick_wakes_at(&s, live_min, "100-tube soonest");

    for (i = 0; i < N; i++) {
        heapremove(&tbs[i]->delay, live[i]->heap_index);
        job_free(live[i]);
    }

    /* Clean up */
    for (i = 0; i < N; i++) {
        job_free(jbs[i]);
        tube_dref(tbs[i]);
    }
}

/* --- job_delay_less: comparator used by per-tube delay heaps --- */

void
cttest_job_delay_less_ordering()
{
    now = nanoseconds();
    Tube *t = make_tube("jdl");
    tube_iref(t);

    Job *early = make_job(1, 0, 1, 0, t);
    Job *late = make_job(1, 0, 1, 0, t);
    early->r.deadline_at = now + 1000000000LL;
    late->r.deadline_at = now + 5000000000LL;

    assertf(job_delay_less(early, late) == 1,
            "earlier deadline must be less");
    assertf(job_delay_less(late, early) == 0,
            "later deadline must not be less");

    /* Same deadline: tie-break by id (smaller id wins) */
    late->r.deadline_at = early->r.deadline_at;
    assertf(job_delay_less(early, late) == 1,
            "same deadline, smaller id must be less");

    /* The "smaller id wins" claim rested on make_job's allocation order;
     * say it out loud so the tie assertions mean what they read. */
    assertf(early->r.id < late->r.id,
            "fixture: `early` must own the smaller id (%llu vs %llu)",
            (unsigned long long)early->r.id, (unsigned long long)late->r.id);

    /* Both halves of the tie contract. `<=` on the id tie-break passes
     * the assertion above and still corrupts every heap it drives,
     * because a job would then sort before itself. */
    assertf(job_delay_less(late, early) == 0,
            "same deadline, larger id must not be less");
    assertf(job_delay_less(early, early) == 0,
            "a job must never sort before itself (comparator must be "
            "irreflexive or siftdown never terminates)");
    assertf(job_delay_less(late, late) == 0,
            "self-compare must be false for the larger id too");

    job_free(early);
    job_free(late);
    tube_dref(t);
}

/* --- heap ordering preserved after multiple insert/remove cycles --- */

void
cttest_delay_heap_insert_remove_cycle()
{
    now = nanoseconds();
    Tube *t = make_tube("cycle");
    tube_iref(t);

    /* The two assignments below re-install wiring make_tube already owns
     * — dead setup that would MASK a regression in make_tube. Pin the
     * wiring first so dropping it from make_tube reddens here instead of
     * being silently repaired by the test itself. */
    assertf(t->delay.less == job_delay_less,
            "make_tube must wire delay.less to job_delay_less");
    assertf(t->delay.setpos == job_setpos,
            "make_tube must wire delay.setpos to job_setpos");
    assertf(t->ready.less == job_pri_less,
            "make_tube must wire ready.less to job_pri_less");
    assertf(t->ready.setpos == job_setpos,
            "make_tube must wire ready.setpos to job_setpos");

    t->delay.less = job_delay_less;
    t->delay.setpos = job_setpos;

    const int N = 50;
    Job *jobs[50];
    int i;

    srand(42);
    for (i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1, 0, t);
        jobs[i]->r.deadline_at = now + (rand() % 1000 + 1) * 1000000LL;
        jobs[i]->r.state = Delayed;
        heapinsert(&t->delay, jobs[i]);
    }

    /* Remove all via heapremove(0) — must come out in deadline order */
    int64 prev_deadline = 0;
    for (i = 0; i < N; i++) {
        Job *j = heapremove(&t->delay, 0);
        assertf(j, "remove %d must succeed", i);
        assertf(j->r.deadline_at >= prev_deadline,
                "must be sorted: job %d deadline %lld < prev %lld",
                i, (long long)j->r.deadline_at, (long long)prev_deadline);
        prev_deadline = j->r.deadline_at;
    }
    assertf(t->delay.len == 0, "heap must be empty after removing all");

    for (i = 0; i < N; i++)
        job_free(jobs[i]);

    /* tube_free (via tube_dref) frees t->delay.data — don't double free */
    tube_dref(t);
}

/* --- prot_remove_tube: cleanup on tube free --- */

void
cttest_prot_remove_tube_cleans_pause()
{
    now = nanoseconds();
    prot_init();

    /* Create a paused tube, then free it via tube_dref.
     * tube_free calls prot_remove_tube which must handle a paused tube. */
    Tube *t = make_tube("pause-free");
    tube_iref(t);

    /* Simulate pause */
    t->pause = 5000000000LL;
    t->unpause_at = now + 5000000000LL;

    /* Free: refs 1→0 → tube_free → prot_remove_tube.
     * prot_remove_tube sees t->pause > 0 and must clean up the
     * pause-heap state. Must not crash, must not leave stale state. */
    tube_dref(t);

    /* Nothing was read after the free above, so "must not leave stale
     * state" was never checked — and t->in_pause_heap was never set, so
     * the guarded branch was never even entered. Here is the delay-heap
     * half with the branch actually taken: a tube freed while still
     * REGISTERED must be unlinked from delay_tube_heap, or
     * soonest_delayed_job keeps dereferencing freed memory at the heap
     * root. `doomed` owns the earlier deadline, so it IS the root; a
     * live second tube keeps delayed_ct positive so prottick really
     * consults it. */
    mem_trim_rate = 0;
    Server s = {0};

    Tube *doomed = tube_find_or_make("delayfree-doomed");
    Tube *keeper = tube_find_or_make("delayfree-keeper");
    assertf(doomed && keeper, "tubes must allocate");
    tube_iref(doomed);
    tube_iref(keeper);

    int64 base = nanoseconds();
    Job *dj = make_job(1, 10000000000LL, 1000000000, 0, doomed);
    Job *kj = make_job(1, 40000000000LL, 1000000000, 0, keeper);
    assertf(dj && kj, "delay-heap jobs must allocate");
    dj->r.deadline_at = base + 10000000000LL;
    kj->r.deadline_at = base + 40000000000LL;
    Job *both[2] = {dj, kj};
    replay_delayed(&s, both, 2);
    assertf(doomed->in_delay_heap == 1 && keeper->in_delay_heap == 1,
            "both tubes must be registered before the free");

    job_free(dj);      /* drops the doomed tube's last outside reference */
    tube_dref(doomed); /* refs 1 -> 0  =>  tube_free -> prot_remove_tube */

    assert_tick_wakes_at(&s, kj->r.deadline_at, "after freeing the root tube");

    heapremove(&keeper->delay, kj->heap_index);
    job_free(kj);
    tube_dref(keeper);
}

void
cttest_prot_remove_tube_no_crash_unpaused()
{
    now = nanoseconds();
    prot_init();

    /* Free a non-paused tube — prot_remove_tube must be a no-op. */
    Tube *t = make_tube("clean-free");
    tube_iref(t);

    assertf(t->pause == 0, "fresh tube must not be paused");
    assertf(t->in_delay_heap == 0, "fresh tube must not be in delay heap");

    tube_dref(t); /* → tube_free → prot_remove_tube (all zero, no-op) */

    /* Both assertions above are preconditions on a zalloc'd struct;
     * nothing at all was read AFTER the free, which is where a "no-op"
     * that is not a no-op would show. Read the global timer state now:
     * it must report no work, and it must still be able to take a new
     * registration and surface it through prottick. */
    mem_trim_rate = 0;
    Server s = {0};
    int64 idle = prottick(&s);
    assertf(idle == PROTTICK_IDLE_PERIOD,
            "freeing an unregistered tube must leave no phantom timer, "
            "got period %" PRId64, idle);

    Tube *after = tube_find_or_make("clean-free-after");
    assertf(after, "tube must allocate");
    tube_iref(after);
    Job *aj = make_job(1, 25000000000LL, 1000000000, 0, after);
    assertf(aj, "job must allocate");
    aj->r.deadline_at = nanoseconds() + 25000000000LL;
    replay_delayed(&s, &aj, 1);
    assert_tick_wakes_at(&s, aj->r.deadline_at, "delay heap after tube free");

    heapremove(&after->delay, aj->heap_index);
    job_free(aj);
    tube_dref(after);
}

/* --- ms_remove_at: O(1) hinted removal --- */

void
cttest_ms_remove_at_correct_hint()
{
    Ms a;
    ms_init(&a, NULL, NULL);
    int x = 1, y = 2, z = 3;
    ms_append(&a, &x); /* index 0 */
    ms_append(&a, &y); /* index 1 */
    ms_append(&a, &z); /* index 2 */

    /* Remove y at index 1 with correct hint — must succeed in O(1) */
    int r = ms_remove_at(&a, 1, &y);
    assertf(r == 1, "ms_remove_at with correct hint must succeed");
    assertf(a.len == 2, "len must be 2 after removal");

    /* y must not be findable */
    assertf(!ms_contains(&a, &y), "y must be gone");
    /* x and z must still be present */
    assertf(ms_contains(&a, &x), "x must remain");
    assertf(ms_contains(&a, &z), "z must remain");

    ms_clear(&a);
}

void
cttest_ms_remove_at_stale_hint()
{
    Ms a;
    ms_init(&a, NULL, NULL);
    int x = 1, y = 2, z = 3;
    ms_append(&a, &x);
    ms_append(&a, &y);
    ms_append(&a, &z);

    /* Remove z with WRONG hint (index 0, but z is at index 2).
     * Must fall back to linear scan and still succeed. */
    int r = ms_remove_at(&a, 0, &z);
    assertf(r == 1, "ms_remove_at with stale hint must still succeed via fallback");
    assertf(a.len == 2, "len must be 2");
    assertf(!ms_contains(&a, &z), "z must be gone");

    /* Out-of-range hint — the third class of stale hint, and the only
     * one that is a straight overread if the fast path is not bounds
     * gated. It must fall back to the scan and take the right element. */
    int r2 = ms_remove_at(&a, 99, &y);
    assertf(r2 == 1,
            "ms_remove_at with an out-of-range hint must still succeed "
            "via the scan, got %d", r2);
    assertf(a.len == 1 && ms_contains(&a, &x) && !ms_contains(&a, &y),
            "the out-of-range removal must take y and spare x (len %zu)",
            a.len);

    ms_clear(&a);
}

void
cttest_ms_remove_at_not_found()
{
    Ms a;
    ms_init(&a, NULL, NULL);
    int x = 1, y = 2;
    ms_append(&a, &x);

    /* Try to remove y which is not in the set */
    int r = ms_remove_at(&a, 0, &y);
    assertf(r == 0, "removing non-member must return 0");
    assertf(a.len == 1, "len must be unchanged");

    ms_clear(&a);
}

/* --- realloc in heap.c: grow under stress --- */

void
cttest_heap_realloc_stress()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int N = 1000;
    int i;
    now = nanoseconds();

    Tube *t = make_tube("heap-realloc");
    tube_iref(t);

    Job *jobs[1000];
    for (i = 0; i < N; i++) {
        jobs[i] = make_job(i, 0, 1, 0, t);
        assertf(jobs[i], "alloc job %d", i);
        int r = heapinsert(&h, jobs[i]);
        assertf(r, "heapinsert %d must succeed", i);
    }
    assertf(h.len == (size_t)N, "heap must have %d items, got %zu", N, h.len);
    assertf(h.cap >= (size_t)N, "cap must be >= %d", N);

    /* Remove all — must come out in priority order */
    uint32 prev_pri = 0;
    for (i = 0; i < N; i++) {
        Job *j = heapremove(&h, 0);
        assertf(j, "remove %d", i);
        assertf(j->r.pri >= prev_pri,
                "must be sorted: job %d pri %u < prev %u",
                i, j->r.pri, prev_pri);
        prev_pri = j->r.pri;
    }
    assertf(h.len == 0, "heap must be empty");

    for (i = 0; i < N; i++)
        job_free(jobs[i]);
    free(h.data);
    tube_dref(t);
}

/* --- realloc in ms.c: grow under stress --- */

void
cttest_ms_realloc_stress()
{
    Ms a;
    ms_init(&a, NULL, NULL);
    const int N = 1000;
    int items[1000];
    int i;

    for (i = 0; i < N; i++) {
        items[i] = i;
        int r = ms_append(&a, &items[i]);
        assertf(r, "ms_append %d must succeed", i);
    }
    assertf(a.len == (size_t)N, "ms must have %d items", N);

    /* Remove all by value — each is O(n) but we're testing correctness */
    for (i = 0; i < N; i++) {
        int r = ms_remove(&a, &items[i]);
        assertf(r, "ms_remove %d must succeed", i);
    }
    assertf(a.len == 0, "ms must be empty after removing all");

    free(a.items);
}

/* --- TCP_NODELAY: verify setsockopt doesn't crash on server start --- */
/* (This is covered by integration tests in testserv.c which fork a server
 * and connect to it. The accept path now calls setsockopt(TCP_NODELAY).
 * If it crashed, testserv would fail.) */

/* --- hash rehash without OOM latch --- */

void
cttest_job_hash_rehash_recovery()
{
    now = nanoseconds();
    Tube *t = make_tube("rehash");
    tube_iref(t);

    /* Create enough jobs to trigger rehash (load factor 4x at cap=12289) */
    const int N = 200;
    Job *jobs[200];
    int i;
    for (i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1, 0, t);
        assertf(jobs[i], "alloc job %d", i);
    }

    /* All jobs must be findable */
    for (i = 0; i < N; i++) {
        Job *found = job_find(jobs[i]->r.id);
        assertf(found == jobs[i],
                "job %d must be findable by id %llu",
                i, (unsigned long long)jobs[i]->r.id);
    }

    /* Free all */
    for (i = 0; i < N; i++)
        job_free(jobs[i]);

    /* Verify all gone */
    assertf(get_all_jobs_used() == 0,
            "all jobs must be freed, got %zu", get_all_jobs_used());

    tube_dref(t);
}

/* --- conn_ready: paused tube must not count as ready --- */

void
cttest_conn_ready_excludes_paused()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("ready-pause");
    tube_iref(t);

    /* Add a ready job to the tube */
    Job *j = make_job(1, 0, 1, 0, t);
    heapinsert(&t->ready, j);
    j->r.state = Ready;
    assertf(t->ready.len == 1, "tube must have 1 ready job");

    /* Create a connection watching this tube */
    Conn *c = make_conn(0, 0, t, t); /* state 0 = STATE_WANT_COMMAND */
    assertf(c, "make_conn must succeed");

    /* Unpauseed: conn_ready must return 1 */
    assertf(conn_ready(c) == 1, "conn_ready must be 1 when tube has ready jobs");

    /* Pause the tube */
    t->pause = 5000000000LL;

    /* Paused: conn_ready must return 0 */
    assertf(conn_ready(c) == 0,
            "conn_ready must be 0 when tube is paused, even with ready jobs");

    /* Unpause and verify recovery */
    t->pause = 0;
    assertf(conn_ready(c) == 1, "conn_ready must recover after unpause");

    /* Clean up */
    heapremove(&t->ready, j->heap_index);
    job_free(j);
    connclose(c);
    tube_dref(t);
}

/* --- job_hash downscale: never below initial size --- */

void
cttest_job_hash_no_downscale_at_initial()
{
    now = nanoseconds();
    Tube *t = make_tube("downscale");
    tube_iref(t);

    /* Create and immediately free jobs.
     * With cur_prime=0 guard, no downscale rehash should happen. */
    const int N = 50;
    Job *jobs[50];
    int i;

    for (i = 0; i < N; i++) {
        jobs[i] = make_job(1, 0, 1, 0, t);
        assertf(jobs[i], "alloc %d", i);
    }
    uint64 churned = jobs[0]->r.id;
    for (i = 0; i < N; i++)
        job_free(jobs[i]);

    /* The churn itself was never verified: a job_free that leaves the
     * id in the table, or a downscale that drops live entries, both
     * pass a bare "a fresh job is findable" probe. */
    assertf(get_all_jobs_used() == 0,
            "the churn must drain the id table, got %zu still used",
            get_all_jobs_used());
    assertf(job_find(churned) == NULL,
            "a freed job must leave no stale hash entry, id=%llu",
            (unsigned long long)churned);

    /* Hash table should still be functional */
    Job *j = make_job(1, 0, 1, 0, t);
    assertf(j, "post-churn alloc must succeed");
    assertf(job_find(j->r.id) == j, "post-churn find must work");
    job_free(j);

    tube_dref(t);
}

/* --- Tube hash --- */

void
cttest_tube_name_hash_deterministic()
{
    /* Same name must always produce same hash. */
    uint h1 = tube_name_hash("email");
    uint h2 = tube_name_hash("email");
    assertf(h1 == h2, "hash must be deterministic: %u != %u", h1, h2);

    /* Different names should (almost certainly) differ. */
    uint h3 = tube_name_hash("video");
    assertf(h1 != h3, "email and video should hash differently");

    /* Empty string is valid. */
    uint h4 = tube_name_hash("");
    uint h5 = tube_name_hash("");
    assertf(h4 == h5, "empty hash must be deterministic");
    assertf(h4 != h1, "empty must differ from email");

    /* "email" vs "video" differ in byte 0, so a hash that stops reading
     * after the first 8 (or 16) bytes still separates them. Feed a
     * family that differs ONLY in the tail: 64 names sharing a 24-byte
     * prefix must produce 64 distinct values. A truncating hash yields
     * 1. (A genuine 32-bit collision inside 64 keys has probability
     * ~5e-7, so this is a real signal, not a lottery.) */
    const char *prefix = "billing-eu-west-shard-00"; /* 24 bytes */
    int distinct = distinct_tail_hashes(prefix, 64);
    assertf(distinct == 64,
            "hash ignores the tail: only %d distinct values across 64 names "
            "sharing the %zu-byte prefix '%s'",
            distinct, strlen(prefix), prefix);
}

// ─── Hash distribution fairness ────────────────────────────
// 1000 random tube names across 8 buckets must not all land
// in the same bucket. This catches degenerate hash functions.

void
cttest_hash_distribution_fairness()
{
    int nbuckets = 8;
    int counts[8] = {0};
    int i;

    for (i = 0; i < 1000; i++) {
        char name[64];
        snprintf(name, sizeof(name), "workload-%d-task-%d", i / 10, i % 10);
        uint h = tube_name_hash(name) % nbuckets;
        assertf(h < (uint)nbuckets, "bucket out of bounds");
        counts[h]++;
    }

    for (i = 0; i < nbuckets; i++) {
        assertf(counts[i] > 20,
                "bucket %d got only %d of 1000 tubes — hash is degenerate",
                i, counts[i]);
    }

    for (i = 0; i < nbuckets; i++) {
        assertf(counts[i] < 300,
                "bucket %d got %d of 1000 tubes — hash is heavily skewed",
                i, counts[i]);
    }
}

// ─── Large-scale rehash correctness ─────────────────────────
// Insert 60,000 jobs (enough to trigger rehash from primes[0]=12289
// to primes[1]=24593 at 4x load factor ~49156), then verify every
// single job is findable, then delete all and verify empty.

void
cttest_job_hash_large_scale_rehash()
{
    Tube *t = make_tube("rehash-tube");
    tube_iref(t);

    int N = 60000;
    uint64 *ids = malloc(N * sizeof(uint64));
    assertf(ids != NULL, "OOM allocating id array");

    // Insert
    for (int i = 0; i < N; i++) {
        Job *j = make_job(1, 0, 1000000000, 4, t);
        assertf(j != NULL, "job %d must allocate", i);
        ids[i] = j->r.id;
    }

    // Every job must be findable
    for (int i = 0; i < N; i++) {
        Job *j = job_find(ids[i]);
        assertf(j != NULL, "job %"PRIu64" not found after rehash", ids[i]);
        assertf(j->r.id == ids[i], "wrong job returned for id %"PRIu64, ids[i]);
    }

    // Delete all
    for (int i = 0; i < N; i++) {
        Job *j = job_find(ids[i]);
        assertf(j != NULL, "job %"PRIu64" vanished before delete", ids[i]);
        job_free(j);
    }

    assertf(get_all_jobs_used() == 0,
            "all 60K jobs must be freed, got %zu", get_all_jobs_used());

    free(ids);
    tube_dref(t);
}


// ─── wyhash: avalanche on 1-bit input change ───────────────────
// A good hash flips ~50% of output bits for a 1-bit input flip.
// DJB2 notoriously leaves the low bits almost unchanged. Require
// at least 10 of 32 bits flipped — a conservative lower bound.
void
cttest_tube_name_hash_avalanche()
{
    char a[16] = "workload-task-X";
    char b[16] = "workload-task-Y";  // diff one byte, one bit: 'X'^'Y' = 1

    uint ha = tube_name_hash_n(a, 15);
    uint hb = tube_name_hash_n(b, 15);
    assertf(ha != hb, "single-byte diff must produce different hashes");

    int flipped = __builtin_popcount(ha ^ hb);
    assertf(flipped >= 10,
            "avalanche weak: only %d/32 bits flipped between '%s' and '%s' "
            "(0x%08x vs 0x%08x)", flipped, a, b, ha, hb);
}

// ─── Adversarial key family: incremental length ────────────────
// A pathological hash (e.g. plain DJB2 with no finalizer, or a
// hash that ignores length) collapses "a", "aa", "aaa", ... into
// one or two buckets. The test catches the catastrophic regime,
// not statistical outliers: with N=1024 items in 32 buckets
// (mean=32, stddev≈5.7) a healthy hash fills every bucket and
// keeps the maximum within ~3σ of the mean.
void
cttest_tube_name_hash_adversarial_family()
{
    enum { N = 1024, NBUCKETS = 32 };
    int buckets[NBUCKETS] = {0};

    char name[N + 1];
    memset(name, 'a', N);
    name[N] = '\0';

    for (int len = 1; len <= N; len++) {
        uint h = tube_name_hash_n(name, (size_t)len);
        buckets[h % NBUCKETS]++;
    }

    int empty = 0, max = 0;
    for (int i = 0; i < NBUCKETS; i++) {
        if (buckets[i] == 0) empty++;
        if (buckets[i] > max) max = buckets[i];
    }

    // Healthy hash: 0 empty buckets, max ≲ 60 (~3σ above mean=32).
    // Broken hash collapses to one bucket: empty≈31, max=N.
    assertf(empty <= 2,
            "wyhash clusters 'a'×N family: %d empty buckets / %d",
            empty, NBUCKETS);
    assertf(max <= 60,
            "wyhash peaks at %d in one bucket (mean=%d) — clustering",
            max, N / NBUCKETS);
}

// ─── Length-invariant determinism ──────────────────────────────
// tube_name_hash_n(s, len) must equal tube_name_hash(s) when s is
// NUL-terminated with length `len`. Catches a bug where _n picks
// up the trailing NUL or misreads len.
void
cttest_tube_name_hash_n_matches_name_hash()
{
    const char *samples[] = {
        "", "a", "default", "email-priority-high",
        "this-is-a-much-longer-tube-name-to-stress-the-16-byte-branch",
    };
    for (size_t i = 0; i < sizeof(samples) / sizeof(samples[0]); i++) {
        size_t len = strlen(samples[i]);
        uint ha = tube_name_hash(samples[i]);
        uint hb = tube_name_hash_n(samples[i], len);
        assertf(ha == hb,
                "hash drift for '%s': strlen path=%u explicit path=%u",
                samples[i], ha, hb);
    }
}

// ─── Hash distribution at full TUBE_HASH_SIZE (4096) ───────────
// Heavier than cttest_hash_distribution_fairness: 10K realistic
// tube names, all 4096 buckets, p99 load factor must be bounded.
void
cttest_tube_name_hash_full_table_fairness()
{
    enum { N = 10000, NB = 4096 };
    int *buckets = calloc(NB, sizeof(int));
    assertf(buckets, "calloc must succeed");

    for (int i = 0; i < N; i++) {
        char name[64];
        snprintf(name, sizeof name, "queue-%d-shard-%d", i / 97, i % 97);
        uint h = tube_name_hash(name) % NB;
        buckets[h]++;
    }

    int empty = 0, max = 0;
    for (int i = 0; i < NB; i++) {
        if (buckets[i] == 0) empty++;
        if (buckets[i] > max) max = buckets[i];
    }

    // Poisson with mean 10000/4096 ≈ 2.44: P(bucket empty) ≈ e^-2.44 ≈ 0.087.
    // Expected ~356 empty. Allow wide margin (up to 900 ≈ 22%).
    assertf(empty < 900,
            "too many empty buckets: %d / %d — hash is skewed", empty, NB);
    // Max bucket > 12 would indicate clustering; Poisson tail predicts < 15.
    assertf(max <= 15,
            "max bucket depth %d too high — clustering detected", max);

    free(buckets);
}


// #N5 regression: enqueue_reserved_jobs must null j->reserver before
// the Conn pointer can be recycled. Previously the back-pointer stayed
// set; is_job_reserved_by_conn's state==Reserved guard saved us today,
// but a future reader without that gate would UAF the returned-to-pool
// Conn slab.
void
cttest_enqueue_reserved_jobs_clears_reserver()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("rsv-clear");
    tube_iref(t);

    Conn *c = make_conn(0, 0, t, t);
    assertf(c, "make_conn must succeed");
    c->srv = &srv;

    /* Build a reserved job by hand — we avoid the network state
     * machine so the test stays a pure unit test. */
    Job *j = make_job(1, 0, 1000000000LL, 0, t);
    assertf(j, "job must allocate");
    j->r.state = Reserved;
    j->r.deadline_at = now + 1000000000LL;
    job_list_insert(&c->reserved_jobs, j);
    j->reserver = c;
    t->stat.reserved_ct = 1;
    global_stat.reserved_ct = 1;

    assertf(j->reserver == c, "precondition: reserver set");

    enqueue_reserved_jobs(c);

    assertf(j->reserver == NULL,
            "enqueue_reserved_jobs must null reserver, got %p",
            (void *)j->reserver);
    assertf(j->r.state == Ready,
            "job must be re-enqueued as Ready, got state=%d", j->r.state);
    assertf(global_stat.reserved_ct == 0,
            "reserved_ct must be decremented, got %" PRIu64,
            global_stat.reserved_ct);

    /* Clean up: remove from ready heap then free. */
    heapremove(&t->ready, j->heap_index);
    job_free(j);
    connclose(c);
    tube_dref(t);
}


// #N5 regression (multi-job): a mutation that nulls only the first
// reserver and forgets the loop body would pass the single-job test
// above. This variant seeds THREE reserved jobs and asserts every one
// has reserver cleared + global counter drained to zero. Also locks
// the empty-list no-op path via a pre-check.
void
cttest_enqueue_reserved_jobs_clears_all_reservers()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("rsv-multi");
    tube_iref(t);

    Conn *c = make_conn(0, 0, t, t);
    assertf(c, "make_conn must succeed");
    c->srv = &srv;

    /* Empty-list guard: enqueue_reserved_jobs on an empty conn must be
     * a clean no-op — no counter drift, no crash. */
    uint64 pre = global_stat.reserved_ct;
    enqueue_reserved_jobs(c);
    assertf(global_stat.reserved_ct == pre,
            "empty reserved_jobs must leave reserved_ct unchanged "
            "(was %" PRIu64 ", now %" PRIu64 ")",
            pre, global_stat.reserved_ct);

    /* Seed three reserved jobs with distinct priorities so they occupy
     * distinct slots in the heap after re-enqueue. */
    Job *jobs[3];
    for (int i = 0; i < 3; i++) {
        jobs[i] = make_job((uint32)(i + 1), 0, 1000000000LL, 0, t);
        assertf(jobs[i], "job %d must allocate", i);
        jobs[i]->r.state = Reserved;
        jobs[i]->r.deadline_at = now + 1000000000LL;
        job_list_insert(&c->reserved_jobs, jobs[i]);
        jobs[i]->reserver = c;
    }
    t->stat.reserved_ct = 3;
    global_stat.reserved_ct = 3;

    enqueue_reserved_jobs(c);

    for (int i = 0; i < 3; i++) {
        assertf(jobs[i]->reserver == NULL,
                "job %d reserver must be nulled, got %p",
                i, (void *)jobs[i]->reserver);
        assertf(jobs[i]->r.state == Ready,
                "job %d must be Ready after re-enqueue, got state=%d",
                i, jobs[i]->r.state);
    }
    assertf(global_stat.reserved_ct == 0,
            "reserved_ct must be 0 after all three re-enqueued, got %" PRIu64,
            global_stat.reserved_ct);
    assertf(t->stat.reserved_ct == 0,
            "tube reserved_ct must be 0, got %" PRIu64, t->stat.reserved_ct);

    /* Clean up: remove all from ready heap, free, close conn. */
    for (int i = 0; i < 3; i++) {
        heapremove(&t->ready, jobs[i]->heap_index);
        job_free(jobs[i]);
    }
    connclose(c);
    tube_dref(t);
}


// #7 net.c: make_server_socket for a unix: path longer than
// sizeof(sun_path)-1 must be rejected with -1 BEFORE any socket() /
// bind() / listen() syscall. Catches a regression that silently
// truncates sun_path (observed historically in other daemons via
// strncpy semantics).
void
cttest_make_server_socket_unix_path_too_long()
{
    // Linux sun_path is 108 bytes. We build "unix:" + 150 'x' so that
    // the path arg to make_unix_socket is 150 chars — well over the
    // 107 effective limit (maxlen = sizeof(sun_path) - 1).
    char addr[256];
    const size_t pad = 150;
    strcpy(addr, "unix:");
    memset(addr + 5, 'x', pad);
    addr[5 + pad] = '\0';

    int fd = make_server_socket(addr, "0");
    assertf(fd == -1,
            "make_server_socket must reject over-long unix path, got fd=%d", fd);
}


/* ------------------------------------------------------------------ */
/* prottick -m trim cadence (mem-trim-never-fires-when-idle)          */
/* ------------------------------------------------------------------ */

// An idle server must keep waking at the -m cadence. Pre-fix, prottick
// never fed the trim deadline into its return value, so an idle server
// parked in epoll for the 1h default and -m silently never fired in
// exactly its target scenario (mass delete, then workers idle on plain
// reserve). Kill shot: on the old code both calls return 1h.
void
cttest_prottick_idle_trim_deadline_bounds_period()
{
    now = nanoseconds();
    prot_init();
    Server s = {0};

    mem_trim_rate = 5000000000LL; // -m 5

    // First tick either fires the trim (last_trim = now) or is already
    // counting down from process start; both leave a live deadline.
    int64 p1 = prottick(&s);
    assertf(p1 > 0 && p1 <= mem_trim_rate,
            "tick 1: period must be bounded by the -m deadline, got %" PRId64,
            p1);

    // Second tick: the server is completely idle (no delayed jobs,
    // no conns) — only the trim deadline can bound it.
    int64 p2 = prottick(&s);
    assertf(p2 > 0 && p2 <= mem_trim_rate,
            "tick 2: idle period must stay at the -m cadence, got %" PRId64,
            p2);
}

// -m 0 must NOT add gratuitous wake-ups: a fully idle server keeps the
// 1h park. Proves the clamp lives inside the mem_trim_rate guard.
void
cttest_prottick_trim_disabled_keeps_idle_period()
{
    now = nanoseconds();
    prot_init();
    Server s = {0};

    mem_trim_rate = 0;

    int64 p = prottick(&s);
    assertf(p == 0x34630B8A000LL,
            "with -m off an idle tick must return the 1h period, got %" PRId64,
            p);
}


/* ------------------------------------------------------------------ */
/* conn pool drain at trim (conn-pool-not-drained-at-trim)            */
/* ------------------------------------------------------------------ */

// The -m trim tick must be able to hand the conn slab pool back to
// glibc: pooled Conns are live allocations malloc_trim(0) cannot
// reclaim (up to CONN_POOL_MAX * sizeof(Conn) ~ 1.2MB of resident
// slack after any connection burst). Counter discipline (#2): the pool
// length is zeroed exactly as entries are freed; a double drain must
// be a no-op (a double free here dies under the ASan loadtest gate).
void
cttest_conn_pool_drain_balances_counter()
{
    now = nanoseconds();
    prot_init();

    int count = -1;
    conn_pool_drain();              // cold drain: no-op
    get_conn_pool_stats(&count);
    assertf(count == 0, "cold pool must report 0, got %d", count);
    conn_pool_drain();              // double drain: idempotent
    get_conn_pool_stats(&count);
    assertf(count == 0, "double drain must stay 0, got %d", count);

    Tube *t = tube_find_or_make("connpool-bal");
    tube_iref(t);

    int base = -1;
    get_conn_pool_stats(&base);

    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), 0, t, t);
        assertf(cs[i], "make_conn %d must succeed", i);
    }
    for (int i = 0; i < 3; i++)
        connclose(cs[i]);

    get_conn_pool_stats(&count);
    assertf(count == base + 3,
            "three closed conns must pool (base %d, got %d)", base, count);

    conn_pool_drain();
    get_conn_pool_stats(&count);
    assertf(count == 0, "drain must empty the pool, got %d", count);

    // The pool must keep working after a drain: take/put re-pools.
    Conn *c = make_conn(dup(2), 0, t, t);
    assertf(c, "make_conn after drain must succeed");
    // The counter alone cannot tell "freed the structs" from "zeroed the
    // count and kept them": a pool take bumps gen, a fresh zalloc leaves
    // it at 0. The three conns drained above had each been pooled once,
    // so a retained struct comes back with gen >= 1.
    assertf(c->gen == 0,
            "drain must actually release the pooled structs: post-drain "
            "make_conn handed back a recycled conn (gen=%" PRIu64 ")",
            c->gen);
    connclose(c);
    get_conn_pool_stats(&count);
    assertf(count == 1, "post-drain close must re-pool, got %d", count);

    conn_pool_drain();
    tube_dref(t);
}

// Drain must free ONLY pooled (closed) conns. Live conns keep working
// end-to-end afterwards; under the ASan loadtest gate any touch of
// freed memory here is a hard failure.
void
cttest_conn_pool_drain_spares_live_conns()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("connpool-live");
    tube_iref(t);

    Conn *p1 = make_conn(dup(2), 0, t, t);
    Conn *p2 = make_conn(dup(2), 0, t, t);
    Conn *l1 = make_conn(dup(2), 0, t, t);
    Conn *l2 = make_conn(dup(2), 0, t, t);
    assertf(p1 && p2 && l1 && l2, "conns must allocate");
    connclose(p1);
    connclose(p2);

    conn_pool_drain();
    int count = -1;
    get_conn_pool_stats(&count);
    assertf(count == 0, "drain must empty the pool, got %d", count);

    // Exercise the live conns after the drain.
    assertf(l1->watch.len == 1 && l1->watch.items[0] == t,
            "live conn watch must be intact after drain");
    assertf(enqueue_waiting_conn(l1) == 1,
            "live conn must still be able to wait after drain");
    assertf(t->waiting_conns.len == 1 && t->waiting_conns.items[0] == l1,
            "live conn must actually register as waiting");
    remove_waiting_conn(l1);

    connclose(l1);
    connclose(l2);
    get_conn_pool_stats(&count);
    assertf(count == 2, "live conns must re-pool after drain, got %d", count);

    conn_pool_drain();
    tube_dref(t);
}


/* ------------------------------------------------------------------ */
/* waitpos hint invariant (waiting-conns-linear-scan)                 */
/* ------------------------------------------------------------------ */

// Invariant: for every WAITING conn c and every watch index i,
// c->watch.items[i]->waiting_conns.items[c->waitpos[i]] == c.
// Checked EXACTLY (not via membership) after every operation, so the
// ms_remove_at stale-hint fallback cannot silently mask hint rot.
static void
assert_waitpos_exact(Conn **cs, int n, const char *ctx, int step)
{
    for (int k = 0; k < n; k++) {
        Conn *c = cs[k];
        if (!c || !conn_waiting(c))
            continue;
        for (size_t i = 0; i < c->watch.len; i++) {
            Tube *t = c->watch.items[i];
            assertf(c->waitpos[i] < t->waiting_conns.len,
                    "%s step %d: conn %d hint %zu out of range for tube %s "
                    "(len %zu)", ctx, step, k, c->waitpos[i], t->name,
                    t->waiting_conns.len);
            assertf(t->waiting_conns.items[c->waitpos[i]] == c,
                    "%s step %d: conn %d hint %zu rotted for tube %s",
                    ctx, step, k, c->waitpos[i], t->name);
        }
    }
}

// Random interleave of every production mutation of the waiting sets:
// enqueue_waiting_conn, remove_waiting_conn, the ms_take + other-tubes
// removal sequence of process_tube, the single-tube removal of
// OP_IGNORE (which also swap-removes the watch entry while the conn
// STAYS waiting on the rest — the on_watch_remove parallel-swap path),
// and re-watching while not waiting. srand(42) determinism per test
// rules.
void
cttest_waitpos_hint_invariant_churn()
{
    now = nanoseconds();
    prot_init();

    enum { NCONN = 8, STEPS = 600 };
    Tube *tubes3[3];
    tubes3[0] = tube_find_or_make("wp-churn-a");
    tubes3[1] = tube_find_or_make("wp-churn-b");
    tubes3[2] = tube_find_or_make("wp-churn-c");
    assertf(tubes3[0] && tubes3[1] && tubes3[2], "tubes must allocate");
    for (int i = 0; i < 3; i++)
        tube_iref(tubes3[i]);

    Conn *cs[NCONN];
    for (int k = 0; k < NCONN; k++) {
        cs[k] = make_conn(dup(2), 0, tubes3[0], tubes3[0]);
        assertf(cs[k], "conn %d must allocate", k);
        // Overlapping multi-watch: every conn watches tube a, most
        // watch one or both of b/c.
        if (k % 2 == 0)
            assertf(ms_append(&cs[k]->watch, tubes3[1]), "watch b");
        if (k % 3 != 1)
            assertf(ms_append(&cs[k]->watch, tubes3[2]), "watch c");
    }

    srand(42);
    for (int step = 0; step < STEPS; step++) {
        Conn *c = cs[rand() % NCONN];
        int op = rand() % 100;

        if (conn_waiting(c)) {
            if (op < 40) {
                remove_waiting_conn(c);
            } else if (op < 70 && c->watch.len > 1) {
                // OP_IGNORE replica: drop one watched tube while the
                // conn keeps waiting on the others.
                size_t wi = (size_t)(rand() % (int)c->watch.len);
                Tube *t = c->watch.items[wi];
                t->stat.waiting_ct--;
                ms_remove_at(&t->waiting_conns, c->waitpos[wi], c);
                ms_remove_at(&c->watch, wi, t);
            } else {
                // process_tube replica: take the oldest waiter from a
                // random tube, then unregister it from its other tubes.
                Tube *t = tubes3[rand() % 3];
                Conn *v = ms_take(&t->waiting_conns);
                if (v) {
                    t->stat.waiting_ct--;
                    v->type &= ~CONN_TYPE_WAITING;
                    global_stat.waiting_ct--;
                    for (size_t i = 0; i < v->watch.len; i++) {
                        Tube *other = v->watch.items[i];
                        if (other == t)
                            continue;
                        other->stat.waiting_ct--;
                        ms_remove_at(&other->waiting_conns, v->waitpos[i], v);
                    }
                }
            }
        } else {
            if (op < 30) {
                // Re-watch a tube this conn dropped (only legal while
                // not waiting; OP_WATCH on a waiting conn re-enqueues).
                Tube *t = tubes3[rand() % 3];
                if (!ms_contains(&c->watch, t))
                    assertf(ms_append(&c->watch, t), "re-watch");
            } else {
                assertf(enqueue_waiting_conn(c) == 1, "enqueue must succeed");
            }
        }

        assert_waitpos_exact(cs, NCONN, "churn", step);
    }

    // Teardown: every waiter out, every set empty, counters balanced.
    for (int k = 0; k < NCONN; k++)
        if (conn_waiting(cs[k]))
            remove_waiting_conn(cs[k]);
    for (int i = 0; i < 3; i++) {
        assertf(tubes3[i]->waiting_conns.len == 0,
                "tube %d waiting set must drain to 0, got %zu",
                i, tubes3[i]->waiting_conns.len);
        assertf(tubes3[i]->stat.waiting_ct == 0,
                "tube %d waiting_ct must balance (#2), got %" PRIu64,
                i, tubes3[i]->stat.waiting_ct);
    }
    assertf(global_stat.waiting_ct == 0,
            "global waiting_ct must balance, got %" PRIu64,
            global_stat.waiting_ct);

    for (int k = 0; k < NCONN; k++)
        connclose(cs[k]);
    for (int i = 0; i < 3; i++)
        tube_dref(tubes3[i]);
}


/* ------------------------------------------------------------------ */
/* conn defer-free: stale epoll batch events (UAF guard)               */
/* ------------------------------------------------------------------ */

// While a drain is bracketed by conn_defer_free_begin/end, connclose
// must NOT return the struct to the slab pool: ep_buf may still hold
// events whose data.ptr targets it, and a pooled struct could be
// recycled by make_conn before the stale event is dispatched.
void
cttest_conn_defer_free_holds_struct_during_drain()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("conndefer");
    tube_iref(t);

    conn_pool_drain();
    int count = -1;
    get_conn_pool_stats(&count);
    assertf(count == 0, "pool must start empty, got %d", count);

    Conn *c = make_conn(dup(2), 0, t, t);
    assertf(c, "make_conn must succeed");

    conn_defer_free_begin();
    connclose(c);

    get_conn_pool_stats(&count);
    assertf(count == 0,
            "closed conn must stay out of the pool during drain, got %d",
            count);
    assertf(c->sock.fd == -1,
            "closed conn must keep the dead-fd marker for prothandle");

    // make_conn must not hand out the deferred struct.
    Conn *c2 = make_conn(dup(2), 0, t, t);
    assertf(c2, "make_conn must succeed");
    assertf(c2 != c, "reusing a deferred conn mid-drain would be UAF");
    connclose(c2); // also deferred

    // Nested begin/end: only the outermost end releases.
    conn_defer_free_begin();
    conn_defer_free_end();
    get_conn_pool_stats(&count);
    assertf(count == 0,
            "inner end must not release while outer begin is active");

    conn_defer_free_end();
    get_conn_pool_stats(&count);
    assertf(count == 2,
            "both conns must pool when the outermost end runs, got %d",
            count);

    // Reuse is allowed again after the drain; the gen counter must
    // advance on pool take so any external stale reference is detectable.
    uint64 old_gen = c->gen;
    Conn *c3 = make_conn(dup(2), 0, t, t);
    assertf(c3 == c2 || c3 == c, "post-drain make_conn must reuse pool");
    Conn *c4 = make_conn(dup(2), 0, t, t);
    Conn *reused = (c3 == c) ? c3 : c4;
    assertf(reused->gen == old_gen + 1,
            "pool reuse must bump gen (%" PRIu64 " -> %" PRIu64 ")",
            old_gen, reused->gen);
    connclose(c3);
    connclose(c4);

    conn_pool_drain();
    tube_dref(t);
}


/* --- epollq double-insert guard (in_epollq) --- */

// One dispatch tick can legitimately epollq_add the SAME conn twice
// with another conn queued in between: reserve's slow path registers
// 'h' (wait_for_job), then process_tube serves that conn in the same
// dispatch and re-arms 'r' (reply fast path → conn_want_command).
// Without the in_epollq membership guard the second blind prepend
// overwrote X->next (which pointed at Y), turning the list into an X
// self-cycle and ORPHANING Y: its queued sockwant never ran, so its
// kernel registration stayed hangup-only while y.rw already claimed
// the new mode — the server went permanently deaf to Y (invariants
// #5/#10).
//
// This is the direct, deterministic version of the old network test:
// the truncate-based integration scenario reached the same X,Y,X tick
// through zombie ready-heap roots; with truncate gone the tick cannot
// be reproduced over the wire (a parked reserve halts the conn's
// pipeline loop, so the mid-dispatch re-serve can no longer be
// scripted from the client side), so the invariant is checked straight
// on the list.
void
cttest_epollq_double_insert_does_not_orphan_worker()
{
    prot_init();

    static Conn x, y; // worker X double-queued, worker Y queued between
    memset(&x, 0, sizeof x);
    memset(&y, 0, sizeof y);
    x.srv = y.srv = &srv;                       // connsched heap access
    x.pending_timeout = y.pending_timeout = -1; // make connsched a no-op
    x.sock.fd = y.sock.fd = -1;                 // apply skips sockwant
    job_list_reset(&x.reserved_jobs);           // conntickat walks these
    job_list_reset(&y.reserved_jobs);

    // The tick: X parks in reserve ('h'), a put wakes Y mid-dispatch
    // ('r'), then X's own wait is served ('r') — the double insert.
    epollq_test_add(&x, 'h');
    epollq_test_add(&y, 'r');
    epollq_test_add(&x, 'r');

    // Walk the pending list: it must terminate and hold both conns
    // exactly once. Without the guard the walk either cycles on X
    // forever or never reaches Y — both mean Y is orphaned.
    int saw_x = 0, saw_y = 0, steps = 0;
    for (Conn *c = epollq_test_head(); c; c = c->next) {
        assertf(++steps <= 2,
                "epollq walk must terminate: double insert cycled the list");
        if (c == &x) saw_x++;
        else if (c == &y) saw_y++;
        else assertf(0, "unexpected conn %p in epollq", (void *)c);
    }
    assertf(saw_x == 1 && saw_y == 1,
            "double insert must keep both conns queued exactly once,"
            " got x=%d y=%d (orphan!)", saw_x, saw_y);

    // The single apply must use the freshest rw for X, not the stale 'h'.
    assertf(x.rw == 'r',
            "re-arm must win: x.rw must be 'r', got '%c'", x.rw);
    assertf(y.rw == 'r', "y.rw must be 'r', got '%c'", y.rw);

    // Apply drains the whole list and clears membership on BOTH conns;
    // an orphaned Y would keep in_epollq set and miss its sockwant.
    epollq_test_apply();
    assertf(epollq_test_head() == NULL, "apply must drain the list");
    assertf(!x.in_epollq && !y.in_epollq,
            "apply must clear in_epollq on both conns, got x=%d y=%d",
            x.in_epollq, y.in_epollq);
    assertf(x.next == NULL && y.next == NULL,
            "apply must unlink both conns");

    // Re-queue after apply: a conn must be insertable again (the flag
    // was really cleared, not just the head pointer advanced).
    epollq_test_add(&y, 'h');
    assertf(epollq_test_head() == &y && y.in_epollq,
            "conn must be re-insertable after apply");
    epollq_test_apply();
}


// connsched OOM recovery: when heapinsert(&srv->conns) fails, the conn's
// timers (TTR / reserve deadline / idle) vanish from prottick — the heap
// itself cannot enumerate its missing members. connsched must flag the
// heap degraded and conn_sched_recover must rebuild membership from the
// live-conns list, including surviving a SECOND OOM during the rebuild.
void
cttest_connsched_oom_recovery_reinserts_conn()
{
    now = nanoseconds();
    prot_init();

    Tube *t = tube_find_or_make("connsched-oom");
    tube_iref(t);

    /* Private Server so the test controls the heap's len==cap edge and
     * connclose stays away from the global listener socket. */
    Server s;
    memset(&s, 0, sizeof s);
    s.conns.less = conn_less;
    s.conns.setpos = conn_setpos;
    s.sock.fd = -1;
    s.sock.added = 1;

    Conn *c = make_conn(dup(2), 0, t, t);
    assertf(c, "make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 5; // conntickat: now + 5s

    /* A second conn with NO outstanding timeout: recovery must skip it
     * (conntickat returns 0), proving the walk re-derives eligibility
     * instead of blindly reinserting every live conn. */
    Conn *d = make_conn(dup(2), 0, t, t);
    assertf(d, "make_conn d must succeed");
    d->srv = &s;

    /* len==cap==0: the insert must grow → realloc. Kill it. */
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(c);
    assertf(!c->in_conns, "failed heapinsert must leave in_conns=0");
    assertf(s.conns.len == 0, "heap must stay empty after OOM");
    assertf(fault_hits(FAULT_REALLOC) == 1, "realloc fault must fire once");

    /* First recovery attempt also OOMs: must report still-degraded and
     * leave the conn out — not corrupt or forget it. */
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    assertf(conn_sched_recover() == 1,
            "recovery under continued OOM must report still degraded");
    assertf(!c->in_conns && s.conns.len == 0,
            "conn must still be out of the heap after failed recovery");

    /* OOM over: recovery reinserts with a tickat rebuilt from the
     * conn's live timeout state. */
    assertf(conn_sched_recover() == 0, "recovery must clear the flag");
    assertf(c->in_conns, "recovery must reinsert the dropped conn");
    assertf(s.conns.len == 1 && s.conns.data[0] == c,
            "heap must contain exactly the recovered conn");
    assertf(!d->in_conns, "conn without a timeout must stay out");
    int64 want = now + 5LL * 1000000000;
    assertf(c->tickat > now - 1000000000LL && c->tickat <= want,
            "tickat must be rebuilt from pending_timeout, got %" PRId64,
            c->tickat);
    /* The window above also accepts a deadline a full second in the
     * PAST, i.e. a conn that wakes prottick on every tick after any OOM
     * episode. `now` does not move inside conn_sched_recover, so the
     * promised value is exact: pending_timeout seconds from the cached
     * clock, nothing rebuilt from a zero/stale base. */
    assertf(c->tickat == want,
            "recovery must rebuild tickat exactly from pending_timeout: "
            "want %" PRId64 ", got %" PRId64 " (delta %" PRId64 "ns)",
            want, c->tickat, c->tickat - want);

    /* The recovered entry is fully functional: connsched's remove path
     * (tickpos from conn_setpos during recovery) must unlink it. */
    c->pending_timeout = -1;
    connsched(c);
    assertf(!c->in_conns && s.conns.len == 0,
            "connsched must remove the recovered conn cleanly");

    /* Live-list hygiene: closing both conns must leave the list empty,
     * so a later recovery on a stale flag walks nothing. */
    connclose(c);
    connclose(d);
    assertf(conn_sched_recover() == 0,
            "recovery with no live conns must be a no-op");

    free(s.conns.data);
    tube_dref(t);
}


/* ============================================================
 * ANGRY TESTS — epollq_add / dur_remove / dur_batch_pending.
 * These are prot.c internals whose only public surface is the
 * test hook (epollq_test_*) or the exported dur_* API, so they are
 * driven directly instead of over the wire.
 * ============================================================ */

/* epollq_add's contract is "the single apply always uses the freshest
 * rw". The interest byte 0 means "park this fd out of epoll", which is
 * exactly what reply() does when it defers a job reply under -D; the
 * re-arm that follows must win. A membership guard placed before the
 * `c->rw = rw` store would let the stale 0 stand and leave the socket
 * out of epoll for good — the connection then never wakes again.
 * (The sibling test above covers the 'h' -> 'r' order; this one covers
 * the parked -> writable order, which is the one -D depends on.) */
void
cttest_epollq_add_zero_then_write_applies_the_freshest_interest()
{
    prot_init();

    static Conn parked;
    memset(&parked, 0, sizeof parked);
    parked.srv = &srv;
    parked.pending_timeout = -1;
    parked.sock.fd = -1;
    job_list_reset(&parked.reserved_jobs);

    epollq_test_add(&parked, 0);
    epollq_test_add(&parked, 'w');

    int queued = 0;
    for (Conn *c = epollq_test_head(); c; c = c->next) {
        assertf(++queued <= 1, "epollq must not cycle on a repeated add");
    }
    char applied = parked.rw;
    epollq_test_apply();

    assertf(queued == 1 && applied == 'w' && epollq_test_head() == NULL
            && !parked.in_epollq,
            "a conn parked out of epoll and re-armed for write in the same"
            " tick must be queued once and apply 'w'; queued=%d rw='%c'",
            queued, applied ? applied : '0');
}

/* dur_remove is documented as safe to call for a conn that is not in
 * the batch — connclose() calls it unconditionally. A version that
 * skipped the in_dur_batch guard would decrement dur_batch_n below zero
 * and hand the next flush a pointer out of an empty array. */
void
cttest_dur_remove_of_an_unregistered_conn_leaves_the_batch_empty()
{
    prot_init();

    static Conn stranger;
    memset(&stranger, 0, sizeof stranger);
    stranger.srv = &srv;
    stranger.sock.fd = -1;
    stranger.dur_batch_idx = 7;   // stale index from an earlier batch

    dur_remove(&stranger);

    assertf(dur_batch_pending() == 0 && stranger.in_dur_batch == 0
            && stranger.dur_reply_len == 0,
            "removing a conn that was never registered must leave the"
            " batch empty and the conn unflagged; pending=%d flag=%d"
            " len=%d", dur_batch_pending(), (int)stranger.in_dur_batch,
            (int)stranger.dur_reply_len);
}

/* dur_batch_pending's whole promise is the boolean: false when the
 * batch holds nothing, so the event loop parks normally instead of
 * spinning. A `>= 0` comparison would make every idle tick claim work. */
void
cttest_dur_batch_pending_is_false_when_nothing_is_deferred()
{
    prot_init();

    int pending = dur_batch_pending();

    assertf(pending == 0,
            "with no conn deferred the batch must report nothing pending,"
            " got %d", pending);
}


// net.c binds IPv6 as well as IPv4 — README lists it as supported — and
// nothing exercised it. The v6 path differs in more than the address
// family: it sets IPV6_V6ONLY so a v6 listener does not silently
// swallow v4 traffic, and the verbose line brackets the address. A
// listener that came back unusable, or that answered on v4 as well,
// would have gone unnoticed.
void
cttest_make_server_socket_binds_ipv6_loopback(void)
{
    int fd = make_server_socket("::1", "0");
    if (fd == -1) {
        // No IPv6 on this machine at all: nothing to assert, and
        // failing here would only report the environment.
        return;
    }

    struct sockaddr_in6 sa;
    socklen_t len = sizeof sa;
    assertf(getsockname(fd, (struct sockaddr *)&sa, &len) == 0,
            "the listener must be a real bound socket: %s", strerror(errno));
    assertf(sa.sin6_family == AF_INET6,
            "make_server_socket(\"::1\") must produce an AF_INET6 socket, "
            "got family %d", sa.sin6_family);
    assertf(ntohs(sa.sin6_port) != 0,
            "the kernel must have assigned a port");

    // V6ONLY: a v6 listener must not also accept v4-mapped traffic, or
    // "bind ::1" quietly becomes "bind everything". Note this assertion
    // does not distinguish our setsockopt from the kernel default —
    // most systems already ship net.ipv6.bindv6only=1 — so it catches
    // the regression only where the default is 0. It is still the
    // property worth stating.
    int v6only = -1;
    socklen_t olen = sizeof v6only;
    assertf(getsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, &olen) == 0,
            "IPV6_V6ONLY must be readable: %s", strerror(errno));
    assertf(v6only == 1,
            "a v6 listener must be v6-only, got %d", v6only);

    // And it really is listening: a connect must be accepted by the
    // kernel's backlog.
    int c = socket(AF_INET6, SOCK_STREAM, 0);
    assertf(c >= 0, "setup: client socket");
    assertf(connect(c, (struct sockaddr *)&sa, sizeof sa) == 0,
            "the listener must accept connections: %s", strerror(errno));
    close(c);
    close(fd);
}
