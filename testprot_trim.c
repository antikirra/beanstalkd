// Angry tests for the periodic memory trim (prot.c: trim_worthwhile,
// the -m block of prottick, and the accounting that feeds it).
//
// Contract: malloc_trim stops the event loop for as long as it takes to
// walk the heap, so prottick may run it only when the walk is paid for:
// at least 1MiB to give back, and at least an eighth of the live bytes
// it has to step over. Freed memory must still go back when it is.

#include "dat.h"
#include "ct/ct.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define MIB ((size_t)1 << 20)

// The decision, as a table: reclaimable bytes against live bytes.
void
cttest_trim_worthwhile_follows_its_two_thresholds(void)
{
    static const struct {
        size_t reclaimable, live;
        int want;
        const char *why;
    } tt[] = {
        { 0,            0,          0, "nothing to return" },
        { MIB - 1,      0,          0, "one byte under the floor" },
        { MIB,          0,          1, "exactly the floor, empty heap" },
        { MIB,          8 * MIB,    1, "exactly an eighth of live" },
        { MIB,          8 * MIB + 8, 0, "just under an eighth of live" },
        { 64 * MIB,     600 * MIB,  0, "a tenth of a large heap" },
        { 80 * MIB,     600 * MIB,  1, "over an eighth of a large heap" },
        { SIZE_MAX,     SIZE_MAX,   1, "no overflow at the top" },
        { 0,            SIZE_MAX,   0, "idle server, huge heap" },
    };
    for (size_t i = 0; i < sizeof tt / sizeof tt[0]; i++) {
        int got = trim_worthwhile(tt[i].reclaimable, tt[i].live);
        assertf(got == tt[i].want,
                "%s: trim_worthwhile(%zu, %zu) = %d, want %d",
                tt[i].why, tt[i].reclaimable, tt[i].live, got, tt[i].want);
    }
}

// Puts n jobs of the smallest class into the pool and returns how many
// it holds afterwards.
static int
pool_fill(int n)
{
    Job **v = calloc((size_t)n, sizeof *v);
    assertf(v != NULL, "setup: calloc");
    for (int i = 0; i < n; i++) {
        v[i] = allocate_job(8);
        assertf(v[i] != NULL, "setup: allocate_job");
    }
    for (int i = 0; i < n; i++)
        job_free(v[i]);
    free(v);
    int count;
    get_job_pool_stats(NULL, &count);
    return count;
}

static void
trim_setup(Server *s)
{
    memset(s, 0, sizeof *s);
    job_pool_drain();
    conn_pool_drain();
    mem_released = 0;
    mem_trim_rate = 1;      // every tick is past the -m cadence
}

// The stall this exists to prevent: a server whose heap is large and
// whose last interval freed next to nothing. The pool is the hot path's
// cache; emptying it every interval was also throwing that away.
void
cttest_prottick_leaves_the_heap_alone_when_too_little_was_freed(void)
{
    Server s;
    trim_setup(&s);
    int pooled = pool_fill(64);
    assertf(pooled == 64, "setup: 64 jobs must be pooled, got %d", pooled);
    size_t before = mem_released = MIB / 2;

    prottick(&s);

    int count;
    get_job_pool_stats(NULL, &count);
    assertf(count == pooled && mem_released == before,
            "half a MiB freed is not worth a walk of the heap: pool went "
            "%d -> %d, released %zu -> %zu",
            pooled, count, before, mem_released);
}

// ...and the half that must not be lost with it: enough freed memory
// does get returned, pools first.
void
cttest_prottick_trims_and_drains_the_pools_once_enough_was_freed(void)
{
    Server s;
    trim_setup(&s);
    int pooled = pool_fill(64);
    assertf(pooled == 64, "setup: 64 jobs must be pooled, got %d", pooled);
    // Enough for the floor and for an eighth of whatever this process
    // holds alive.
    mem_released = MIB + mem_held;

    prottick(&s);

    int count;
    get_job_pool_stats(NULL, &count);
    assertf(count == 0 && mem_released == 0,
            "with enough to return the pools are drained and the count "
            "starts over: pool holds %d, released %zu", count, mem_released);
}

// -m 0 keeps meaning "never".
void
cttest_prottick_never_trims_when_m_is_zero(void)
{
    Server s;
    trim_setup(&s);
    mem_trim_rate = 0;
    int pooled = pool_fill(64);
    mem_released = 512 * MIB;

    prottick(&s);

    int count;
    get_job_pool_stats(NULL, &count);
    assertf(count == pooled && mem_released == 512 * MIB,
            "-m 0 disables the trim outright: pool went %d -> %d",
            pooled, count);
}

// The accounting is what the decision stands on. A job the pool cannot
// take goes back to malloc, and both counters must say so by exactly
// the size malloc had given it.
void
cttest_mem_accounting_follows_a_job_out_of_the_allocator(void)
{
    job_pool_drain();
    size_t held0 = mem_held;
    Job *j = allocate_job(200000);      // above the largest pool class
    assertf(j != NULL, "setup: allocate_job");
    size_t took = mem_held - held0;
    size_t released0 = mem_released;

    job_free(j);

    assertf(took >= sizeof(Job) + 200000
            && mem_held == held0
            && mem_released - released0 == took,
            "a 200000-byte job must be counted in and out by its real "
            "size: took %zu, held %zu -> %zu, released +%zu",
            took, held0, mem_held, mem_released - released0);
}

// A pooled job is still malloc'ed memory: it stays in mem_held and is
// not yet released. Counting it as released would trim for nothing;
// dropping it from held would under-count the heap.
void
cttest_mem_accounting_keeps_a_pooled_job_as_held(void)
{
    job_pool_drain();
    size_t held0 = mem_held;
    size_t released0 = mem_released;
    Job *j = allocate_job(8);
    assertf(j != NULL, "setup: allocate_job");
    size_t took = mem_held - held0;

    job_free(j);                        // goes to the pool

    assertf(took > 0 && mem_held == held0 + took
            && mem_released == released0
            && job_pool_bytes() > 0,
            "a pooled job is held, not released: held +%zu (want +%zu), "
            "released +%zu, pool %zu bytes",
            mem_held - held0, took, mem_released - released0,
            job_pool_bytes());
}
