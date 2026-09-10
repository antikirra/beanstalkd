// Angry tests for get_conn_pool_stats (conn.c).
//
// A test-only accessor, which is exactly why it deserves attention: every
// pool test in the repository trusts it, so a reader that lies or that
// mutates the pool while reading makes a whole family of tests green for
// the wrong reason.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Documented ceiling of the slab pool (conn.c CONN_POOL_MAX). */
#define GCS_POOL_CEILING 256
#define GCS_UNOPENED_FD_BASE 920000

static void
gcs_setup(void)
{
    fault_clear_all();
    progname = "testconn_get_conn_pool_stats";
    prot_init();
    conn_pool_drain();
}

static Tube *
gcs_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
gcs_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}


// The NULL destination is documented as ignored. Writing through it
// would take down the caller instead of returning nothing.
void
cttest_get_conn_pool_stats_ignores_a_null_destination(void)
{
    gcs_setup();
    Tube *t = gcs_tube("gcs-null");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    connclose(c);

    get_conn_pool_stats(NULL);

    assertf(gcs_pooled() == 1,
            "a call with no destination must change nothing, the pool now "
            "reports %d", gcs_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// A read must not move the number it reports. A counter that is consumed
// as it is read makes every pool test in the repository measure the
// number of times it was asked rather than the pool.
void
cttest_get_conn_pool_stats_gives_the_same_answer_on_repeated_reads(void)
{
    gcs_setup();
    Tube *t = gcs_tube("gcs-repeat");
    Conn *cs[2];
    for (int i = 0; i < 2; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }
    for (int i = 0; i < 2; i++)
        connclose(cs[i]);

    int a = gcs_pooled();
    int b = gcs_pooled();
    int c = gcs_pooled();

    assertf(a == 2 && b == 2 && c == 2,
            "three reads of an unchanged pool must all report 2, got "
            "%d/%d/%d", a, b, c);

    conn_pool_drain();
    tube_dref(t);
}


// Reading the pool must not take anything out of it. The only way to see
// that from outside is to ask make_conn afterwards: a struct that came
// back from the pool carries an advanced generation, a fresh allocation
// starts at zero.
void
cttest_get_conn_pool_stats_leaves_the_struct_it_counted_available_for_reuse(void)
{
    gcs_setup();
    Tube *t = gcs_tube("gcs-pure");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: make_conn must succeed");
    connclose(first);
    assertf(gcs_pooled() == 1, "setup: exactly one struct must be pooled");
    gcs_pooled();
    gcs_pooled();

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(second && second->gen > 0,
            "the counted struct must still be in the pool after the reads: "
            "make_conn handed back a conn with generation %" PRIu64,
            second ? second->gen : 0);

    connclose(second);
    conn_pool_drain();
    tube_dref(t);
}


// The reported number is a pool length, and the pool has a ceiling. A
// counter that keeps climbing past it is the first symptom of a pool that
// grows without bound.
void
cttest_get_conn_pool_stats_never_reports_more_than_the_pool_ceiling(void)
{
    gcs_setup();
    Tube *t = gcs_tube("gcs-ceiling");
    enum { N = GCS_POOL_CEILING + 4 };
    static Conn *cs[N];
    for (int i = 0; i < N; i++) {
        cs[i] = make_conn(GCS_UNOPENED_FD_BASE + i, STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
    }

    for (int i = 0; i < N; i++)
        connclose(cs[i]);

    assertf(gcs_pooled() <= GCS_POOL_CEILING,
            "the pool cannot hold more than %d structs, %d closes made it "
            "report %d", GCS_POOL_CEILING, N, gcs_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// Conns parked by an open defer batch are not in the pool and must not be
// counted as if they were: a test that trusted the inflated number would
// believe a struct is available for reuse while it is still frozen.
void
cttest_get_conn_pool_stats_leaves_parked_conns_out_of_its_count(void)
{
    gcs_setup();
    Tube *t = gcs_tube("gcs-parked");
    Conn *pooled = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(pooled, "setup: the first conn must allocate");
    connclose(pooled);
    Conn *parked[3];
    for (int i = 0; i < 3; i++) {
        parked[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(parked[i], "setup: parked conn %d must allocate", i);
    }
    // Read the count after the allocations: make_conn serves itself from
    // the pool, so a baseline taken before them measures conns that are
    // no longer there. What the batch must not do is GROW the pool.
    int before = gcs_pooled();
    conn_defer_free_begin();

    for (int i = 0; i < 3; i++)
        connclose(parked[i]);

    assertf(gcs_pooled() == before,
            "parked conns are not pooled conns: the count went from %d to "
            "%d during the batch", before, gcs_pooled());

    conn_defer_free_end();
    conn_pool_drain();
    tube_dref(t);
}
