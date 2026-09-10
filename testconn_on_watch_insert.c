// Angry tests for on_watch_insert (conn.c, static).
//
// Registered by make_conn as the watch set's oninsert and reached through
// ms_append. It owns two pieces of bookkeeping per watched tube — a
// reference and the watching count `stats-tube` publishes — and both are
// paired against on_watch_remove. An increment that fires the wrong
// number of times either keeps a dead tube alive forever or frees one
// that conns are still watching.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
owi_setup(void)
{
    fault_clear_all();
    progname = "testconn_on_watch_insert";
    prot_init();
    conn_pool_drain();
}

static Tube *
owi_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}


// A worker that watches three tubes must appear in the watching count of
// all three, once each.
void
cttest_on_watch_insert_counts_every_appended_tube_exactly_once(void)
{
    owi_setup();
    Tube *home = owi_tube("owi-home");
    Tube *a = owi_tube("owi-a");
    Tube *b = owi_tube("owi-b");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(c, "setup: make_conn must succeed");
    uint abefore = a->watching_ct, bbefore = b->watching_ct;

    assertf(ms_append(&c->watch, a), "setup: the first append must succeed");
    assertf(ms_append(&c->watch, b), "setup: the second append must succeed");

    assertf(a->watching_ct == abefore + 1 && b->watching_ct == bbefore + 1,
            "each appended tube gains exactly one watcher: a %u->%u, "
            "b %u->%u", abefore, a->watching_ct, bbefore, b->watching_ct);

    connclose(c);
    tube_dref(home);
    tube_dref(a);
    tube_dref(b);
}


// The reference is what keeps a tube from being freed while a conn still
// watches it. Without it, the last dref elsewhere frees the Tube under
// conn_ready and remove_waiting_conn.
void
cttest_on_watch_insert_takes_a_reference_on_the_tube_it_watches(void)
{
    owi_setup();
    Tube *home = owi_tube("owi-ref-home");
    Tube *watched = owi_tube("owi-ref-watched");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(c, "setup: make_conn must succeed");
    uint before = watched->refs;

    assertf(ms_append(&c->watch, watched), "setup: the append must succeed");

    assertf(watched->refs == before + 1,
            "watching a tube must hold it alive: refs %u became %u",
            before, watched->refs);

    connclose(c);
    tube_dref(home);
    tube_dref(watched);
}


// ms_append does not deduplicate; only the protocol layer checks
// ms_contains first. The counting rule here is per append, and it has to
// stay per append or the paired removals stop balancing.
void
cttest_on_watch_insert_counts_a_repeated_append_of_the_same_tube_again(void)
{
    owi_setup();
    Tube *home = owi_tube("owi-dup-home");
    Tube *twice = owi_tube("owi-dup-twice");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(c, "setup: make_conn must succeed");
    uint before = twice->watching_ct;

    assertf(ms_append(&c->watch, twice), "setup: the first append must succeed");
    assertf(ms_append(&c->watch, twice), "setup: the second append must succeed");

    assertf(twice->watching_ct == before + 2,
            "two appends of one tube are two watch entries to unwind: "
            "%u became %u", before, twice->watching_ct);

    connclose(c);
    tube_dref(home);
    tube_dref(twice);
}


// One conn's watch must not disturb another tube's books. The callback
// gets the Ms and the index too, and both are meant to be unused.
void
cttest_on_watch_insert_leaves_the_tubes_it_was_not_given_alone(void)
{
    owi_setup();
    Tube *home = owi_tube("owi-iso-home");
    Tube *mine = owi_tube("owi-iso-mine");
    Tube *yours = owi_tube("owi-iso-yours");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, home, home);
    assertf(c, "setup: make_conn must succeed");
    uint ybefore_watch = yours->watching_ct, ybefore_refs = yours->refs;

    assertf(ms_append(&c->watch, mine), "setup: the append must succeed");

    assertf(yours->watching_ct == ybefore_watch && yours->refs == ybefore_refs,
            "watching one tube must not touch another: watching %u->%u, "
            "refs %u->%u", ybefore_watch, yours->watching_ct,
            ybefore_refs, yours->refs);

    connclose(c);
    tube_dref(home);
    tube_dref(mine);
    tube_dref(yours);
}
