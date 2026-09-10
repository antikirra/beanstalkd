// Angry tests for conn_sched_recover (conn.c).
//
// The repair pass for conns whose tick-heap insert was refused. Nothing
// else can find them: the heap cannot enumerate its missing members, so
// the live-conns list plus the degraded flag are the whole mechanism.
// Two failure directions matter — giving up on a conn (its TTR is lost
// until it disconnects) and claiming success while one is still out (the
// caller stops shortening its park and never retries).

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
csr_setup(Server *s)
{
    fault_clear_all();
    progname = "testconn_conn_sched_recover";
    prot_init();
    conn_pool_drain();
    memset(s, 0, sizeof *s);
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;
    s->sock.fd = -1;
    s->sock.added = 1;
    srv.idle_timeout = 0;
    now = 500000000000LL;
}

static Tube *
csr_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

// How many heap slots point at c. Anything but 0 or 1 means one of the
// entries carries a position that belongs to the other.
static int
csr_occurrences(Server *s, Conn *c)
{
    int n = 0;
    for (size_t i = 0; i < s->conns.len; i++)
        if (s->conns.data[i] == c)
            n++;
    return n;
}

// Fills the heap to exactly len == cap so the next insert must grow,
// which is the only point a refused allocation can be injected.
static void
csr_fill_to_capacity(Server *s, Conn **pad, int n, Tube *t)
{
    for (int i = 0; i < n; i++) {
        pad[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(pad[i], "setup: padding conn %d must allocate", i);
        pad[i]->srv = s;
        pad[i]->pending_timeout = 3000 + i;
        connsched(pad[i]);
        assertf(pad[i]->in_conns, "setup: padding conn %d must be scheduled", i);
    }
    assertf(s->conns.len == s->conns.cap,
            "setup: the heap must be exactly full, %zu of %zu",
            s->conns.len, s->conns.cap);
}


// With no refused insert on record there is nothing to repair, and a
// conn that simply has no reason to be in the heap must not be dragged
// into it by a recovery pass.
void
cttest_conn_sched_recover_schedules_nothing_while_the_heap_is_healthy(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-healthy");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 60;

    int r = conn_sched_recover();

    assertf(r == 0 && s.conns.len == 0 && !c->in_conns,
            "a healthy heap needs no repair: returned %d, heap holds %zu, "
            "conn in_conns %d", r, s.conns.len, c->in_conns);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// The core repair: a conn dropped by a refused insert comes back with a
// tick derived from its current state.
void
cttest_conn_sched_recover_reinserts_a_conn_that_a_refused_insert_dropped(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-reinsert");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 60;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(c);
    assertf(!c->in_conns, "setup: the insert must have been refused");

    int r = conn_sched_recover();

    assertf(r == 0 && c->in_conns == 1 && s.conns.len == 1
            && s.conns.data[0] == c,
            "the dropped conn must be back in the heap and the flag "
            "cleared: returned %d, in_conns %d, heap holds %zu",
            r, c->in_conns, s.conns.len);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// A pass that could not place everybody must say so: the caller uses the
// answer to shorten its park and try again instead of sleeping for an
// hour with a conn's timers invisible.
void
cttest_conn_sched_recover_reports_still_degraded_when_a_retry_is_refused(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-still-bad");
    Conn *pad[2];
    csr_fill_to_capacity(&s, pad, 2, t);
    Conn *a = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *b = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(a && b, "setup: both victims must allocate");
    a->srv = b->srv = &s;
    a->pending_timeout = 40;
    b->pending_timeout = 50;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(a);
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(b);
    assertf(!a->in_conns && !b->in_conns, "setup: both inserts must be refused");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    int r = conn_sched_recover();

    assertf(r != 0,
            "a pass that left a conn out of the heap must report still "
            "degraded, returned %d (heap holds %zu of 4 conns)",
            r, s.conns.len);

    conn_sched_recover();
    connclose(a);
    connclose(b);
    connclose(pad[0]);
    connclose(pad[1]);
    free(s.conns.data);
    tube_dref(t);
}


// The other half of a partial pass: the conn it did manage to place must
// stay placed. Abandoning the whole pass on the first refusal costs the
// conns that could have been saved.
void
cttest_conn_sched_recover_keeps_the_conn_it_saved_when_a_retry_is_refused(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-partial");
    Conn *pad[2];
    csr_fill_to_capacity(&s, pad, 2, t);
    Conn *a = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    Conn *b = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(a && b, "setup: both victims must allocate");
    a->srv = b->srv = &s;
    a->pending_timeout = 40;
    b->pending_timeout = 50;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(a);
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(b);
    assertf(!a->in_conns && !b->in_conns, "setup: both inserts must be refused");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    conn_sched_recover();

    assertf(a->in_conns + b->in_conns == 1 && s.conns.len == 3,
            "one refusal must not cost the conn the pass already placed: "
            "victims in heap %d, heap holds %zu",
            a->in_conns + b->in_conns, s.conns.len);

    conn_sched_recover();
    connclose(a);
    connclose(b);
    connclose(pad[0]);
    connclose(pad[1]);
    free(s.conns.data);
    tube_dref(t);
}


// Conns that are already scheduled must be skipped. Inserting one twice
// leaves a duplicate entry whose position is stale for one of the copies.
void
cttest_conn_sched_recover_skips_a_conn_that_is_already_scheduled(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-skip");
    Conn *healthy = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(healthy, "setup: the healthy conn must allocate");
    healthy->srv = &s;
    healthy->pending_timeout = 70;
    connsched(healthy);
    assertf(healthy->in_conns && s.conns.len == 1,
            "setup: the healthy conn must be scheduled");
    Conn *victim = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(victim, "setup: the victim must allocate");
    victim->srv = &s;
    victim->pending_timeout = 80;
    /* len 1 < cap 2, so drop the victim by refusing its insert on a
       heap that is already full. */
    Conn *pad = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(pad, "setup: the padding conn must allocate");
    pad->srv = &s;
    pad->pending_timeout = 90;
    connsched(pad);
    assertf(s.conns.len == s.conns.cap, "setup: the heap must be full");
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(victim);
    assertf(!victim->in_conns, "setup: the victim's insert must be refused");

    conn_sched_recover();

    assertf(csr_occurrences(&s, healthy) == 1 && s.conns.len == 3,
            "an already-scheduled conn must not be inserted a second time: "
            "it appears %d times in a heap of %zu",
            csr_occurrences(&s, healthy), s.conns.len);

    connclose(healthy);
    connclose(victim);
    connclose(pad);
    free(s.conns.data);
    tube_dref(t);
}


// Recovery rebuilds the tick from the conn's state at recovery time, not
// from whatever was computed when the insert was refused. A conn whose
// timeout changed in between must be scheduled for the new one.
void
cttest_conn_sched_recover_rebuilds_the_wake_up_from_the_current_timeout(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-stale");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 5;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(c);
    assertf(!c->in_conns, "setup: the insert must have been refused");
    c->pending_timeout = 50;
    int64 want = now + 50LL * 1000000000LL;

    conn_sched_recover();

    assertf(c->in_conns && c->tickat == want,
            "the rebuilt wake-up must use the conn's current timeout: want "
            "%" PRId64 ", got %" PRId64 " (in_conns %d)",
            want, c->tickat, c->in_conns);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// Once everything is back the flag must be down, or prottick keeps
// clamping its park to a second forever — a permanent 1Hz wake-up floor
// on an otherwise idle server.
void
cttest_conn_sched_recover_reports_clean_on_the_pass_after_a_repair(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-clean");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 35;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(c);
    assertf(conn_sched_recover() == 0, "setup: the repair pass must succeed");
    size_t len = s.conns.len;

    int r = conn_sched_recover();

    assertf(r == 0 && s.conns.len == len,
            "a repaired heap needs no second pass: returned %d, heap went "
            "from %zu to %zu", r, len, s.conns.len);

    connclose(c);
    free(s.conns.data);
    tube_dref(t);
}


// A conn that closed before the repair pass ran is off the live list and
// its struct is back in the slab pool. Rescheduling it would put pooled
// memory into the tick heap.
void
cttest_conn_sched_recover_never_reschedules_a_conn_that_has_closed(void)
{
    Server s;
    csr_setup(&s);
    Tube *t = csr_tube("csr-closed");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    c->pending_timeout = 55;
    fault_set(FAULT_REALLOC, 0, ENOMEM);
    connsched(c);
    assertf(!c->in_conns, "setup: the insert must have been refused");
    connclose(c);

    int r = conn_sched_recover();

    assertf(s.conns.len == 0 && r == 0,
            "a closed conn must never come back into the heap: heap holds "
            "%zu, recovery returned %d", s.conns.len, r);

    free(s.conns.data);
    tube_dref(t);
}
