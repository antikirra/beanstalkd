// Angry tests for make_conn (conn.c).
//
// The accept path. Half its contract is about what a RECYCLED struct must
// look like — the pool-reuse memset covers only [0, offsetof(Conn, cmd)),
// so every field that must read zero on reuse depends on staying inside
// that prefix, and dat.h names a stale durable-batch flag as a hung
// client. The other half is the two failure paths, which must leave the
// counters, the tube books and the caller's descriptor exactly as they
// found them.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void
mkc_setup(void)
{
    fault_clear_all();
    progname = "testconn_make_conn";
    prot_init();
    conn_pool_drain();
}

static Tube *
mkc_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
mkc_pooled(void)
{
    int n = -1;
    get_conn_pool_stats(&n);
    return n;
}

// Name of the first field that did not come back zero on a recycled
// struct, or NULL when the whole hot prefix was cleared.
static const char *
mkc_first_dirty_field(Conn *c)
{
    if (c->in_epollq) return "in_epollq";
    if (c->halfclosed) return "halfclosed";
    if (c->out_job) return "out_job";
    if (c->soonest_job) return "soonest_job";
    if (c->in_conns) return "in_conns";
    if (c->type) return "type";
    if (c->waitpos) return "waitpos";
    if (c->waitpos_cap) return "waitpos_cap";
    if (c->tickat) return "tickat";
    if (c->tickpos) return "tickpos";
    if (c->in_dur_batch) return "in_dur_batch";
    if (c->dur_batch_idx) return "dur_batch_idx";
    if (c->dur_reply_len) return "dur_reply_len";
    return NULL;
}

// Name of the first field that sits OUTSIDE the range the pool-reuse
// memset clears, or NULL when every one of them is inside it.
static const char *
mkc_first_field_past_the_memset(void)
{
    size_t stop = offsetof(Conn, cmd);
    if (offsetof(Conn, in_dur_batch) >= stop) return "in_dur_batch";
    if (offsetof(Conn, dur_batch_idx) >= stop) return "dur_batch_idx";
    if (offsetof(Conn, dur_reply_len) >= stop) return "dur_reply_len";
    if (offsetof(Conn, waitpos) >= stop) return "waitpos";
    if (offsetof(Conn, waitpos_cap) >= stop) return "waitpos_cap";
    if (offsetof(Conn, last_activity_at) >= stop) return "last_activity_at";
    if (offsetof(Conn, in_epollq) >= stop) return "in_epollq";
    if (offsetof(Conn, live_next) >= stop) return "live_next";
    if (offsetof(Conn, live_prev) >= stop) return "live_prev";
    if (offsetof(Conn, tickat) >= stop) return "tickat";
    if (offsetof(Conn, tickpos) >= stop) return "tickpos";
    if (offsetof(Conn, type) >= stop) return "type";
    if (offsetof(Conn, in_conns) >= stop) return "in_conns";
    return NULL;
}


// The start state decides which branch of the protocol FSM the first
// event takes. h_accept passes STATE_WANT_COMMAND; prot_replay does not.
void
cttest_make_conn_starts_the_conn_in_the_state_it_was_given(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-state");
    char want = STATE_WANT_ENDLINE;

    Conn *c = make_conn(dup(2), want, t, t);

    assertf(c && c->state == want,
            "the conn must start in the state it was given: want %d, got %d",
            (int)want, c ? (int)c->state : -1);

    connclose(c);
    tube_dref(t);
}


// A fresh conn is not waiting on anything. A pending_timeout of 0 would
// schedule an immediate, permanent wake-up for a client that has not even
// sent its first command.
void
cttest_make_conn_hands_back_a_conn_with_no_reserve_timeout_pending(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-pending");

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(c && c->pending_timeout == -1,
            "a fresh conn waits forever by default: pending_timeout is %d",
            c ? c->pending_timeout : 0);

    connclose(c);
    tube_dref(t);
}


// The use tube and the watch tube are different arguments with different
// counters. Swapping them makes `stats-tube` attribute every client to
// the wrong tube.
void
cttest_make_conn_counts_the_used_tube_only_as_used(void)
{
    mkc_setup();
    Tube *used = mkc_tube("mkc-used");
    Tube *watched = mkc_tube("mkc-watched");
    uint using_before = used->using_ct, watching_before = used->watching_ct;

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, used, watched);

    assertf(c && used->using_ct == using_before + 1
            && used->watching_ct == watching_before,
            "the used tube gains a user and no watcher: using %u->%u, "
            "watching %u->%u", using_before, used->using_ct,
            watching_before, used->watching_ct);

    connclose(c);
    tube_dref(used);
    tube_dref(watched);
}


// The mirror check, so an argument swap cannot pass by moving both
// counters on both tubes.
void
cttest_make_conn_counts_the_watched_tube_only_as_watched(void)
{
    mkc_setup();
    Tube *used = mkc_tube("mkc-used2");
    Tube *watched = mkc_tube("mkc-watched2");
    uint using_before = watched->using_ct, watching_before = watched->watching_ct;

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, used, watched);

    assertf(c && watched->watching_ct == watching_before + 1
            && watched->using_ct == using_before,
            "the watched tube gains a watcher and no user: watching %u->%u, "
            "using %u->%u", watching_before, watched->watching_ct,
            using_before, watched->using_ct);

    connclose(c);
    tube_dref(used);
    tube_dref(watched);
}


// last_activity_at is the baseline the -I idle sweep measures from. A
// conn born with a zero baseline is already infinitely idle and gets
// reaped before it can send anything.
void
cttest_make_conn_stamps_the_idle_baseline_with_the_cached_clock(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-baseline");
    now = 777000000000LL;

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(c && c->last_activity_at == now,
            "the idle baseline must be the cached clock %" PRId64 ", got "
            "%" PRId64, now, c ? c->last_activity_at : 0);

    connclose(c);
    tube_dref(t);
}


// The same on the pool path, where the memset has just zeroed the field:
// a recycled struct that keeps a zero baseline is reaped immediately.
void
cttest_make_conn_stamps_the_idle_baseline_on_a_recycled_struct_too(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-baseline-reuse");
    now = 100000000000LL;
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connclose(first);
    now = 900000000000LL;

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);

    assertf(second && second->last_activity_at == now,
            "a recycled conn's idle baseline must be the current clock "
            "%" PRId64 ", got %" PRId64, now, second ? second->last_activity_at : 0);

    connclose(second);
    tube_dref(t);
}


// The watch append is the last thing that can fail. The tube books must
// look exactly as they did before the attempt, or a tube is kept alive
// (or its watcher count inflated) by a connection that never existed.
void
cttest_make_conn_holds_no_tube_reference_when_the_watch_append_fails(void)
{
    mkc_setup();
    Tube *used = mkc_tube("mkc-oom-used");
    Tube *watched = mkc_tube("mkc-oom-watched");
    uint refs = watched->refs, watching = watched->watching_ct;
    uint using = used->using_ct;
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, used, watched);

    assertf(c == NULL && watched->refs == refs
            && watched->watching_ct == watching && used->using_ct == using,
            "a failed accept must hold nothing: refs %u->%u, watching "
            "%u->%u, using %u->%u (make_conn returned %p)",
            refs, watched->refs, watching, watched->watching_ct,
            using, used->using_ct, (void *)c);

    tube_dref(used);
    tube_dref(watched);
}


// The struct that failed is fully formed and must go back to the slab
// pool rather than being dropped on the floor: leaking one per refused
// accept is a leak that grows exactly when memory is already short.
void
cttest_make_conn_returns_a_struct_that_failed_to_the_slab_pool(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-oom-pool");
    assertf(mkc_pooled() == 0, "setup: the pool must start empty");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c == NULL, "setup: the accept must have failed");

    assertf(mkc_pooled() == 1,
            "the struct that failed must be reusable, the pool reports %d",
            mkc_pooled());

    conn_pool_drain();
    tube_dref(t);
}


// The caller owns the descriptor until make_conn succeeds. Closing it on
// the failure path makes h_accept close a descriptor number the kernel
// may already have handed to the next accept.
void
cttest_make_conn_leaves_the_descriptor_open_for_its_caller_after_a_failure(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-oom-fd");
    int sp[2];
    assertf(socketpair(AF_UNIX, SOCK_STREAM, 0, sp) == 0,
            "setup: the socketpair must be created");
    fault_set(FAULT_REALLOC, 0, ENOMEM);

    Conn *c = make_conn(sp[0], STATE_WANT_COMMAND, t, t);
    assertf(c == NULL, "setup: the accept must have failed");

    assertf(fcntl(sp[0], F_GETFD) != -1,
            "a failed accept must leave the caller's descriptor open, "
            "fcntl reports errno %d", errno);

    close(sp[0]);
    close(sp[1]);
    conn_pool_drain();
    tube_dref(t);
}


// Every hot field poisoned on a pooled struct must read zero again on
// the next take. The three durable-batch scalars are in the list because
// dat.h calls a stale in_dur_batch a hung client: reply() would buffer
// acks into a conn that is not in the batch array.
void
cttest_make_conn_clears_every_hot_field_of_a_recycled_struct(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-poison");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    connclose(first);
    assertf(mkc_pooled() == 1, "setup: the struct must be pooled");
    first->in_epollq = 1;
    first->halfclosed = 1;
    first->out_job = (Job *)(void *)&first;
    first->soonest_job = (Job *)(void *)&first;
    first->in_conns = 1;
    first->type = CONN_TYPE_PRODUCER | CONN_TYPE_WORKER | CONN_TYPE_WAITING;
    first->waitpos = (size_t *)(void *)&first;
    first->waitpos_cap = 99;
    first->tickat = 123456789;
    first->tickpos = 4242;
    first->in_dur_batch = 1;
    first->dur_batch_idx = 7;
    first->dur_reply_len = 99;

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second == first, "setup: the pooled struct must be handed back");

    assertf(mkc_first_dirty_field(second) == NULL,
            "every hot field must be cleared on reuse, %s came back dirty",
            mkc_first_dirty_field(second));

    connclose(second);
    tube_dref(t);
}


// The memset stops at cmd[]. Any field moved past it silently stops
// being cleared on reuse, and the compile-time assertion in conn.c only
// checks that cmd[] is early — not that these fields precede it.
void
cttest_make_conn_keeps_every_field_it_must_clear_before_the_buffers(void)
{
    mkc_setup();

    const char *late = mkc_first_field_past_the_memset();

    assertf(late == NULL,
            "field %s sits at or past cmd[] (offset %zu), so it would keep "
            "its value across pool reuse", late ? late : "",
            (size_t)offsetof(Conn, cmd));
}


// The generation counter is the only thing distinguishing a recycled
// struct from the connection that used to live there. It must advance on
// every take, or a reference captured before the close still looks current.
void
cttest_make_conn_advances_the_generation_when_it_reuses_a_struct(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-gen");
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    uint64 before = first->gen;
    connclose(first);

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second == first, "setup: the pooled struct must be handed back");

    assertf(second->gen == before + 1,
            "reuse must advance the generation: %" PRIu64 " became %" PRIu64,
            before, second->gen);

    connclose(second);
    tube_dref(t);
}


// An all-zero list header is not an empty list. A recycled conn whose
// reserved-jobs header was merely zeroed reports that it holds jobs, and
// conntickat then follows a null chain looking for the soonest deadline.
void
cttest_make_conn_gives_a_recycled_conn_a_properly_empty_reserved_list(void)
{
    mkc_setup();
    Tube *t = mkc_tube("mkc-reserved");
    Server s;
    memset(&s, 0, sizeof s);
    s.conns.less = conn_less;
    s.conns.setpos = conn_setpos;
    s.sock.fd = -1;
    s.sock.added = 1;
    Conn *first = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(first, "setup: the first make_conn must succeed");
    first->srv = &s;
    now = 200000000000LL;
    Job j;
    memset(&j, 0, sizeof j);
    job_list_reset(&j);
    j.tube = t;
    j.r.ttr = 5000000000LL;
    conn_reserve_job(first, &j);
    connclose(first);

    Conn *second = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(second == first, "setup: the pooled struct must be handed back");

    assertf(job_list_is_empty(&second->reserved_jobs),
            "a recycled conn must start with a well-formed empty reserved "
            "list: next %p, prev %p, header %p",
            (void *)second->reserved_jobs.next,
            (void *)second->reserved_jobs.prev,
            (void *)&second->reserved_jobs);

    connclose(second);
    free(s.conns.data);
    tube_dref(t);
}
