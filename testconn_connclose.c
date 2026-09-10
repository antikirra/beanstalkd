// Angry tests for connclose (conn.c).
//
// The teardown path, and the one function in conn.c with a dozen separate
// promises. Every one of them is somebody's cleanup: a descriptor, a
// durable-commit slot, two global counters, two tube counters, a heap
// position, a live-list link, a hint array, the reserved jobs, and the
// struct itself. They are attacked one at a time so a red test names the
// promise that broke rather than "close is wrong".

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Defined in prot.c; the waiting counters connclose is expected to unwind. */
extern struct stats global_stat;

static void
ccl_setup(Server *s)
{
    fault_clear_all();
    progname = "testconn_connclose";
    prot_init();
    conn_pool_drain();
    job_pool_drain();
    memset(s, 0, sizeof *s);
    s->conns.less = conn_less;
    s->conns.setpos = conn_setpos;
    s->sock.fd = -1;
    s->sock.added = 1;
    srv.idle_timeout = 0;
}

static Tube *
ccl_tube(const char *name)
{
    Tube *t = tube_find_or_make(name);
    assertf(t, "setup: tube %s must allocate", name);
    tube_iref(t);
    return t;
}

static int
ccl_jobs_pooled(void)
{
    int n = -1;
    get_job_pool_stats(NULL, &n);
    return n;
}


// prothandle can reach a conn twice inside one event batch. The used
// tube's count must move exactly once, or the tube is destroyed while a
// live conn still points at it.
void
cttest_connclose_gives_the_used_tube_back_exactly_once_across_two_calls(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-use-twice");
    uint before = t->using_ct;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(t->using_ct == before + 1, "setup: the tube must be in use");

    connclose(c);
    connclose(c);

    assertf(t->using_ct == before,
            "two closes must give the tube back once: using_ct %u became %u",
            before, t->using_ct);

    free(s.conns.data);
    tube_dref(t);
}


// Same for the watch set: the second close must not run ms_clear again
// on a set that is already empty and drop a reference nobody holds.
void
cttest_connclose_gives_the_watched_tube_back_exactly_once_across_two_calls(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-watch-twice");
    uint before = t->watching_ct, refs = t->refs;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;

    connclose(c);
    connclose(c);

    assertf(t->watching_ct == before && t->refs == refs,
            "two closes must unwind the watch once: watching %u->%u, "
            "refs %u->%u", before, t->watching_ct, refs, t->refs);

    free(s.conns.data);
    tube_dref(t);
}


// The descriptor must really be handed back to the kernel. A conn whose
// socket outlives it burns a file descriptor for the life of the process
// and leaves the peer waiting on a connection nobody will ever read.
void
cttest_connclose_hands_the_descriptor_back_to_the_kernel(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-fd");
    int sp[2];
    assertf(socketpair(AF_UNIX, SOCK_STREAM, 0, sp) == 0,
            "setup: the socketpair must be created");
    Conn *c = make_conn(sp[0], STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(fcntl(sp[0], F_GETFD) != -1, "setup: the descriptor must be open");

    connclose(c);

    assertf(fcntl(sp[0], F_GETFD) == -1 && errno == EBADF,
            "the conn's descriptor must be closed: fcntl still reports it "
            "open (errno %d)", errno);

    close(sp[1]);
    free(s.conns.data);
    tube_dref(t);
}


// The dead-fd marker is what prothandle uses to skip a stale event for a
// conn that closed earlier in the same batch. Leaving a live descriptor
// number behind makes that check pass for the recycled struct.
void
cttest_connclose_leaves_the_dead_conn_marker_for_the_dispatcher(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-marker");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;

    connclose(c);

    assertf(c->sock.fd == -1,
            "a closed conn must carry the dead-fd marker, sock.fd is %d",
            c->sock.fd);

    free(s.conns.data);
    tube_dref(t);
}


// The hint array dies with the conn. The pool-reuse memset would zero the
// pointer without freeing it, so the free has to happen here — and the
// pointer has to be cleared, or a second close frees it again.
void
cttest_connclose_lets_go_of_the_waiting_hint_array(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-waitpos");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");
    assertf(c->waitpos != NULL && c->waitpos_cap >= 1,
            "setup: waiting must have allocated a hint array");

    connclose(c);

    assertf(c->waitpos == NULL && c->waitpos_cap == 0,
            "a closed conn must own no hint array: pointer %p, capacity %zu",
            (void *)c->waitpos, c->waitpos_cap);

    free(s.conns.data);
    tube_dref(t);
}


// A peek or stats reply leaves a Copy job attached. Nobody else owns it,
// so the close is its only chance to be released.
void
cttest_connclose_releases_an_out_job_that_is_a_copy(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-copy");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    Job *copy = allocate_job(8);
    assertf(copy, "setup: the copy job must allocate");
    copy->tube = NULL;
    copy->r.state = Copy;
    c->out_job = copy;
    int before = ccl_jobs_pooled();

    connclose(c);

    assertf(ccl_jobs_pooled() == before + 1,
            "the copy the conn was sending must be released: job pool %d "
            "became %d", before, ccl_jobs_pooled());

    free(s.conns.data);
    tube_dref(t);
}


// A job that is not a Copy belongs to the queue, not to the conn. Freeing
// it here destroys a job another data structure still references.
void
cttest_connclose_leaves_an_out_job_that_is_not_a_copy_to_its_owner(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-noncopy");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    Job *owned = allocate_job(8);
    assertf(owned, "setup: the job must allocate");
    owned->tube = NULL;
    owned->r.state = Ready;
    c->out_job = owned;
    int before = ccl_jobs_pooled();

    connclose(c);

    assertf(ccl_jobs_pooled() == before,
            "a job that is not a copy must survive the close: job pool %d "
            "became %d", before, ccl_jobs_pooled());

    free(s.conns.data);
    tube_dref(t);
}


// in_job and out_job can be the same pointer: conn_timeout replaces a
// job being written out with a Copy and stores it in out_job. The
// unconditional release of in_job followed by the Copy check on out_job
// must not release the same job twice.
void
cttest_connclose_must_not_release_an_aliased_job_twice(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-alias");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    Job *shared = allocate_job(8);
    assertf(shared, "setup: the job must allocate");
    shared->tube = NULL;
    shared->r.state = Copy;
    c->in_job = shared;
    c->out_job = shared;
    int before = ccl_jobs_pooled();

    connclose(c);

    assertf(ccl_jobs_pooled() == before + 1,
            "one job reachable through two pointers must be released once: "
            "job pool %d became %d", before, ccl_jobs_pooled());

    free(s.conns.data);
    tube_dref(t);
}


// Closing the middle of three scheduled conns must remove exactly that
// conn and leave the other two findable at their recorded positions —
// heapremove is driven by the stored index, so a wrong index silently
// unschedules a different connection.
void
cttest_connclose_removes_its_own_entry_from_the_server_tick_heap(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-heap");
    now = 500000000000LL;
    Conn *cs[3];
    for (int i = 0; i < 3; i++) {
        cs[i] = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
        assertf(cs[i], "setup: make_conn %d must succeed", i);
        cs[i]->srv = &s;
        cs[i]->pending_timeout = 10 + i * 10;
        connsched(cs[i]);
        assertf(cs[i]->in_conns, "setup: conn %d must be scheduled", i);
    }

    connclose(cs[1]);

    assertf(s.conns.len == 2
            && s.conns.data[cs[0]->tickpos] == cs[0]
            && s.conns.data[cs[2]->tickpos] == cs[2],
            "closing one scheduled conn must leave the other two at their "
            "recorded positions: heap holds %zu, positions %zu/%zu",
            s.conns.len, cs[0]->tickpos, cs[2]->tickpos);

    connclose(cs[0]);
    connclose(cs[2]);
    free(s.conns.data);
    tube_dref(t);
}


// A closing conn must leave every waiting set it joined. One left behind
// is a dangling pointer that process_tube will hand a job to.
void
cttest_connclose_leaves_every_waiting_set_the_conn_had_joined(void)
{
    Server s;
    ccl_setup(&s);
    Tube *ta = ccl_tube("ccl-wait-a");
    Tube *tb = ccl_tube("ccl-wait-b");
    Tube *tc = ccl_tube("ccl-wait-c");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(ms_append(&c->watch, tb) && ms_append(&c->watch, tc),
            "setup: the extra watches must succeed");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");

    connclose(c);

    assertf(ta->waiting_conns.len == 0 && tb->waiting_conns.len == 0
            && tc->waiting_conns.len == 0,
            "no tube may still be waiting on a closed conn: sets hold "
            "%zu/%zu/%zu", ta->waiting_conns.len, tb->waiting_conns.len,
            tc->waiting_conns.len);

    free(s.conns.data);
    tube_dref(ta);
    tube_dref(tb);
    tube_dref(tc);
}


// The waiting counters are unsigned and appear verbatim in `stats`. A
// waiting conn owes one global decrement and one per watched tube.
void
cttest_connclose_gives_back_every_waiting_count_a_waiting_conn_held(void)
{
    Server s;
    ccl_setup(&s);
    Tube *ta = ccl_tube("ccl-wct-a");
    Tube *tb = ccl_tube("ccl-wct-b");
    uint64 gbefore = global_stat.waiting_ct;
    uint64 abefore = ta->stat.waiting_ct, bbefore = tb->stat.waiting_ct;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(ms_append(&c->watch, tb), "setup: the second watch must succeed");
    assertf(enqueue_waiting_conn(c) == 1, "setup: the conn must wait");

    connclose(c);

    assertf(global_stat.waiting_ct == gbefore
            && ta->stat.waiting_ct == abefore
            && tb->stat.waiting_ct == bbefore,
            "closing a waiting conn must balance every waiting count: "
            "global %" PRIu64 "->%" PRIu64 ", a %" PRIu64 "->%" PRIu64
            ", b %" PRIu64 "->%" PRIu64,
            gbefore, global_stat.waiting_ct, abefore, ta->stat.waiting_ct,
            bbefore, tb->stat.waiting_ct);

    free(s.conns.data);
    tube_dref(ta);
    tube_dref(tb);
}


// Most conns are not waiting when they close. Decrementing anyway wraps
// an unsigned zero into a number the operator will read as four billion
// blocked workers.
void
cttest_connclose_touches_no_waiting_count_for_a_conn_that_was_not_waiting(void)
{
    Server s;
    ccl_setup(&s);
    Tube *ta = ccl_tube("ccl-nowait-a");
    Tube *tb = ccl_tube("ccl-nowait-b");
    uint64 gbefore = global_stat.waiting_ct;
    uint64 abefore = ta->stat.waiting_ct, bbefore = tb->stat.waiting_ct;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, ta, ta);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    assertf(ms_append(&c->watch, tb), "setup: the second watch must succeed");
    assertf(!conn_waiting(c), "setup: the conn must not be waiting");

    connclose(c);

    assertf(global_stat.waiting_ct == gbefore
            && ta->stat.waiting_ct == abefore
            && tb->stat.waiting_ct == bbefore,
            "closing a conn that never waited must move no waiting count: "
            "global %" PRIu64 "->%" PRIu64 ", a %" PRIu64 "->%" PRIu64
            ", b %" PRIu64 "->%" PRIu64,
            gbefore, global_stat.waiting_ct, abefore, ta->stat.waiting_ct,
            bbefore, tb->stat.waiting_ct);

    free(s.conns.data);
    tube_dref(ta);
    tube_dref(tb);
}


// A worker that disconnects mid-job must not take the job with it: the
// reservation goes back to the tube for somebody else to take.
void
cttest_connclose_returns_a_held_reservation_to_its_tube(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-reserved");
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;
    now = 300000000000LL;
    Job j;
    memset(&j, 0, sizeof j);
    job_list_reset(&j);
    j.tube = t;
    j.r.ttr = 5000000000LL;
    j.r.pri = 7;
    conn_reserve_job(c, &j);
    size_t ready_before = t->ready.len;

    connclose(c);

    assertf(j.r.state == Ready && t->ready.len == ready_before + 1,
            "a reservation held at close must go back to the tube: state "
            "%d, ready holds %zu (was %zu)",
            (int)j.r.state, t->ready.len, ready_before);

    free(s.conns.data);
    tube_dref(t);
}


// Not every conn has a server yet: make_conn returns before h_accept
// assigns one, and the teardown must survive that window rather than
// following a null pointer into the listener re-arm.
void
cttest_connclose_completes_for_a_conn_that_was_never_given_a_server(void)
{
    Server s;
    ccl_setup(&s);
    Tube *t = ccl_tube("ccl-nosrv");
    uint before = count_cur_conns();
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    assertf(c->srv == NULL, "setup: the conn must have no server yet");

    connclose(c);

    assertf(count_cur_conns() == before,
            "a conn with no server must still be fully reclaimed: live "
            "count %u became %u", before, count_cur_conns());

    free(s.conns.data);
    tube_dref(t);
}


// The listener is parked out of epoll while the server is at its
// connection ceiling; a close is what makes room, so it must re-arm the
// listener — and only when the listener is not already registered.
void
cttest_connclose_re_arms_a_listener_that_is_out_of_epoll(void)
{
    Server s;
    ccl_setup(&s);
    assertf(sockinit() == 0, "setup: the epoll instance must be created");
    Tube *t = ccl_tube("ccl-rearm");
    int sp[2];
    assertf(socketpair(AF_UNIX, SOCK_STREAM, 0, sp) == 0,
            "setup: the socketpair must be created");
    s.sock.fd = sp[0];
    s.sock.added = 0;
    Conn *c = make_conn(dup(2), STATE_WANT_COMMAND, t, t);
    assertf(c, "setup: make_conn must succeed");
    c->srv = &s;

    connclose(c);

    assertf(s.sock.added == 1 && s.sock.rw_cached == 'r',
            "closing a conn must put the parked listener back in epoll: "
            "added %d, cached mode %d", s.sock.added, s.sock.rw_cached);

    close(sp[0]);
    close(sp[1]);
    free(s.conns.data);
    tube_dref(t);
}


