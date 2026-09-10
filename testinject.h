#include <stdatomic.h>
// Syscall error injection for hostile tests.
// Uses GNU ld --wrap to intercept libc calls at link time.
// Zero overhead in the production binary.
//
// Usage:
//   fault_set(FAULT_MALLOC, 0, ENOMEM);   // fail next malloc
//   fault_set(FAULT_WRITE, 3, EIO);       // skip 3, fail 4th write
//   ... call code under test ...
//   assertf(fault_hits(FAULT_MALLOC) == 1, "expected 1 injection");
//   fault_clear_all();

#ifndef TESTINJECT_H
#define TESTINJECT_H

enum {
    FAULT_MALLOC,
    FAULT_CALLOC,
    FAULT_REALLOC,
    FAULT_WRITE,
    FAULT_WRITEV,
    FAULT_READ,
    FAULT_OPEN,
    FAULT_FTRUNCATE,
    FAULT_UNLINK,
    FAULT_FDATASYNC,
    FAULT_STAT,
    FAULT_PTHREAD_CREATE,
    FAULT_FALLOCATE,
    FAULT_SETSOCKOPT,
    FAULT_COUNT
};

// The wrapped syscalls fire on whichever thread makes the call, and the
// fsync thread is one of them, so every field here is touched from more
// than one thread. Relaxed atomics: the counters only have to be
// individually coherent, and the tests read them after the work is done.
struct fault {
    _Atomic int countdown;  // 0=off, 1=fail next, N=skip N-1 then fail
    _Atomic int err;        // errno to inject (0=use sensible default)
    _Atomic int shortn;     // >0: pass only this many bytes through and
                            // report that count, instead of failing.
                            // Write paths only (write/writev).
    _Atomic int hits;       // number of times fault was injected
    _Atomic int calls;      // total invocations of the wrapped call, fault
                            // or not; useful to prove a code path reached
                            // the syscall on success tests where no fault
                            // fires.
};

extern struct fault faults[];

// Arm: skip `after` successful calls, then fail.
// after=0 means fail immediately on next call.
void fault_set(int which, int after, int err);

// Arm a SHORT write: skip `after` calls, then let only `nbytes` of the
// next one through and report that count. A short write is not an
// error — it is the kernel taking what fits in the socket buffer — and
// it is the only way to reach the retry paths in reply()/h_conn from a
// unit test. FAULT_WRITE and FAULT_WRITEV only.
void fault_set_short(int which, int after, int nbytes);

void fault_clear(int which);
void fault_clear_all(void);
int  fault_hits(int which);
int  fault_calls(int which);

// One-shot hook invoked immediately before the real epoll_pwait, then
// auto-disarmed. Unlike the fault table above it never fails the call;
// it exists to land work (e.g. kill(getpid(), SIGTERM)) deterministically
// inside the historical check-then-block window between srvserve's
// shutdown_requested test and the kernel parking the thread in epoll.
// Cleared by fault_clear_all().
extern void (*epoll_pwait_pre_hook)(void);

#endif
