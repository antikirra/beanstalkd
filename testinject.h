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
    FAULT_COUNT
};

struct fault {
    int countdown;  // 0=off, 1=fail next, N=skip N-1 then fail
    int err;        // errno to inject (0=use sensible default)
    int hits;       // number of times fault was injected
};

extern struct fault faults[];

// Arm: skip `after` successful calls, then fail.
// after=0 means fail immediately on next call.
void fault_set(int which, int after, int err);

void fault_clear(int which);
void fault_clear_all(void);
int  fault_hits(int which);

#endif
