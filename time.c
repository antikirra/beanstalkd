#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

int64
nanoseconds(void)
{
    struct timespec ts;

    // CLOCK_MONOTONIC_COARSE: ~5ns vs ~25ns for CLOCK_MONOTONIC.
    // Resolution is jiffy-based (~1-4ms), adequate for beanstalkd
    // where job delays and TTR are in whole seconds.
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);

    return ((int64)ts.tv_sec)*1000000000 + (int64)ts.tv_nsec;
}

// What to add to a nanoseconds() reading to get wall-clock nanoseconds
// since the Unix epoch. The scheduler runs on the monotonic clock, but
// that clock's epoch is the last boot, so nothing on it may be written
// to the WAL: file.c translates with this at the file boundary.
// Sampled on every call rather than once at startup, so a wall clock
// set after boot (NTP) is picked up; both coarse clocks advance on the
// same timer tick, so the difference carries no jitter of its own.
int64
walclock_offset(void)
{
    struct timespec wall, mono;

    clock_gettime(CLOCK_REALTIME_COARSE, &wall);
    clock_gettime(CLOCK_MONOTONIC_COARSE, &mono);

    return ((int64)wall.tv_sec - (int64)mono.tv_sec)*1000000000
        + ((int64)wall.tv_nsec - (int64)mono.tv_nsec);
}
