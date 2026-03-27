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
