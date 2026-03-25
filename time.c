#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

int64
nanoseconds(void)
{
    struct timespec ts;

    int r = clock_gettime(CLOCK_MONOTONIC, &ts);
    if (r != 0) return warnx("clock_gettime"), -1; // can't happen

    return ((int64)ts.tv_sec)*1000000000 + (int64)ts.tv_nsec;
}
