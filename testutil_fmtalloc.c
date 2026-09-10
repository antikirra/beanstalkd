// Angry tests for fmtalloc (util.c).
//
// dat.h:342 declares it printf-checked; util.c:151-155 documents the
// contract: a freshly malloc'd, NUL-terminated string equal to the
// printf rendering, sized to hold it exactly, or NULL when the
// allocation fails or the format cannot be encoded. Callers such as
// fileinit own the buffer and check for NULL.
//
// The whole function is two vsnprintf passes around one malloc, so the
// interesting failures are all off-by-one in that arithmetic: a size
// pass that measures something other than the render pass, a missing
// +1 that eats the last character, and a rendering large enough that
// any hidden fixed buffer would show.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
fa_setup(void)
{
    fault_clear_all();
    progname = "testutil_fmtalloc";
    log_json = 0;
}


// Returns the first length at which the result is not exactly a copy of
// its argument, or -1. Each case is a %s of a string of that length: the
// buffer must hold every byte plus the terminator, whatever the length
// does to malloc's size classes or to any internal buffering.
static int
fa_first_length_not_rendered_whole(void)
{
    static const int lens[] = { 0, 1, 4095, 4096, 4097, 70000, -1 };

    for (int i = 0; lens[i] >= 0; i++) {
        size_t len = (size_t)lens[i];
        char *filler = malloc(len + 1);
        assertf(filler != NULL, "setup: malloc %zu", len + 1);
        memset(filler, 'k', len);
        filler[len] = 0;

        char *got = fmtalloc("%s", filler);
        int ok = got != NULL && strlen(got) == len
                 && memcmp(got, filler, len + 1) == 0;
        free(got);
        free(filler);
        if (!ok) return lens[i];
    }
    return -1;
}


// Every conversion the callers in this tree actually use, in one
// format, compared against the same rendering done by the C library
// into a buffer that is certainly large enough. Anything fmtalloc does
// differently — a truncated tail, a dropped conversion, a rounded size —
// shows up as a byte difference rather than as a plausible-looking
// string.
void
cttest_fmtalloc_renders_the_same_bytes_as_a_generously_sized_snprintf(void)
{
    fa_setup();
    const char *path = "/var/lib/beanstalkd/binlog.7";
    size_t big = SIZE_MAX;
    long long low = INT64_MIN;
    char want[512];
    snprintf(want, sizeof want, "%s|%zu|%lld|%05d|%.3s|%%",
             path, big, low, -42, "abcdef");

    char *got = fmtalloc("%s|%zu|%lld|%05d|%.3s|%%",
                         path, big, low, -42, "abcdef");
    int same = got != NULL && strcmp(got, want) == 0;
    char copy[512];
    snprintf(copy, sizeof copy, "%s", got ? got : "(null)");
    free(got);

    assertf(same, "want [%s] got [%s]", want, copy);
}


// The size pass and the render pass must agree at every length, and the
// +1 for the terminator must be there at all of them. A missing +1 is
// invisible for short strings on most allocators and eats the last
// character of every path fileinit builds.
void
cttest_fmtalloc_sizes_the_buffer_for_the_exact_rendering_at_every_length(void)
{
    fa_setup();

    int bad = fa_first_length_not_rendered_whole();

    assertf(bad < 0,
            "a %d-character rendering must come back whole and "
            "NUL-terminated", bad);
}


// One rendering, one allocation. A retry loop that grows a buffer until
// the rendering fits would pass every other test in this file while
// doubling the allocator traffic of every WAL path built at startup.
void
cttest_fmtalloc_asks_the_allocator_exactly_once_for_one_rendering(void)
{
    fa_setup();

    char *got = fmtalloc("binlog.%d", 7);
    int calls = fault_calls(FAULT_MALLOC);
    int ok = got != NULL && strcmp(got, "binlog.7") == 0;
    free(got);

    assertf(calls == 1 && ok,
            "one rendering must cost exactly one allocation, got %d "
            "(result correct: %d)", calls, ok);
}


// The documented failure mode, and life after it: a caller that handles
// NULL keeps running, and the next rendering must be complete and
// correct rather than a leftover of the failed one.
void
cttest_fmtalloc_renders_correctly_again_after_a_failed_allocation(void)
{
    fa_setup();
    fault_set(FAULT_MALLOC, 0, ENOMEM);

    char *failed = fmtalloc("%s/%d", "wal", 3);
    fault_clear_all();
    char *got = fmtalloc("%s/%d", "wal", 3);
    int ok = got != NULL && strcmp(got, "wal/3") == 0;
    char copy[64];
    snprintf(copy, sizeof copy, "%s", got ? got : "(null)");
    free(got);
    free(failed);

    assertf(failed == NULL && ok,
            "the call after a refused allocation must render in full, "
            "got [%s] (failed call returned %s)",
            copy, failed == NULL ? "NULL" : "a buffer");
}
