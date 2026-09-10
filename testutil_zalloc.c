// Angry tests for zalloc (util.c:167-170).
//
// "Zalloc allocates n bytes of zeroed memory and returns a pointer to
// it. If insufficient memory is available, zalloc returns 0. Uses
// calloc which avoids memset for mmap'd pages on glibc."
//
// It backs new(T) for Conn, Tube and File, every one of which reads
// fields it never assigns — a Conn whose pointers come back holding
// somebody else's freed data is a use-after-free that no test of the
// caller would explain. Fresh pages from the kernel are already zero,
// so an allocation that has never been used cannot tell zeroed memory
// from unzeroed: these tests dirty the heap first.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
za_setup(void)
{
    fault_clear_all();
    progname = "testutil_zalloc";
    log_json = 0;
}


// Returns the index of the first byte that is not zero, or -1.
static long
za_first_dirty_byte(const unsigned char *p, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (p[i] != 0) return (long)i;
    }
    return -1;
}


// Returns the first size whose allocation is not aligned for every
// object type, or -1.
static long
za_first_misaligned_size(void)
{
    size_t sizes[] = { 1, 3, 7, 17, sizeof(Conn), sizeof(Tube), sizeof(File) };

    for (size_t i = 0; i < sizeof sizes / sizeof sizes[0]; i++) {
        void *p = zalloc(sizes[i]);
        assertf(p != NULL, "setup: zalloc %zu", sizes[i]);
        int aligned = ((uintptr_t)p % _Alignof(max_align_t)) == 0;
        free(p);
        if (!aligned) return (long)sizes[i];
    }
    return -1;
}


// The assertion that actually kills a calloc -> malloc rewrite: hand
// the allocator a block it has just seen dirtied and freed, and demand
// it comes back clean.
void
cttest_zalloc_zeroes_a_block_whose_pages_were_dirty(void)
{
    za_setup();
    size_t n = 4096;
    unsigned char *dirty = malloc(n);
    assertf(dirty != NULL, "setup: malloc %zu", n);
    memset(dirty, 0xAA, n);
    free(dirty);

    unsigned char *p = zalloc(n);
    assertf(p != NULL, "setup: zalloc %zu", n);
    long bad = za_first_dirty_byte(p, n);
    free(p);

    assertf(bad < 0,
            "byte %ld of a %zu-byte allocation still holds data from the "
            "block that was freed before it", bad, n);
}


// Every byte, including the last one, at a size well past any page or
// size-class boundary. A zeroing loop that stops one short leaves the
// tail of a large structure holding whatever was there.
void
cttest_zalloc_zeroes_an_eight_megabyte_block_to_its_last_byte(void)
{
    za_setup();
    size_t n = 8u << 20;

    unsigned char *p = zalloc(n);
    assertf(p != NULL, "setup: zalloc %zu", n);
    long bad = za_first_dirty_byte(p, n);
    free(p);

    assertf(bad < 0,
            "byte %ld of a %zu-byte allocation is not zero", bad, n);
}


// The documented failure mode. A size arithmetic that wraps — rounding
// up, multiplying two arguments — turns an impossible request into a
// small successful one, and every caller then writes past it.
void
cttest_zalloc_returns_zero_for_a_request_the_allocator_cannot_serve(void)
{
    za_setup();

    // volatile so the sizes are not compile-time constants: zalloc is
    // declared with alloc_size, and GCC rejects a provably impossible
    // request at compile time. What is under test is what the allocator
    // does with one at runtime.
    volatile size_t huge = SIZE_MAX;
    void *whole = zalloc(huge);
    void *half = zalloc(huge / 2);

    // Nothing is freed here on purpose. The contract says both calls
    // return 0, so there is nothing to give back; and if one of them
    // did hand out memory the assert below ends this forked test
    // process anyway, which is what makes the promise visible.
    assertf(whole == NULL && half == NULL,
            "impossible requests must return 0, got %p for SIZE_MAX and "
            "%p for SIZE_MAX/2", whole, half);
}


// The comment claims the mmap zero-page optimisation of calloc, which
// only holds while calloc is what gets called. A malloc+memset rewrite
// passes every zeroing test above and quietly touches every page of
// every large allocation.
void
cttest_zalloc_asks_calloc_rather_than_zeroing_a_malloc_itself(void)
{
    za_setup();

    void *p = zalloc(64);
    int callocs = fault_calls(FAULT_CALLOC);
    int mallocs = fault_calls(FAULT_MALLOC);
    free(p);

    assertf(callocs == 1 && mallocs == 0,
            "one zalloc must be one calloc and no malloc, got %d calloc(s) "
            "and %d malloc(s)", callocs, mallocs);
}


// Awkward sizes are the ones an offset-by-a-header reimplementation
// gets wrong, and every struct new(T) serves is loaded through pointers
// that assume natural alignment.
void
cttest_zalloc_returns_memory_aligned_for_every_object_it_serves(void)
{
    za_setup();

    long bad = za_first_misaligned_size();

    assertf(bad < 0,
            "a %ld-byte allocation came back misaligned for "
            "max_align_t (%zu)", bad, (size_t)_Alignof(max_align_t));
}


// The guarantee has to survive the macro, because that is how conn.c,
// tube.c and walg.c consume it. Dirty a block of exactly the size the
// macro will ask for, free it, then take it back through new(Conn).
void
cttest_zalloc_hands_the_new_macro_a_struct_with_every_byte_cleared(void)
{
    za_setup();
    unsigned char *dirty = malloc(sizeof(Conn));
    assertf(dirty != NULL, "setup: malloc %zu", sizeof(Conn));
    memset(dirty, 0x5A, sizeof(Conn));
    free(dirty);

    Conn *c = new(Conn);
    assertf(c != NULL, "setup: new(Conn)");
    long bad = za_first_dirty_byte((unsigned char *)c, sizeof(Conn));
    free(c);

    assertf(bad < 0,
            "byte %ld of a %zu-byte Conn from new() is not zero",
            bad, sizeof(Conn));
}
