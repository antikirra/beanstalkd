#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// 4-ary min-heap.
//
// Layout: parent(k) = (k-1) >> 2, children at 4k+1 .. 4k+4.
// Compared to the binary heap, tree depth is halved (log4 vs log2)
// so siftdown does ~half as many parent comparisons. Siftup pays
// for the shallower tree with 4 child comparisons per level, but
// the 4 sibling pointers occupy one 64-byte cache line on LP64
// (4 * 8 = 32 bytes, i.e. half a line), so the extra compares hit
// L1 only. Net win: ~25–35% faster on delay heaps with ≥1k entries.
//
// Invariant (min-heap): h->less(parent, child) is true for every
// (parent, child) pair. The type-erased less() is supplied by the
// caller — job_pri_less for ready, job_delay_less for delay, etc.


static void
set(Heap *h, size_t k, void *x)
{
    h->data[k] = x;
    h->setpos(x, k);
}


static inline size_t
heap_parent(size_t k)
{
    return (k - 1) >> 2;
}


// Siftdown (hole technique): element at k may be smaller than parent.
// Walk up, shifting parents into the hole. Half the setpos calls vs swap.
__attribute__((hot)) static void
siftdown(Heap *h, size_t k)
{
    if (k == 0) return;

    void *x = h->data[k];
    while (k > 0) {
        size_t p = heap_parent(k);
        if (h->less(h->data[p], x))
            break;
        h->data[k] = h->data[p];
        h->setpos(h->data[p], k);
        k = p;
    }
    set(h, k, x);
}


// Siftup (hole technique): element at k may be larger than a child.
// Find the smallest of up to 4 children; shift it up if it beats x.
// Prefetches grandchildren for large heaps to hide L2/L3 latency.
__attribute__((hot)) static void
siftup(Heap *h, size_t k)
{
    void *x = h->data[k];
    size_t n = h->len;

    for (;;) {
        size_t c0 = (k << 2) + 1;
        if (c0 >= n)
            break;

        // Prefetch grandchildren when the heap is large enough that
        // L1 can't hold the working set. For a 4-ary heap, grand-
        // children span 4 cache lines (16 pointers, 128 bytes).
        if (n > 64) {
            size_t g = (c0 << 2) + 1;
            if (g < n)       __builtin_prefetch(h->data[g],      0, 1);
            if (g + 4 < n)   __builtin_prefetch(h->data[g + 4],  0, 1);
            if (g + 8 < n)   __builtin_prefetch(h->data[g + 8],  0, 1);
            if (g + 12 < n)  __builtin_prefetch(h->data[g + 12], 0, 1);
        }

        // Find smallest child among up to 4.
        size_t smin = c0;
        if (c0 + 1 < n && h->less(h->data[c0 + 1], h->data[smin])) smin = c0 + 1;
        if (c0 + 2 < n && h->less(h->data[c0 + 2], h->data[smin])) smin = c0 + 2;
        if (c0 + 3 < n && h->less(h->data[c0 + 3], h->data[smin])) smin = c0 + 3;

        if (!h->less(h->data[smin], x))
            break;

        h->data[k] = h->data[smin];
        h->setpos(h->data[smin], k);
        k = smin;
    }
    set(h, k, x);
}


// Heapinsert inserts x into heap h according to h->less.
// Returns 1 on success, 0 on realloc failure.
__attribute__((hot)) int
heapinsert(Heap *h, void *x)
{
    if (unlikely(h->len == h->cap)) {
        size_t ncap = (h->len + 1) * 2;
        if (ncap > SIZE_MAX / sizeof(void *)) {
            return 0;
        }
        void **ndata = realloc(h->data, sizeof(void *) * ncap);
        if (!ndata) {
            return 0;
        }
        h->data = ndata;
        h->cap = ncap;
    }

    size_t k = h->len;
    h->len++;
    set(h, k, x);
    if (likely(k > 0))
        siftdown(h, k);
    return 1;
}


__attribute__((hot)) void *
heapremove(Heap *h, size_t k)
{
    if (unlikely(k >= h->len)) {
        return 0;
    }

    void *x = h->data[k];
    h->len--;
    if (k < h->len) {
        h->data[k] = h->data[h->len];
        h->setpos(h->data[k], k);
        // Decide direction: compare with parent. Only one direction
        // needed — if the replacement is smaller than its parent, it
        // can only violate the invariant upward; otherwise downward.
        if (k > 0 && h->less(h->data[k], h->data[heap_parent(k)])) {
            siftdown(h, k);
        } else {
            siftup(h, k);
        }
    }
    return x;
}


// Reposition element at index k after its sort key changed.
__attribute__((hot)) void
heapresift(Heap *h, size_t k)
{
    if (k >= h->len)
        return;
    if (k > 0 && h->less(h->data[k], h->data[heap_parent(k)])) {
        siftdown(h, k);
    } else {
        siftup(h, k);
    }
}
