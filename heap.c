#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


static void
set(Heap *h, size_t k, void *x)
{
    h->data[k] = x;
    h->setpos(x, k);
}


// Siftdown using hole technique (half the setpos calls vs swap).
__attribute__((hot)) static void
siftdown(Heap *h, size_t k)
{
    if (k == 0) return;

    void *x = h->data[k];
    while (k > 0) {
        size_t p = (k - 1) / 2;
        if (h->less(h->data[p], x))
            break;
        // Shift parent down into hole.
        h->data[k] = h->data[p];
        h->setpos(h->data[p], k);
        k = p;
    }
    // Place element at final position.
    set(h, k, x);
}


// Siftup using hole technique.
__attribute__((hot)) static void
siftup(Heap *h, size_t k)
{
    void *x = h->data[k];
    for (;;) {
        size_t l = k * 2 + 1;
        size_t r = k * 2 + 2;

        // Find the smallest child, comparing against saved element x.
        size_t s = k;
        if (l < h->len && h->less(h->data[l], x)) s = l;
        // Right child must beat the current best (which is either x or data[l]).
        if (r < h->len) {
            void *best = (s == k) ? x : h->data[s];
            if (h->less(h->data[r], best)) s = r;
        }

        if (s == k)
            break;

        // Shift child up into hole.
        h->data[k] = h->data[s];
        h->setpos(h->data[s], k);
        k = s;
    }
    // Place element at final position.
    set(h, k, x);
}


// Heapinsert inserts x into heap h according to h->less.
// It returns 1 on success, otherwise 0.
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
        // Only one direction needed: compare with parent to decide.
        if (k > 0 && h->less(h->data[k], h->data[(k - 1) / 2])) {
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
    if (k > 0 && h->less(h->data[k], h->data[(k - 1) / 2])) {
        siftdown(h, k);
    } else {
        siftup(h, k);
    }
}
