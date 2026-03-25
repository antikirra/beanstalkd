#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>

void
cttest_heap_remove_empty()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    void *got = heapremove(&h, 0);
    assertf(got == NULL, "remove from empty heap must return NULL");
}

void
cttest_heap_remove_out_of_bounds()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j = make_job(1, 0, 1, 0, 0);
    heapinsert(&h, j);

    void *got = heapremove(&h, 999);
    assertf(got == NULL, "out-of-bounds remove must return NULL");

    got = heapremove(&h, 0);
    assertf(got == j, "valid remove must work");

    free(h.data);
    job_free(j);
}

void
cttest_heap_remove_last()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *a = make_job(1, 0, 1, 0, 0);
    Job *b = make_job(2, 0, 1, 0, 0);
    Job *c = make_job(3, 0, 1, 0, 0);
    heapinsert(&h, a);
    heapinsert(&h, b);
    heapinsert(&h, c);

    /* remove last element by index */
    Job *got = heapremove(&h, h.len - 1);
    assertf(got != NULL, "removing last must succeed");
    assertf(h.len == 2, "heap must have 2 elements");

    /* remaining must still be valid heap */
    Job *first = heapremove(&h, 0);
    Job *second = heapremove(&h, 0);
    assertf(first->r.pri <= second->r.pri, "heap order must hold");

    free(h.data);
    job_free(a);
    job_free(b);
    job_free(c);
}

void
cttest_heap_delay_ordering()
{
    Heap h = {
        .less = job_delay_less,
        .setpos = job_setpos,
    };

    Job *a = make_job(1, 0, 1, 0, 0);
    Job *b = make_job(1, 0, 1, 0, 0);
    Job *c = make_job(1, 0, 1, 0, 0);
    a->r.deadline_at = 3000;
    b->r.deadline_at = 1000;
    c->r.deadline_at = 2000;

    heapinsert(&h, a);
    heapinsert(&h, b);
    heapinsert(&h, c);

    Job *first = heapremove(&h, 0);
    Job *second = heapremove(&h, 0);
    Job *third = heapremove(&h, 0);

    assertf(first == b, "earliest deadline must come first");
    assertf(second == c, "middle deadline second");
    assertf(third == a, "latest deadline last");

    free(h.data);
    job_free(a);
    job_free(b);
    job_free(c);
}
