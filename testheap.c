#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "ct/ct.h"


void
cttest_heap_insert_one()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j = make_job(1, 0, 1, 0, 0);
    assertf(j, "allocate job");

    heapinsert(&h, j);
    assertf(h.len == 1, "h should contain one item.");
    assertf(j->heap_index == 0, "should match");

    assert(heapremove(&h, 0));
    job_free(j);
    free(h.data);
}

void
cttest_heap_insert_and_remove_one()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    Job *j1 = make_job(1, 0, 1, 0, 0);
    assertf(j1, "allocate job");

    int r = heapinsert(&h, j1);
    assertf(r, "insert should succeed");

    Job *got = heapremove(&h, 0);
    assertf(got == j1, "j1 should come back out");
    assertf(h.len == 0, "h should be empty.");

    free(h.data);
    job_free(j1);
}

void
cttest_heap_priority()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    Job *j, *j1, *j2, *j3;

    j1 = make_job(1, 0, 1, 0, 0);
    j2 = make_job(2, 0, 1, 0, 0);
    j3 = make_job(3, 0, 1, 0, 0);
    assertf(j1, "allocate job");
    assertf(j2, "allocate job");
    assertf(j3, "allocate job");

    int r = heapinsert(&h, j2);
    assertf(r, "insert should succeed");
    assertf(j2->heap_index == 0, "should match");

    r = heapinsert(&h, j3);
    assertf(r, "insert should succeed");
    assertf(j2->heap_index == 0, "should match");
    assertf(j3->heap_index == 1, "should match");

    r = heapinsert(&h, j1);
    assertf(r, "insert should succeed");
    assertf(j1->heap_index == 0, "should match");
    assertf(j2->heap_index == 2, "should match");
    assertf(j3->heap_index == 1, "should match");

    j = heapremove(&h, 0);
    assertf(j == j1, "j1 should come out first.");
    assertf(j2->heap_index == 0, "should match");
    assertf(j3->heap_index == 1, "should match");

    j = heapremove(&h, 0);
    assertf(j == j2, "j2 should come out second.");
    assertf(j3->heap_index == 0, "should match");

    j = heapremove(&h, 0);
    assertf(j == j3, "j3 should come out third.");

    free(h.data);
    job_free(j1);
    job_free(j2);
    job_free(j3);
}

void
cttest_heap_fifo_property()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    Job *j, *j3a, *j3b, *j3c;

    j3a = make_job(3, 0, 1, 0, 0);
    j3b = make_job(3, 0, 1, 0, 0);
    j3c = make_job(3, 0, 1, 0, 0);
    assertf(j3a, "allocate job");
    assertf(j3b, "allocate job");
    assertf(j3c, "allocate job");

    int r = heapinsert(&h, j3a);
    assertf(r, "insert should succeed");
    assertf(h.data[0] == j3a, "j3a should be in pos 0");
    assertf(j3a->heap_index == 0, "should match");

    r = heapinsert(&h, j3b);
    assertf(r, "insert should succeed");
    assertf(h.data[1] == j3b, "j3b should be in pos 1");
    assertf(j3a->heap_index == 0, "should match");
    assertf(j3b->heap_index == 1, "should match");

    r = heapinsert(&h, j3c);
    assertf(r, "insert should succeed");
    assertf(h.data[2] == j3c, "j3c should be in pos 2");
    assertf(j3a->heap_index == 0, "should match");
    assertf(j3b->heap_index == 1, "should match");
    assertf(j3c->heap_index == 2, "should match");

    j = heapremove(&h, 0);
    assertf(j == j3a, "j3a should come out first.");
    assertf(j3b->heap_index == 0, "should match");
    assertf(j3c->heap_index == 1, "should match");

    j = heapremove(&h, 0);
    assertf(j == j3b, "j3b should come out second.");
    assertf(j3c->heap_index == 0, "should match");

    j = heapremove(&h, 0);
    assertf(j == j3c, "j3c should come out third.");

    free(h.data);
    job_free(j3a);
    job_free(j3b);
    job_free(j3c);
}

void
cttest_heap_many_jobs()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int n = 20;
    Job *j;

    int i;
    for (i = 0; i < n; i++) {
        j = make_job(1 + rand() % 8192, 0, 1, 0, 0);
        assertf(j, "allocation");
        int r = heapinsert(&h, j);
        assertf(r, "heapinsert");
    }

    uint last_pri = 0;
    for (i = 0; i < n; i++) {
        j = heapremove(&h, 0);
        assertf(j->r.pri >= last_pri, "should come out in order");
        last_pri = j->r.pri;
        assert(j);
        job_free(j);
    }
    free(h.data);
}

typedef struct { int64 key; size_t pos; } HeapTestItem;

static int
heap_test_less(void *a, void *b)
{
    return ((HeapTestItem *)a)->key < ((HeapTestItem *)b)->key;
}

static void
heap_test_setpos(void *x, size_t i)
{
    ((HeapTestItem *)x)->pos = i;
}

static void
heap_test_assert_valid(Heap *h)
{
    size_t i;
    for (i = 0; i < h->len; i++) {
        HeapTestItem *it = h->data[i];
        assertf(it->pos == i, "pos hint must match slot");
        if (i > 0) {
            HeapTestItem *p = h->data[(i - 1) >> 2];
            assertf(p->key <= it->key, "min-heap invariant violated");
        }
    }
}

// Regression test for upstream beanstalkd PR #670: removing the last
// element when its key equals its parent's must not leave the removed
// element in the live heap (upstream used k <= h->len, causing a
// use-after-free once the caller freed the returned element).
void
cttest_heap_remove_last_equal()
{
    Heap h = {
        .less = heap_test_less,
        .setpos = heap_test_setpos,
    };

    HeapTestItem a = { .key = 50 };
    HeapTestItem b = { .key = 100 };
    HeapTestItem c = { .key = 100 };
    HeapTestItem d = { .key = 100 };

    heapinsert(&h, &a);
    heapinsert(&h, &b);
    heapinsert(&h, &c);
    heapinsert(&h, &d);

    void *got = heapremove(&h, h.len - 1);
    assertf(got == &d, "last element should come back");

    size_t i;
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    assertf(h.len == 3, "len should drop to 3 after one removal");
    heap_test_assert_valid(&h);

    /* remove the remaining elements in an order that stresses equal
     * keys, checking ownership and the invariant after each removal */
    got = heapremove(&h, 0);
    assertf(got == &a, "min should come out first");
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    heap_test_assert_valid(&h);

    got = heapremove(&h, h.len - 1);
    assertf(got == &b || got == &c, "an equal-key item should come back");
    for (i = 0; i < h.len; i++) {
        assertf(h.data[i] != got, "removed item must not remain in the heap");
    }
    heap_test_assert_valid(&h);

    got = heapremove(&h, 0);
    assertf(got == &b || got == &c, "the last equal-key item should come back");
    assertf(h.len == 0, "heap should be empty");

    /* Same key everywhere: in a binary heap this is the exact setup
     * whose unconditional siftdown dragged the removed element back
     * into the live heap. In our 4-ary heap the last element's parent
     * is slot 0, so every removal from the tail has an equal parent. */
    HeapTestItem items[8];
    int j;
    for (j = 0; j < 8; j++) {
        items[j].key = 100;
        heapinsert(&h, &items[j]);
    }
    while (h.len > 0) {
        got = heapremove(&h, h.len - 1);
        for (i = 0; i < h.len; i++) {
            assertf(h.data[i] != got, "removed item must not remain in the heap");
        }
        heap_test_assert_valid(&h);
    }

    free(h.data);
}

void
cttest_heap_remove_k()
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    const int n = 50;
    const int mid = 25;

    int c, i;
    for (c = 0; c < 50; c++) {
        for (i = 0; i < n; i++) {
            Job *j = make_job(1 + rand() % 8192, 0, 1, 0, 0);
            assertf(j, "allocation");
            int r = heapinsert(&h, j);
            assertf(r, "heapinsert");
        }

        /* remove one from the middle */
        Job *j0 = heapremove(&h, mid);
        assertf(j0, "j0 should not be NULL");
        job_free(j0);

        /* now make sure the rest are still a valid heap */
        uint last_pri = 0;
        for (i = 1; i < n; i++) {
            Job *j = heapremove(&h, 0);
            assertf(j->r.pri >= last_pri, "should come out in order");
            last_pri = j->r.pri;
            assertf(j, "j should not be NULL");
            job_free(j);
        }
    }
    free(h.data);
}

void
ctbench_heap_insert(int n)
{
    Job **j = calloc(n, sizeof *j);
    int i;
    for (i = 0; i < n; i++) {
        j[i] = make_job(1, 0, 1, 0, 0);
        assert(j[i]);
        j[i]->r.pri = -j[i]->r.id;
    }
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };

    ctresettimer();
    for (i = 0; i < n; i++) {
        heapinsert(&h, j[i]);
    }
    ctstoptimer();

    for (i = 0; i < n; i++)
        job_free(heapremove(&h, 0));
    free(h.data);
    free(j);
}

void
ctbench_heap_remove(int n)
{
    Heap h = {
        .less = job_pri_less,
        .setpos = job_setpos,
    };
    int i;
    for (i = 0; i < n; i++) {
        Job *j = make_job(1, 0, 1, 0, 0);
        assertf(j, "allocate job");
        heapinsert(&h, j);
    }
    assertf(n > 0, "n must be positive");
    Job **jj = calloc((unsigned)n, sizeof(Job *)); // temp storage to deallocate jobs later

    ctresettimer();
    for (i = 0; i < n; i++) {
        jj[i] = (Job *)heapremove(&h, 0);
    }
    ctstoptimer();

    free(h.data);
    for (i = 0; i < n; i++)
        job_free(jj[i]);
    free(jj);
}
