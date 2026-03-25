#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>

void
cttest_ms_take_single_element()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    int x = 42;
    ms_append(&a, &x);
    assertf(a.len == 1, "len must be 1");

    void *got = ms_take(&a);
    assertf(got == &x, "must get x back");
    assertf(a.len == 0, "must be empty after take");

    /* second take from empty — must not crash */
    got = ms_take(&a);
    assertf(got == NULL, "take from empty must be NULL");

    free(a.items);
}

void
cttest_ms_take_empty()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    void *got = ms_take(&a);
    assertf(got == NULL, "take from never-filled ms must be NULL");
}

void
cttest_ms_remove_nonexistent()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    int x = 1, y = 2;
    ms_append(&a, &x);

    int r = ms_remove(&a, &y);
    assertf(!r, "removing non-member must return 0");
    assertf(a.len == 1, "len must not change");

    free(a.items);
}

void
cttest_ms_contains_empty()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    int x = 1;
    assertf(!ms_contains(&a, &x), "empty ms must not contain anything");
}

void
cttest_ms_stress_append_remove()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    int items[1000];
    int i;

    /* fill */
    for (i = 0; i < 1000; i++) {
        items[i] = i;
        int r = ms_append(&a, &items[i]);
        assertf(r, "append %d must succeed", i);
    }
    assertf(a.len == 1000, "must have 1000 items");

    /* remove every other */
    for (i = 0; i < 1000; i += 2) {
        int r = ms_remove(&a, &items[i]);
        assertf(r, "remove %d must succeed", i);
    }
    assertf(a.len == 500, "must have 500 items after removing evens");

    /* verify odds remain */
    for (i = 1; i < 1000; i += 2) {
        assertf(ms_contains(&a, &items[i]), "odd %d must still be present", i);
    }

    /* verify evens gone */
    for (i = 0; i < 1000; i += 2) {
        assertf(!ms_contains(&a, &items[i]), "even %d must be gone", i);
    }

    ms_clear(&a);
    free(a.items);
}

static int callback_count = 0;

static void
count_insert(Ms *a, void *item, size_t i)
{
    (void)a; (void)item; (void)i;
    callback_count++;
}

static void
count_remove(Ms *a, void *item, size_t i)
{
    (void)a; (void)item; (void)i;
    callback_count--;
}

void
cttest_ms_callbacks()
{
    Ms a;
    callback_count = 0;
    ms_init(&a, count_insert, count_remove);

    int x = 1, y = 2;
    ms_append(&a, &x);
    assertf(callback_count == 1, "insert callback must fire");

    ms_append(&a, &y);
    assertf(callback_count == 2, "insert callback fires again");

    ms_remove(&a, &x);
    assertf(callback_count == 1, "remove callback must fire");

    ms_clear(&a);
    assertf(callback_count == 0, "clear must call remove for all");
    free(a.items);
}
