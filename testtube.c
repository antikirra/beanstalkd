#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void
cttest_tube_make_and_verify()
{
    Tube *t = make_tube("test");
    assertf(t != NULL, "make_tube returned NULL");
    assertf(strcmp(t->name, "test") == 0, "name must be 'test', got '%s'", t->name);
    assertf(t->refs == 0, "fresh tube must have refs=0, got %u", t->refs);
    assertf(t->ready.len == 0, "ready heap must be empty");
    assertf(t->delay.len == 0, "delay heap must be empty");
    assertf(t->stat.urgent_ct == 0, "stats must be zeroed");
    assertf(t->using_ct == 0, "using_ct must be 0");
    assertf(t->watching_ct == 0, "watching_ct must be 0");
    assertf(t->pause == 0, "pause must be 0");
    /* tube with refs=0 — dref frees it */
    tube_dref(t);
}

void
cttest_tube_refcount_lifecycle()
{
    Tube *t = make_tube("lifecycle");

    /* fresh tube: refs=0 */
    assertf(t->refs == 0, "start: refs must be 0");

    tube_iref(t);
    assertf(t->refs == 1, "after iref: refs must be 1");

    tube_iref(t);
    assertf(t->refs == 2, "after 2x iref: refs must be 2");

    tube_dref(t);
    assertf(t->refs == 1, "after dref: refs must be 1");

    /* this dref drops to 0 — tube is freed */
    tube_dref(t);
    /* t is now freed. Any access would be UAF. */
}

void
cttest_tube_assign_swap()
{
    Tube *a = NULL;
    Tube *t = make_tube("assign");

    TUBE_ASSIGN(a, t);
    assertf(a == t, "a must point to t after assign");
    assertf(t->refs == 1, "refs must be 1 after TUBE_ASSIGN from NULL");

    /* assign to NULL must dref, dropping refs to 0, freeing tube */
    TUBE_ASSIGN(a, NULL);
    assertf(a == NULL, "a must be NULL after assign NULL");
    /* t is freed here */
}

void
cttest_tube_assign_replace()
{
    Tube *a = NULL;
    Tube *t1 = make_tube("first");
    Tube *t2 = make_tube("second");

    TUBE_ASSIGN(a, t1);
    assertf(a == t1, "a == t1");
    assertf(t1->refs == 1, "t1 refs=1");

    /* replace: must dref t1, iref t2 */
    TUBE_ASSIGN(a, t2);
    assertf(a == t2, "a == t2");
    assertf(t2->refs == 1, "t2 refs=1");
    /* t1 was freed by TUBE_ASSIGN when refs dropped to 0 */

    TUBE_ASSIGN(a, NULL);
}

void
cttest_tube_find_or_make_dedup()
{
    ms_init(&tubes, NULL, NULL);

    Tube *t1 = tube_find_or_make("unique");
    assertf(t1, "must create");
    assertf(tubes.len == 1, "one tube in global set");

    Tube *t2 = tube_find_or_make("unique");
    assertf(t2 == t1, "must return SAME pointer, not create duplicate");
    assertf(tubes.len == 1, "still one tube — no duplicate");

    Tube *t3 = tube_find_or_make("other");
    assertf(t3 != t1, "different name must create different tube");
    assertf(tubes.len == 2, "two tubes now");

    /* cleanup: iref then dref to trigger free + removal from tubes */
    tube_iref(t1);
    tube_dref(t1);
    tube_iref(t3);
    tube_dref(t3);
    ms_clear(&tubes);
}

void
cttest_tube_hash_find_100()
{
    ms_init(&tubes, NULL, NULL);

    char name[32];
    Tube *ptrs[100];
    int i;

    for (i = 0; i < 100; i++) {
        snprintf(name, sizeof name, "tube-%03d", i);
        ptrs[i] = tube_find_or_make(name);
        assertf(ptrs[i], "create tube %d", i);
        tube_iref(ptrs[i]);
    }
    assertf(tubes.len == 100, "100 tubes");

    /* hash must find every one by name */
    for (i = 0; i < 100; i++) {
        snprintf(name, sizeof name, "tube-%03d", i);
        Tube *found = tube_find_name(name, strlen(name));
        assertf(found == ptrs[i], "hash must find EXACT pointer for tube %d", i);
    }

    /* nonexistent must return NULL */
    assertf(tube_find_name("nope", 4) == NULL, "nonexistent");
    assertf(tube_find_name("tube-999", 8) == NULL, "out of range");
    assertf(tube_find_name("", 0) == NULL, "empty string");

    for (i = 0; i < 100; i++)
        tube_dref(ptrs[i]);
    ms_clear(&tubes);
}

void
cttest_tube_name_truncation()
{
    /* name longer than MAX_TUBE_NAME_LEN must be truncated */
    char name[MAX_TUBE_NAME_LEN + 50];
    memset(name, 'X', sizeof name - 1);
    name[sizeof name - 1] = '\0';

    Tube *t = make_tube(name);
    assertf(t, "make_tube with long name");
    assertf(strlen(t->name) == MAX_TUBE_NAME_LEN - 1,
            "name must be truncated to %d, got %zu", MAX_TUBE_NAME_LEN - 1, strlen(t->name));
    assertf(t->name[MAX_TUBE_NAME_LEN - 1] == '\0', "must be NUL-terminated");
    tube_dref(t);
}

void
cttest_tube_find_in_watch_set()
{
    /* tube_find with Ms must use linear scan */
    Ms watch;
    ms_init(&watch, NULL, NULL);

    Tube *a = make_tube("alpha");
    Tube *b = make_tube("beta");
    tube_iref(a);
    tube_iref(b);
    ms_append(&watch, a);
    ms_append(&watch, b);

    assertf(tube_find(&watch, "alpha") == a, "must find alpha");
    assertf(tube_find(&watch, "beta") == b, "must find beta");
    assertf(tube_find(&watch, "gamma") == NULL, "must not find gamma");

    ms_clear(&watch);
    tube_dref(a);
    tube_dref(b);
}

// ─── Tube hash collision chain stress ───────────────────────
// Create 500 tubes, delete alternating ones, verify survivors.
// With 256 hash buckets, 500 tubes guarantees chain length ~2.
// This attacks collision resolution: if unlinking breaks a chain,
// a later lookup silently returns NULL for a tube that exists.

void
cttest_tube_hash_collision_stress_500()
{
    char name[32];
    Tube *tubes[500];

    // Create 500 tubes
    for (int i = 0; i < 500; i++) {
        snprintf(name, sizeof(name), "collision-%d", i);
        tubes[i] = tube_find_or_make(name);
        assertf(tubes[i] != NULL, "tube %d must be created", i);
        tube_iref(tubes[i]); // hold strong ref
    }

    // Verify all findable
    for (int i = 0; i < 500; i++) {
        snprintf(name, sizeof(name), "collision-%d", i);
        Tube *found = tube_find_name(name, strlen(name));
        assertf(found == tubes[i],
                "tube %d must be findable after creation", i);
    }

    // Delete every other tube (odd indices)
    for (int i = 1; i < 500; i += 2) {
        tube_dref(tubes[i]); // release our ref
        tubes[i] = NULL;
    }

    // Survivors (even indices) must still be findable
    for (int i = 0; i < 500; i += 2) {
        snprintf(name, sizeof(name), "collision-%d", i);
        Tube *found = tube_find_name(name, strlen(name));
        assertf(found == tubes[i],
                "surviving tube %d must be findable after alternating deletes", i);
    }

    // Deleted tubes must NOT be findable
    for (int i = 1; i < 500; i += 2) {
        snprintf(name, sizeof(name), "collision-%d", i);
        Tube *found = tube_find_name(name, strlen(name));
        assertf(found == NULL,
                "deleted tube %d must NOT be findable", i);
    }

    // Clean up survivors
    for (int i = 0; i < 500; i += 2) {
        tube_dref(tubes[i]);
    }
}
