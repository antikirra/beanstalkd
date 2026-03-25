#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct Ms tubes;

// Hash table for O(1) global tube lookup by name.
#define TUBE_HASH_SIZE 256
static Tube *tube_ht[TUBE_HASH_SIZE];

static uint
tube_hash(const char *name)
{
    uint h = 5381;
    while (*name)
        h = h * 33 + (unsigned char)*name++;
    return h % TUBE_HASH_SIZE;
}

static void
tube_ht_add(Tube *t)
{
    uint i = tube_hash(t->name);
    t->ht_next = tube_ht[i];
    tube_ht[i] = t;
}

static void
tube_ht_remove(Tube *t)
{
    uint i = tube_hash(t->name);
    Tube **slot = &tube_ht[i];
    while (*slot && *slot != t)
        slot = &(*slot)->ht_next;
    if (*slot)
        *slot = (*slot)->ht_next;
    t->ht_next = NULL;
}

// tube_find_name finds a tube by name in the global hash table. O(1).
Tube *
tube_find_name(const char *name)
{
    uint i = tube_hash(name);
    Tube *t = tube_ht[i];
    while (t) {
        if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0)
            return t;
        t = t->ht_next;
    }
    return NULL;
}


Tube *
make_tube(const char *name)
{
    Tube *t = new(Tube);
    if (!t)
        return NULL;

    strncpy(t->name, name, MAX_TUBE_NAME_LEN);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') {
        t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
        twarnx("truncating tube name");
    }

    t->ready.less = job_pri_less;
    t->delay.less = job_delay_less;
    t->ready.setpos = job_setpos;
    t->delay.setpos = job_setpos;

    Job j = {.tube = NULL};
    t->buried = j;
    t->buried.prev = t->buried.next = &t->buried;
    ms_init(&t->waiting_conns, NULL, NULL);

    return t;
}

static void
tube_free(Tube *t)
{
    tube_ht_remove(t);
    ms_remove(&tubes, t);
    free(t->ready.data);
    free(t->delay.data);
    ms_clear(&t->waiting_conns);
    free(t);
}

void
tube_dref(Tube *t)
{
    if (!t) return;
    if (t->refs < 1) {
        twarnx("refs is zero for tube: %s", t->name);
        return;
    }

    --t->refs;
    if (t->refs < 1)
        tube_free(t);
}

void
tube_iref(Tube *t)
{
    if (!t) return;
    ++t->refs;
}

static Tube *
make_and_insert_tube(const char *name)
{
    int r;
    Tube *t = NULL;

    t = make_tube(name);
    if (!t)
        return NULL;

    /* We want this global tube list to behave like "weak" refs, so don't
     * increment the ref count. */
    r = ms_append(&tubes, t);
    if (!r)
        return tube_dref(t), (Tube *) 0;

    tube_ht_add(t);
    return t;
}

Tube *
tube_find(Ms *tubeset, const char *name)
{
    size_t i;

    for (i = 0; i < tubeset->len; i++) {
        Tube *t = tubeset->items[i];
        if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0)
            return t;
    }
    return NULL;
}

Tube *
tube_find_or_make(const char *name)
{
    Tube *t = tube_find_name(name);
    if (t)
        return t;
    return make_and_insert_tube(name);
}
