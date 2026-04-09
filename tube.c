#include "dat.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

struct Ms tubes;

// Hash table for O(1) global tube lookup by name.
#define TUBE_HASH_SIZE 4096
static Tube *tube_ht[TUBE_HASH_SIZE];

// tube_name_hash returns a finalized DJB2 hash of the tube name.
// Used for tube hash table lookup.
// Finalizer mixes high bits into low bits for better distribution
// under power-of-2 modulo (TUBE_HASH_SIZE).
uint
tube_name_hash(const char *name)
{
    uint h = 5381;
    while (*name)
        h = h * 33 + (unsigned char)*name++;
    // Finalizer: mix high bits down for better low-bit distribution.
    h ^= h >> 16;
    h *= 0x45d9f3b;
    h ^= h >> 16;
    return h;
}

static void
tube_ht_add(Tube *t)
{
    uint i = t->name_hash % TUBE_HASH_SIZE;
    t->ht_next = tube_ht[i];
    tube_ht[i] = t;
}

static void
tube_ht_remove(Tube *t)
{
    uint i = t->name_hash % TUBE_HASH_SIZE;
    Tube **slot = &tube_ht[i];
    while (*slot && *slot != t)
        slot = &(*slot)->ht_next;
    if (*slot)
        *slot = (*slot)->ht_next;
    t->ht_next = NULL;
}

// tube_find_name_h finds a tube by name with a precomputed hash.
// Hash-first filter skips memcmp on non-matching chain entries.
Tube *
tube_find_name_h(const char *name, size_t len, uint h)
{
    Tube *t = tube_ht[h % TUBE_HASH_SIZE];
    while (t) {
        if (t->name_hash == h && t->name_len == len
            && memcmp(t->name, name, len) == 0)
            return t;
        if (t->ht_next) __builtin_prefetch(t->ht_next, 0, 1);
        t = t->ht_next;
    }
    return NULL;
}

// tube_find_name finds a tube by name in the global hash table. O(1).
Tube *
tube_find_name(const char *name, size_t len)
{
    return tube_find_name_h(name, len, tube_name_hash(name));
}


Tube *
make_tube(const char *name)
{
    Tube *t = new(Tube);
    if (!t)
        return NULL;

    size_t nlen = strlen(name);
    if (nlen >= MAX_TUBE_NAME_LEN) {
        twarnx("truncating tube name");
        nlen = MAX_TUBE_NAME_LEN - 1;
    }
    memcpy(t->name, name, nlen);
    t->name[nlen] = '\0';
    t->name_len = nlen;
    t->name_hash = tube_name_hash(t->name);

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

void
tube_free(Tube *t)
{
    prot_remove_tube(t);
    tube_ht_remove(t);
    ms_remove(&tubes, t);
    free(t->ready.data);
    free(t->delay.data);
    ms_clear(&t->waiting_conns);
    free(t);
}

// tube_dref is now static inline in dat.h
// tube_free is called from inline tube_dref when refs reaches 0.
// tube_iref is now static inline in dat.h

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
    if (!r) {
        ms_clear(&t->waiting_conns);
        free(t->ready.data);
        free(t->delay.data);
        free(t);
        return NULL;
    }

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
tube_find_or_make_n(const char *name, size_t len)
{
    Tube *t = tube_find_name(name, len);
    if (t)
        return t;
    return make_and_insert_tube(name);
}

Tube *
tube_find_or_make(const char *name)
{
    return tube_find_or_make_n(name, strlen(name));
}
