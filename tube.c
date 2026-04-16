#include "dat.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

struct Ms tubes;

// Hash table for O(1) global tube lookup by name.
#define TUBE_HASH_SIZE 4096
static Tube *tube_ht[TUBE_HASH_SIZE];

// wyhash (Wang Yi, public domain: github.com/wangyi-fudan/wyhash) —
// v4 final. Chosen over DJB2 for better avalanche on short strings
// (tube names are ≤200 bytes), faster per-byte throughput on
// modern CPUs, and uniform low-bit distribution for power-of-2
// bucket moduli. CRC32C is NOT suitable here: it targets random
// corruption detection, not uniform hashing.
//
// Seed is fixed (WY_SEED) to preserve hash stability across server
// restarts. Randomised seeding for anti-adversarial protection can
// be layered later without ABI changes — the API takes (name, len).

#define WY_P0 0xa0761d6478bd642fULL
#define WY_P1 0xe7037ed1a0b428dbULL
#define WY_P2 0x8ebc6af09c88c6e3ULL
#define WY_P3 0x589965cc75374cc3ULL
#define WY_SEED WY_P3

static inline uint64_t
wy_mum(uint64_t a, uint64_t b)
{
    __uint128_t r = (__uint128_t)a * b;
    return (uint64_t)r ^ (uint64_t)(r >> 64);
}

static inline uint64_t
wy_r8(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, sizeof v);
    return v;
}

static inline uint64_t
wy_r4(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, sizeof v);
    return v;
}

static inline uint64_t
wy_r3(const uint8_t *p, size_t k)
{
    return (((uint64_t)p[0]) << 16) | (((uint64_t)p[k >> 1]) << 8) | p[k - 1];
}

static uint64_t
wyhash64(const void *key, size_t len, uint64_t seed)
{
    const uint8_t *p = (const uint8_t *)key;
    seed ^= wy_mum(seed ^ WY_P0, WY_P1);
    uint64_t a, b;

    if (len <= 16) {
        if (len >= 4) {
            a = (wy_r4(p) << 32) | wy_r4(p + ((len >> 3) << 2));
            b = (wy_r4(p + len - 4) << 32)
              | wy_r4(p + len - 4 - ((len >> 3) << 2));
        } else if (len > 0) {
            a = wy_r3(p, len);
            b = 0;
        } else {
            a = b = 0;
        }
    } else {
        size_t i = len;
        if (i >= 48) {
            uint64_t s1 = seed, s2 = seed;
            do {
                seed = wy_mum(wy_r8(p)      ^ WY_P0, wy_r8(p + 8)  ^ seed);
                s1   = wy_mum(wy_r8(p + 16) ^ WY_P1, wy_r8(p + 24) ^ s1);
                s2   = wy_mum(wy_r8(p + 32) ^ WY_P2, wy_r8(p + 40) ^ s2);
                p += 48;
                i -= 48;
            } while (i >= 48);
            seed ^= s1 ^ s2;
        }
        while (i > 16) {
            seed = wy_mum(wy_r8(p) ^ WY_P0, wy_r8(p + 8) ^ seed);
            i -= 16;
            p += 16;
        }
        a = wy_r8(p + i - 16);
        b = wy_r8(p + i - 8);
    }

    return wy_mum(WY_P0 ^ len, wy_mum(a ^ WY_P0, b ^ seed));
}

// tube_name_hash_n: length-aware variant to avoid strlen() at hot callers
// that already know the name length (tube_find_name, dispatch_cmd).
uint
tube_name_hash_n(const char *name, size_t len)
{
    uint64_t h = wyhash64(name, len, WY_SEED);
    return (uint)(h ^ (h >> 32));
}

// tube_name_hash: zero-terminated convenience wrapper.
uint
tube_name_hash(const char *name)
{
    return tube_name_hash_n(name, strlen(name));
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
    return tube_find_name_h(name, len, tube_name_hash_n(name, len));
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
    t->name_hash = tube_name_hash_n(t->name, t->name_len);

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
