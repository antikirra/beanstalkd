#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static Tube *dtube;

/* --- nanoseconds() --- */

void
cttest_nanoseconds_positive()
{
    int64 t = nanoseconds();
    assertf(t > 0, "nanoseconds must be positive, got %lld", (long long)t);
}

void
cttest_nanoseconds_monotonic()
{
    int64 t1 = nanoseconds();
    int64 t2 = nanoseconds();
    assertf(t2 >= t1, "must be monotonic: %lld >= %lld",
            (long long)t2, (long long)t1);
}

void
cttest_nanoseconds_advances()
{
    int64 t1 = nanoseconds();
    // CLOCK_MONOTONIC_COARSE has jiffy resolution (~1-4ms).
    // Sleep 5ms to guarantee crossing a jiffy boundary.
    usleep(5000);
    int64 t2 = nanoseconds();
    assertf(t2 > t1, "time must advance after work: %lld > %lld",
            (long long)t2, (long long)t1);
}

/* --- allocate_job edge cases --- */

void
cttest_allocate_job_zero_body()
{
    Job *j = allocate_job(0);
    assertf(j, "allocate with body_size=0 must succeed");
    assertf(j->r.body_size == 0, "body_size must be 0");
    assertf(j->body == (char*)j + sizeof(Job), "body must point right after Job struct");
    free(j); /* allocate_job doesn't store in hash, so free directly */
}

void
cttest_allocate_job_large_body()
{
    int sz = 1024 * 1024; /* 1MB */
    Job *j = allocate_job(sz);
    assertf(j, "1MB job must allocate");
    assertf(j->r.body_size == sz, "body_size must match");
    /* write to entire body — would crash if allocation too small */
    memset(j->body, 0xAB, sz);
    assertf((unsigned char)j->body[0] == 0xAB, "first byte");
    assertf((unsigned char)j->body[sz-1] == 0xAB, "last byte");
    free(j);
}

/* --- make_job_with_id --- */

void
cttest_make_job_explicit_id()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    uint64 id = 77777;
    Job *j = make_job_with_id(1, 0, 1, 0, dtube, id);
    assertf(j, "must allocate");
    assertf(j->r.id == id, "id must be %llu, got %llu",
            (unsigned long long)id, (unsigned long long)j->r.id);

    /* must be findable */
    assertf(job_find(id) == j, "must find by explicit id");

    job_free(j);
    assertf(job_find(id) == NULL, "must not find after free");
}

void
cttest_make_job_auto_id_increments()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job *a = make_job(1, 0, 1, 0, dtube);
    Job *b = make_job(1, 0, 1, 0, dtube);
    assertf(b->r.id == a->r.id + 1, "auto IDs must be sequential: %llu, %llu",
            (unsigned long long)a->r.id, (unsigned long long)b->r.id);
    job_free(a);
    job_free(b);
}

/* --- job_list double insert --- */

void
cttest_job_list_double_insert_noop()
{
    TUBE_ASSIGN(dtube, make_tube("default"));
    Job head;
    job_list_reset(&head);

    Job *j = make_job(1, 0, 1, 0, dtube);
    job_list_insert(&head, j);

    /* second insert of same job must be no-op (already in a list) */
    job_list_insert(&head, j);

    /* remove once — must succeed */
    Job *got = job_list_remove(j);
    assertf(got == j, "first remove must work");

    /* list must be empty now — second insert was no-op */
    assertf(job_list_is_empty(&head), "list must be empty — double insert was no-op");

    job_free(j);
}

/* --- fmtalloc --- */

void
cttest_fmtalloc_empty()
{
    char *s = fmtalloc("%s", "");
    assertf(s, "must not return NULL");
    assertf(strlen(s) == 0, "must be empty string");
    assertf(s[0] == '\0', "must be NUL terminated");
    free(s);
}

void
cttest_fmtalloc_long()
{
    char *s = fmtalloc("%0*d", 500, 0);
    assertf(s, "must not return NULL");
    assertf(strlen(s) == 500, "must be 500 chars, got %zu", strlen(s));
    free(s);
}

void
cttest_fmtalloc_format()
{
    char *s = fmtalloc("x=%d y=%s", 42, "hello");
    assertf(s, "must not return NULL");
    assertf(strcmp(s, "x=42 y=hello") == 0, "format mismatch: '%s'", s);
    free(s);
}

/* --- zalloc --- */

void
cttest_zalloc_zeroed()
{
    int *p = zalloc(sizeof(int) * 10);
    assertf(p, "must allocate");
    for (int i = 0; i < 10; i++) {
        assertf(p[i] == 0, "zalloc must zero memory, index %d = %d", i, p[i]);
    }
    free(p);
}

/* --- conn_less ordering --- */

void
cttest_conn_less_ordering()
{
    Conn a = { .tickat = 100 };
    Conn b = { .tickat = 200 };

    assertf(conn_less(&a, &b), "earlier tickat must be less");
    assertf(!conn_less(&b, &a), "later must not be less");
    assertf(!conn_less(&a, &a), "equal must not be less");
}

/* --- heap with conn_less for timeout scheduling --- */

void
cttest_heap_conn_timeout_ordering()
{
    Heap h = {
        .less = conn_less,
        .setpos = conn_setpos,
    };

    Conn conns[5];
    memset(conns, 0, sizeof conns);
    conns[0].tickat = 500;
    conns[1].tickat = 100;
    conns[2].tickat = 300;
    conns[3].tickat = 50;
    conns[4].tickat = 200;

    int i;
    for (i = 0; i < 5; i++)
        heapinsert(&h, &conns[i]);

    /* must come out in tickat order: 50, 100, 200, 300, 500 */
    int64 expected[] = {50, 100, 200, 300, 500};
    for (i = 0; i < 5; i++) {
        Conn *c = heapremove(&h, 0);
        assertf(c, "remove %d", i);
        assertf(c->tickat == expected[i],
                "conn %d: expected tickat=%lld, got %lld",
                i, (long long)expected[i], (long long)c->tickat);
    }
    free(h.data);
}

/* --- ms_take after alternating insert/take --- */

void
cttest_ms_take_interleaved()
{
    Ms a;
    ms_init(&a, NULL, NULL);

    int x = 1, y = 2, z = 3;

    ms_append(&a, &x);
    void *got = ms_take(&a);
    assertf(got == &x, "must get x");
    assertf(a.len == 0, "must be empty");

    ms_append(&a, &y);
    ms_append(&a, &z);
    got = ms_take(&a);
    assertf(got != NULL, "must get something");
    got = ms_take(&a);
    assertf(got != NULL, "must get second");
    got = ms_take(&a);
    assertf(got == NULL, "empty after 2 takes");

    free(a.items);
}

/* --- tube_find_name returns NULL for every possible bad input --- */

void
cttest_tube_find_name_adversarial()
{
    ms_init(&tubes, NULL, NULL);

    /* empty hash table */
    assertf(tube_find_name("anything", 8) == NULL, "empty table must return NULL");
    assertf(tube_find_name("", 0) == NULL, "empty string must return NULL");

    /* create one tube, search for variations */
    Tube *t = tube_find_or_make("exact");
    tube_iref(t);

    assertf(tube_find_name("exact", 5) == t, "must find exact");
    assertf(tube_find_name("EXACT", 5) == NULL, "case sensitive");
    assertf(tube_find_name("exact ", 6) == NULL, "trailing space");
    assertf(tube_find_name(" exact", 6) == NULL, "leading space");
    assertf(tube_find_name("exac", 4) == NULL, "prefix");
    assertf(tube_find_name("exactt", 6) == NULL, "suffix");

    tube_dref(t);
    ms_clear(&tubes);
}

/* --- primes[] table sanity --- */

// Deterministic Miller-Rabin for n < 3.3e18 using the 12-witness set
// {2,3,5,7,11,13,17,19,23,29,31,37}. Covers all primes in the table
// (max = 1.73e18 on LP64).

static uint64_t
mulmod64(uint64_t a, uint64_t b, uint64_t m)
{
    return (uint64_t)(((__uint128_t)a * b) % m);
}

static uint64_t
powmod(uint64_t base, uint64_t exp, uint64_t mod)
{
    uint64_t res = 1;
    base %= mod;
    while (exp) {
        if (exp & 1)
            res = mulmod64(res, base, mod);
        base = mulmod64(base, base, mod);
        exp >>= 1;
    }
    return res;
}

static int
mr_composite(uint64_t a, uint64_t d, int s, uint64_t n)
{
    uint64_t x = powmod(a, d, n);
    if (x == 1 || x == n - 1)
        return 0;
    for (int r = 0; r < s - 1; r++) {
        x = mulmod64(x, x, n);
        if (x == n - 1)
            return 0;
    }
    return 1;
}

static int
is_prime_u64(uint64_t n)
{
    if (n < 2) return 0;
    if (n < 4) return 1;
    if ((n & 1) == 0) return 0;

    uint64_t d = n - 1;
    int s = 0;
    while ((d & 1) == 0) { d >>= 1; s++; }

    static const uint64_t witnesses[] = {
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37
    };
    for (size_t i = 0; i < sizeof(witnesses) / sizeof(witnesses[0]); i++) {
        uint64_t a = witnesses[i];
        if (a >= n) continue;
        if (mr_composite(a, d, s, n))
            return 0;
    }
    return 1;
}

// Hostile: every entry in primes[] must be prime. A typo in the hand-written
// table silently degrades hash-table distribution (collisions cluster around
// composite "pseudo-primes"), invisible until production latency spikes.
void
cttest_primes_all_prime()
{
    assertf(primes_len > 0, "primes_len must be > 0, got %zu", primes_len);
    for (size_t i = 0; i < primes_len; i++) {
        assertf(is_prime_u64((uint64_t)primes[i]),
                "primes[%zu] = %zu is NOT prime", i, primes[i]);
    }
}

// Strictly monotone: the rehash code picks the next bucket count by
// stepping forward in this list. A non-increasing entry would make
// upscale pick a smaller table — silent performance cliff.
void
cttest_primes_strictly_monotone()
{
    for (size_t i = 1; i < primes_len; i++) {
        assertf(primes[i] > primes[i - 1],
                "primes not strictly monotone at index %zu: "
                "prev=%zu curr=%zu",
                i, primes[i - 1], primes[i]);
    }
}

// Growth invariant: each step at least ~2x the previous.
// The rehash upscales at 4x load; if two adjacent primes were closer
// than 2x, we'd rehash without gaining headroom.
//
// The bound is computed in __uint128_t so a future entry close to
// 2^63 does not silently overflow the check into "always passes".
void
cttest_primes_doubling()
{
    for (size_t i = 1; i < primes_len; i++) {
        __uint128_t prev = (__uint128_t)primes[i - 1];
        __uint128_t bound = prev * 2 - prev / 8;   // ~1.875x
        assertf((__uint128_t)primes[i] >= bound,
                "primes[%zu]=%zu not >= ~1.875x primes[%zu]=%zu",
                i, primes[i], i - 1, primes[i - 1]);
    }
}
