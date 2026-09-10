#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <time.h>

static Tube *dtube;

/* --- nanoseconds() --- */

// Independent reference clock. nanoseconds() reads
// CLOCK_MONOTONIC_COARSE, which shares its epoch with CLOCK_MONOTONIC
// and only lags it by at most one jiffy — so the two must agree far
// more closely than any unit, scale or origin mistake would allow.
// Reading the reference here (not inside time.c) keeps it an oracle:
// the test never learns its expectation from the code under attack.
static int64
mono_ref_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((int64)ts.tv_sec) * 1000000000LL + (int64)ts.tv_nsec;
}

// One jiffy is at most 10ms at HZ=100; 100ms leaves 10x headroom for
// scheduling noise while staying 1000x tighter than a microsecond-unit
// mistake and astronomically tighter than a constant.
#define CLOCK_SLACK_NS 100000000LL

void
cttest_nanoseconds_positive()
{
    int64 t = nanoseconds();
    int64 ref = mono_ref_ns();
    assertf(t > 0, "nanoseconds must be positive, got %lld", (long long)t);
    // Sign alone pins nothing: microseconds, milliseconds, a tick count
    // and a hard-coded constant are all positive. The value must be a
    // NANOSECOND count on the monotonic epoch.
    assertf(t > ref - CLOCK_SLACK_NS && t < ref + CLOCK_SLACK_NS,
            "nanoseconds() must report nanoseconds on the monotonic epoch: "
            "got %lld, reference %lld, slack %lld",
            (long long)t, (long long)ref, (long long)CLOCK_SLACK_NS);
}

void
cttest_nanoseconds_monotonic()
{
    int64 ref1 = mono_ref_ns();
    int64 t1 = nanoseconds();
    int64 t2 = nanoseconds();
    int64 ref2 = mono_ref_ns();
    assertf(t2 >= t1, "must be monotonic: %lld >= %lld",
            (long long)t2, (long long)t1);
    // Ordering on its own is satisfied by a frozen clock and by any
    // constant. Both samples must additionally fall inside the window an
    // independent monotonic clock measured around them.
    assertf(t1 >= ref1 - CLOCK_SLACK_NS && t2 <= ref2 + CLOCK_SLACK_NS,
            "samples must track an independent monotonic clock: "
            "t1=%lld t2=%lld window=[%lld,%lld]",
            (long long)t1, (long long)t2,
            (long long)(ref1 - CLOCK_SLACK_NS),
            (long long)(ref2 + CLOCK_SLACK_NS));
}

void
cttest_nanoseconds_advances()
{
    int64 ref1 = mono_ref_ns();
    int64 t1 = nanoseconds();
    // CLOCK_MONOTONIC_COARSE has jiffy resolution: 10ms at HZ=100,
    // 4ms at HZ=250. Sleep 30ms (> 2 jiffies at HZ=100) to guarantee
    // crossing a jiffy boundary.
    usleep(30000);
    int64 t2 = nanoseconds();
    int64 ref2 = mono_ref_ns();
    int64 elapsed = t2 - t1;
    int64 reference = ref2 - ref1;
    // Two jiffies of slack: the coarse clock may round the interval down
    // by one jiffy at each end.
    int64 slack = 20000000LL;
    assertf(t2 > t1, "time must advance after work: %lld > %lld",
            (long long)t2, (long long)t1);
    // "Advances" is also true of a counter stepping one nanosecond per
    // call and of a clock reporting the wrong unit. The measured
    // interval must match what an independent clock saw over the very
    // same sleep.
    assertf(elapsed > reference - slack && elapsed < reference + slack,
            "elapsed must match the reference interval: got %lldns, "
            "reference %lldns, slack %lldns",
            (long long)elapsed, (long long)reference, (long long)slack);
}

/* --- wal_crc32c --- */

// Known-answer: CRC32C("123456789") with init 0xFFFFFFFF and final
// xor 0xFFFFFFFF is 0xE3069283, the CRC-32/ISCSI check value.
// Guards all three wal_crc32c implementations (SSE4.2, ARM ACLE,
// software table) against bit-level drift.
void
cttest_crc32c_known_vector()
{
    const char *s = "123456789";
    uint32 c = wal_crc32c(WAL_CRC32C_INIT, s, 9) ^ WAL_CRC32C_XOR;
    assertf(c == 0xE3069283u,
            "CRC32C(\"123456789\") = 0x%08x, want 0xE3069283", c);
}

// Streaming must equal one-shot: exercises the 8-byte main loop plus
// every tail length (0..7 bytes).
void
cttest_crc32c_incremental()
{
    const char *s = "123456789";
    uint32 one = wal_crc32c(WAL_CRC32C_INIT, s, 9);
    for (size_t split = 0; split <= 9; split++) {
        uint32 c = wal_crc32c(WAL_CRC32C_INIT, s, split);
        c = wal_crc32c(c, s + split, 9 - split);
        assertf(c == one, "split at %zu: 0x%08x != 0x%08x",
                split, c, one);
    }
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

    /* The sibling boundary of body_size=0: a negative size would wrap
       the malloc and hand back a slab smaller than the Job header, so it
       must never produce a Job at all. */
    assertf(allocate_job(-1) == NULL,
            "body_size=-1 must be rejected, not wrapped into a short slab");
    assertf(allocate_job(INT_MIN) == NULL,
            "body_size=INT_MIN must be rejected");

    /* The zero-body slab must belong to the size class it is later filed
       under. allocate_job debits sizeof(Job)+slab when it reuses a
       pooled entry, while job_free credited sizeof(Job)+(64<<class)+PAD:
       a slab sized from the wrong class either misses the pool entirely
       or leaves the books off zero. */
    Job *a = allocate_job(0);
    assertf(a, "zero-body job must allocate");
    job_free(a);
    size_t bytes;
    int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "freed zero-body job must be pooled, got count=%d", count);
    Job *b = allocate_job(0);
    assertf(b == a, "pool must hand the same zero-body slab back: got %p, want %p",
            (void *)b, (void *)a);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "pool accounting must balance after reuse: count=%d bytes=%zu",
            count, bytes);
    free(b);
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

    /* Reading back your own memset proves nothing: a short slab does not
       fault, it quietly overlaps whatever the allocator hands out next.
       Fill a second body of the same size and require the first one's
       tail to survive it. */
    Job *k = allocate_job(sz);
    assertf(k, "second 1MB job must allocate");
    memset(k->body, 0xCD, sz);
    assertf((unsigned char)j->body[sz-1] == 0xAB,
            "1MB slabs must not overlap: tail of the first body reads 0x%02x "
            "after filling the second",
            (unsigned char)j->body[sz-1]);
    free(k);
    free(j);

    /* Nothing above measures the SLAB, only the bytes just written, so a
       slab sized from the wrong class is invisible. The largest poolable
       body exposes it: job_free credits sizeof(Job)+(64<<class)+PAD and
       allocate_job debits sizeof(Job)+slab when it takes the entry back,
       so the two must be the same number. */
    int pooled_sz = 65536 + 2; /* 64KiB user body + the \r\n trailer */
    Job *p1 = allocate_job(pooled_sz);
    assertf(p1, "max poolable body must allocate");
    memset(p1->body, 0xEE, pooled_sz);
    job_free(p1);
    size_t bytes;
    int count;
    get_job_pool_stats(&bytes, &count);
    assertf(count == 1, "freed max-class job must be pooled, got count=%d", count);
    Job *p2 = allocate_job(pooled_sz);
    assertf(p2 == p1, "pool must hand the same slab back: got %p, want %p",
            (void *)p2, (void *)p1);
    get_job_pool_stats(&bytes, &count);
    assertf(count == 0 && bytes == 0,
            "pool accounting must balance after reuse: count=%d bytes=%zu",
            count, bytes);
    free(p2);
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

    /* Auto ids that only count upward among themselves are not unique:
       a WAL replay hands out explicit ids through the same counter. The
       id right at the counter's head is the hostile one — if an explicit
       id does not push the counter past itself, the very next auto id
       hands the SAME id to a second job and the first one becomes
       unreachable in the hash table. */
    uint64 taken = b->r.id + 1;
    Job *c = make_job_with_id(1, 0, 1, 0, dtube, taken);
    assertf(c->r.id == taken, "explicit id must be honoured: %llu",
            (unsigned long long)c->r.id);
    Job *d = make_job(1, 0, 1, 0, dtube);
    assertf(d->r.id != taken,
            "auto id must not reuse the explicit id %llu",
            (unsigned long long)taken);
    assertf(job_find(taken) == c,
            "the explicitly-numbered job must stay reachable after the next "
            "auto id is handed out");

    job_free(d);
    job_free(c);
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
    /* An empty result is exactly where a missing +1 for the terminator
       hides: the buffer is never written, and on a virgin heap the byte
       it hands back is already zero. Poison the smallest allocation size
       first and free it, so the next malloc of that class comes back
       dirty and only an actually-written NUL can satisfy the assertions
       below. */
    unsigned char *poison[8];
    for (int i = 0; i < 8; i++) {
        poison[i] = malloc(1);
        assertf(poison[i], "poison block %d must allocate", i);
        *poison[i] = 0xA5;
    }
    for (int i = 0; i < 8; i++)
        free(poison[i]);

    char *s = fmtalloc("%s", "");
    assertf(s, "must not return NULL");
    assertf(strlen(s) == 0, "must be empty string");
    assertf(s[0] == '\0', "must be NUL terminated, got 0x%02x",
            (unsigned char)s[0]);
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

    /* Memory taken from a virgin heap is zero whether zalloc zeroes it or
       not — a fresh process cannot tell calloc from malloc. Dirty a batch
       of same-sized blocks, hand them back, and demand that what zalloc
       recycles comes out clean. */
    enum { NBLOCK = 64, BLOCKSZ = sizeof(int) * 10 };
    unsigned char *dirty[NBLOCK];
    for (int i = 0; i < NBLOCK; i++) {
        dirty[i] = malloc(BLOCKSZ);
        assertf(dirty[i], "dirty block %d must allocate", i);
        memset(dirty[i], 0xA5, BLOCKSZ);
    }
    for (int i = 0; i < NBLOCK; i++)
        free(dirty[i]);

    unsigned char *recycled[NBLOCK];
    for (int i = 0; i < NBLOCK; i++) {
        recycled[i] = zalloc(BLOCKSZ);
        assertf(recycled[i], "recycled block %d must allocate", i);
    }
    size_t surviving = 0;
    for (int i = 0; i < NBLOCK; i++) {
        for (size_t k = 0; k < (size_t)BLOCKSZ; k++) {
            if (recycled[i][k] != 0)
                surviving++;
        }
    }
    for (int i = 0; i < NBLOCK; i++)
        free(recycled[i]);
    assertf(surviving == 0,
            "zalloc must zero recycled memory: %zu poisoned bytes survived "
            "across %d blocks of %zu bytes",
            surviving, NBLOCK, (size_t)BLOCKSZ);
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

    /* tickat is an int64 nanosecond deadline and passes 2^32 after ~4.3
       seconds of uptime. A comparison narrowed to 32 bits agrees with
       the two small literals above and then inverts here. */
    Conn below = { .tickat = 4294967295LL }; /* 2^32 - 1 */
    Conn above = { .tickat = 4294967296LL }; /* 2^32     */
    assertf(conn_less(&below, &above),
            "ordering must hold across the 2^32 boundary: %lld < %lld",
            (long long)below.tickat, (long long)above.tickat);
    assertf(!conn_less(&above, &below),
            "the 2^32 boundary must not invert the order");

    /* The idiomatic wrong spelling in this codebase is the subtraction
       form (prot.c already subtracts on this very field), which agrees
       with `<` everywhere except where the difference overflows. */
    Conn least = { .tickat = INT64_MIN };
    Conn most  = { .tickat = INT64_MAX };
    assertf(conn_less(&least, &most),
            "INT64_MIN must order before INT64_MAX");
    assertf(!conn_less(&most, &least),
            "INT64_MAX must not order before INT64_MIN");

    /* Two DISTINCT conns with the same deadline is the normal case: one
       prottick pass arms many conns off the same `now`. A `<=` spelling
       makes each of them "less" than the other and breaks the heap's
       strict ordering. */
    int64 same = 300;
    Conn tie1 = { .tickat = same };
    Conn tie2 = { .tickat = same };
    assertf(!conn_less(&tie1, &tie2) && !conn_less(&tie2, &tie1),
            "distinct conns sharing tickat=%lld must not be less either way",
            (long long)same);
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
        assertf(heapinsert(&h, &conns[i]) == 1,
                "heapinsert must report success for conn %d", i);

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

    /* Draining from index 0 never consults tickpos, yet production
       removes and resifts a conn BY c->tickpos (conn.c: heapresift on a
       rescheduled deadline, heapremove on connclose). setpos must
       therefore track every move the heap makes. 21 entries reach depth
       2 in the 4-ary tree, so the position of a mid-heap element is only
       correct if it was rewritten during the sift chain. */
    Heap deep = {
        .less = conn_less,
        .setpos = conn_setpos,
    };
    enum { NDEEP = 21 };
    Conn many[NDEEP];
    memset(many, 0, sizeof many);
    for (i = 0; i < NDEEP; i++)
        many[i].tickat = (int64)(NDEEP - i) * 1000; /* inserted worst-first */
    for (i = 0; i < NDEEP; i++)
        assertf(heapinsert(&deep, &many[i]) == 1,
                "heapinsert must report success for deep conn %d", i);

    Conn *target = &many[7];
    Conn *pulled = heapremove(&deep, target->tickpos);
    assertf(pulled == target,
            "heapremove at the conn's own tickpos must unlink that conn: "
            "got tickat=%lld, want tickat=%lld",
            pulled ? (long long)pulled->tickat : -1LL,
            (long long)target->tickat);

    Conn *root = heapremove(&deep, 0);
    assertf(root == &many[NDEEP - 1],
            "the smallest deadline must still be at the root after a "
            "removal by position: got tickat=%lld, want %lld",
            root ? (long long)root->tickat : -1LL,
            (long long)many[NDEEP - 1].tickat);
    free(deep.data);

    /* Deadlines are absolute nanosecond timestamps well past 2^32. A
       comparison narrowed to 32 bits keeps the small fixtures above in
       order and silently inverts the real ones. */
    Heap wide = {
        .less = conn_less,
        .setpos = conn_setpos,
    };
    Conn late = { .tickat = 4294967296LL }; /* 2^32     */
    Conn soon = { .tickat = 4294967295LL }; /* 2^32 - 1 */
    assertf(heapinsert(&wide, &late) == 1, "wide insert must succeed");
    assertf(heapinsert(&wide, &soon) == 1, "wide insert must succeed");
    Conn *first = heapremove(&wide, 0);
    assertf(first == &soon,
            "the earlier deadline must pop first across the 2^32 boundary: "
            "got tickat=%lld, want %lld",
            first ? (long long)first->tickat : -1LL,
            (long long)soon.tickat);
    free(wide.data);
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

    /* A name is (bytes, length), not a C string: a NUL inside the buffer
       must not end the comparison early and let a shorter tube answer
       for a longer request. */
    Tube *ab = tube_find_or_make("ab");
    tube_iref(ab);
    assertf(tube_find_name("ab", 2) == ab, "must find the two-byte name");
    assertf(tube_find_name("ab\0cd", 5) == NULL,
            "a NUL inside the buffer must not truncate the lookup");
    assertf(tube_find_name("\0b", 2) == NULL,
            "a leading NUL must not match anything stored");

    /* Length boundary: MAX_TUBE_NAME_LEN-1 is the longest name a tube can
       hold, so both of its neighbours must miss. */
    char longname[MAX_TUBE_NAME_LEN + 1];
    memset(longname, 'z', sizeof longname);
    longname[MAX_TUBE_NAME_LEN - 1] = '\0';
    Tube *lt = tube_find_or_make(longname);
    tube_iref(lt);
    assertf(tube_find_name(longname, MAX_TUBE_NAME_LEN - 1) == lt,
            "the longest legal name (%d bytes) must be findable",
            MAX_TUBE_NAME_LEN - 1);
    assertf(tube_find_name(longname, MAX_TUBE_NAME_LEN - 2) == NULL,
            "one byte short of the longest name must miss");
    longname[MAX_TUBE_NAME_LEN - 1] = 'z';
    longname[MAX_TUBE_NAME_LEN] = '\0';
    assertf(tube_find_name(longname, MAX_TUBE_NAME_LEN) == NULL,
            "one byte past the longest name must miss");

    tube_dref(lt);
    tube_dref(ab);
    tube_dref(t);
    ms_clear(&tubes);

    /* One tube in the table never walks a collision chain, so a bucket
       that keeps only its newest entry passes everything above. The
       table has a fixed bucket count, so overfilling it forces chains by
       pigeonhole: every name inserted must still be findable. */
    enum { NTUBE = 5000 };
    char name[32];
    for (int i = 0; i < NTUBE; i++) {
        snprintf(name, sizeof name, "chain-%d", i);
        Tube *ct = tube_find_or_make(name);
        assertf(ct, "tube %s must be created", name);
        tube_iref(ct);
    }
    int missing = 0;
    int wrongname = 0;
    for (int i = 0; i < NTUBE; i++) {
        snprintf(name, sizeof name, "chain-%d", i);
        Tube *ct = tube_find_name(name, strlen(name));
        if (!ct)
            missing++;
        else if (strcmp(ct->name, name) != 0)
            wrongname++;
    }
    assertf(missing == 0 && wrongname == 0,
            "every one of %d tubes must survive bucket collisions: "
            "%d unreachable, %d answered by the wrong tube",
            NTUBE, missing, wrongname);
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
    // Negative control FIRST: the whole verdict below rides on a
    // hand-rolled oracle, and an oracle that certifies everything is an
    // all-green lie. These composites are the ones a weak primality test
    // waves through: Carmichael numbers (561, 1105, 1729, 2465), the
    // classic strong pseudoprime to bases 2,3,5,7 (3215031751), a square
    // of a prime, and a semiprime of two 31-bit primes.
    uint64_t composites[] = {
        4, 9, 561, 1105, 1729, 2465, 3215031751ULL,
        4294967297ULL,                 /* 641 * 6700417 */
        (uint64_t)2147483647 * 2,      /* 2 * a Mersenne prime */
        (uint64_t)1000003 * 1000033,
    };
    size_t certified = 0;
    for (size_t i = 0; i < sizeof composites / sizeof composites[0]; i++) {
        if (is_prime_u64(composites[i]))
            certified++;
    }
    assertf(certified == 0,
            "the primality oracle must reject known composites: "
            "%zu of %zu were certified prime",
            certified, sizeof composites / sizeof composites[0]);

    // Positive control: the smallest primes the oracle must accept, so a
    // "reject everything" oracle cannot silently pass either.
    assertf(is_prime_u64(2) && is_prime_u64(3) && is_prime_u64(1000003)
            && is_prime_u64(2147483647),
            "the primality oracle must accept known primes");

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

// Growth invariant: each step at least 2x the previous.
// The rehash upscales once the table is loaded past its capacity; if two
// adjacent primes were closer than 2x, we'd rehash without gaining
// headroom. The stated rule is "at least 2x", so the check demands 2x —
// a 1.9x entry is precisely the cliff this test exists to stop.
//
// The bounds are computed in __uint128_t so a future entry close to
// 2^63 does not silently overflow the check into "always passes".
void
cttest_primes_doubling()
{
    for (size_t i = 1; i < primes_len; i++) {
        __uint128_t prev = (__uint128_t)primes[i - 1];
        __uint128_t bound = prev * 2;
        assertf((__uint128_t)primes[i] >= bound,
                "primes[%zu]=%zu not >= 2x primes[%zu]=%zu",
                i, primes[i], i - 1, primes[i - 1]);
    }

    // The other side of the same boundary, never checked before: the
    // step must not overshoot so far that the table downscales again the
    // moment it has grown. job.c upscales at used > cap*2 and downscales
    // at used < cap/8, so a step wider than 16x lands straight back
    // under the downscale threshold and the table oscillates. 8x keeps a
    // full factor of margin over the real 2x steps.
    for (size_t i = 1; i < primes_len; i++) {
        __uint128_t prev = (__uint128_t)primes[i - 1];
        __uint128_t bound = prev * 8;
        assertf((__uint128_t)primes[i] <= bound,
                "primes[%zu]=%zu overshoots 8x primes[%zu]=%zu — the table "
                "would downscale immediately after growing",
                i, primes[i], i - 1, primes[i - 1]);
    }
}
