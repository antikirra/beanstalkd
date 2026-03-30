#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static uint64 next_id = 1;

void
job_init_id(int worker_id, int nworkers)
{
    if (nworkers > 1) {
        next_id = (uint64)worker_id + 1;
    }
}

// Size-classed free list for O(1) job reuse across varied body sizes.
// Jobs are allocated at power-of-2 boundaries (64, 128, ..., 65536).
// Any body_size within a class reuses entries from that class.
// Entries linked via ht_next (safe: job is removed from hash before pooling).
#define POOL_NCLASS     11          // classes: 64, 128, 256, ..., 65536
#define POOL_PER_CLASS  512         // max entries per class
#define POOL_MEM_MAX    (8 << 20)   // 8MB total across all classes

static Job    *pool_head[POOL_NCLASS];
static int     pool_len[POOL_NCLASS];
static size_t  pool_mem;

// pool_class returns the class index (0..POOL_NCLASS-1) for body_size,
// or -1 if too large to pool.
static int
pool_class(int body_size)
{
    if (body_size < 0)      return -1;
    if (body_size <= 64)    return 0;
    if (body_size <= 128)   return 1;
    if (body_size <= 256)   return 2;
    if (body_size <= 512)   return 3;
    if (body_size <= 1024)  return 4;
    if (body_size <= 2048)  return 5;
    if (body_size <= 4096)  return 6;
    if (body_size <= 8192)  return 7;
    if (body_size <= 16384) return 8;
    if (body_size <= 32768) return 9;
    if (body_size <= 65536) return 10;
    return -1;
}


static int cur_prime = 0;

static Job *all_jobs_init[12289] = {0};
static Job **all_jobs = all_jobs_init;
static size_t all_jobs_cap = 12289; /* == primes[0] */
static size_t all_jobs_used = 0;

// Incremental rehash state. During migration, entries live in both tables.
// Each find/insert/delete migrates a few buckets from old to new,
// spreading the O(n) cost across many operations instead of one pause.
static Job **rehash_old;
static size_t rehash_old_cap;
static size_t rehash_pos;

// Migrate up to 16 buckets from the old table to the new table.
// Bounded work per call: O(16 + entries_in_16_buckets).
static void
rehash_step(void)
{
    if (!rehash_old) return;

    int steps = 16;
    while (steps-- > 0 && rehash_pos < rehash_old_cap) {
        while (rehash_old[rehash_pos]) {
            Job *j = rehash_old[rehash_pos];
            rehash_old[rehash_pos] = j->ht_next;
            size_t index = j->r.id % all_jobs_cap;
            j->ht_next = all_jobs[index];
            all_jobs[index] = j;
        }
        rehash_pos++;
    }

    if (rehash_pos >= rehash_old_cap) {
        if (rehash_old != all_jobs_init) {
            free(rehash_old);
        }
        rehash_old = NULL;
        rehash_old_cap = 0;
    }
}

static void
rehash_start(int is_upscaling)
{
    if (rehash_old) return; /* already in progress */

    int d = is_upscaling ? 1 : -1;
    if (cur_prime + d >= NUM_PRIMES) return;
    if (cur_prime + d < 0) return;

    size_t new_cap = primes[cur_prime + d];
    Job **new_table = calloc(new_cap, sizeof(Job *));
    if (!new_table) {
        twarnx("Failed to allocate %zu new hash buckets", new_cap);
        return;
    }

    rehash_old = all_jobs;
    rehash_old_cap = all_jobs_cap;
    rehash_pos = 0;

    all_jobs = new_table;
    all_jobs_cap = new_cap;
    cur_prime += d;
}

static void
store_job(Job *j)
{
    size_t index = j->r.id % all_jobs_cap;
    j->ht_next = all_jobs[index];
    all_jobs[index] = j;
    all_jobs_used++;

    if (rehash_old) rehash_step();

    /* accept a load factor of 4 */
    if (!rehash_old && all_jobs_used > (all_jobs_cap << 1))
        rehash_start(1);
}

__attribute__((hot)) Job *
job_find(uint64 job_id)
{
    size_t index = job_id % all_jobs_cap;
    Job *jh = all_jobs[index];

    while (jh && jh->r.id != job_id) {
        if (jh->ht_next) __builtin_prefetch(jh->ht_next, 0, 1);
        jh = jh->ht_next;
    }

    // Check old table if rehash in progress and not found in new.
    if (!jh && rehash_old) {
        index = job_id % rehash_old_cap;
        jh = rehash_old[index];
        while (jh && jh->r.id != job_id) {
            if (jh->ht_next) __builtin_prefetch(jh->ht_next, 0, 1);
            jh = jh->ht_next;
        }
    }

    if (rehash_old) rehash_step();
    return jh;
}

Job *
allocate_job(int body_size)
{
    Job *j = NULL;
    int cls = pool_class(body_size);
    int asize = cls >= 0 ? (64 << cls) : body_size;

    // Reuse from size-class pool. O(1) lookup, guaranteed fit.
    if (likely(cls >= 0 && pool_head[cls])) {
        j = pool_head[cls];
        pool_head[cls] = j->ht_next;
        pool_len[cls]--;
        pool_mem -= sizeof(Job) + asize;
    }

    if (!j) {
        j = malloc(sizeof(Job) + asize);
        if (!j) {
            twarnx("OOM");
            return (Job *) 0;
        }
    }

    memset(&j->r, 0, sizeof(Jobrec));
    j->r.created_at = now ? now : nanoseconds();
    j->r.body_size = body_size;
    j->heap_index = 0;
    j->tube = NULL;
    j->reserver = NULL;
    j->body = (char *)j + sizeof(Job);
    j->prev = j;
    j->next = j;
    j->ht_next = NULL;
    j->file = NULL;
    j->fnext = NULL;
    j->fprev = NULL;
    j->walresv = 0;
    j->walused = 0;
    return j;
}

Job *
make_job_with_id(uint32 pri, int64 delay, int64 ttr,
                 int body_size, Tube *tube, uint64 id)
{
    Job *j;

    j = allocate_job(body_size);
    if (!j) {
        twarnx("OOM");
        return (Job *) 0;
    }

    if (id) {
        j->r.id = id;
        // Advance next_id past recovered ID, maintaining interleaving stride.
        int step = srv.nworkers > 1 ? srv.nworkers : 1;
        if (id >= next_id) {
            // Round up to next valid ID for this worker.
            uint64 base = (srv.worker_id >= 0 ? srv.worker_id : 0) + 1;
            next_id = id + step - ((id - base) % step);
            if (next_id <= id) next_id = id + step; // overflow guard
        }
        if (!next_id) next_id = (srv.worker_id >= 0 ? srv.worker_id : 0) + 1;
    } else {
        j->r.id = next_id;
        // Interleaved IDs: worker K gets K+1, K+N+1, K+2N+1, ...
        int step = srv.nworkers > 1 ? srv.nworkers : 1;
        next_id += step;
        if (!next_id) next_id = (srv.worker_id >= 0 ? srv.worker_id : 0) + 1;
    }
    j->r.pri = pri;
    j->r.delay = delay;
    j->r.ttr = ttr;

    store_job(j);

    TUBE_ASSIGN(j->tube, tube);

    return j;
}

static void
job_hash_free(Job *j)
{
    Job **slot;

    // Search new table first.
    slot = &all_jobs[j->r.id % all_jobs_cap];
    while (*slot && *slot != j) slot = &(*slot)->ht_next;
    if (*slot) {
        *slot = (*slot)->ht_next;
        --all_jobs_used;
        goto done;
    }

    // Search old table if rehash in progress.
    if (rehash_old) {
        slot = &rehash_old[j->r.id % rehash_old_cap];
        while (*slot && *slot != j) slot = &(*slot)->ht_next;
        if (*slot) {
            *slot = (*slot)->ht_next;
            --all_jobs_used;
        }
    }

done:
    if (rehash_old) rehash_step();

    // Downscale when the hashmap is too sparse, but never below initial size.
    // Only when no rehash in progress.
    if (!rehash_old && cur_prime > 0 && all_jobs_used < (all_jobs_cap >> 3))
        rehash_start(0);
}

void
job_free(Job *j)
{
    if (!j) return;

    int is_copy = (j->r.state == Copy);

    TUBE_ASSIGN(j->tube, NULL);
    if (!is_copy) job_hash_free(j);

    // Pool regular jobs for reuse; copies, oversized, and excess go to free().
    int cls = pool_class(j->r.body_size);
    if (!is_copy && cls >= 0) {
        size_t entry_bytes = sizeof(Job) + (size_t)(64 << cls);
        if (pool_len[cls] < POOL_PER_CLASS
            && pool_mem + entry_bytes <= POOL_MEM_MAX) {
            j->ht_next = pool_head[cls];
            pool_head[cls] = j;
            pool_len[cls]++;
            pool_mem += entry_bytes;
            return;
        }
    }

    free(j);
}

void
job_setpos(void *j, size_t pos)
{
    ((Job *)j)->heap_index = pos;
}

__attribute__((hot)) int
job_pri_less(void *ja, void *jb)
{
    Job *a = (Job *)ja;
    Job *b = (Job *)jb;
    if (a->r.pri < b->r.pri) return 1;
    if (a->r.pri > b->r.pri) return 0;
    return a->r.id < b->r.id;
}

__attribute__((hot)) int
job_delay_less(void *ja, void *jb)
{
    Job *a = ja;
    Job *b = jb;
    if (a->r.deadline_at < b->r.deadline_at) return 1;
    if (a->r.deadline_at > b->r.deadline_at) return 0;
    return a->r.id < b->r.id;
}

Job *
job_copy(Job *j)
{
    if (!j)
        return NULL;

    Job *n = malloc(sizeof(Job) + j->r.body_size);
    if (!n) {
        twarnx("OOM");
        return (Job *) 0;
    }

    memcpy(n, j, sizeof(Job) + j->r.body_size);
    n->body = (char *)n + sizeof(Job);
    job_list_reset(n);

    n->file = NULL; /* copies do not have refcnt on the wal */
    n->ht_next = NULL;
    n->fnext = NULL;
    n->fprev = NULL;

    n->tube = 0; /* Don't use memcpy for the tube, which we must refcount. */
    TUBE_ASSIGN(n->tube, j->tube);

    /* Mark this job as a copy so it can be appropriately freed later on */
    n->r.state = Copy;

    return n;
}

const char *
job_state(Job *j)
{
    if (j->r.state == Ready) return "ready";
    if (j->r.state == Reserved) return "reserved";
    if (j->r.state == Buried) return "buried";
    if (j->r.state == Delayed) return "delayed";
    return "invalid";
}

// job_list_reset detaches head from the list,
// marking the list starting in head pointing to itself.
Job *
job_list_remove(Job *j)
{
    if (!j) return NULL;
    if (job_list_is_empty(j)) return NULL; /* not in a doubly-linked list */

    j->next->prev = j->prev;
    j->prev->next = j->next;

    job_list_reset(j);

    return j;
}

void
job_list_insert(Job *head, Job *j)
{
    if (!job_list_is_empty(j)) return; /* already in a linked list */

    j->prev = head->prev;
    j->next = head;
    head->prev->next = j;
    head->prev = j;
}

/* for unit tests */
size_t
get_all_jobs_used()
{
    return all_jobs_used;
}
