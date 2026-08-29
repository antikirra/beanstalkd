#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <signal.h>
#include <pthread.h>

typedef unsigned char uchar;
typedef uchar         byte;
typedef unsigned int  uint;
typedef int32_t       int32;
typedef uint32_t      uint32;
typedef int64_t       int64;
typedef uint64_t      uint64;

typedef struct Ms     Ms;
typedef struct Job    Job;
typedef struct Tube   Tube;
typedef struct Conn   Conn;
typedef struct Heap   Heap;
typedef struct Jobrec Jobrec;
typedef struct File   File;
typedef struct Socket Socket;
typedef struct Server Server;
typedef struct Wal    Wal;

typedef void(*Handle)(void*, int rw);
typedef int(*FAlloc)(int, int);


// NUM_PRIMES is used in the jobs hashing.
#if _LP64
#define NUM_PRIMES 48
#else
#define NUM_PRIMES 19
#endif

// The name of a tube cannot be longer than MAX_TUBE_NAME_LEN-1
#define MAX_TUBE_NAME_LEN 201

// A command can be at most LINE_BUF_SIZE chars, including "\r\n". This value
// MUST be enough to hold the longest possible command ("pause-tube a{200} 4294967295\r\n")
// or reply line ("USING a{200}\r\n").
#define LINE_BUF_SIZE (11 + MAX_TUBE_NAME_LEN + 12)

#define min(a,b) ((a)<(b)?(a):(b))

// Jobs with priority less than URGENT_THRESHOLD are counted as urgent.
#define URGENT_THRESHOLD 1024

// The default maximum job size.
#define JOB_DATA_SIZE_LIMIT_DEFAULT ((1 << 16) - 1)

// The maximum value that job_data_size_limit can be set to via "-z".
// It could be up to INT32_MAX-2 (~2GB), but set it to 1024^3 (1GB).
// The width is restricted by Jobrec.body_size that is int32.
#define JOB_DATA_SIZE_LIMIT_MAX 1073741824

// The default value for the fsync (-f) parameter, milliseconds.
#define DEFAULT_FSYNC_MS 50

// Use this macro to designate unused parameters in functions.
#define UNUSED_PARAMETER(x) (void)(x)

// Branch prediction hints.
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// version is defined in vers.c, see vers.sh for details.
extern const char version[];

// verbose holds the count of -V parameters; it's a verbosity level.
extern int verbose;

// log_json enables structured JSON output via --log-json. When set,
// warn()/warnx() emit one JSON object per line to stderr instead of the
// human-readable "<progname>: msg" format. Verbose stdout traces
// (-V) are unaffected — they remain plain text by design.
extern int log_json;

extern struct Server srv;
extern volatile sig_atomic_t shutdown_requested;

// Replaced by tests to simulate failures.
extern FAlloc falloc;

// Connection I/O state machine. Defined here so non-prot.c modules
// (conn.c's idle scheduler) can reason about state without literal 0.
// Order is load-bearing: STATE_WANT_COMMAND must be 0 (matches the
// zero-init from make_conn for new connections).
#define STATE_WANT_COMMAND  0
#define STATE_WANT_DATA     1
#define STATE_SEND_JOB      2
#define STATE_SEND_WORD     3
#define STATE_WAIT          4
#define STATE_BITBUCKET     5
#define STATE_CLOSE         6
#define STATE_WANT_ENDLINE  7

// stats structure holds counters for operations, both globally and per tube.
struct stats {
    uint64 urgent_ct;
    uint64 waiting_ct;
    uint64 buried_ct;
    uint64 reserved_ct;
    uint64 pause_ct;
    uint64 total_delete_ct;
    uint64 total_jobs_ct;
};


// less_fn is used by the 4-ary min-heap (heap.c) to determine the order of elements.
typedef int(*less_fn)(void*, void*);

// setpos_fn is used by the 4-ary min-heap (heap.c) to record the new positions of elements
// whenever they get moved or inserted.
typedef void(*setpos_fn)(void*, size_t);

struct Heap {
    size_t  cap;                // capacity of the heap
    size_t  len;                // amount of elements in the heap
    void    **data;             // actual elements

    less_fn   less;
    setpos_fn setpos;
};
int   heapinsert(Heap *h, void *x);
void* heapremove(Heap *h, size_t k);
void  heapresift(Heap *h, size_t k);


struct Socket {
    // Descriptor for the socket.
    int    fd;

    // f can point to srvaccept or prothandle.
    Handle f;

    // x is passed as first parameter to f.
    void   *x;

    // 1 if the socket is registered in epoll, 0 otherwise.
    int    added;

    // Cached registered event mode to skip redundant epoll_ctl calls.
    // 0=none, 'r'=read, 'w'=write, 'h'=hangup.
    char   rw_cached;
};

int sockinit(void);

// sockwant updates event filter for the socket s. rw designates
// the kind of event we should be notified about:
// 'r' - read
// 'w' - write
// 'h' - hangup (closed connection)
// 0   - ignore this socket
int sockwant(Socket *s, int rw);

// socknext waits for the next event at most timeout nanoseconds.
// If event happens before timeout then s points to the corresponding socket,
// and the kind of event is returned. In case of timeout, 0 is returned.
int socknext(Socket **s, int64 timeout);

// ms_event_fn is called with the element being inserted/removed and its position.
typedef void(*ms_event_fn)(Ms *a, void *item, size_t i);

// Resizable multiset
struct Ms {
    size_t len;                // amount of stored elements
    size_t cap;                // capacity
    size_t last;               // position of last taken element
    void **items;

    ms_event_fn oninsert;      // called on insertion of an element
    ms_event_fn onremove;      // called on removal of an element
};

void ms_init(Ms *a, ms_event_fn oninsert, ms_event_fn onremove);
void ms_clear(Ms *a);
int ms_append(Ms *a, void *item);
int ms_remove(Ms *a, void *item);
// ms_remove_at: O(1) removal with a position hint; falls back to the
// O(n) ms_remove scan when the hint is stale. Prod user: waiting_conns
// hint-based removal (Conn.waitpos).
int ms_remove_at(Ms *a, size_t i, void *item);
int ms_contains(Ms *a, void *item);
void *ms_take(Ms *a);


enum // Jobrec.state
{
    Invalid,
    Ready,
    Reserved,
    Buried,
    Delayed,
    Copy
};

enum
{
    Walver = 8
};

// If you modify Jobrec struct, you must increment Walver above.
//
// This workflow is expected:
// 1. If any change needs to be made to the format, first increment Walver.
// 2. If and only if this is the first such change since the last release:
//    a. Copy-paste relevant file-reading functions in file.c and
//       add the old version number to their names. For example,
//       if you are incrementing Walver from 7 to 8, copy readrec to readrec7.
//       (Currently, there is only one such function, readrec. But if
//       a future readrec calls other version-specific functions,
//       those will have to be copied too.)
// 3. Add a switch case to fileread for the old version.
// 4. Modify the current reading function (readrec) to reflect your change.
//
// Incrementing Walver for every change, even if not every version
// will be released, is helpful even if it "wastes" version numbers.
// It is a really easy thing to do and it means during development
// you won't have to worry about misinterpreting the contents of a binlog
// that you generated with a dev copy of beanstalkd.
//
// v8 (current): per-record CRC32C trailer (Castagnoli, 4 bytes LE) for
// silent-corruption detection. See crc32c.c.
// v7: per-record layout without checksum, ns-resolution timestamps.

// WAL CRC32C (Castagnoli, SSE4.2 hardware-accelerated).
// Usage: c = WAL_CRC32C_INIT; c = wal_crc32c(c, buf, n); ... ; c ^= WAL_CRC32C_XOR;
// Serialize as 4-byte little-endian trailer at end of each v8 WAL record.
#define WAL_CRC32C_INIT 0xFFFFFFFFu
#define WAL_CRC32C_XOR  0xFFFFFFFFu
uint32 wal_crc32c(uint32 crc, const void *buf, size_t n);

struct Jobrec {
    uint64 id;
    uint32 pri;
    int64  delay;
    int64  ttr;
    int32  body_size;
    int64  created_at;

    // deadline_at is a timestamp, in nsec, that points to:
    // * time when job will become ready for delayed job,
    // * time when TTR is about to expire for reserved job,
    // * undefined otherwise.
    int64  deadline_at;

    uint32 reserve_ct;
    uint32 timeout_ct;
    uint32 release_ct;
    uint32 bury_ct;
    uint32 kick_ct;
    byte   state;
};
_Static_assert(sizeof(Jobrec) == 80, "Jobrec size changed — increment Walver");
_Static_assert(sizeof(int) == 4, "WAL format assumes 4-byte int for namelen");

struct Job {
    // persistent fields; these get written to the wal
    Jobrec r;

    // hot bookkeeping fields — accessed on every reserve/delete/timeout
    size_t heap_index;          // where is this job in its current heap
    Tube *tube;
    void *reserver;
    char *body;                 // written separately to the wal

    // cold bookkeeping fields — accessed less frequently
    Job *prev, *next;           // linked list of jobs
    Job *ht_next;               // Next job in a hash table list
    File *file;
    Job  *fnext;
    Job  *fprev;
    int64 walresv;
    int64 walused;
};

// Tube struct layout: hot scheduling fields packed in first 4 cache lines
// (256 bytes). name[201] and buried sentinel at end to avoid cache
// pollution — process_tube and enqueue_job access ready/delay/waiting_conns
// on every iteration without loading the name bytes.
struct Tube {
    // --- cache line 1 (0-63): identity, flags, counters ---
    uint refs;
    uint name_hash;                     // cached tube_name_hash(name)
    int  in_delay_heap;                 // 1 if tube is in global delay heap
    int  in_pause_heap;                 // 1 if tube is in pause heap
    uint using_ct;
    uint watching_ct;
    Heap ready;                         // +32: job priority queue (40B, straddles CL1-2)

    // --- cache line 2 (64-127): delay heap, heap indices ---
    Heap delay;                         // +72: delayed job queue
    size_t delay_heap_index;            // position in global delay tube heap
    size_t pause_heap_index;            // position in global pause tube heap

    // --- cache line 3 (128-191): pause, waiting conns ---
    int64 pause;                        // duration of the current pause, or 0, in nsec
    int64 unpause_at;                   // timestamp when to unpause, in nsec
    Ms waiting_conns;                   // conns waiting for a job at this moment

    // --- cache line 4 (192-255): stats ---
    struct stats stat;

    // --- cold fields: lookup, list management ---
    Tube *ht_next;                      // hash table chain for global tube lookup
    size_t name_len;                    // cached strlen(name)
    Job buried;                         // linked list header
    char name[MAX_TUBE_NAME_LEN];       // tube name (cold: only on lookup/stats)
};


// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>: <errno_msg>
// (a JSON object instead when --log-json is active; see log_json)
#define twarn(...) __twarn(__VA_ARGS__, "")

// Hack to quiet the compiler. When VA_ARGS in twarn() has one element,
// e.g. twarn("OOM"), its replaced with __twarn("OOM", ""),
// thus VA_ARGS is expanded to at least one element in warn().
#define __twarn(fmt, ...) \
    warn("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>
// (a JSON object instead when --log-json is active; see log_json)
#define twarnx(...) __twarnx(__VA_ARGS__, "")

// See __twarn macro.
#define __twarnx(fmt, ...) \
    warnx("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

void warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void warnx(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
// JSON-escape src into dst. Writes at most dst_size-1 chars + NUL.
// Truncation-safe: never writes past dst_size, always NUL-terminates
// when dst_size > 0. Returns number of chars written (excluding NUL).
// Exposed for testing the escape table.
size_t json_escape(char *dst, size_t dst_size, const char *src);
char* fmtalloc(char *fmt, ...) __attribute__((format(printf, 1, 2)));
void* zalloc(size_t n);
#define new(T) zalloc(sizeof(T))
void optparse(Server*, char**);

extern const char *progname;

int64 nanoseconds(void);

// Cached nanoseconds() for the current tick. Main thread only.
extern int64 now;

// Interval in nanoseconds between malloc_trim() calls.
// Set via -m flag (in seconds). 0 disables trim.
// Default: 60 seconds. Only effective on glibc.
extern int64 mem_trim_rate;

int   rawfalloc(int fd, int len);

// Take ID for a jobs from next_id and allocate and store the job.
#define make_job(pri,delay,ttr,body_size,tube) \
    make_job_with_id(pri,delay,ttr,body_size,tube,0)

Job *allocate_job(int body_size);
Job *make_job_with_id(uint32 pri, int64 delay, int64 ttr,
                      int body_size, Tube *tube, uint64 id);
void job_free(Job *j);

/* Lookup a job by job ID */
Job *job_find(uint64 job_id);

/* the void* parameters are really job pointers */
void job_setpos(void *j, size_t pos);
int job_pri_less(void *ja, void *jb);
int job_delay_less(void *ja, void *jb);

Job *job_copy(Job *j);

const char * job_state(Job *j);

static inline void job_list_reset(Job *head) {
    head->prev = head;
    head->next = head;
}
static inline int job_list_is_empty(Job *head) {
    return head->next == head && head->prev == head;
}
Job *job_list_remove(Job *j);
void job_list_insert(Job *head, Job *j);

// Free every pooled job back to glibc and reset the size-class free lists.
// Called on the periodic -m trim tick before malloc_trim(0) so the trim can
// reclaim the pool's pages.
void job_pool_drain(void);

/* for unit tests */
size_t get_all_jobs_used(void);
void get_job_pool_stats(size_t *bytes, int *count);


extern struct Ms tubes;

Tube *make_tube(const char *name);
void  tube_free(Tube *t);
// Inline refcount; tube_free called when refs drops to 0.
static inline void tube_dref(Tube *t) {
    if (!t) return;
    if (t->refs < 1) return; // safety: already zero
    --t->refs;
    if (t->refs < 1) tube_free(t);
}
static inline void tube_iref(Tube *t) { if (t) ++t->refs; }
Tube *tube_find(Ms *tubeset, const char *name);
Tube *tube_find_name(const char *name, size_t len);
uint  tube_name_hash(const char *name);
uint  tube_name_hash_n(const char *name, size_t len);
Tube *tube_find_or_make(const char *name);
Tube *tube_find_or_make_n(const char *name, size_t len);
#define TUBE_ASSIGN(a,b) do { \
    Tube *_tb = (b); \
    if ((a) != _tb) { tube_dref(a); (a) = _tb; tube_iref(a); } \
} while(0)


Conn *make_conn(int fd, char start_state, Tube *use, Tube *watch);

// Free every pooled Conn back to glibc and reset the slab free list.
// Called on the periodic -m trim tick (next to job_pool_drain) so
// malloc_trim(0) can reclaim the pool's pages.
void conn_pool_drain(void);

/* for unit tests */
void get_conn_pool_stats(int *count);

// Defer Conn slab-pool returns while an epoll event batch drains, so a
// conn closed mid-batch stays frozen (not reused) until every stale
// event that could still reference it has been dispatched/skipped.
void conn_defer_free_begin(void);
void conn_defer_free_end(void);

uint count_cur_conns(void);
uint count_tot_conns(void);
uint count_cur_producers(void);
uint count_cur_workers(void);


extern size_t primes[];
extern const size_t primes_len;


extern size_t job_data_size_limit;

void prot_init(void);
int64 prottick(Server *s);
void prot_remove_tube(Tube *t);

void remove_waiting_conn(Conn *c);
/* Not static: exposed for hostile unit tests in testprot2.c. */
int enqueue_waiting_conn(Conn *c);

// conn_waitpos_reserve grows c->waitpos to mirror n watch entries.
// Returns 1 on success, 0 on OOM. Sole caller: enqueue_waiting_conn —
// the only writer of hints — so the array never lags c->watch while
// the conn is waiting.
int conn_waitpos_reserve(Conn *c, size_t n);

// onremove callback for every tube's waiting_conns (registered in
// make_tube): after ms_delete swap-moves the last conn into slot i,
// refresh that conn's waitpos hint for this tube.
void on_waiting_conn_remove(Ms *a, void *item, size_t i);

void enqueue_reserved_jobs(Conn *c);

void enter_drain_mode(int sig);
void h_accept(const int fd, const short which, Server *s);

int  prot_replay(Server *s, Job *list);


int make_server_socket(char *host, char *port);


// CONN_TYPE_* are bit masks used to track the type of connection.
// A put command adds the PRODUCER type, "reserve*" adds the WORKER type.
// If connection awaits for data, then it has WAITING type.
#define CONN_TYPE_PRODUCER 1
#define CONN_TYPE_WORKER   2
#define CONN_TYPE_WAITING  4

// Soft cap for the per-conn deferred-ack buffer (invariant #16). The
// reply() defer hook appends a SEND_WORD ack only while the result stays
// <= DUR_REPLY_SOFT_MAX; past that it appends into the slack below and
// pushes the whole buffer out immediately, preserving reply order. The
// hard buffer therefore carries LINE_BUF_SIZE bytes of slack so the
// overflowing line ALWAYS fits (any single reply line is at most
// LINE_BUF_SIZE bytes) — no reply can ever be dropped for lack of room.
#define DUR_REPLY_SOFT_MAX 4096

// Conn struct layout: hot fields packed in first 3 cache lines (192 bytes),
// large buffers (cmd[], reply_buf[], dur_reply_buf[]) at end to avoid cache
// pollution. make_conn's pool-reuse memset zeroes only [0, offsetof(cmd)),
// so every field that must read as 0 on a recycled conn goes before cmd[].
struct Conn {
    // --- cache line 1 (0-63): core state, accessed on every event ---
    Server *srv;
    Socket sock;
    char   state;       // see the STATE_* description
    char   type;        // combination of CONN_TYPE_* values
    byte   in_conns;    // 1 if the conn is in srv->conns heap, 0 otherwise
    byte   in_epollq;   // 1 if the conn is linked into prot.c's epollq list;
                        // guards epollq_add against double-insert (a second
                        // add in one tick would overwrite ->next, orphaning
                        // every conn queued earlier and leaving its epoll
                        // registration stale). Lives in padding before rw.
    int    rw;          // currently want: 'r', 'w', or 'h'
    int    pending_timeout; // -1 = forever
    Tube   *use;        // tube currently in use

    // --- cache line 2 (64-127): scheduling, job I/O ---
    Conn   *next;       // only used in epollq functions
    uint64 gen;         // generation counter, incremented on pool reuse;
                        // test-only / reserved: written by conn.c pool
                        // reuse, read only by tests
    int64  tickat;      // time at which to do more work; determines pos in heap
    size_t tickpos;     // position in srv->conns, stale when in_conns=0
    Conn   *live_next;  // intrusive list of ALL live conns (conn.c); lets
    Conn   *live_prev;  // conn_sched_recover rebuild srv->conns after a
                        // heapinsert OOM dropped a conn's timers
    Job    *soonest_job;// memoization of the soonest job
    Job    *out_job;    // a job to be sent to the client
    int64  last_activity_at; // ns timestamp of last command processed; powers -I
    int    out_job_sent;// how many bytes of *out_job were sent already
    char   halfclosed;

    // --- cache line 3 (128-191): reply, command metadata ---
    char *reply;
    int  reply_len;
    int  reply_sent;
    size_t cmd_len;
    size_t cmd_read;
    int64 in_job_read;
    Job   *in_job;              // a job to be read from the client

    // --- cache line 4+: watch, reserved jobs ---
    Ms     watch;               // set of watched tubes (upstream-compatible)
    size_t *waitpos;            // parallel to watch (cap >= watch.len while
                                // waiting): waitpos[i] is this conn's index in
                                // watch.items[i]->waiting_conns, kept fresh by
                                // on_waiting_conn_remove so waiting-set removal
                                // is O(1) ms_remove_at instead of a linear scan.
                                // Meaningful only while CONN_TYPE_WAITING is set.
    size_t waitpos_cap;         // allocated entries; freed in connclose. Both
                                // fields sit before cmd[] so the pool-reuse
                                // memset zeroes them (array freed by then).
    Job reserved_jobs;          // linked list header

    // --- deferred-reply batch for durable group commit (invariant #16) ---
    // in_dur_batch = 1 iff this conn has staged walwrite's this tick
    // whose ack is still waiting for the epoll-end walcommit(). When set,
    // reply() appends to dur_reply_buf instead of writing to the socket;
    // the flush pass in dur_flush_all() drains the buffer after fdatasync
    // completes (or sends INTERNAL_ERROR if the commit failed).
    // dur_batch_idx is the position in the global batch array, used by
    // connclose() for O(1) swap-remove.
    // These three scalars MUST stay before cmd[]: the pool-reuse memset
    // in make_conn re-zeroes them. A stale in_dur_batch on a recycled
    // conn would make reply() buffer acks into a conn that is absent
    // from dur_batch_arr, hanging the client. The buffer itself lives
    // in the large-buffer section below; its bytes are meaningful only
    // up to dur_reply_len, so it needs no zeroing on reuse.
    int    in_dur_batch;
    int    dur_batch_idx;
    int    dur_reply_len;

    // --- large buffers at end to avoid cache pollution; NOT zeroed on
    // pool reuse (memset stops at cmd[]) — contents are valid only up to
    // cmd_len / reply_len / dur_reply_len respectively ---
    char   cmd[LINE_BUF_SIZE];     // this string is NOT NUL-terminated
    char   reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated
    char   dur_reply_buf[DUR_REPLY_SOFT_MAX + LINE_BUF_SIZE];
};
int  conn_less(void *ca, void *cb);
void conn_setpos(void *c, size_t i);
void connsched(Conn *c);
int  conn_sched_recover(void);
void connclose(Conn *c);
void connsetproducer(Conn *c);
void connsetworker(Conn *c);
// Skip function call when type flag already set.
#define CONNSETPRODUCER(c) do { if (likely((c)->type & CONN_TYPE_PRODUCER)) {} else connsetproducer(c); } while(0)
#define CONNSETWORKER(c) do { if (likely((c)->type & CONN_TYPE_WORKER)) {} else connsetworker(c); } while(0)
Job *connsoonestjob(Conn *c);
int  conndeadlinesoon(Conn *c);
int conn_ready(Conn *c);
void conn_reserve_job(Conn *c, Job *j);
#define conn_waiting(c) ((c)->type & CONN_TYPE_WAITING)

// Durable group-commit batch (invariant #16). WAL-dirty callsites mark
// the conn via dur_enqueue() before reply(); reply() then appends the
// ack text to c->dur_reply_buf instead of writing to the socket. After
// the main loop's walcommit(), dur_flush_all(ok) drains every buffered
// conn — sending the acks on success, or INTERNAL_ERROR followed by
// shutdown(SHUT_WR) + close on commit failure (the FIN resolves every
// outstanding pipelined command as "connection closed" instead of
// leaving the client's reply stream misaligned). No-op outside durable
// mode.
//
// A pipelined STATE_SEND_JOB reply (peek/stats/reserve — never
// WAL-dirty itself) landing while acks are staged is deferred into the
// batch as well: reply() appends the header behind the acks, primes the
// SEND_JOB FSM and parks the fd out of epoll (rw=0) so no event can
// run the FSM before the commit; dur_flush_all re-arms 'w' on success
// and the FSM's writev emits acks+header+body in wire order. This keeps
// the group commit at ONE fdatasync per tick for the worker idiom
// "delete N\r\nreserve\r\n" (an inline walcommit here used to cost one
// fdatasync per occurrence).
//
// DUR_BATCH_MAX bounds how many distinct WAL-dirty conns can defer acks
// in one tick. When the batch is full, dur_enqueue drains it inline
// (walcommit + dur_flush_all — one extra fdatasync per DUR_BATCH_MAX
// dirty conns) and registers into the fresh batch, keeping the
// "ack ⇒ durable" contract. dur_enqueue returns 0 only when that inline
// commit failed: the staged record was rolled back and the WAL is
// disabled, so the caller must reply INTERNAL_ERROR instead of a
// success ack. Defined here so hostile tests exercise the boundary.
#ifndef DUR_BATCH_MAX
#define DUR_BATCH_MAX 256
#endif
int  dur_enqueue(Conn *c);
void dur_remove(Conn *c);
void dur_flush_all(int ok);
int  dur_batch_pending(void);

// Test-only hooks into prot.c's epollq (testprot2.c): build/drain the
// pending sockwant list on fake conns (sock.fd == -1) to check the
// in_epollq double-insert guard without a network race.
void  epollq_test_add(Conn *c, char rw);
void  epollq_test_apply(void);
Conn *epollq_test_head(void);




enum
{
    Filesizedef = (10 << 20)
};

struct Wal {
    int    filesize;
    int    use;
    char   *dir;
    File   *head;
    File   *cur;
    File   *tail;
    int    nfile;
    int    next;
    int64  resv;  // bytes reserved
    int64  alive; // bytes in use
    int64  nmig;  // migrations
    int64  nrec;  // records written ever
    int    wantsync; // do we sync to disk?
    int64  syncrate; // how often we sync to disk, in nanoseconds
    int    durable_sync; // -D: records are staged by walwrite (writev +
                         // accounting); one fdatasync per event-loop
                         // tick via walcommit (group commit, invariant
                         // #16). Replies are held by dur_flush_all
                         // until the commit, so "ack ⇒ durable" still
                         // holds.
    // commit_failed: a mid-tick WAL failure (rotation commit or writev)
    // disabled w->use after records staged earlier in the tick were
    // already lost (closed unsynced or ftruncated away).
    // walcommit() must report failure for that tick even though
    // w->use==0 — otherwise dur_flush_all(1) would ghost-ack clients
    // for records that never became durable (invariants #14/#16). Set
    // alongside w->use=0 in walg.c; cleared by the next walcommit().
    int    commit_failed;
    int64  lastsync;
    int64  lastcompact;

    // Per-Wal async fsync thread state.
    pthread_t       sync_thread;
    pthread_mutex_t sync_mu;
    pthread_cond_t  sync_cond;
    int             sync_fd;
    int             sync_stop;
    _Atomic int     sync_err;
    int             sync_on;
};
int  waldirlock(Wal*);
void walinit(Wal*, Job *list);
// walwrite stages a record: writev + accounting, fdatasync deferred.
// walcommit() issues one fdatasync covering every staged record in
// w->cur; the serv main loop calls it once per epoll drain (group
// commit — invariant #16).
int  walwrite(Wal*, Job*);
int  walcommit(Wal*);
int  walmaint(Wal*);
int  walresvput(Wal*, Job*);
int  walresvupdate(Wal*);
void walresvreturn(Wal*, int);
void walgc(Wal*);
void walsyncstart(Wal*);
void walsyncstop(Wal*);


// Buffered reader for WAL recovery — reduces syscalls by ~95%.
typedef struct ReadBuf {
    char buf[65536];
    int  pos;
    int  filled;
} ReadBuf;

struct File {
    File *next;
    uint refs;
    int  seq;
    int  iswopen; // is open for writing
    int  fd;
    int  free;
    int  resv;
    // uncommitted_bytes: sum of bytes staged by filewritev since the last
    // filewrcommit. file_stage_account increments it in EVERY mode (the
    // staging path is mode-independent), and walcommit → filewrcommit runs
    // once per main-loop tick regardless of mode, draining it back to 0 —
    // only the fdatasync inside filewrite_commit_durable is gated on
    // durable_sync. Used for group-commit rollback: if the deferred
    // fdatasync fails, we ftruncate this many bytes off the tail and undo
    // accounting (see filewrcommit).
    int  uncommitted_bytes;
    // uncommitted_alive: the portion of uncommitted_bytes still counted
    // in w->alive. filewrjobshort immediately undoes its own alive
    // contribution (#622 dead space), so on commit failure only this
    // value — not the whole batch — is reverted from w->alive;
    // reverting uncommitted_bytes would subtract short-record bytes
    // twice and drive alive negative.
    int  uncommitted_alive;
    char *path;
    Wal  *w;
    ReadBuf *rbuf; // optional buffered reader, set during recovery

    Job jlist;    // jobs written in this file
};
int  fileinit(File*, Wal*, int);
Wal* fileadd(File*, Wal*);
void fileincref(File*);
void filedecref(File*);
void fileaddjob(File*, Job*);
void filermjob(File*, Job*);
int  fileread(File*, Job *list);
void filewopen(File*);
void filewclose(File*);
// filewrjob{short,full} stage a WAL record (writev + accounting;
// fdatasync deferred). filewrcommit issues one fdatasync covering every
// staged record in f and either clears uncommitted_bytes on success or
// ftruncates the tail + rolls back global counters on failure. See
// invariant #16.
int  filewrjobshort(File*, Job*);
int  filewrjobfull(File*, Job*);
int  filewrcommit(File*);


#define Portdef "11300"

struct Server {
    char *port;
    char *addr;
    char *user;
    int  cpu;           // CPU core to pin main thread (-1 = no pinning)

    // Soft cap on simultaneous connections (-c N). 0 = unlimited.
    // When count_cur_conns() reaches this, h_accept closes the freshly
    // accepted fd without ever creating a Conn. EMFILE remains the kernel
    // hard cap; -c is the operator's explicit ceiling below it.
    uint   maxconn;

    // Idle connection timeout (-I SEC) in nanoseconds. 0 = off.
    // A conn is "idle" only when sitting in STATE_WANT_COMMAND with no
    // reserved jobs, no pending reserve-with-timeout, and not in a
    // waiting set — a worker blocked on reserve is NOT idle.
    int64  idle_timeout;

    // HTTP health endpoint on the beanstalk port (-H). 0 = off.
    // When enabled, a "GET ..." or "HEAD ..." command is intercepted
    // before the beanstalk dispatcher and answered with a minimal
    // HTTP/1.0 response, then the conn is closed. drain_mode → 503,
    // otherwise 200. No beanstalk client ever sends GET/HEAD as a
    // command verb, so wire compat is preserved when -H is on.
    int    http_health;

    Wal    wal;
    Socket sock;

    // Connections that must produce deadline or timeout, ordered by the time.
    Heap   conns;
};
void srv_acquire_wal(Server *s);
void srvserve(Server *s);

// Shutdown wake eventfd (self-pipe trick), serv.c. srv_wake_init
// creates the fd (call before installing signal handlers); srvserve
// registers it in the epoll set. srv_wake is async-signal-safe and is
// called from the SIGTERM/SIGUSR1 handlers after they set their flag,
// so a signal landing in the window between srvserve's flag check and
// the epoll syscall wakes the wait via fd readiness instead of leaving
// the server parked for the full timeout. No-op if never initialized.
int  srv_wake_init(void);
void srv_wake(void);
void srvaccept(Server *s, int ev);
