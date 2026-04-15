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

extern struct Server srv;
extern volatile sig_atomic_t shutdown_requested;

// Replaced by tests to simulate failures.
extern FAlloc falloc;

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


// less_fn is used by the binary heap to determine the order of elements.
typedef int(*less_fn)(void*, void*);

// setpos_fn is used by the binary heap to record the new positions of elements
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

    // added value is platform dependend: on OSX it can be > 1.
    // Value of 1 - socket was already added to event notifications,
    // otherwise it is 0.
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
// v5: legacy, us-resolution timestamps.

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
    // (8 bytes remaining in CL4)

    // --- cold fields: lookup, list management ---
    Tube *ht_next;                      // hash table chain for global tube lookup
    size_t name_len;                    // cached strlen(name)
    Job buried;                         // linked list header
    char name[MAX_TUBE_NAME_LEN];       // tube name (cold: only on lookup/stats)
};


// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>: <errno_msg>
#define twarn(...) __twarn(__VA_ARGS__, "")

// Hack to quiet the compiler. When VA_ARGS in twarn() has one element,
// e.g. twarn("OOM"), its replaced with __twarn("OOM", ""),
// thus VA_ARGS is expanded to at least one element in warn().
#define __twarn(fmt, ...) \
    warn("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

// Prints warning message on stderr in the format:
// <progname>: FILE:LINE in FUNC: <fmt>
#define twarnx(...) __twarnx(__VA_ARGS__, "")

// See __twarn macro.
#define __twarnx(fmt, ...) \
    warnx("%s:%d in %s: " fmt "%s", __FILE__, __LINE__, __func__, __VA_ARGS__)

void warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void warnx(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
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
Job *make_job_with_id(uint pri, int64 delay, int64 ttr,
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

/* for unit tests */
size_t get_all_jobs_used(void);


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
Tube *tube_find_name_h(const char *name, size_t len, uint hash);
uint  tube_name_hash(const char *name);
Tube *tube_find_or_make(const char *name);
Tube *tube_find_or_make_n(const char *name, size_t len);
#define TUBE_ASSIGN(a,b) do { \
    Tube *_tb = (b); \
    if ((a) != _tb) { tube_dref(a); (a) = _tb; tube_iref(a); } \
} while(0)


Conn *make_conn(int fd, char start_state, Tube *use, Tube *watch);

uint count_cur_conns(void);
uint count_tot_conns(void);
uint count_cur_producers(void);
uint count_cur_workers(void);


extern size_t primes[];


extern size_t job_data_size_limit;

void prot_init(void);
int64 prottick(Server *s);
void prot_remove_tube(Tube *t);

void remove_waiting_conn(Conn *c);

void enqueue_reserved_jobs(Conn *c);

void enter_drain_mode(int sig);
void h_accept(const int fd, const short which, Server *s);

int  prot_replay(Server *s, Job *list);


int make_server_socket(char *host, char *port);
int make_nonblocking(int fd);


// CONN_TYPE_* are bit masks used to track the type of connection.
// A put command adds the PRODUCER type, "reserve*" adds the WORKER type.
// If connection awaits for data, then it has WAITING type.
#define CONN_TYPE_PRODUCER 1
#define CONN_TYPE_WORKER   2
#define CONN_TYPE_WAITING  4

// Conn struct layout: hot fields packed in first 3 cache lines (192 bytes),
// large buffers (cmd[], reply_buf[]) at end to avoid cache pollution.
struct Conn {
    // --- cache line 1 (0-63): core state, accessed on every event ---
    Server *srv;
    Socket sock;
    char   state;       // see the STATE_* description
    char   type;        // combination of CONN_TYPE_* values
    byte   in_conns;    // 1 if the conn is in srv->conns heap, 0 otherwise
    int    rw;          // currently want: 'r', 'w', or 'h'
    int    pending_timeout; // -1 = forever
    Tube   *use;        // tube currently in use

    // --- cache line 2 (64-127): scheduling, job I/O ---
    Conn   *next;       // only used in epollq functions
    uint64 gen;         // generation counter, incremented on pool reuse
    int64  tickat;      // time at which to do more work; determines pos in heap
    size_t tickpos;     // position in srv->conns, stale when in_conns=0
    Job    *soonest_job;// memoization of the soonest job
    Job    *out_job;    // a job to be sent to the client
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
    Job reserved_jobs;          // linked list header

    // --- large buffers at end to avoid cache pollution ---
    char   cmd[LINE_BUF_SIZE];     // this string is NOT NUL-terminated
    char   reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated
};
int  conn_less(void *ca, void *cb);
void conn_setpos(void *c, size_t i);
void connsched(Conn *c);
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
    int64  lastsync;
    int64  lastcompact;

    // Per-Wal async fsync thread state.
    // Async fsync thread state.
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
int  walwrite(Wal*, Job*);
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
int  filewrjobshort(File*, Job*);
int  filewrjobfull(File*, Job*);


#define Portdef "11300"

struct Server {
    char *port;
    char *addr;
    char *user;
    int  cpu;           // CPU core to pin main thread (-1 = no pinning)

    Wal    wal;
    Socket sock;

    // Connections that must produce deadline or timeout, ordered by the time.
    Heap   conns;
};
void srv_acquire_wal(Server *s);
void srvserve(Server *s);
void srvaccept(Server *s, int ev);
