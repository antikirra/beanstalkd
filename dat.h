#include <stdint.h>
#include <stdlib.h>
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
    Walver = 7
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

struct Tube {
    uint refs;
    char name[MAX_TUBE_NAME_LEN];
    size_t name_len;                    // cached strlen(name)
    uint   name_hash;                   // cached tube_name_hash(name)
    Tube *ht_next; // hash table chain for global tube lookup
    Heap ready;
    Heap delay;
    size_t delay_heap_index;    // position in global delay tube heap
    int in_delay_heap;          // 1 if tube is in global delay heap
    size_t pause_heap_index;    // position in global pause tube heap
    int in_pause_heap;          // 1 if tube is in pause heap
    Ms waiting_conns;           // conns waiting for the job at this moment
    struct stats stat;
    uint using_ct;
    uint watching_ct;

    // pause is set to the duration of the current pause, otherwise 0, in nsec.
    int64 pause;

    // unpause_at is a timestamp when to unpause the tube, in nsec.
    int64 unpause_at;

    Job buried;                 // linked list header
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
void job_init_id(int worker_id, int nworkers);

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
uint  tube_name_hash(const char *name);
Tube *tube_find_or_make(const char *name);
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

// Migration message sent alongside the fd via SCM_RIGHTS.
#define MIG_MSG_MAGIC 0x4D494742  // "MIGB"
struct MigMsg {
    uint32 magic;
    char   use_tube[MAX_TUBE_NAME_LEN];    // c->use->name
    char   watch_tube[MAX_TUBE_NAME_LEN];  // c->watch->name
    char   cmd[LINE_BUF_SIZE];             // buffered command data
    size_t cmd_len;                         // bytes in cmd
    char   pending_reply[LINE_BUF_SIZE];   // reply to send after migration
    int    pending_reply_len;
    byte   type;                            // CONN_TYPE_* flags
};

void h_accept_migrated(int cfd, Server *s, struct MigMsg *mm);

// Command forwarding between workers for remote tube operations
// (stats-tube, pause-tube). Sent WITHOUT fd (no SCM_RIGHTS needed).
#define CMD_FWD_MAGIC  0x434D4446  // "CMDF"
#define CMD_REPLY_MAGIC 0x52504C59 // "RPLY"

struct CmdFwdMsg {
    uint32 magic;
    char   cmd[LINE_BUF_SIZE];
    size_t cmd_len;
    int    from_worker;           // who to send reply back to
    uint32 seq;                   // sequence number for reply routing
};

#define CMD_FWD_REPLY_SIZE 4096
struct CmdReplyMsg {
    uint32 magic;
    char   data[CMD_FWD_REPLY_SIZE];
    int    data_len;
    uint32 seq;                   // echo back sequence from CmdFwdMsg
};
// Put forwarding: forward a put command + body to the tube's owner worker.
// Uses a flexible array member for the body.
// Put forwarding body limit. AF_UNIX SOCK_SEQPACKET max message ~212KB
// (SO_SNDBUF default). Use job_data_size_limit default (65535) as cap.
#define PUT_FWD_MAGIC 0x50555446  // "PUTF"
#define PUT_FWD_MAX_BODY 65535
struct PutFwdMsg {
    uint32 magic;
    int    from_worker;
    uint32 seq;
    uint32 pri;
    int64  delay;
    int64  ttr;
    char   tube[MAX_TUBE_NAME_LEN];
    int    body_size;
    char   body[PUT_FWD_MAX_BODY + 2];
};

int  prot_replay(Server *s, Job *list);
int  pending_fwd_find(Server *s, uint32 seq);

// Control message from master to worker: update peer_fd for a restarted worker.
// Sent via ctl_fd with SCM_RIGHTS carrying the new socketpair end.
#define CTL_PEER_UPDATE_MAGIC 0x50455552  // "PEER"
struct CtlPeerUpdate {
    uint32 magic;
    int    peer_idx;    // which peer_fd slot to update
};


int make_server_socket(char *host, char *port);
int make_nonblocking(int fd);


// CONN_TYPE_* are bit masks used to track the type of connection.
// A put command adds the PRODUCER type, "reserve*" adds the WORKER type.
// If connection awaits for data, then it has WAITING type.
#define CONN_TYPE_PRODUCER 1
#define CONN_TYPE_WORKER   2
#define CONN_TYPE_WAITING  4

struct Conn {
    Server *srv;
    Socket sock;
    char   state;       // see the STATE_* description
    char   type;        // combination of CONN_TYPE_* values
    Conn   *next;       // only used in epollq functions
    Tube   *use;        // tube currently in use
    uint64 gen;         // generation counter, incremented on pool reuse
    int64  tickat;      // time at which to do more work; determines pos in heap
    size_t tickpos;     // position in srv->conns, stale when in_conns=0
    byte   in_conns;    // 1 if the conn is in srv->conns heap, 0 otherwise
    byte   fwd_pending; // 1 if waiting for forwarded command reply
    Job    *soonest_job;// memoization of the soonest job
    int    rw;          // currently want: 'r', 'w', or 'h'

    // How long client should "wait" for the next job; -1 means forever.
    int    pending_timeout;

    // Used to inform state machine that client no longer waits for the data.
    char   halfclosed;

    char   cmd[LINE_BUF_SIZE];     // this string is NOT NUL-terminated
    size_t cmd_len;
    size_t cmd_read;

    char *reply;
    int  reply_len;
    int  reply_sent;
    char reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated

    // How many bytes of in_job->body have been read so far. If in_job is NULL
    // while in_job_read is nonzero, we are in bit bucket mode and
    // in_job_read's meaning is inverted -- then it counts the bytes that
    // remain to be thrown away.
    int64 in_job_read;
    Job   *in_job;              // a job to be read from the client

    Job *out_job;               // a job to be sent to the client
    int out_job_sent;           // how many bytes of *out_job were sent already

    Tube   *watch;              // the single watched tube
    size_t  watch_idx;          // position in watch->waiting_conns for O(1) removal
    Job reserved_jobs;          // linked list header
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
    // Moved from walg.c statics to support multiple Wal instances (sharding).
    pthread_t       sync_thread;
    pthread_mutex_t sync_mu;
    pthread_cond_t  sync_cond;
    int             sync_fd;
    int             sync_stop;
    int             sync_err;
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

#define MAX_WORKERS 64

struct Server {
    char *port;
    char *addr;
    char *user;
    int  cpu;           // CPU core to pin main thread (-1 = no pinning)

    Wal    wal;
    Socket sock;

    // Connections that must produce deadline or timeout, ordered by the time.
    Heap   conns;

    // Per-shard WAL instances. Jobs are routed to shards by tube name hash.
    // Parallelizes disk I/O (writes + fsync) across CPU count threads.
    // NULL and nshards=0 when WAL is disabled or legacy single-WAL mode.
    Wal    *shards;
    int    nshards;

    // Multi-process workers. Master forks nworkers children, each with own
    // SO_REUSEPORT socket, epoll loop, tubes, and WAL shard.
    int    nworkers;          // 0 = legacy single-process mode
    int    worker_id;         // -1 for master, 0..N-1 for workers
    pid_t  worker_pids[MAX_WORKERS]; // master: child PIDs
    int    peer_fd[MAX_WORKERS];     // Unix sockets to peer workers
    int    ctl_fd;                   // control pipe from master (recv new peer fds)

    // Pending forwarded commands: hash table for concurrent in-flight forwards.
    // Each entry tracks the connection waiting for a reply, with generation
    // and sequence number to detect stale/reused Conn pointers.
    // Indexed by (seq & PENDING_FWD_MASK) with linear probing.
    #define PENDING_FWD_SLOTS 64
    #define PENDING_FWD_MASK  (PENDING_FWD_SLOTS - 1)
    struct {
        Conn   *conn;
        uint64  gen;      // conn generation at time of forward
        uint32  seq;      // sequence number sent with the forward
        int64   at;       // nanoseconds when forward was sent
    } pending_fwd[PENDING_FWD_SLOTS];
    uint32  pending_fwd_seq;        // next sequence number to assign
    int     pending_fwd_used;       // number of occupied slots (for scan skip)
};
void srv_acquire_wal(Server *s);
int  detect_ncpu(void);
void srvserve(Server *s);
void srvaccept(Server *s, int ev);

// SCM_RIGHTS fd passing for connection migration between workers.
int  send_fd(int sock, int fd, void *buf, size_t buflen);
int  recv_fd(int sock, void *buf, size_t buflen);

// Shared memory stats for cross-worker aggregation.
// Each worker writes its own slot; stats command sums all slots.
#define SHARED_STATS_OPS 26
#define SHARED_MAX_TUBES 1024
struct SharedStats {
    uint64 ready_ct;
    uint64 delayed_ct;
    uint64 buried_ct;
    uint64 reserved_ct;
    uint64 urgent_ct;
    uint64 waiting_ct;
    uint64 timeout_ct;
    uint64 total_jobs_ct;
    uint64 op_ct[SHARED_STATS_OPS];
    uint32 cur_conn_ct;
    uint32 cur_producer_ct;
    uint32 cur_worker_ct;
    uint32 tot_conn_ct;
    uint32 tube_count;
    char   tube_names[SHARED_MAX_TUBES][MAX_TUBE_NAME_LEN];
};

// Pointer to mmap'd array of nworkers SharedStats entries.
extern struct SharedStats *shared_stats;
