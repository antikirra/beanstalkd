// Angry tests for the time base of WAL records (file.c).
//
// Contract: the two timestamps of a Jobrec, created_at and deadline_at,
// are WALL-CLOCK nanoseconds in the file — the meaning upstream v7 gave
// them, and the only one that survives a reboot: the monotonic clock
// the scheduler runs on restarts from zero with the host. So the writer
// translates on the way out, the reader on the way in, and whatever the
// file says, a delayed job never waits longer than its own delay and no
// job is younger than zero.
//
// Everything is driven through the public doors — filewrjobfull,
// filewrjobshort, fileread — over binlogs built in the test's temp dir.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define WT_S ((int64)1000000000)
#define WT_DAY (86400 * WT_S)

static void
wt_setup(void)
{
    fault_clear_all();
    progname = "testfile_waltime";
}

static void
wt_put(int fd, const void *p, size_t n)
{
    ssize_t w = write(fd, p, n);
    assertf(w == (ssize_t)n, "setup: short write of %zu bytes", n);
}

// Appends one full record the way the given format version lays it
// out: v8 carries a CRC32C trailer, v7 does not.
static void
wt_rec(int fd, int ver, const char *tube, const Jobrec *jr, const char *body)
{
    int namelen = (int)strlen(tube);
    wt_put(fd, &namelen, sizeof namelen);
    wt_put(fd, tube, (size_t)namelen);
    wt_put(fd, jr, sizeof *jr);
    wt_put(fd, body, (size_t)jr->body_size);
    if (ver < 8)
        return;
    uint32 c = WAL_CRC32C_INIT;
    c = wal_crc32c(c, &namelen, sizeof namelen);
    c = wal_crc32c(c, tube, (size_t)namelen);
    c = wal_crc32c(c, jr, sizeof *jr);
    c = wal_crc32c(c, body, (size_t)jr->body_size);
    c ^= WAL_CRC32C_XOR;
    unsigned char tr[4] = {
        (unsigned char)(c), (unsigned char)(c >> 8),
        (unsigned char)(c >> 16), (unsigned char)(c >> 24),
    };
    wt_put(fd, tr, sizeof tr);
}

static Jobrec
wt_delayed(uint64 id, int64 delay, int64 created_at, int64 deadline_at)
{
    Jobrec jr;
    memset(&jr, 0, sizeof jr);
    jr.id = id;
    jr.pri = 9;
    jr.delay = delay;
    jr.ttr = 60 * WT_S;
    jr.body_size = 3;
    jr.created_at = created_at;
    jr.deadline_at = deadline_at;
    jr.state = Delayed;
    return jr;
}

// Writes a one-record binlog of version ver and replays it. Returns
// the job the reader rebuilt.
static Job *
wt_replay_one(const char *name, int ver, const Jobrec *jr)
{
    static char path[512];
    int k = snprintf(path, sizeof path, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path");
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(fd >= 0, "setup: create fixture: %s", strerror(errno));
    wt_put(fd, &ver, sizeof ver);
    wt_rec(fd, ver, "default", jr, "d\r\n");
    char z[512];
    memset(z, 0, sizeof z);
    wt_put(fd, z, sizeof z);
    close(fd);

    static Wal w;
    static File f;
    static Job list;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    f.w = &w;
    f.path = path;
    f.fd = open(path, O_RDONLY);
    assertf(f.fd >= 0, "setup: open fixture: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&f, &w);
    w.cur = &f;
    fileread(&f, &list);
    close(f.fd);
    f.fd = -1;

    Job *j = job_find(jr->id);
    assertf(j != NULL, "setup: job %" PRIu64 " did not replay", jr->id);
    return j;
}

// A binlog open for writing, as walg hands it to the record writers.
static int
wt_wopen(char *path, size_t n, const char *name, Wal *w, File *f)
{
    int k = snprintf(path, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path");
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC, 0600);
    assertf(fd > 2, "setup: create binlog: %s", strerror(errno));
    int ver = Walver;
    wt_put(fd, &ver, sizeof ver);

    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = 1 << 20;
    w->use = 1;
    w->resv = 1 << 20;
    f->w = w;
    f->fd = fd;
    f->iswopen = 1;
    f->free = (1 << 20) - 4;
    f->resv = 1 << 20;
    f->refs = 1;
    w->head = f;
    w->tail = f;
    w->cur = f;
    return fd;
}

static Job *
wt_job(const char *tube, uint64 id, byte state, int64 delay,
       int64 created_at, int64 deadline_at)
{
    Tube *t = make_tube(tube);
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(3);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 9;
    j->r.delay = delay;
    j->r.ttr = 60 * WT_S;
    j->r.created_at = created_at;
    j->r.deadline_at = deadline_at;
    j->r.state = state;
    memcpy(j->body, "d\r\n", 3);
    j->walresv = 1 << 20;
    j->walused = 0;
    return j;
}


// The wall clock, read independently of the code under test.
static int64
wt_wall(void)
{
    struct timespec ts;
    assertf(clock_gettime(CLOCK_REALTIME, &ts) == 0, "setup: clock_gettime");
    return (int64)ts.tv_sec * WT_S + ts.tv_nsec;
}

// Everything in a Jobrec that is not a timestamp must reach the disk
// exactly as it is in memory.
static void
wt_same_but_time(const Jobrec *disk, const Jobrec *mem)
{
    assertf(disk->id == mem->id && disk->pri == mem->pri
            && disk->delay == mem->delay && disk->ttr == mem->ttr
            && disk->body_size == mem->body_size
            && disk->reserve_ct == mem->reserve_ct
            && disk->timeout_ct == mem->timeout_ct
            && disk->release_ct == mem->release_ct
            && disk->bury_ct == mem->bury_ct
            && disk->kick_ct == mem->kick_ct
            && disk->state == mem->state,
            "translating the timestamps must leave every other field of "
            "the record alone");
}


// The upgrade path README promises: a binlog written by upstream
// replays transparently. Upstream stamped the record with wall-clock
// time, 30s of delay still to run and a job created 100s ago.
static void
wt_upstream_record_keeps_its_time(const char *name, int ver)
{
    wt_setup();
    int64 wall = wt_wall();
    Jobrec jr = wt_delayed(701, 30 * WT_S, wall - 100 * WT_S,
                           wall + 30 * WT_S);

    Job *j = wt_replay_one(name, ver, &jr);
    int64 mono = nanoseconds();
    int64 left = j->r.deadline_at - mono;
    int64 age = mono - j->r.created_at;

    assertf(left > 25 * WT_S && left <= 30 * WT_S,
            "a job with 30s of delay left must replay with 30s left, "
            "got %" PRId64 "s", left / WT_S);
    assertf(age >= 99 * WT_S && age < 105 * WT_S,
            "a job created 100s ago must replay 100s old, got %" PRId64 "s",
            age / WT_S);
}

void
cttest_waltime_an_upstream_v7_delayed_job_keeps_its_time_left(void)
{
    wt_upstream_record_keeps_its_time("up7.binlog", 7);
}

void
cttest_waltime_a_wall_clock_v8_delayed_job_keeps_its_time_left(void)
{
    wt_upstream_record_keeps_its_time("up8.binlog", Walver);
}


// Downtime counts. A delay that ran out while the server was off must
// not start over, and must not wait at all.
void
cttest_waltime_a_delay_that_expired_while_down_replays_due(void)
{
    wt_setup();
    int64 wall = wt_wall();
    Jobrec jr = wt_delayed(702, 30 * WT_S, wall - 100 * WT_S,
                           wall - 5 * WT_S);

    Job *j = wt_replay_one("expired.binlog", Walver, &jr);

    assertf(j->r.deadline_at <= nanoseconds(),
            "a deadline 5s in the past must replay as already due, but "
            "%" PRId64 "s are left",
            (j->r.deadline_at - nanoseconds()) / WT_S);
}


// The reboot case, on a binlog an earlier build of this fork wrote: its
// timestamps are monotonic readings of a boot that is gone — here one
// that had been up 30 days longer than this one. Nothing can recover
// the true deadline, but the job asked for 30s and must not get more.
void
cttest_waltime_a_monotonic_stamp_from_another_boot_cannot_outwait_its_delay(void)
{
    wt_setup();
    int64 mono = nanoseconds();
    Jobrec jr = wt_delayed(703, 30 * WT_S, mono + 30 * WT_DAY - 10 * WT_S,
                           mono + 30 * WT_DAY + 20 * WT_S);

    Job *j = wt_replay_one("otherboot.binlog", Walver, &jr);
    int64 after = nanoseconds();
    int64 left = j->r.deadline_at - after;

    assertf(left <= 30 * WT_S,
            "a job put with a 30s delay must never wait longer than 30s, "
            "but %" PRId64 "s are left", left / WT_S);
    assertf(j->r.created_at <= after,
            "no job is created in the future: age would be %" PRId64 "s",
            (after - j->r.created_at) / WT_S);
}


// The wall clock is not ours: it can be stepped back between the write
// and the replay (NTP, a restored snapshot, a dead RTC battery). The
// file then claims an hour of delay for a job that asked for 30s.
void
cttest_waltime_a_wall_clock_step_cannot_stretch_a_delay(void)
{
    wt_setup();
    int64 wall = wt_wall();
    Jobrec jr = wt_delayed(704, 30 * WT_S, wall + 3570 * WT_S,
                           wall + 3600 * WT_S);

    Job *j = wt_replay_one("stepped.binlog", Walver, &jr);
    int64 after = nanoseconds();
    int64 left = j->r.deadline_at - after;

    assertf(left <= 30 * WT_S,
            "a job put with a 30s delay must never wait longer than 30s, "
            "but %" PRId64 "s are left", left / WT_S);
    assertf(j->r.created_at <= after,
            "no job is created in the future: age would be %" PRId64 "s",
            (after - j->r.created_at) / WT_S);
}


// The writer's half: what lands in the file is wall-clock time, so the
// next boot — or upstream — can read it.
static void
wt_writer_stamps_wall_time(const char *name, int full)
{
    wt_setup();
    char path[512];
    Wal w;
    File f;
    int fd = wt_wopen(path, sizeof path, name, &w, &f);

    int64 mono = nanoseconds();
    Job *j = wt_job("walltube", 705, Delayed, 30 * WT_S,
                    mono - 100 * WT_S, mono + 30 * WT_S);
    Jobrec mem = j->r;

    int r = full ? filewrjobfull(&f, j) : filewrjobshort(&f, j);
    assertf(r == 1, "setup: the record must be written, got %d", r);
    int64 wall = wt_wall();

    Jobrec disk;
    off_t at = 4 + 4 + (full ? (off_t)j->tube->name_len : 0);
    assertf(pread(fd, &disk, sizeof disk, at) == (ssize_t)sizeof disk,
            "setup: pread Jobrec");

    int64 dd = disk.deadline_at - (wall + 30 * WT_S);
    int64 dc = disk.created_at - (wall - 100 * WT_S);
    assertf(dd > -WT_S && dd < WT_S,
            "deadline_at in the file must be the wall-clock instant the "
            "delay ends; it is off by %" PRId64 "s", dd / WT_S);
    assertf(dc > -WT_S && dc < WT_S,
            "created_at in the file must be the wall-clock instant of the "
            "put; it is off by %" PRId64 "s", dc / WT_S);
    wt_same_but_time(&disk, &mem);
    assertf(memcmp(&j->r, &mem, sizeof mem) == 0,
            "writing a record must not change the job in memory: the "
            "scheduler keeps running on the monotonic clock");
    close(fd);
}

void
cttest_waltime_a_full_record_carries_wall_clock_time(void)
{
    wt_writer_stamps_wall_time("wfull.binlog", 1);
}

void
cttest_waltime_a_short_record_carries_wall_clock_time(void)
{
    wt_writer_stamps_wall_time("wshort.binlog", 0);
}


// A plain restart on the same boot. Binlogs written by earlier builds
// of this fork carry monotonic stamps, and on the boot that wrote them
// those are exactly right: the job must keep the 10s it has left, not
// lose them and not get its full 30s again.
void
cttest_waltime_a_monotonic_stamp_from_this_boot_is_kept(void)
{
    wt_setup();
    int64 mono = nanoseconds();
    Jobrec jr = wt_delayed(706, 30 * WT_S, mono - 50 * WT_S,
                           mono + 10 * WT_S);

    Job *j = wt_replay_one("sameboot.binlog", Walver, &jr);

    assertf(j->r.deadline_at == mono + 10 * WT_S,
            "the deadline must survive untouched, moved by %" PRId64 "ns",
            j->r.deadline_at - (mono + 10 * WT_S));
    assertf(j->r.created_at == mono - 50 * WT_S,
            "created_at must survive untouched, moved by %" PRId64 "ns",
            j->r.created_at - (mono - 50 * WT_S));
}


// What the writer puts down the reader must take back up: a job that
// goes through the file returns on the scheduler's clock where it left.
void
cttest_waltime_a_written_job_replays_with_the_time_it_had(void)
{
    wt_setup();
    char path[512];
    Wal w;
    File f;
    int fd = wt_wopen(path, sizeof path, "round.binlog", &w, &f);

    int64 mono = nanoseconds();
    Job *j = wt_job("roundtube", 707, Delayed, 30 * WT_S,
                    mono - 100 * WT_S, mono + 20 * WT_S);
    assertf(filewrjobfull(&f, j) == 1, "setup: the record must be written");
    close(fd);

    Wal rw;
    File rf;
    Job list;
    memset(&rw, 0, sizeof rw);
    memset(&rf, 0, sizeof rf);
    rf.w = &rw;
    rf.path = path;
    rf.fd = open(path, O_RDONLY);
    assertf(rf.fd >= 0, "setup: reopen binlog: %s", strerror(errno));
    job_list_reset(&list);
    fileadd(&rf, &rw);
    rw.cur = &rf;
    fileread(&rf, &list);
    close(rf.fd);

    Job *got = list.next;
    assertf(got != &list && got->r.id == 707, "setup: job 707 did not replay");

    int64 dd = got->r.deadline_at - (mono + 20 * WT_S);
    int64 dc = got->r.created_at - (mono - 100 * WT_S);
    assertf(dd > -WT_S / 10 && dd < WT_S / 10,
            "the deadline moved by %" PRId64 "ns on its way through the file",
            dd);
    assertf(dc > -WT_S / 10 && dc < WT_S / 10,
            "created_at moved by %" PRId64 "ns on its way through the file",
            dc);
}


// reserve saturates a huge TTR's deadline to INT64_MAX. Translating
// that to another time base must saturate too, not wrap into the past.
void
cttest_waltime_a_saturated_deadline_stays_saturated_in_the_file(void)
{
    wt_setup();
    char path[512];
    Wal w;
    File f;
    int fd = wt_wopen(path, sizeof path, "sat.binlog", &w, &f);

    Job *j = wt_job("sattube", 708, Reserved, 0, nanoseconds(), INT64_MAX);
    assertf(filewrjobfull(&f, j) == 1, "setup: the record must be written");

    Jobrec disk;
    off_t at = 4 + 4 + (off_t)j->tube->name_len;
    assertf(pread(fd, &disk, sizeof disk, at) == (ssize_t)sizeof disk,
            "setup: pread Jobrec");
    assertf(disk.deadline_at == INT64_MAX,
            "a deadline of INT64_MAX must stay INT64_MAX in the file, got "
            "%" PRId64, disk.deadline_at);
    close(fd);
}
