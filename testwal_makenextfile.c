// Angry tests for makenextfile (walg.c) — the creation of a binlog.
//
// It has to produce a file the reader will later accept, register it in
// the right place, advance the sequence exactly once, and make the new
// name durable. On failure it has to look as if it was never called: a
// bumped sequence strands a number forever, and a half-registered file
// is one walgc away from a double free.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
mk_setup(void)
{
    fault_clear_all();
    progname = "testwal_makenextfile";
    now = 0;
}

static void
mk_wal(Wal *w, Job *l, char *dir)
{
    memset(w, 0, sizeof *w);
    memset(l, 0, sizeof *l);
    l->prev = l->next = l;
    w->dir = dir;
    w->filesize = 4096;
    w->use = 1;
    walinit(w, l);
    assertf(w->nfile == 1, "setup: a fresh wal holds exactly one file");
}

static void
mk_subdir(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: subdir path did not fit");
    assertf(mkdir(buf, 0700) == 0, "setup: mkdir %s: %s", buf, strerror(errno));
}

static Job *
mk_job(uint64 id, int body)
{
    Tube *t = make_tube("k");
    assertf(t != NULL, "setup: make_tube");
    Job *j = allocate_job(body);
    assertf(j != NULL, "setup: allocate_job");
    TUBE_ASSIGN(j->tube, t);
    j->r.id = id;
    j->r.pri = 5;
    j->r.ttr = 120000000000LL;
    j->r.body_size = body;
    j->r.state = Ready;
    memset(j->body, 'k', (size_t)body);
    j->body[body-2] = '\r';
    j->body[body-1] = '\n';
    return j;
}

static int
mk_binlogs(const char *dir)
{
    DIR *d = opendir(dir);
    assertf(d != NULL, "setup: opendir %s: %s", dir, strerror(errno));
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)))
        if (strncmp(e->d_name, "binlog.", 7) == 0) n++;
    assertf(closedir(d) == 0, "setup: closedir");
    return n;
}

static int
mk_count(Job *l)
{
    int n = 0;
    for (Job *j = l->next; j != l; j = j->next) n++;
    return n;
}

static int
mk_exists(const char *dir, int seq)
{
    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.%d", dir, seq);
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    return stat(path, &st) == 0;
}

// Reserves delete slots until the current file runs out of room, which
// is the production route to a second binlog. Stops as soon as the wal
// grows or a reservation is refused.
static void
mk_try_grow(Wal *w)
{
    for (int i = 0; i < 500; i++) {
        if (w->nfile > 1) return;
        if (walresvupdate(w) == 0) return;
    }
    assertf(0, "setup: the wal neither grew nor refused a reservation");
}


// The new binlog is preallocated to the configured size. The reservation
// arithmetic hands out exactly that many bytes, so a file that is any
// smaller is a file the wal will overrun.
void
cttest_makenextfile_preallocates_the_new_binlog_to_the_configured_size(void)
{
    mk_setup();
    Wal w;
    Job list;
    mk_wal(&w, &list, ctdir());

    char path[512];
    int k = snprintf(path, sizeof path, "%s/binlog.1", ctdir());
    assertf(k > 0 && (size_t)k < sizeof path, "setup: fixture path did not fit");
    struct stat st;
    assertf(stat(path, &st) == 0, "setup: stat %s: %s", path, strerror(errno));

    assertf(st.st_size == (off_t)w.filesize,
            "the binlog must be preallocated to %d bytes, it is %lld",
            w.filesize, (long long)st.st_size);
}


// A new binlog is empty except for its version header, and its free
// count has to say so — the whole rest of the file is what reservations
// are handed out of.
void
cttest_makenextfile_offers_every_byte_but_the_version_header(void)
{
    mk_setup();
    Wal w;
    Job list;
    mk_wal(&w, &list, ctdir());

    assertf(w.cur->free == w.filesize - (int)sizeof(int),
            "a fresh binlog offers filesize minus the header (%d bytes), "
            "it reports %d", w.filesize - (int)sizeof(int), w.cur->free);
}


// ...and nothing is reserved in it yet.
void
cttest_makenextfile_starts_the_new_binlog_with_no_reservation(void)
{
    mk_setup();
    Wal w;
    Job list;
    mk_wal(&w, &list, ctdir());

    assertf(w.cur->resv == 0,
            "a fresh binlog has no reservations, it reports %d", w.cur->resv);
}


// The new file goes on the end of the list, and the sequence advances
// past it exactly once. Both are read by walgc and by the next
// allocation.
void
cttest_makenextfile_appends_the_new_binlog_and_advances_the_sequence(void)
{
    mk_setup();
    Wal w;
    Job list;
    mk_wal(&w, &list, ctdir());
    int seq = w.next;

    mk_try_grow(&w);

    assertf(w.tail->seq == seq && w.next == seq + 1,
            "the new file must be the tail with sequence %d and leave next "
            "at %d: tail is binlog.%d and next is %d",
            seq, seq + 1, w.tail->seq, w.next);
}


// The directory entry for a new binlog is only durable after the
// directory itself is synced. Losing that sync loses the file name on a
// crash even though every byte of its contents was written.
void
cttest_makenextfile_syncs_the_directory_for_the_name_it_created(void)
{
    mk_setup();
    Wal w;
    Job list;
    memset(&w, 0, sizeof w);
    memset(&list, 0, sizeof list);
    list.prev = list.next = &list;
    w.dir = ctdir();
    w.filesize = 4096;
    w.use = 1;
    fault_clear_all();

    walinit(&w, &list);

    assertf(fault_calls(FAULT_FDATASYNC) == 1,
            "creating one binlog owes exactly one directory sync, got %d "
            "fdatasync() call(s)", fault_calls(FAULT_FDATASYNC));
}


// "The file exists" is not the promise. The promise is a file the
// recovery reader will accept, header and all — which is only visible by
// writing a record into it and replaying the directory afresh.
void
cttest_makenextfile_writes_a_binlog_a_later_replay_can_read(void)
{
    mk_setup();
    Wal w;
    Job list;
    mk_wal(&w, &list, ctdir());
    Job *j = mk_job(802, 12);
    j->walresv = walresvput(&w, j);
    assertf(j->walresv > 0, "setup: the reservation must succeed");
    assertf(walwrite(&w, j) != 0, "setup: the record must stage");
    assertf(walcommit(&w) == 1, "setup: the commit must succeed");

    Wal again;
    Job replayed;
    memset(&again, 0, sizeof again);
    memset(&replayed, 0, sizeof replayed);
    replayed.prev = replayed.next = &replayed;
    again.dir = ctdir();
    again.filesize = 4096;
    again.use = 1;
    walinit(&again, &replayed);

    assertf(mk_count(&replayed) == 1,
            "the binlog this call created must be replayable: the second "
            "recovery found %d job(s)", mk_count(&replayed));
}


// A creation that failed must leave nothing behind: no registered file
// and no stray binlog in the directory.
void
cttest_makenextfile_leaves_the_directory_untouched_when_it_fails(void)
{
    mk_setup();
    char dir[512];
    mk_subdir(dir, sizeof dir, "fail");
    Wal w;
    Job list;
    mk_wal(&w, &list, dir);
    int before = mk_binlogs(dir);
    fault_set(FAULT_OPEN, 0, EACCES);

    mk_try_grow(&w);

    assertf(mk_binlogs(dir) == before,
            "a failed creation must leave no file behind: the directory "
            "held %d binlog(s) and now holds %d", before, mk_binlogs(dir));
}


// ...and it must not have registered anything either, or walgc will one
// day walk into a File that was freed on the failure path.
void
cttest_makenextfile_registers_no_file_when_it_fails(void)
{
    mk_setup();
    char dir[512];
    mk_subdir(dir, sizeof dir, "unregistered");
    Wal w;
    Job list;
    mk_wal(&w, &list, dir);
    fault_set(FAULT_OPEN, 0, EACCES);

    mk_try_grow(&w);

    assertf(w.nfile == 1 && w.tail == w.head,
            "nothing was created, so the wal must still hold its one "
            "original file: nfile is %d", w.nfile);
}


// The sequence the failed attempt would have used must still be the next
// one, so a retry produces the file the failure was going to.
void
cttest_makenextfile_reuses_the_sequence_after_a_failed_attempt(void)
{
    mk_setup();
    char dir[512];
    mk_subdir(dir, sizeof dir, "retry");
    Wal w;
    Job list;
    mk_wal(&w, &list, dir);
    int seq = w.next;
    fault_set(FAULT_OPEN, 0, EACCES);
    mk_try_grow(&w);
    assertf(w.nfile == 1, "setup: the first attempt must have failed");
    fault_clear_all();

    mk_try_grow(&w);

    assertf(mk_exists(dir, seq),
            "the retry must use the sequence the failed attempt reserved: "
            "binlog.%d is not in the directory", seq);
}
