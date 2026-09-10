// Angry tests for filewopen (file.c) — it creates a binlog, preallocates
// it, stamps the version header and publishes f->free/f->resv. Every
// failure path has to leave nothing behind: no descriptor, no reference,
// and above all no half-born file for the next start to replay.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>

static void
wo_setup(void)
{
    fault_clear_all();
    progname = "testfile_filewopen";
}

static void
wo_path(char *buf, size_t n, const char *name)
{
    int k = snprintf(buf, n, "%s/%s", ctdir(), name);
    assertf(k > 0 && (size_t)k < n, "setup: fixture path did not fit");
    unlink(buf);
}

static void
wo_wal(Wal *w, File *f, char *path, int filesize)
{
    memset(w, 0, sizeof *w);
    memset(f, 0, sizeof *f);
    w->filesize = filesize;
    f->w = w;
    f->fd = -1;
    f->path = path;
}

static int
wo_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

// The next descriptor number open() will hand out; probing it before a
// failure lets the test prove the slot is still free afterwards.
static int
wo_probe_fd(void)
{
    int fd = open("/dev/null", O_RDONLY);
    assertf(fd >= 0, "setup: probe open");
    close(fd);
    return fd;
}

static int
wo_falloc_enospc(int fd, int len)
{
    UNUSED_PARAMETER(fd);
    UNUSED_PARAMETER(len);
    return ENOSPC;
}

static int
wo_falloc_ok(int fd, int len)
{
    UNUSED_PARAMETER(fd);
    UNUSED_PARAMETER(len);
    return 0;
}

// Honours the documented "may also reset fd's offset to 0" freedom by
// doing the opposite of what filewopen quietly assumes.
static int
wo_falloc_moves_offset(int fd, int len)
{
    UNUSED_PARAMETER(len);
    assertf(lseek(fd, 64, SEEK_SET) == 64, "setup: falloc double lseek");
    return 0;
}


// Contract (1): a binlog that could not be preallocated must not stay
// on disk. walscandir would pick it up on the next start and replay a
// file that holds a header and nothing else.
void
cttest_filewopen_unlinks_the_file_when_the_preallocation_fails(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9001");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 4096);

    FAlloc saved = falloc;
    falloc = wo_falloc_enospc;
    filewopen(&f);
    falloc = saved;

    assertf(!wo_exists(path),
            "a binlog whose space could not be reserved must be removed, "
            "not left for the next start to find");
}


// The same path, resource side: the descriptor was opened before the
// failure, so it is the one that can really leak.
void
cttest_filewopen_closes_its_descriptor_when_the_preallocation_fails(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9002");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 4096);

    int slot = wo_probe_fd();
    FAlloc saved = falloc;
    falloc = wo_falloc_enospc;
    filewopen(&f);
    falloc = saved;

    assertf(fcntl(slot, F_GETFD) == -1,
            "filewopen leaked descriptor %d on the preallocation-failure "
            "path", slot);

    unlink(path);
}


// A header write that lands PARTIALLY is not covered by the `n < 0`
// half of the guard. RLIMIT_FSIZE produces exactly that: two of the
// four header bytes on disk and a positive return.
void
cttest_filewopen_treats_a_short_header_write_as_a_failure(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9003");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 4096);

    struct sigaction ign, old;
    memset(&ign, 0, sizeof ign);
    memset(&old, 0, sizeof old);
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    assertf(sigaction(SIGXFSZ, &ign, &old) == 0, "setup: ignore SIGXFSZ");

    struct rlimit lim, oldlim;
    assertf(getrlimit(RLIMIT_FSIZE, &oldlim) == 0, "setup: getrlimit");
    lim = oldlim;
    lim.rlim_cur = 2;                 // room for half the version header
    assertf(setrlimit(RLIMIT_FSIZE, &lim) == 0, "setup: setrlimit");

    FAlloc saved = falloc;
    falloc = wo_falloc_ok;            // no preallocation to trip the limit
    filewopen(&f);
    falloc = saved;

    assertf(setrlimit(RLIMIT_FSIZE, &oldlim) == 0, "setup: restore rlimit");
    assertf(sigaction(SIGXFSZ, &old, NULL) == 0, "setup: restore SIGXFSZ");

    assertf(!f.iswopen,
            "a version header only half written leaves an unreadable "
            "binlog and must be treated as a failure");

    unlink(path);
}


void
cttest_filewopen_publishes_the_space_left_after_the_header(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9004");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 4096);

    FAlloc saved = falloc;
    falloc = wo_falloc_ok;
    filewopen(&f);
    falloc = saved;

    assertf(f.free == 4096 - (int)sizeof(int),
            "free space must be the preallocation minus the header: "
            "expected %d, got %d", 4096 - (int)sizeof(int), f.free);

    if (f.fd >= 0) close(f.fd);
    unlink(path);
}


// f->free feeds the reservation arithmetic and the closing truncate. A
// filesize smaller than the header makes it negative, which later turns
// "drop the unused tail" into "extend the file with a hole".
void
cttest_filewopen_must_not_publish_negative_free_space(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9005");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 3);         // smaller than the 4-byte header

    FAlloc saved = falloc;
    falloc = wo_falloc_ok;
    filewopen(&f);
    falloc = saved;

    assertf(!f.iswopen || f.free >= 0,
            "an opened binlog must never publish negative free space, "
            "got iswopen=%d free=%d", f.iswopen, f.free);

    if (f.fd >= 0) close(f.fd);
    unlink(path);
}


// rawfalloc's contract says it MAY reset the offset to 0 — "may", not
// "does". filewopen never seeks before stamping the header, so an
// allocator that leaves the offset elsewhere puts the version bytes in
// the middle of the file and preallocated zeroes at its start.
void
cttest_filewopen_writes_the_version_header_at_the_start_of_the_file(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9007");

    Wal w;
    File f;
    wo_wal(&w, &f, path, 4096);

    FAlloc saved = falloc;
    falloc = wo_falloc_moves_offset;
    filewopen(&f);
    falloc = saved;

    int fd = open(path, O_RDONLY);
    assertf(fd >= 0, "setup: reopen created binlog");
    int ver = 0;
    assertf(pread(fd, &ver, sizeof ver, 0) == (ssize_t)sizeof ver,
            "setup: pread header");
    close(fd);

    assertf(ver == Walver,
            "the version header must sit at offset 0 whatever the "
            "allocator left the offset at, got %d", ver);

    if (f.fd >= 0) close(f.fd);
    unlink(path);
}


// The binlog is opened without O_TRUNC, so whatever a file of the same
// name already held survives past the freshly written header. The only
// symptom is jobs coming back from a file this writer never wrote.
void
cttest_filewopen_leaves_no_stale_bytes_from_an_older_file_of_the_same_name(void)
{
    wo_setup();
    char path[512];
    wo_path(path, sizeof path, "binlog.9008");

    int seed = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    assertf(seed >= 0, "setup: seed file");
    char junk[4096];
    memset(junk, 0xAB, sizeof junk);
    assertf(write(seed, junk, sizeof junk) == (ssize_t)sizeof junk,
            "setup: fill the stale file");
    close(seed);

    Wal w;
    File f;
    wo_wal(&w, &f, path, 200);      // the writer claims only 200 bytes

    filewopen(&f);

    int fd = open(path, O_RDONLY);
    assertf(fd >= 0, "setup: reopen created binlog");
    unsigned char after_header[4] = {0xAB, 0xAB, 0xAB, 0xAB};
    assertf(pread(fd, after_header, 4, 4) == 4, "setup: pread past header");
    close(fd);

    unsigned char zero[4] = {0, 0, 0, 0};
    assertf(memcmp(after_header, zero, 4) == 0,
            "a freshly opened binlog must contain only its own bytes: the "
            "previous file's content survived past the header");

    if (f.fd >= 0) close(f.fd);
    unlink(path);
}
