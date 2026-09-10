#include "ct/ct.h"
#include "dat.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

// Runs optparse in a child so that a usage() exit can be observed rather
// than merely survived, and returns the child's exit status. Negative
// results mean the child never exited normally at all.
static int
optparse_exit_status(char **args)
{
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        FILE *quiet = freopen("/dev/null", "w", stderr);
        (void)quiet;
        optparse(&srv, args);
        _exit(86); // optparse returned where it promised to exit
    }
    int status = 0;
    if (waitpid(pid, &status, 0) != pid) return -2;
    if (!WIFEXITED(status)) return -3;
    return WEXITSTATUS(status);
}

void
cttest_allocf()
{
    char *got;

    got = fmtalloc("hello, %s %d", "world", 5);
    assertf(strcmp("hello, world 5", got) == 0, "got \"%s\"", got);
    free(got);

    // Zero-length result: the sizing pass must still leave room for the
    // NUL and hand back an empty C string, not NULL and not one byte of
    // whatever malloc had lying around.
    got = fmtalloc("%s", "");
    assertf(got != NULL && got[0] == 0,
        "an empty result must be an empty string, got [%s]",
        got ? got : "(null)");
    free(got);

    // A result far longer than any stack buffer, with a literal '%' at
    // the very end: the size pass must measure the WHOLE rendering, and
    // the second pass must write all of it.
    char big[513];
    memset(big, 'q', 512);
    big[512] = 0;
    got = fmtalloc("<%s>%%", big);
    assertf(got != NULL && strlen(got) == 515 && got[0] == '<'
            && memcmp(got + 1, big, 512) == 0
            && got[513] == '>' && got[514] == '%',
        "a 515-byte result must come back whole, got %zu bytes",
        got ? strlen(got) : (size_t)0);
    free(got);

    // U+00E9 has no representation in the C locale, so vsnprintf reports
    // an encoding error instead of a length. The promise callers such as
    // fileinit are written against is NULL — never a block whose contents
    // were never written.
    wchar_t wide[] = { 0x00e9, 0 };
    got = fmtalloc("%ls", wide);
    assertf(got == NULL,
        "an unformattable argument must yield NULL, got [%s]",
        got ? got : "(null)");
    free(got);
}

void
cttest_opt_none()
{
    char *args[] = {
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.port, Portdef) == 0);
    assert(srv.addr == NULL);
    assert(job_data_size_limit == JOB_DATA_SIZE_LIMIT_DEFAULT);
    assert(srv.wal.filesize == Filesizedef);
    assert(srv.wal.wantsync == 1);
    assert(srv.wal.syncrate == DEFAULT_FSYNC_MS*1000000);
    assert(srv.user == NULL);
    assert(srv.wal.dir == NULL);
    assert(srv.wal.use == 0);
    assert(verbose == 0);

    // Comparing a default against the macro that defines it cannot fail.
    // These are the numbers README and doc/protocol.txt promise the
    // operator and every client library ships with, so pin the values,
    // not the spelling.
    assertf(strcmp(srv.port, "11300") == 0,
        "the documented default port is 11300, got %s", srv.port);
    assertf(job_data_size_limit == 65535,
        "the documented default max job size is 65535, got %zu",
        job_data_size_limit);
    assertf(srv.wal.filesize == 10485760,
        "the documented default WAL file size is 10MB, got %d",
        srv.wal.filesize);
    assertf(srv.wal.syncrate == 50000000,
        "the documented default fsync interval is 50ms, got %lld",
        (long long)srv.wal.syncrate);

    // An argv of {NULL} never enters optparse's loop, so everything above
    // holds just as well for an optparse whose body was deleted. Parse a
    // real flag: it must take effect, and it must leave every other
    // default exactly where it was.
    char *one[] = {
        "-V",
        NULL,
    };

    optparse(&srv, one);
    assertf(verbose == 1,
        "optparse must actually consume argv, got verbose=%d", verbose);
    assertf(strcmp(srv.port, Portdef) == 0
            && srv.addr == NULL
            && job_data_size_limit == JOB_DATA_SIZE_LIMIT_DEFAULT
            && srv.wal.filesize == Filesizedef
            && srv.wal.wantsync == 1
            && srv.wal.syncrate == DEFAULT_FSYNC_MS*1000000
            && srv.user == NULL
            && srv.wal.dir == NULL
            && srv.wal.use == 0,
        "parsing one unrelated flag must not disturb any other default");
}

void
cttest_optminus()
{
    char *args[] = {
        "-",
        NULL,
    };

    // "Some kind of termination happened" is not the contract: a bare "-"
    // is a rejected argument, and the process must say so with the usage
    // status. Exiting 0 would tell an init system the daemon shut down
    // cleanly on a command line it never accepted.
    int status = optparse_exit_status(args);
    assertf(status == 5,
        "a bare \"-\" must exit with the usage status 5, got %d", status);
}

void
cttest_optp()
{
    char *args[] = {
        "-p1234",
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.port, "1234") == 0);

    char *separate[] = {
        "-p",
        "5678",
        NULL,
    };

    optparse(&srv, separate);
    assertf(strcmp(srv.port, "5678") == 0,
        "-p must also take a separated argument, got %s", srv.port);

    // The other half of the flag's contract: a flag that needs an
    // argument and does not get one is a usage error, not a silent
    // fallback to the default port.
    char *missing[] = {
        "-p",
        NULL,
    };

    int status = optparse_exit_status(missing);
    assertf(status == 5,
        "-p with no argument must exit with the usage status 5, got %d",
        status);
}

void
cttest_optl()
{
    char *args[] = {
        "-llocalhost",
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.addr, "localhost") == 0);
}

void
cttest_optlseparate()
{
    char *args[] = {
        "-l",
        "localhost",
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.addr, "localhost") == 0);
}

void
cttest_optz()
{
    char *args[] = {
        "-z1234",
        NULL,
    };

    optparse(&srv, args);
    assert(job_data_size_limit == 1234);
}

void
cttest_optz_more_than_max()
{
    char *args[] = {
        "-z1073741825",
        NULL,
    };

    optparse(&srv, args);
    assert(job_data_size_limit == 1073741824);
}

void
cttest_opts()
{
    char *args[] = {
        "-s1234",
        NULL,
    };

    optparse(&srv, args);
    assert(srv.wal.filesize == 1234);

    // Top of the range usage() documents. wal.filesize is an int, and a
    // clamp that fires one value early would quietly shrink an operator's
    // largest legal WAL segment.
    char *at_max[] = {
        "-s2147483647",
        NULL,
    };

    optparse(&srv, at_max);
    assertf(srv.wal.filesize == INT_MAX,
        "-s at the documented maximum must be kept exactly, got %d",
        srv.wal.filesize);

    // One past the range: parse_size_t reads it as a size_t, and the
    // narrowing to int is where #722 lived. Whatever policy the flag
    // applies to an out-of-range value, the result must still be a
    // usable file size — never a wrapped negative one.
    char *over[] = {
        "-s2147483648",
        NULL,
    };

    optparse(&srv, over);
    assertf(srv.wal.filesize >= 1,
        "an out-of-range -s must never narrow to a non-positive size, got %d",
        srv.wal.filesize);
}

void
cttest_optf()
{
    char *args[] = {
        "-f1234",
        NULL,
    };

    // wantsync is already 1 in the srv initializer; without a sentinel
    // the assertion below cannot tell "-f enabled it" from "nobody
    // touched it".
    srv.wal.wantsync = 0;
    srv.wal.durable_sync = 0;

    optparse(&srv, args);
    assert(srv.wal.syncrate == 1234000000);
    assert(srv.wal.wantsync == 1);
    assertf(srv.wal.durable_sync == 0,
        "-f must not turn on durable group commit, got %d",
        srv.wal.durable_sync);

    // "use -f0 for always fsync": zero is a documented value of this
    // flag, not an out-of-range input to be replaced by the default.
    char *always[] = {
        "-f0",
        NULL,
    };

    srv.wal.syncrate = 987;
    srv.wal.wantsync = 0;

    optparse(&srv, always);
    assertf(srv.wal.syncrate == 0,
        "-f0 must mean always fsync (interval 0), got %lld",
        (long long)srv.wal.syncrate);
    assertf(srv.wal.wantsync == 1,
        "-f0 must still ask for fsync, got %d", srv.wal.wantsync);
}

void
cttest_optF()
{
    char *args[] = {
        "-f1234",
        "-F",
        NULL,
    };

    srv.wal.durable_sync = 0;

    optparse(&srv, args);
    assert(srv.wal.wantsync == 0);
    // -F turns the async sync thread off. It is not -D, and it is not a
    // reset: the interval the operator set with -f, and the durability
    // mode they did not ask for, are none of its business.
    assertf(srv.wal.syncrate == 1234000000LL,
        "-F must leave the -f interval alone, got %lld",
        (long long)srv.wal.syncrate);
    assertf(srv.wal.durable_sync == 0,
        "-F must not imply durable group commit, got %d",
        srv.wal.durable_sync);
}

void
cttest_optu()
{
    char *args[] = {
        "-ukr",
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.user, "kr") == 0);
}

void
cttest_optb()
{
    char *args[] = {
        "-bfoo",
        NULL,
    };

    optparse(&srv, args);
    assert(strcmp(srv.wal.dir, "foo") == 0);
    assert(srv.wal.use == 1);
}

void
cttest_optV()
{
    char *args[] = {
        "-V",
        NULL,
    };

    optparse(&srv, args);
    assert(verbose == 1);
}

void
cttest_optV_V()
{
    char *args[] = {
        "-V",
        "-V",
        NULL,
    };

    optparse(&srv, args);
    assert(verbose == 2);
}

void
cttest_optVVV()
{
    char *args[] = {
        "-VVV",
        NULL,
    };

    optparse(&srv, args);
    assert(verbose == 3);
}

void
cttest_optVFVu()
{
    char *args[] = {
        "-VFVukr",
        NULL,
    };

    optparse(&srv, args);
    assert(verbose == 2);
    assert(srv.wal.wantsync == 0);
    assert(strcmp(srv.user, "kr") == 0);

    // The tail of a bundle belongs to the flag that takes an argument,
    // whole, even when it is spelled out of flag characters. Parsing
    // "VFV" as three more flags would drop privileges to the wrong user
    // AND silently turn fsync off.
    char *flaggy[] = {
        "-uVFV",
        NULL,
    };

    verbose = 0;
    srv.wal.wantsync = 1;

    optparse(&srv, flaggy);
    assertf(strcmp(srv.user, "VFV") == 0,
        "-uVFV must set the user to VFV, got %s", srv.user);
    assertf(verbose == 0,
        "the argument of -u must not be parsed as flags, got verbose=%d",
        verbose);
    assertf(srv.wal.wantsync == 1,
        "the argument of -u must not be parsed as -F, got wantsync=%d",
        srv.wal.wantsync);
}
