// Angry tests for vwarnx (util.c), reached through warn()/warnx() — it
// is static, and the mode switch is only observable from outside.
//
// It has two jobs: consult log_json at CALL time, and emit exactly one
// line per call in whichever mode that is. The text form
// "<progname>: <msg>[: <errno text>]" is what every existing operator
// script greps; the progname prefix belongs to text mode alone and must
// never appear inside a JSON record. Text mode reaches stderr through
// three or four separate stdio calls, so the "one line per call"
// promise is a great deal weaker there than in the JSON path — which is
// what the concurrency test below is for.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void
vw_setup(void)
{
    fault_clear_all();
    progname = "beanstalkd";
    log_json = 0;
}


static void
vw_capture(const char *path)
{
    fflush(stderr);
    assertf(freopen(path, "w+", stderr) != NULL,
            "setup: freopen stderr: %s", strerror(errno));
    setvbuf(stderr, NULL, _IONBF, 0);
}


static void
vw_slurp(const char *path, char *buf, size_t len)
{
    FILE *f = fopen(path, "r");
    assertf(f != NULL, "setup: fopen %s: %s", path, strerror(errno));
    size_t n = fread(buf, 1, len - 1, f);
    fclose(f);
    buf[n] = 0;
}


static const char *vw_payload =
    "0123456789 abcdefghij 0123456789 abcdefghij";

static int vw_rounds;

static void *
vw_spam(void *unused)
{
    UNUSED_PARAMETER(unused);
    for (int i = 0; i < vw_rounds; i++) {
        warnx("%s", vw_payload);
    }
    return NULL;
}


// Returns the index of the first line that is not exactly `want`, or -1;
// *count receives the number of lines seen.
static int
vw_first_line_unlike(const char *buf, const char *want, int *count)
{
    size_t wantlen = strlen(want);
    const char *p = buf;
    int n = 0;

    *count = 0;
    while (*p) {
        const char *e = strchr(p, '\n');
        if (!e) return n;
        if ((size_t)(e - p) + 1 != wantlen) return n;
        if (memcmp(p, want, wantlen) != 0) return n;
        n++;
        p = e + 1;
    }
    *count = n;
    return -1;
}


// The mode is a global an operator can flip; nothing may cache it. A
// mode read once at startup would keep writing text lines into a
// pipeline that is already parsing JSON.
void
cttest_vwarnx_reads_the_output_mode_at_every_call(void)
{
    vw_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());

    vw_capture(path);
    warnx("plain line");
    log_json = 1;
    warnx("structured line");
    char buf[2048];
    vw_slurp(path, buf, sizeof buf);
    const char *second = strchr(buf, '\n');
    second = second ? second + 1 : "";

    assertf(strncmp(buf, "beanstalkd: plain line\n", 23) == 0
            && second[0] == '{'
            && strstr(second, "beanstalkd") == NULL
            && strstr(second, "\"msg\":\"structured line\"") != NULL,
            "flipping log_json between two calls must switch the form of "
            "the second record only, got [%s]", buf);
}


// warn() attaches the system error text, warnx() never does. Compare
// against the C library's own strerror rather than a spelling, and put
// both calls in one file so a mutation that leaks the errno into warnx
// (or drops it from warn) shows up as a whole-file mismatch.
void
cttest_vwarnx_appends_the_error_text_only_for_the_caller_that_asked(void)
{
    vw_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *sys = strerror(EACCES);
    char want[512];
    snprintf(want, sizeof want,
             "beanstalkd: open /etc/shadow: %s\nbeanstalkd: open /etc/shadow\n",
             sys);

    vw_capture(path);
    errno = EACCES;
    warn("open %s", "/etc/shadow");
    errno = EACCES;
    warnx("open %s", "/etc/shadow");
    char buf[1024];
    vw_slurp(path, buf, sizeof buf);

    assertf(strcmp(buf, want) == 0,
            "warn must append the errno text and warnx must not; want "
            "[%s] got [%s]", want, buf);
}


// A warning whose text renders to nothing is still a warning. Swallowing
// it loses the only evidence that the code path ran at all.
void
cttest_vwarnx_emits_a_line_for_a_message_that_renders_to_nothing(void)
{
    vw_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *empty = "";

    vw_capture(path);
    warnx("%s", empty);
    char buf[512];
    vw_slurp(path, buf, sizeof buf);

    assertf(strcmp(buf, "beanstalkd: \n") == 0,
            "an empty message must still produce one prefixed line, got [%s]",
            buf);
}


// Caller data is data. A second format pass over the rendered text would
// read arguments that were never passed — the classic format-string bug,
// reachable here through any tube or job name an operator can choose.
void
cttest_vwarnx_treats_percent_signs_in_the_data_as_data(void)
{
    vw_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *payload = "100% done %s %d %n";
    char want[256];
    snprintf(want, sizeof want, "beanstalkd: %s\n", payload);

    vw_capture(path);
    warnx("%s", payload);
    char buf[512];
    vw_slurp(path, buf, sizeof buf);

    assertf(strcmp(buf, want) == 0,
            "conversion specifiers arriving as data must be printed "
            "verbatim; want [%s] got [%s]", want, buf);
}


// "Exactly one line per call" in the mode that has no single-write
// protection. The JSON path builds the record in one buffer and hands it
// to one fputs; the text path makes three or four stdio calls, so two
// threads can land between them.
void
cttest_vwarnx_never_interleaves_two_text_mode_warnings_on_one_line(void)
{
    vw_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    char want[256];
    snprintf(want, sizeof want, "beanstalkd: %s\n", vw_payload);
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    int nthread = online > 3 ? (int)online : 3;
    if (nthread > 8) nthread = 8;
    vw_rounds = 64;
    pthread_t th[8];

    vw_capture(path);
    for (int i = 0; i < nthread; i++) {
        assertf(pthread_create(&th[i], NULL, vw_spam, NULL) == 0,
                "setup: pthread_create %d", i);
    }
    for (int i = 0; i < nthread; i++) {
        assertf(pthread_join(th[i], NULL) == 0, "setup: pthread_join %d", i);
    }
    char *buf = malloc(1 << 20);
    assertf(buf != NULL, "setup: malloc");
    vw_slurp(path, buf, 1 << 20);
    int count = 0;
    int bad = vw_first_line_unlike(buf, want, &count);
    int torn_at = bad;
    int want_count = nthread * vw_rounds;
    free(buf);

    assertf(torn_at < 0 && count == want_count,
            "%d threads emitting %d text warnings each must leave %d "
            "identical lines, got %d with line %d torn",
            nthread, vw_rounds, want_count, count, torn_at);
}
