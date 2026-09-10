// Angry tests for vlog_json (util.c), reached through warnx()/warn()
// with log_json set — it is static, so the public surface is the only
// honest way in.
//
// README documents one single-line JSON object per warning:
// {"ts":...,"level":"warn|error","msg":"...","errno":"..."}. util.c:92
// promises more than that: ONE stdio write, so a warning racing in from
// the fsync thread can never interleave half a record into a line.
// Everything that breaks here breaks in the log pipeline, not in the
// server: a torn record, a record without its closing brace, or a
// message that carries its own newline all end as silently dropped
// lines in a shipper.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static void
vj_setup(void)
{
    fault_clear_all();
    progname = "testutil_vlog_json";
    log_json = 1;
}


// Unbuffered on purpose: a fully buffered stderr would hide exactly the
// interleaving the atomicity comment claims cannot happen.
static void
vj_capture(const char *path)
{
    fflush(stderr);
    assertf(freopen(path, "w+", stderr) != NULL,
            "setup: freopen stderr: %s", strerror(errno));
    setvbuf(stderr, NULL, _IONBF, 0);
}


static void
vj_slurp(const char *path, char *buf, size_t len)
{
    FILE *f = fopen(path, "r");
    assertf(f != NULL, "setup: fopen %s: %s", path, strerror(errno));
    size_t n = fread(buf, 1, len - 1, f);
    fclose(f);
    buf[n] = 0;
}


// Returns the index of the first line that is not one complete JSON
// object, or -1 when every line is; *count receives the number of lines
// seen. "Complete" means: starts with '{', ends with '}', and contains
// no second '{' — a torn record shows up as either a missing brace or a
// second object opening mid-line.
static int
vj_first_malformed_line(const char *buf, int *count)
{
    int n = 0;
    const char *p = buf;

    *count = 0;
    while (*p) {
        const char *e = strchr(p, '\n');
        if (!e || e == p) return n;
        if (p[0] != '{') return n;
        if (e[-1] != '}') return n;
        if (memchr(p + 1, '{', (size_t)(e - p - 1)) != NULL) return n;
        n++;
        p = e + 1;
    }
    *count = n;
    return -1;
}


// The msg field, read the way a JSON parser reads it: a legal string
// body that is properly terminated. A truncation that cuts an escape in
// half fails here even though the record still looks like an object.
static int
vj_msg_is_legal(const char *line)
{
    const char *p = strstr(line, "\"msg\":\"");
    if (!p) return 0;
    p += 7;
    while (*p) {
        unsigned char c = (unsigned char)*p++;
        if (c == '"') return 1;
        if (c < 0x20) return 0;
        if (c != '\\') continue;
        char e = *p++;
        if (e == 'u') {
            for (int k = 0; k < 4; k++) {
                if (!isxdigit((unsigned char)*p++)) return 0;
            }
            continue;
        }
        if (e == 0 || !strchr("\"\\/bfnrt", e)) return 0;
    }
    return 0;
}


static int vj_rounds;

static void *
vj_spam(void *unused)
{
    UNUSED_PARAMETER(unused);
    for (int i = 0; i < vj_rounds; i++) {
        warnx("concurrent record from a worker thread");
    }
    return NULL;
}


// A message far longer than the internal render buffer. Losing the tail
// of the operator's text is acceptable; losing the closing brace is not,
// because the record then never reaches the log store at all.
void
cttest_vlog_json_keeps_a_five_thousand_character_warning_on_one_line(void)
{
    vj_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    char big[5001];
    memset(big, 'q', sizeof big - 1);
    big[sizeof big - 1] = 0;

    vj_capture(path);
    warnx("%s", big);
    char buf[16384];
    vj_slurp(path, buf, sizeof buf);
    int count = 0;
    int bad = vj_first_malformed_line(buf, &count);

    assertf(bad < 0 && count == 1,
            "a %zu-character warning must still be exactly one complete "
            "object; line %d is malformed, %d line(s) written",
            sizeof big - 1, bad, count);
}


// 1000 bytes that each escape to six: the escaped form is three times
// the escape buffer, so the truncation happens INSIDE json_escape and
// the record is assembled around a cut message. The cut must land on an
// escape boundary or the msg field stops being a JSON string.
void
cttest_vlog_json_closes_the_record_when_the_escaped_message_overflows(void)
{
    vj_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    char big[1001];
    memset(big, 0x01, sizeof big - 1);
    big[sizeof big - 1] = 0;

    vj_capture(path);
    warnx("%s", big);
    char buf[16384];
    vj_slurp(path, buf, sizeof buf);
    int count = 0;
    int bad = vj_first_malformed_line(buf, &count);

    assertf(bad < 0 && count == 1 && vj_msg_is_legal(buf),
            "%zu bytes that each expand six-to-one must still leave one "
            "complete object with a legal msg string; line %d malformed, "
            "%d line(s)",
            sizeof big - 1, bad, count);
}


// The whole point of escaping in a line-delimited format: a newline
// inside the operator's message must not become a newline in the log.
void
cttest_vlog_json_keeps_a_newline_inside_the_message_off_the_wire(void)
{
    vj_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *payload = "first\nsecond";

    vj_capture(path);
    warnx("%s", payload);
    char buf[2048];
    vj_slurp(path, buf, sizeof buf);
    size_t len = strlen(buf);
    int count = 0;
    int bad = vj_first_malformed_line(buf, &count);

    assertf(bad < 0 && count == 1 && vj_msg_is_legal(buf)
            && strchr(buf, '\n') == buf + len - 1,
            "a message containing a newline must stay on one line: got "
            "%d line(s), first malformed %d, record [%s]",
            count, bad, buf);
}


// The fractional part of ts is milliseconds, zero-padded. Without the
// padding, 7 ms renders as ".7" — seven hundred milliseconds to every
// numeric consumer. It only shows up in the first tenth of a second, so
// wait for that window instead of hoping for it.
void
cttest_vlog_json_pads_a_sub_hundred_millisecond_timestamp_to_three_digits(void)
{
    vj_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    struct timespec now_ts;
    struct timespec pause = { 0, 1000000 };
    do {
        assertf(clock_gettime(CLOCK_REALTIME, &now_ts) == 0,
                "setup: clock_gettime: %s", strerror(errno));
        if (now_ts.tv_nsec < 100000000L) break;
        nanosleep(&pause, NULL);
    } while (1);

    vj_capture(path);
    warnx("tick");
    char buf[2048];
    vj_slurp(path, buf, sizeof buf);
    const char *ts = strstr(buf, "\"ts\":");
    long long sec = -1;
    char frac[8] = { 0 };
    char sep = 0;
    int fields = sscanf(ts ? ts + 5 : "", "%lld.%3[0-9]%c", &sec, frac, &sep);

    assertf(fields == 3 && strlen(frac) == 3 && sep == ',' && frac[0] == '0',
            "a timestamp %ld ns into its second must render three "
            "fractional digits starting with 0, got [%s]",
            (long)now_ts.tv_nsec, buf);
}


// The atomicity claim at util.c:92, taken literally. Every thread emits
// whole records into one unbuffered stderr; if the record is ever built
// with more than one write, two of them interleave and a line appears
// with a second '{' inside it.
void
cttest_vlog_json_writes_whole_records_from_every_thread(void)
{
    vj_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    int nthread = online > 3 ? (int)online : 3;
    if (nthread > 8) nthread = 8;
    vj_rounds = 64;
    pthread_t th[8];

    vj_capture(path);
    for (int i = 0; i < nthread; i++) {
        assertf(pthread_create(&th[i], NULL, vj_spam, NULL) == 0,
                "setup: pthread_create %d", i);
    }
    for (int i = 0; i < nthread; i++) {
        assertf(pthread_join(th[i], NULL) == 0, "setup: pthread_join %d", i);
    }
    char *buf = malloc(1 << 20);
    assertf(buf != NULL, "setup: malloc");
    vj_slurp(path, buf, 1 << 20);
    int count = 0;
    int bad = vj_first_malformed_line(buf, &count);
    int want = nthread * vj_rounds;
    free(buf);

    assertf(bad < 0 && count == want,
            "%d threads emitting %d records each must leave %d whole "
            "objects, got %d with the first malformed at line %d",
            nthread, vj_rounds, want, count, bad);
}
