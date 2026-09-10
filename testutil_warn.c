// Angry tests for warn (util.c).
//
// dat.h and the code agree on the contract: warn prints the rendered
// message plus the system error text for errno "as captured on entry"
// (util.c:118 says "must be done first thing"), as one text line or one
// JSON object with level "error" and an errno field.
//
// The errno field is the only part of a warning that says WHY something
// failed, so the interesting failures are all about identity: reporting
// some other caller's errno, some other thread's errno, or a stale one
// captured before the caller's. The wording of strerror is the C
// library's business and locale-dependent, so every expectation here is
// computed by calling strerror in the test rather than spelled out.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
wn_setup(void)
{
    fault_clear_all();
    progname = "beanstalkd";
    log_json = 1;
}


static void
wn_capture(const char *path)
{
    fflush(stderr);
    assertf(freopen(path, "w+", stderr) != NULL,
            "setup: freopen stderr: %s", strerror(errno));
    setvbuf(stderr, NULL, _IONBF, 0);
}


static void
wn_slurp(const char *path, char *buf, size_t len)
{
    FILE *f = fopen(path, "r");
    assertf(f != NULL, "setup: fopen %s: %s", path, strerror(errno));
    size_t n = fread(buf, 1, len - 1, f);
    fclose(f);
    buf[n] = 0;
}


// Copies the value of the errno field of one record into out; returns 0
// when the record has no such field.
static int
wn_errno_field(const char *line, char *out, size_t len)
{
    const char *p = strstr(line, "\"errno\":\"");
    if (!p) return 0;
    p += 9;
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < len) out[i++] = *p++;
    out[i] = 0;
    return 1;
}


struct wn_thread {
    int   err;
    char  msg[8];
    int   rounds;
};

static void *
wn_spam(void *arg)
{
    struct wn_thread *t = arg;
    for (int i = 0; i < t->rounds; i++) {
        errno = t->err;
        warn("%s", t->msg);
    }
    return NULL;
}


// Returns the index of the first record whose msg and errno fields do
// not belong together, or -1; *count receives the number of records.
static int
wn_first_mismatched_record(const char *buf, const char *msg_a,
                           const char *want_a, const char *msg_b,
                           const char *want_b, int *count)
{
    const char *p = buf;
    int n = 0;

    *count = 0;
    while (*p) {
        const char *e = strchr(p, '\n');
        if (!e) return n;
        char line[2048];
        size_t len = (size_t)(e - p);
        if (len >= sizeof line) return n;
        memcpy(line, p, len);
        line[len] = 0;

        char got[512];
        if (!wn_errno_field(line, got, sizeof got)) return n;
        if (strstr(line, msg_a)) {
            if (strcmp(got, want_a) != 0) return n;
        } else if (strstr(line, msg_b)) {
            if (strcmp(got, want_b) != 0) return n;
        } else {
            return n;
        }
        n++;
        p = e + 1;
    }
    *count = n;
    return -1;
}


// Two warnings, two different errnos, one process. Each record must
// carry the errno of ITS call — a value captured once and reused, or
// captured after the previous call's, reports the wrong cause for the
// second failure while looking perfectly well formed.
void
cttest_warn_reports_each_callers_own_errno_and_not_an_earlier_one(void)
{
    wn_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    char want_first[512];
    char want_second[512];
    snprintf(want_first, sizeof want_first, "%s", strerror(EACCES));
    snprintf(want_second, sizeof want_second, "%s", strerror(ENOENT));

    wn_capture(path);
    errno = EACCES;
    warn("first failure");
    errno = ENOENT;
    warn("second failure");
    char buf[4096];
    wn_slurp(path, buf, sizeof buf);
    int count = 0;
    int bad = wn_first_mismatched_record(buf, "first failure", want_first,
                                         "second failure", want_second,
                                         &count);

    assertf(bad < 0 && count == 2,
            "each record must carry its own caller's errno (%d then %d); "
            "record %d disagrees, %d record(s) in [%s]",
            EACCES, ENOENT, bad, count, buf);
}


// The mode changes the shape of the record, never the cause it reports.
// A JSON path that renders the errno from anything but the value warn
// captured would still produce a perfectly parseable record.
void
cttest_warn_reports_the_same_error_text_in_both_output_modes(void)
{
    wn_setup();
    char text_path[256];
    char json_path[256];
    snprintf(text_path, sizeof text_path, "%s/text.txt", ctdir());
    snprintf(json_path, sizeof json_path, "%s/json.txt", ctdir());

    log_json = 0;
    wn_capture(text_path);
    errno = EMFILE;
    warn("accept");
    log_json = 1;
    wn_capture(json_path);
    errno = EMFILE;
    warn("accept");
    char text[1024];
    char json[1024];
    wn_slurp(text_path, text, sizeof text);
    wn_slurp(json_path, json, sizeof json);
    const char *suffix = strstr(text, "accept: ");
    char from_text[512] = { 0 };
    if (suffix) {
        snprintf(from_text, sizeof from_text, "%s", suffix + 8);
        char *nl = strchr(from_text, '\n');
        if (nl) *nl = 0;
    }
    char from_json[512] = { 0 };
    wn_errno_field(json, from_json, sizeof from_json);

    assertf(from_text[0] != 0 && strcmp(from_text, from_json) == 0,
            "both modes must report the same cause for errno %d: text "
            "says [%s], JSON says [%s]",
            EMFILE, from_text, from_json);
}


// An errno the C library has never heard of still has to produce a
// record a shipper can parse: a NULL or empty error text spliced into
// the object is how a whole log line stops being JSON.
void
cttest_warn_emits_a_complete_record_for_an_errno_the_library_does_not_know(void)
{
    wn_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    int strange = 12345;

    wn_capture(path);
    errno = strange;
    warn("unmapped failure");
    char buf[2048];
    wn_slurp(path, buf, sizeof buf);
    size_t len = strlen(buf);
    char got[512] = { 0 };
    int has_field = wn_errno_field(buf, got, sizeof got);

    assertf(has_field && got[0] != 0 && buf[0] == '{' && len > 2
            && buf[len - 2] == '}' && buf[len - 1] == '\n',
            "errno %d must still yield one complete record with a "
            "non-empty errno field, got [%s]", strange, buf);
}


// strerror hands back a pointer, and nothing in warn copies what it
// points at. Two threads reporting two different failures at the same
// time must each see their own cause; sharing one buffer means one of
// them silently reports the other's.
void
cttest_warn_reports_each_threads_own_errno_when_warnings_overlap(void)
{
    wn_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    struct wn_thread a = { 12345, "alpha", 200 };
    struct wn_thread b = { 54321, "bravo", 200 };
    char want_a[512];
    char want_b[512];
    snprintf(want_a, sizeof want_a, "%s", strerror(a.err));
    snprintf(want_b, sizeof want_b, "%s", strerror(b.err));
    pthread_t ta, tb;

    wn_capture(path);
    assertf(pthread_create(&ta, NULL, wn_spam, &a) == 0, "setup: create a");
    assertf(pthread_create(&tb, NULL, wn_spam, &b) == 0, "setup: create b");
    assertf(pthread_join(ta, NULL) == 0, "setup: join a");
    assertf(pthread_join(tb, NULL) == 0, "setup: join b");
    char *buf = malloc(1 << 20);
    assertf(buf != NULL, "setup: malloc");
    wn_slurp(path, buf, 1 << 20);
    int count = 0;
    int bad = wn_first_mismatched_record(buf, "alpha", want_a,
                                         "bravo", want_b, &count);
    int want_count = a.rounds + b.rounds;
    free(buf);

    assertf(bad < 0 && count == want_count,
            "each of %d concurrent warnings must report its own thread's "
            "errno (%d vs %d); record %d disagrees, %d record(s) seen",
            want_count, a.err, b.err, bad, count);
}
