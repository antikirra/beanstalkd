// Angry tests for warnx (util.c).
//
// dat.h: warnx prints the rendered message with NO system error text
// attached — one text line, or one JSON object with level "warn" and no
// errno key. The whole difference between warnx and warn is what it
// leaves out, so the attacks here are about contamination: an errno
// that leaks in because it happened to be set, a message that a
// previous warn() left behind, and content that breaks out of the JSON
// string it is supposed to live in.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
wx_setup(void)
{
    fault_clear_all();
    progname = "beanstalkd";
    log_json = 1;
}


static void
wx_capture(const char *path)
{
    fflush(stderr);
    assertf(freopen(path, "w+", stderr) != NULL,
            "setup: freopen stderr: %s", strerror(errno));
    setvbuf(stderr, NULL, _IONBF, 0);
}


static void
wx_slurp(const char *path, char *buf, size_t len)
{
    FILE *f = fopen(path, "r");
    assertf(f != NULL, "setup: fopen %s: %s", path, strerror(errno));
    size_t n = fread(buf, 1, len - 1, f);
    fclose(f);
    buf[n] = 0;
}


// Decodes the msg field of a record back into plain bytes. Returns the
// decoded length, or -1 when the field is missing or is not a legal
// JSON string body. The escape form is not promised; the decoded text
// is.
static int
wx_decode_msg(const char *line, char *out, size_t outlen)
{
    const char *p = strstr(line, "\"msg\":\"");
    size_t o = 0;

    if (!p) return -1;
    p += 7;
    while (*p) {
        unsigned char c = (unsigned char)*p++;
        if (c == '"') { out[o] = 0; return (int)o; }
        if (c < 0x20) return -1;
        if (o + 1 >= outlen) return -1;
        if (c != '\\') { out[o++] = (char)c; continue; }
        char e = *p++;
        switch (e) {
        case '"':  out[o++] = '"';  break;
        case '\\': out[o++] = '\\'; break;
        case '/':  out[o++] = '/';  break;
        case 'b':  out[o++] = '\b'; break;
        case 'f':  out[o++] = '\f'; break;
        case 'n':  out[o++] = '\n'; break;
        case 'r':  out[o++] = '\r'; break;
        case 't':  out[o++] = '\t'; break;
        case 'u': {
            unsigned v = 0;
            for (int k = 0; k < 4; k++) {
                char h = *p++;
                int d;
                if (h >= '0' && h <= '9')      d = h - '0';
                else if (h >= 'a' && h <= 'f') d = h - 'a' + 10;
                else if (h >= 'A' && h <= 'F') d = h - 'A' + 10;
                else return -1;
                v = v * 16 + (unsigned)d;
            }
            if (v > 0xff) return -1;
            out[o++] = (char)v;
            break;
        }
        default:
            return -1;
        }
    }
    return -1;
}


// Copies the part of a record that is independent of when it was
// written: everything from the level field onwards. Two calls with the
// same arguments must produce identical bytes there.
static void
wx_body_after_ts(const char *line, char *out, size_t len)
{
    const char *p = strstr(line, ",\"level\"");
    snprintf(out, len, "%s", p ? p : "");
    char *nl = strchr(out, '\n');
    if (nl) *nl = 0;
}


// errno is ambient: something failed earlier, nobody cleared it, and
// this warning is about something else entirely. warnx must be blind to
// it — a leaked errno turns an informational warning into a report of a
// failure that never happened.
void
cttest_warnx_leaves_the_errno_out_of_the_record_however_it_is_set(void)
{
    wx_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *sys = strerror(EACCES);
    char sys_copy[512];
    snprintf(sys_copy, sizeof sys_copy, "%s", sys);

    wx_capture(path);
    errno = EACCES;
    warnx("tube %s is draining", "default");
    char buf[2048];
    wx_slurp(path, buf, sizeof buf);

    assertf(strstr(buf, "\"errno\"") == NULL
            && strstr(buf, sys_copy) == NULL
            && strstr(buf, "\"level\":\"warn\"") != NULL,
            "a warning raised with errno %d set must carry no errno field "
            "and no [%s], got [%s]", EACCES, sys_copy, buf);
}


// A warning with nothing to say is still a warning that happened. The
// record must exist, with an empty message rather than no message.
void
cttest_warnx_emits_a_record_for_a_message_that_renders_to_nothing(void)
{
    wx_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *empty = "";

    wx_capture(path);
    warnx("%s", empty);
    char buf[1024];
    wx_slurp(path, buf, sizeof buf);
    char msg[64];
    int n = wx_decode_msg(buf, msg, sizeof msg);
    size_t len = strlen(buf);

    assertf(n == 0 && len > 2 && buf[0] == '{' && buf[len - 2] == '}'
            && buf[len - 1] == '\n',
            "an empty message must still be one complete record with an "
            "empty msg, got %d decoded bytes from [%s]", n, buf);
}


// Hostile content through the public entry point: quotes, backslashes,
// a newline, a tab and a low control byte, ending on a backslash so a
// truncation or a missed escape shows up as a dangling one.
void
cttest_warnx_round_trips_quotes_backslashes_and_control_bytes(void)
{
    wx_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    const char *payload = "he said \"stop\"\nrow\ttab \x01 end\\";

    wx_capture(path);
    warnx("%s", payload);
    char buf[2048];
    wx_slurp(path, buf, sizeof buf);
    char msg[512];
    int n = wx_decode_msg(buf, msg, sizeof msg);

    assertf(n == (int)strlen(payload) && strcmp(msg, payload) == 0,
            "the message must decode back to the %zu bytes it was given, "
            "got %d bytes [%s] from [%s]",
            strlen(payload), n, n < 0 ? "" : msg, buf);
}


// warn() before it, warnx() after: the record must not remember
// anything about the failure that was reported in between. strerror
// hands back a pointer into library-owned storage, so "the previous
// error text" is exactly the kind of thing that lingers.
void
cttest_warnx_emits_the_same_record_after_a_warn_in_the_same_process(void)
{
    wx_setup();
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());

    wx_capture(path);
    warnx("probe");
    errno = ENOSPC;
    warn("disk full");
    warnx("probe");
    char buf[4096];
    wx_slurp(path, buf, sizeof buf);
    const char *second = strchr(buf, '\n');
    const char *third = second ? strchr(second + 1, '\n') : NULL;
    char before[1024] = { 0 };
    char after[1024] = { 0 };
    wx_body_after_ts(buf, before, sizeof before);
    if (third) wx_body_after_ts(third + 1, after, sizeof after);

    assertf(before[0] != 0 && strcmp(before, after) == 0,
            "the same warnx call must produce the same record before and "
            "after a warn; before [%s] after [%s]", before, after);
}
