#include "ct/ct.h"
#include "dat.h"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void
cttest_optz_zero_uses_default()
{
    char *args[] = { "-z0", NULL };
    optparse(&srv, args);
    assertf(job_data_size_limit == JOB_DATA_SIZE_LIMIT_DEFAULT,
        "z=0 must fallback to default, got %zu", job_data_size_limit);
}

void
cttest_opts_zero_uses_default()
{
    char *args[] = { "-s0", NULL };
    srv.wal.filesize = 0;
    optparse(&srv, args);
    assertf(srv.wal.filesize == Filesizedef,
        "s=0 must fallback to default, got %d", srv.wal.filesize);
}

void
cttest_optf_huge_capped()
{
    char *args[] = { "-f9999999999", NULL };
    optparse(&srv, args);
    assertf(srv.wal.syncrate <= (int64)1000000000 * 1000000,
        "f overflow must be capped");
    assertf(srv.wal.wantsync == 1, "wantsync must be set");
}

void
cttest_optz_exact_max()
{
    char *args[] = { "-z1073741824", NULL };
    optparse(&srv, args);
    assertf(job_data_size_limit == JOB_DATA_SIZE_LIMIT_MAX,
        "z=max must be accepted exactly, got %zu", job_data_size_limit);
}

void
cttest_optz_one()
{
    char *args[] = { "-z1", NULL };
    optparse(&srv, args);
    assertf(job_data_size_limit == 1,
        "z=1 must be accepted, got %zu", job_data_size_limit);
}

void
cttest_opts_one()
{
    char *args[] = { "-s1", NULL };
    optparse(&srv, args);
    assertf(srv.wal.filesize == 1,
        "s=1 must be accepted, got %d", srv.wal.filesize);
}

// -D enables durable mode: fdatasync blocks inside walwrite so replies
// never precede persistence. Must also silence wantsync — the async
// sync thread would otherwise duplicate the fsync work.
void
cttest_optD_enables_durable_and_disables_wantsync()
{
    srv.wal.durable_sync = 0;
    srv.wal.wantsync = 1;

    char *args[] = { "-D", NULL };
    optparse(&srv, args);

    assertf(srv.wal.durable_sync == 1,
        "-D must set durable_sync=1, got %d", srv.wal.durable_sync);
    assertf(srv.wal.wantsync == 0,
        "-D must clear wantsync, got %d", srv.wal.wantsync);
}

// -D is order-independent: placing it after -f0 must still win and leave
// durable_sync=1 with wantsync=0, so a user can safely append -D to an
// existing command line without reordering.
void
cttest_optD_overrides_prior_fsync_request()
{
    srv.wal.durable_sync = 0;
    srv.wal.wantsync = 0;
    srv.wal.syncrate = 0;

    char *args[] = { "-f0", "-D", NULL };
    optparse(&srv, args);

    assertf(srv.wal.durable_sync == 1,
        "-D after -f0 must still set durable_sync, got %d",
        srv.wal.durable_sync);
    assertf(srv.wal.wantsync == 0,
        "-D must disable the async sync thread, got wantsync=%d",
        srv.wal.wantsync);
}

// Reverse order: -D earlier, -f later. Without the guard in case 'f',
// a later -f would flip wantsync=1 and silently resurrect the async
// sync thread while durable_sync=1 remains, making the server fsync
// twice per write. The fix in util.c (if (!durable_sync) wantsync=1)
// is regression-locked here.
void
cttest_optf_does_not_override_prior_D()
{
    srv.wal.durable_sync = 0;
    srv.wal.wantsync = 0;
    srv.wal.syncrate = 0;

    char *args[] = { "-D", "-f100", NULL };
    optparse(&srv, args);

    assertf(srv.wal.durable_sync == 1,
        "-D before -f100 must remain set, got %d",
        srv.wal.durable_sync);
    assertf(srv.wal.wantsync == 0,
        "-f after -D must not re-enable async sync, got wantsync=%d",
        srv.wal.wantsync);
}

// --- json_escape: the escape table is the load-bearing part of JSON
// output. Failure here would silently produce malformed objects that
// break Loki/fluentd parsers — log-shipping silently drops them and
// nobody notices until an incident. Test every escape branch.

void
cttest_json_escape_passthrough()
{
    char out[64];
    size_t n = json_escape(out, sizeof out, "plain text 123 a-z A-Z");
    assertf(strcmp(out, "plain text 123 a-z A-Z") == 0,
        "ASCII passthrough corrupted: got [%s]", out);
    assertf(n == 22, "length must equal input, got %zu", n);
}

void
cttest_json_escape_empty_input()
{
    char out[16];
    out[0] = 'X';
    size_t n = json_escape(out, sizeof out, "");
    assertf(out[0] == 0, "empty input must produce empty NUL-terminated string");
    assertf(n == 0, "length must be 0, got %zu", n);
}

void
cttest_json_escape_zero_dst()
{
    char out[1] = { 'X' };
    // dst_size=0 is a hostile boundary — must not write a single byte.
    size_t n = json_escape(out, 0, "anything");
    assertf(out[0] == 'X', "dst_size=0 must not touch buffer, got [%c]", out[0]);
    assertf(n == 0, "length must be 0 with dst_size=0, got %zu", n);
}

void
cttest_json_escape_quote_and_backslash()
{
    char out[64];
    json_escape(out, sizeof out, "say \"hi\" \\ end");
    assertf(strcmp(out, "say \\\"hi\\\" \\\\ end") == 0,
        "quote+backslash escape failed: got [%s]", out);
}

void
cttest_json_escape_control_chars()
{
    char out[64];
    json_escape(out, sizeof out, "a\nb\rc\td\be\ff");
    assertf(strcmp(out, "a\\nb\\rc\\td\\be\\ff") == 0,
        "control char escape failed: got [%s]", out);
}

void
cttest_json_escape_low_byte_unicode()
{
    char out[64];
    char in[] = { 'a', 0x01, 'b', 0x1f, 'c', 0 };
    json_escape(out, sizeof out, in);
    assertf(strcmp(out, "a\\u0001b\\u001fc") == 0,
        "low bytes must use \\uXXXX form: got [%s]", out);
}

void
cttest_json_escape_high_bit_passthrough()
{
    // Bytes >= 0x20 (incl. 0x80+) are valid JSON literals; the spec
    // does not require escaping non-ASCII. Must pass through.
    char out[16];
    char in[] = { (char)0xC3, (char)0xA9, 0 }; // UTF-8 "é"
    json_escape(out, sizeof out, in);
    assertf((unsigned char)out[0] == 0xC3 && (unsigned char)out[1] == 0xA9 && out[2] == 0,
        "UTF-8 bytes must pass unchanged");
}

void
cttest_json_escape_truncation_safe()
{
    // Buffer barely fits "ab\\\"" (4 chars + NUL). The next escape
    // would overflow — must stop cleanly and NUL-terminate.
    char out[6];
    memset(out, 'X', sizeof out);
    json_escape(out, sizeof out, "ab\"\"\"\"\"");
    // out must be NUL-terminated within bounds, no buffer overrun.
    int found_nul = 0;
    for (size_t i = 0; i < sizeof out; i++) {
        if (out[i] == 0) { found_nul = 1; break; }
    }
    assertf(found_nul, "must NUL-terminate within buffer");
    assertf(strlen(out) < sizeof out, "strlen must be < dst_size");
}

void
cttest_json_escape_truncation_mid_escape()
{
    // Hostile: only 2 bytes left, escape needs 2 bytes for \" — fits
    // exactly only if NUL also fits. Must not write partial escape.
    char out[3]; // "a" + NUL leaves 1 byte; "\" alone needs 2 → overflow
    json_escape(out, sizeof out, "a\"more");
    assertf(out[2] == 0 || out[1] == 0,
        "must NUL-terminate, got [%02x %02x %02x]",
        (unsigned char)out[0], (unsigned char)out[1], (unsigned char)out[2]);
    assertf(strlen(out) < sizeof out, "no overrun");
}

// --- Flag parsing for --log-json (long-form flag). Order-independence
// matters: users append flags to an existing command line and expect
// them to compose without rewriting argv.

void
cttest_opt_log_json_long_flag()
{
    log_json = 0;
    char *args[] = { "--log-json", NULL };
    optparse(&srv, args);
    assertf(log_json == 1, "--log-json must set log_json=1, got %d", log_json);
}

void
cttest_opt_log_json_default_off()
{
    log_json = 0;
    char *args[] = { NULL };
    optparse(&srv, args);
    assertf(log_json == 0,
        "default must be 0 (no flag, no JSON), got %d", log_json);
}

void
cttest_opt_log_json_with_short_flag()
{
    log_json = 0;
    char *args[] = { "--log-json", "-z2048", NULL };
    optparse(&srv, args);
    assertf(log_json == 1, "--log-json before short flag must set");
    assertf(job_data_size_limit == 2048,
        "short flag after --log-json must still parse, got %zu",
        job_data_size_limit);
}

void
cttest_opt_log_json_after_short_flag()
{
    log_json = 0;
    char *args[] = { "-z4096", "--log-json", NULL };
    optparse(&srv, args);
    assertf(log_json == 1, "--log-json after short flag must set");
    assertf(job_data_size_limit == 4096,
        "short flag before --log-json must still apply, got %zu",
        job_data_size_limit);
}

// --- End-to-end: capture stderr, parse the JSON line. This catches
// any regression in field order, comma placement, or missing braces.

static void
capture_stderr_to(const char *path)
{
    fflush(stderr);
    if (!freopen(path, "w+", stderr)) {
        assertf(0, "freopen stderr: %s", strerror(errno));
    }
}

static size_t
read_file(const char *path, char *buf, size_t buflen)
{
    FILE *f = fopen(path, "r");
    assertf(f != NULL, "fopen %s: %s", path, strerror(errno));
    size_t n = fread(buf, 1, buflen - 1, f);
    fclose(f);
    buf[n] = 0;
    return n;
}

void
cttest_log_json_warnx_emits_warn_level()
{
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    log_json = 1;
    progname = "beanstalkd";
    capture_stderr_to(path);
    warnx("hello world %d", 42);
    fflush(stderr);

    char buf[1024];
    read_file(path, buf, sizeof buf);

    assertf(strstr(buf, "\"level\":\"warn\"") != NULL,
        "warnx must emit level=warn: got [%s]", buf);
    assertf(strstr(buf, "\"msg\":\"hello world 42\"") != NULL,
        "msg field must contain rendered format: got [%s]", buf);
    assertf(strstr(buf, "\"ts\":") != NULL,
        "ts field must be present: got [%s]", buf);
    assertf(strstr(buf, "\"errno\"") == NULL,
        "warnx (no errno) must NOT emit errno field: got [%s]", buf);
    size_t len = strlen(buf);
    assertf(len > 0 && buf[len - 1] == '\n',
        "must end with newline: got [%s]", buf);
}

void
cttest_log_json_warn_emits_error_level_and_errno()
{
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    log_json = 1;
    progname = "beanstalkd";
    errno = ENOENT; // a real strerror exists
    capture_stderr_to(path);
    warn("open failed");
    fflush(stderr);

    char buf[1024];
    read_file(path, buf, sizeof buf);

    assertf(strstr(buf, "\"level\":\"error\"") != NULL,
        "warn (with errno) must emit level=error: got [%s]", buf);
    assertf(strstr(buf, "\"msg\":\"open failed\"") != NULL,
        "msg must be the format result: got [%s]", buf);
    assertf(strstr(buf, "\"errno\":\"") != NULL,
        "errno field must be present for warn(): got [%s]", buf);
}

// Hostile message content: quotes and backslashes inside the format
// string would break the JSON if escaping is not applied.
void
cttest_log_json_escapes_msg_content()
{
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    log_json = 1;
    progname = "beanstalkd";
    capture_stderr_to(path);
    warnx("path=\"%s\" \\ end", "/etc/passwd");
    fflush(stderr);

    char buf[1024];
    read_file(path, buf, sizeof buf);

    // The literal '"' inside msg content must be escaped as \"
    assertf(strstr(buf, "\\\"/etc/passwd\\\"") != NULL,
        "embedded quotes must be escaped: got [%s]", buf);
    // The literal '\' must be doubled.
    assertf(strstr(buf, "\\\\") != NULL,
        "embedded backslash must be escaped: got [%s]", buf);
    // No raw '"' immediately followed by '/etc' — that would mean the
    // escape failed and the string was prematurely terminated.
    assertf(strstr(buf, "\"/etc/passwd\"") == NULL,
        "raw quotes must not appear in msg payload: got [%s]", buf);
}

// JSON mode must NOT prepend the human-readable "<progname>: " prefix.
// That prefix is metadata and would either bleed into the JSON object
// or split a single log record across two lines.
void
cttest_log_json_no_progname_prefix()
{
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    log_json = 1;
    progname = "beanstalkd";
    capture_stderr_to(path);
    warnx("clean message");
    fflush(stderr);

    char buf[1024];
    read_file(path, buf, sizeof buf);

    assertf(buf[0] == '{',
        "first byte must be '{', got [%c] in [%s]", buf[0], buf);
    assertf(strstr(buf, "beanstalkd:") == NULL,
        "JSON mode must not emit progname prefix: got [%s]", buf);
}

// Default text mode regression: log_json=0 must keep the historical
// "<progname>: msg" format. Operators and any existing log-parsing
// scripts depend on this.
void
cttest_log_text_mode_unchanged()
{
    char path[256];
    snprintf(path, sizeof path, "%s/log.txt", ctdir());
    log_json = 0;
    progname = "beanstalkd";
    capture_stderr_to(path);
    warnx("text mode line");
    fflush(stderr);

    char buf[1024];
    read_file(path, buf, sizeof buf);

    assertf(strncmp(buf, "beanstalkd: ", 12) == 0,
        "text mode must start with progname prefix: got [%s]", buf);
    assertf(strstr(buf, "text mode line") != NULL,
        "text mode must contain the message: got [%s]", buf);
    assertf(buf[0] != '{',
        "text mode must not look like JSON: got [%s]", buf);
}
