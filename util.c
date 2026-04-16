#include "dat.h"
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

const char *progname;
int log_json = 0;

size_t
json_escape(char *dst, size_t dst_size, const char *src)
{
    size_t out = 0;
    if (dst_size == 0) return 0;
    while (*src) {
        unsigned char c = (unsigned char)*src++;
        const char *e;
        size_t n;
        char u6[8];
        switch (c) {
            case '"':  e = "\\\""; n = 2; break;
            case '\\': e = "\\\\"; n = 2; break;
            case '\b': e = "\\b";  n = 2; break;
            case '\f': e = "\\f";  n = 2; break;
            case '\n': e = "\\n";  n = 2; break;
            case '\r': e = "\\r";  n = 2; break;
            case '\t': e = "\\t";  n = 2; break;
            default:
                if (c < 0x20) {
                    snprintf(u6, sizeof u6, "\\u%04x", c);
                    e = u6;
                    n = 6;
                } else {
                    if (out + 1 >= dst_size) goto done;
                    dst[out++] = (char)c;
                    continue;
                }
        }
        if (out + n >= dst_size) goto done;
        memcpy(dst + out, e, n);
        out += n;
    }
done:
    dst[out] = 0;
    return out;
}

static void
vlog_json(const char *err, const char *fmt, va_list args)
__attribute__((format(printf, 2, 0)));

static void
vlog_json(const char *err, const char *fmt, va_list args)
{
    char raw[1024];
    char esc_msg[2048];
    char esc_err[256];
    char line[3072];

    if (fmt)
        vsnprintf(raw, sizeof raw, fmt, args);
    else
        raw[0] = 0;
    json_escape(esc_msg, sizeof esc_msg, raw);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    if (err) {
        json_escape(esc_err, sizeof esc_err, err);
        snprintf(line, sizeof line,
            "{\"ts\":%lld.%03ld,\"level\":\"error\","
            "\"msg\":\"%s\",\"errno\":\"%s\"}\n",
            (long long)ts.tv_sec, ts.tv_nsec / 1000000L,
            esc_msg, esc_err);
    } else {
        snprintf(line, sizeof line,
            "{\"ts\":%lld.%03ld,\"level\":\"warn\","
            "\"msg\":\"%s\"}\n",
            (long long)ts.tv_sec, ts.tv_nsec / 1000000L,
            esc_msg);
    }
    // Single fputs → one stdio write under glibc's per-FILE lock.
    // Concurrent warnings from the fsync thread cannot interleave a
    // partial JSON record into the same stderr line.
    fputs(line, stderr);
}

static void
vwarnx(const char *err, const char *fmt, va_list args)
__attribute__((format(printf, 2, 0)));

static void
vwarnx(const char *err, const char *fmt, va_list args)
{
    if (log_json) {
        vlog_json(err, fmt, args);
        return;
    }
    fprintf(stderr, "%s: ", progname);
    if (fmt) {
        vfprintf(stderr, fmt, args);
        if (err) fprintf(stderr, ": %s", err);
    }
    fputc('\n', stderr);
}

void
warn(const char *fmt, ...)
{
    char *err = strerror(errno); /* must be done first thing */
    va_list args;

    va_start(args, fmt);
    vwarnx(err, fmt, args);
    va_end(args);
}

void
warnx(const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vwarnx(NULL, fmt, args);
    va_end(args);
}


char*
fmtalloc(char *fmt, ...)
{
    int n;
    char *buf;
    va_list ap;

    // find out how much space is needed
    va_start(ap, fmt);
    char dummy;
    n = vsnprintf(&dummy, 0, fmt, ap) + 1; // include space for trailing NUL
    va_end(ap);

    buf = malloc(n);
    if (buf) {
        va_start(ap, fmt);
        vsnprintf(buf, n, fmt, ap);
        va_end(ap);
    }
    return buf;
}


// Zalloc allocates n bytes of zeroed memory and
// returns a pointer to it.
// If insufficient memory is available, zalloc returns 0.
// Uses calloc which avoids memset for mmap'd pages on glibc.
void*
zalloc(size_t n)
{
    return calloc(1, n);
}


static void
warn_systemd_ignored_option(char *opt, char *arg)
{
#ifdef HAVE_LIBSYSTEMD
    if (sd_listen_fds(0) > 0) {
        warnx("inherited listen fd; ignoring option: %s %s", opt, arg);
    }
#endif
}


_Noreturn static void
usage(int code)
{
    fprintf(stderr, "Use: %s [OPTIONS]\n"
            "\n"
            "Options:\n"
            " -b DIR   write-ahead log directory\n"
            " -f MS    fsync at most once every MS milliseconds (default is %dms);\n"
            "          use -f0 for \"always fsync\"\n"
            " -F       never fsync\n"
            " -D       durable mode: block on fdatasync inside each WAL\n"
            "          write so replies never precede persistence (implies -F)\n"
            " -l ADDR  listen on address (default is 0.0.0.0)\n"
            " -p PORT  listen on port (default is " Portdef ")\n"
            " -u USER  become user and group\n"
            " -z BYTES set the maximum job size in bytes (default is %d);\n"
            "          max allowed is %d bytes\n"
            " -s BYTES set the size of each write-ahead log file (default is %d);\n"
            "          will be rounded up to a multiple of 4096 bytes\n"
            " -m SEC   return unused memory to OS every SEC seconds (default is 60);\n"
            "          use -m0 to disable\n"
            " -t CPU   pin main thread to CPU core (default is no pinning)\n"
            " -v       show version information\n"
            " -V       increase verbosity\n"
            " -h       show this help\n"
            " --log-json  emit warnings as JSON objects on stderr\n",
            progname,
            DEFAULT_FSYNC_MS,
            JOB_DATA_SIZE_LIMIT_DEFAULT,
            JOB_DATA_SIZE_LIMIT_MAX,
            Filesizedef);
    exit(code);
}


static char *flagusage(char *flag) __attribute__ ((noreturn));
static char *
flagusage(char *flag)
{
    warnx("flag requires an argument: %s", flag);
    usage(5);
}


static size_t
parse_size_t(char *str)
{
    char r, x;
    size_t size;

    r = sscanf(str, "%zu%c", &size, &x);
    if (1 != r) {
        warnx("invalid size: %s", str);
        usage(5);
    }
    return size;
}


void
optparse(Server *s, char **argv)
{
    int64 ms;
    char *arg, *tmp;
#   define EARGF(x) (*arg ? (tmp=arg,arg="",tmp) : *argv ? *argv++ : (x))

    while ((arg = *argv++) && *arg++ == '-' && *arg) {
        // Long-form flag: "--name". Detected before the single-char
        // loop so "--" never leaks into the switch as two flags.
        if (*arg == '-') {
            char *long_name = arg + 1;
            if (strcmp(long_name, "log-json") == 0) {
                log_json = 1;
                continue;
            }
            warnx("unknown flag: --%s", long_name);
            usage(5);
        }
        char c;
        while ((c = *arg++)) {
            switch (c) {
                case 'p':
                    s->port = EARGF(flagusage("-p"));
                    warn_systemd_ignored_option("-p", s->port);
                    break;
                case 'l':
                    s->addr = EARGF(flagusage("-l"));
                    warn_systemd_ignored_option("-l", s->addr);
                    break;
                case 'z':
                    job_data_size_limit = parse_size_t(EARGF(flagusage("-z")));
                    if (job_data_size_limit < 1) {
                        warnx("invalid job size limit, using default");
                        job_data_size_limit = JOB_DATA_SIZE_LIMIT_DEFAULT;
                    }
                    if (job_data_size_limit > JOB_DATA_SIZE_LIMIT_MAX) {
                        warnx("maximum job size was set to %d", JOB_DATA_SIZE_LIMIT_MAX);
                        job_data_size_limit = JOB_DATA_SIZE_LIMIT_MAX;
                    }
                    break;
                case 's':
                    s->wal.filesize = parse_size_t(EARGF(flagusage("-s")));
                    if (s->wal.filesize < 1) {
                        warnx("invalid wal file size, using default");
                        s->wal.filesize = Filesizedef;
                    }
                    break;
                case 'f':
                    ms = (int64)parse_size_t(EARGF(flagusage("-f")));
                    if (ms > 1000000000) {
                        warnx("-f value too large, capping at 1000000000ms");
                        ms = 1000000000;
                    }
                    s->wal.syncrate = ms * 1000000;
                    // -f should not resurrect the async sync thread after
                    // -D asked for blocking fdatasync. -D wins regardless
                    // of position on the command line.
                    if (!s->wal.durable_sync)
                        s->wal.wantsync = 1;
                    break;
                case 'F':
                    s->wal.wantsync = 0;
                    break;
                case 'D':
                    // Durable: blocking fdatasync inside walwrite().
                    // Disable the async sync thread — it would double up.
                    s->wal.durable_sync = 1;
                    s->wal.wantsync = 0;
                    break;
                case 'u':
                    s->user = EARGF(flagusage("-u"));
                    break;
                case 'b':
                    s->wal.dir = EARGF(flagusage("-b"));
                    s->wal.use = 1;
                    break;
                case 'm': {
                    int64 sec = (int64)parse_size_t(EARGF(flagusage("-m")));
                    mem_trim_rate = sec * 1000000000LL;
                    break;
                }
                case 't': {
                    int cpu = (int)parse_size_t(EARGF(flagusage("-t")));
                    int ncpu = (int)sysconf(_SC_NPROCESSORS_ONLN);
                    if (cpu < 0 || cpu >= ncpu) {
                        warnx("-t %d: CPU out of range (0..%d)", cpu, ncpu - 1);
                        usage(5);
                    }
                    s->cpu = cpu;
                    break;
                }
                case 'h':
                    usage(0);
                case 'v':
                    printf("beanstalkd %s\n", version);
                    exit(0);
                case 'V':
                    verbose++;
                    break;
                default:
                    warnx("unknown flag: %s", arg-2);
                    usage(5);
            }
        }
    }
    if (arg) {
        warnx("unknown argument: %s", arg-1);
        usage(5);
    }
}
