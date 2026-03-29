// bench.c — High-performance C benchmark client for beanstalkd.
// Single-threaded, epoll-based, pipelined command batching.
// Measures the TRUE server ceiling, not Python interpreter overhead.
//
// Build: gcc -O2 -o bench bench.c -lm
// Usage: bench [-h host] [-p port] [-c conns] [-n ops] [-P pipeline] [-B body]
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <getopt.h>

#define MAX_CONNS     256
#define PIPELINE_MAX  256
#define BUF_SIZE      (1 << 16)
#define MAX_LAT       (1 << 20)

static int64_t
now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// ── Latency ─────────────────────────────────────────────────

static int64_t lat_buf[MAX_LAT];
static int     lat_n;

static void lat_add(int64_t ns) {
    if (lat_n < MAX_LAT) lat_buf[lat_n++] = ns;
}

static int cmp64(const void *a, const void *b) {
    int64_t d = *(int64_t*)a - *(int64_t*)b;
    return (d > 0) - (d < 0);
}

static void lat_report(void) {
    if (!lat_n) return;
    qsort(lat_buf, lat_n, sizeof(int64_t), cmp64);
    printf("  Latency P50:  %7.1f us\n", lat_buf[lat_n/2] / 1000.0);
    printf("  Latency P99:  %7.1f us\n", lat_buf[(int)(lat_n*0.99)] / 1000.0);
    printf("  Latency P999: %7.1f us\n", lat_buf[(int)(lat_n*0.999)] / 1000.0);
}

// ── Connection state machine ────────────────────────────────

enum phase { PH_SETUP, PH_PUT, PH_RESERVE, PH_DONE };

typedef struct {
    int fd, id;
    enum phase phase;

    char sbuf[BUF_SIZE];
    int  slen, ssent;

    char rbuf[BUF_SIZE];
    int  rlen, rpos;

    int  sent, recvd, target;
    int  pipeline;
    int  body_skip;     // bytes of body+\r\n to skip after RESERVED line

    int64_t send_ts[PIPELINE_MAX]; // per-command send timestamps
    char    del_ids[PIPELINE_MAX][24]; // job IDs for delete
    int     del_head, del_tail;

    int64_t ops;
} Bench;

static int    epfd;
static Bench  bench[MAX_CONNS];
static int    n_active;

// ── Globals ─────────────────────────────────────────────────

static char put_cmd[65536 + 256];
static int  put_cmd_len;
static int  body_size = 128;
static int  n_conns = 8;
static int  n_ops = 10000;
static int  pipeline = 64;
static char *host = "127.0.0.1";
static int  port = 11300;

static void init_put(void) {
    int h = snprintf(put_cmd, 256, "put 1024 0 60 %d\r\n", body_size);
    memset(put_cmd + h, 'A', body_size);
    memcpy(put_cmd + h + body_size, "\r\n", 2);
    put_cmd_len = h + body_size + 2;
}

// ── Socket ──────────────────────────────────────────────────

static int mk_conn(Bench *b) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return -1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in sa = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, host, &sa.sin_addr);
    connect(fd, (struct sockaddr*)&sa, sizeof sa); // EINPROGRESS ok

    b->fd = fd;
    struct epoll_event ev = { .events = EPOLLIN|EPOLLOUT|EPOLLET, .data.ptr = b };
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    return 0;
}

// ── Send / Recv ─────────────────────────────────────────────

static void enqueue(Bench *b, const char *d, int n) {
    if (b->slen + n > BUF_SIZE) return;
    memcpy(b->sbuf + b->slen, d, n);
    b->slen += n;
}

static void flush_send(Bench *b) {
    while (b->ssent < b->slen) {
        int n = write(b->fd, b->sbuf + b->ssent, b->slen - b->ssent);
        if (n <= 0) return;
        b->ssent += n;
    }
    b->slen = b->ssent = 0;
}

static void fill_recv(Bench *b) {
    for (;;) {
        int space = BUF_SIZE - b->rlen;
        if (space <= 0) break;
        int n = read(b->fd, b->rbuf + b->rlen, space);
        if (n <= 0) break;
        b->rlen += n;
    }
}

static char *find_eol(Bench *b) {
    return memmem(b->rbuf + b->rpos, b->rlen - b->rpos, "\r\n", 2);
}

static void consume(Bench *b, int n) {
    b->rpos += n;
    if (b->rpos >= b->rlen) b->rpos = b->rlen = 0;
}

// ── Pipeline fill ───────────────────────────────────────────

static void fill_puts(Bench *b) {
    while (b->sent < b->target && b->sent - b->recvd < b->pipeline) {
        enqueue(b, put_cmd, put_cmd_len);
        b->send_ts[b->sent % PIPELINE_MAX] = now_ns();
        b->sent++;
    }
    flush_send(b);
}

static const char res_cmd[] = "reserve-with-timeout 0\r\n";

static void fill_reserves(Bench *b) {
    while (b->sent < b->target && b->sent - b->recvd < b->pipeline) {
        enqueue(b, res_cmd, sizeof(res_cmd) - 1);
        b->send_ts[b->sent % PIPELINE_MAX] = now_ns();
        b->sent++;
    }
    flush_send(b);
}

// ── Process replies ─────────────────────────────────────────

static void process(Bench *b) {
    fill_recv(b);

    for (;;) {
        // Skip body bytes from RESERVED reply.
        if (b->body_skip > 0) {
            int avail = b->rlen - b->rpos;
            if (avail < b->body_skip) return;
            consume(b, b->body_skip);
            b->body_skip = 0;

            // Got full reserved reply — send delete.
            int idx = b->del_tail++ % PIPELINE_MAX;
            char del[48];
            int dlen = snprintf(del, sizeof del, "delete %s\r\n", b->del_ids[idx]);
            enqueue(b, del, dlen);
            flush_send(b);

            lat_add(now_ns() - b->send_ts[b->recvd % PIPELINE_MAX]);
            b->recvd++;
            b->ops++;

            if (b->recvd >= b->target) {
                // Now wait for all deletes.
                b->phase = PH_DONE;
                // Count outstanding deletes.
                b->target = b->del_head - b->del_tail + (b->del_tail % PIPELINE_MAX ? 0 : 0);
                // Actually, all deletes are already sent inline. We just need to read replies.
                // Switch to a simple drain mode handled below.
            }
            fill_reserves(b);
            continue;
        }

        char *eol = find_eol(b);
        if (!eol) return;
        int llen = eol - (b->rbuf + b->rpos);

        switch (b->phase) {
        case PH_SETUP:
            consume(b, llen + 2);
            b->recvd++;
            if (b->recvd >= 2) { // use + watch replies
                b->recvd = b->sent = 0;
                b->phase = PH_PUT;
                fill_puts(b);
            }
            break;

        case PH_PUT:
            consume(b, llen + 2);
            lat_add(now_ns() - b->send_ts[b->recvd % PIPELINE_MAX]);
            b->recvd++;
            b->ops++;
            if (b->recvd >= b->target) {
                b->recvd = b->sent = 0;
                b->phase = PH_RESERVE;
                fill_reserves(b);
            } else {
                fill_puts(b);
            }
            break;

        case PH_RESERVE: {
            // "RESERVED <id> <bytes>\r\n"
            char *line = b->rbuf + b->rpos;
            if (llen > 9 && line[0] == 'R') {
                char *p = line + 9; // after "RESERVED "
                char *idstart = p;
                while (*p >= '0' && *p <= '9') p++;
                int idlen = p - idstart;
                int idx = b->del_head++ % PIPELINE_MAX;
                memcpy(b->del_ids[idx], idstart, idlen);
                b->del_ids[idx][idlen] = '\0';
                p++; // skip space
                int bsz = 0;
                while (*p >= '0' && *p <= '9') bsz = bsz * 10 + (*p++ - '0');
                consume(b, llen + 2);
                b->body_skip = bsz + 2;
            } else {
                // TIMED_OUT or error
                consume(b, llen + 2);
                b->recvd++;
                if (b->recvd >= b->target) {
                    b->phase = PH_DONE;
                    n_active--;
                }
            }
            break;
        }

        case PH_DONE:
            // Draining delete replies.
            consume(b, llen + 2);
            b->ops++;
            b->del_tail++;
            if (b->del_tail >= b->del_head) {
                n_active--;
            }
            break;
        }
    }
}

// ── Main ────────────────────────────────────────────────────

static void run(void) {
    epfd = epoll_create1(EPOLL_CLOEXEC);
    n_active = n_conns;
    lat_n = 0;
    init_put();

    for (int i = 0; i < n_conns; i++) {
        Bench *b = &bench[i];
        memset(b, 0, sizeof(*b));
        b->id = i;
        b->pipeline = pipeline;
        b->target = n_ops;
        b->phase = PH_SETUP;
        mk_conn(b);

        char cmd[128];
        int len = snprintf(cmd, sizeof cmd, "use t%d\r\nwatch t%d\r\n", i, i);
        enqueue(b, cmd, len);
        flush_send(b);
    }

    struct epoll_event evs[64];
    int64_t t0 = now_ns();

    while (n_active > 0) {
        int n = epoll_wait(epfd, evs, 64, 1000);
        for (int i = 0; i < n; i++) {
            Bench *b = evs[i].data.ptr;
            process(b);
        }
    }

    int64_t elapsed = now_ns() - t0;
    double secs = elapsed / 1e9;
    int64_t total = 0;
    for (int i = 0; i < n_conns; i++) total += bench[i].ops;

    printf("═══════════════════════════════════════════════════\n");
    printf("  C Benchmark: %d conns x %d ops, pipeline=%d, body=%dB\n",
           n_conns, n_ops, pipeline, body_size);
    printf("  Wall:   %.4f s\n", secs);
    printf("  Total:  %ld ops (put + reserve+delete)\n", total);
    printf("  Rate:   %.0f ops/s\n", total / secs);
    lat_report();
    printf("═══════════════════════════════════════════════════\n");

    for (int i = 0; i < n_conns; i++) close(bench[i].fd);
    close(epfd);
}

int main(int argc, char **argv) {
    int opt;
    while ((opt = getopt(argc, argv, "h:p:c:n:P:B:")) != -1) {
        switch (opt) {
        case 'h': host = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'c': n_conns = atoi(optarg); break;
        case 'n': n_ops = atoi(optarg); break;
        case 'P': pipeline = atoi(optarg); break;
        case 'B': body_size = atoi(optarg); break;
        }
    }
    if (n_conns > MAX_CONNS) n_conns = MAX_CONNS;
    if (pipeline > PIPELINE_MAX) pipeline = PIPELINE_MAX;
    run();
    return 0;
}
