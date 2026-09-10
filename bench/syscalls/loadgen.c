// Minimal beanstalk load generator.
// usage: loadgen <port> <iters> [depth] [conns]
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>

static int dial(int port)
{
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
    inet_aton("127.0.0.1", &a.sin_addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0 || connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        perror("connect"); exit(1);
    }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

static char rbuf[1 << 20];
static int  rlen, rpos;

static char *rdline(int fd)
{
    static char out[4096];
    int n = 0;
    for (;;) {
        if (rpos >= rlen) {
            rlen = read(fd, rbuf, sizeof rbuf);
            if (rlen <= 0) { fprintf(stderr, "eof\n"); exit(1); }
            rpos = 0;
        }
        char c = rbuf[rpos++];
        if (n < (int)sizeof out - 1) out[n++] = c;
        if (c == '\n') { out[n] = 0; return out; }
    }
}

static void work(int port, int iters, int depth)
{
    int fd = dial(port);
    static char out[1 << 16];
    long long ids[256];

    for (int i = 0; i < iters; i += depth) {
        int n = 0;
        for (int d = 0; d < depth; d++)
            n += sprintf(out + n, "put 0 0 60 3\r\nabc\r\n");
        if (write(fd, out, n) != n) { perror("write"); exit(1); }
        for (int d = 0; d < depth; d++) rdline(fd);

        n = 0;
        for (int d = 0; d < depth; d++) n += sprintf(out + n, "reserve\r\n");
        if (write(fd, out, n) != n) { perror("write"); exit(1); }
        for (int d = 0; d < depth; d++) {
            char *l = rdline(fd);
            ids[d] = atoll(l + 9);
            rdline(fd);
        }

        n = 0;
        for (int d = 0; d < depth; d++)
            n += sprintf(out + n, "delete %lld\r\n", ids[d]);
        if (write(fd, out, n) != n) { perror("write"); exit(1); }
        for (int d = 0; d < depth; d++) rdline(fd);
    }
    close(fd);
}

int main(int argc, char **argv)
{
    int port  = atoi(argv[1]);
    int iters = atoi(argv[2]);
    int depth = argc > 3 ? atoi(argv[3]) : 1;
    int conns = argc > 4 ? atoi(argv[4]) : 1;

    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);

    if (conns <= 1) {
        work(port, iters, depth);
    } else {
        for (int i = 0; i < conns; i++)
            if (fork() == 0) { work(port, iters, depth); _exit(0); }
        for (int i = 0; i < conns; i++) wait(NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &b);
    double d = (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) / 1e9;
    long long total = (long long)iters * 3 * (conns > 1 ? conns : 1);
    printf("%lld cmds, conns=%d depth=%d: %.3fs = %.0f cmd/s\n",
           total, conns, depth, d, total / d);
    return 0;
}
