#define _GNU_SOURCE
#include "dat.h"
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <linux/socket.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

int
make_nonblocking(int fd)
{
    int flags, r;

    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        twarn("getting flags");
        return -1;
    }
    r = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    if (r == -1) {
        twarn("setting O_NONBLOCK");
        return -1;
    }
    return 0;
}

static int
make_inet_socket(char *host, char *port)
{
    int fd = -1, flags, r;
    struct linger linger = {0, 0};
    struct addrinfo *airoot, *ai, hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    r = getaddrinfo(host, port, &hints, &airoot);
    if (r != 0) {
        twarnx("getaddrinfo(): %s", gai_strerror(r));
        return -1;
    }

    for (ai = airoot; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                    ai->ai_protocol);
        if (fd == -1) {
            twarn("socket()");
            continue;
        }

        flags = 1;
        r = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof flags);
        if (r == -1) {
            twarn("setting SO_REUSEADDR on fd %d", fd);
            close(fd);
            continue;
        }
        r = setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof flags);
        if (r == -1) {
            twarn("setting SO_KEEPALIVE on fd %d", fd);
            close(fd);
            continue;
        }
        r = setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof linger);
        if (r == -1) {
            twarn("setting SO_LINGER on fd %d", fd);
            close(fd);
            continue;
        }
        r = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof flags);
        if (r == -1) {
            twarn("setting TCP_NODELAY on fd %d", fd);
            close(fd);
            continue;
        }

        // Tune socket buffers for high-throughput pipelined workloads.
        {
            int sndbuf = 256 * 1024;
            int rcvbuf = 128 * 1024;
            setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof sndbuf);
            setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);
        }

        // Reduce epoll EPOLLOUT wakeups for pipelined clients.
        {
            int lowat = 16384;
            setsockopt(fd, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &lowat, sizeof lowat);
        }

        // Detect dead connections without application-level keepalive.
        {
            int user_timeout = 30000;
            setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout, sizeof user_timeout);
        }

        // Hint kernel to deliver incoming packets on the CPU where
        // the listening socket is pinned.
        if (srv.cpu >= 0) {
            int cpu = srv.cpu;
            setsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, sizeof cpu);
        }

        // Allow kernel to rehash socket's transport hash for better
        // distribution across RX queues.
        {
            int val = SOCK_TXREHASH_ENABLED;
            setsockopt(fd, SOL_SOCKET, SO_TXREHASH, &val, sizeof val);
        }

        if (host == NULL && ai->ai_family == AF_INET6) {
            flags = 0;
            r = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &flags, sizeof(flags));
            if (r == -1) {
                twarn("setting IPV6_V6ONLY on fd %d", fd);
                close(fd);
                continue;
            }
        }

        r = bind(fd, ai->ai_addr, ai->ai_addrlen);
        if (r == -1) {
            twarn("bind()");
            close(fd);
            continue;
        }
        if (verbose) {
            char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV], *h = host, *p = port;
            struct sockaddr_storage addr;
            socklen_t addrlen;

            addrlen = sizeof(addr);
            r = getsockname(fd, (struct sockaddr *) &addr, &addrlen);
            if (!r) {
                r = getnameinfo((struct sockaddr *) &addr, addrlen,
                                hbuf, sizeof(hbuf),
                                pbuf, sizeof(pbuf),
                                NI_NUMERICHOST|NI_NUMERICSERV);
                if (!r) {
                    h = hbuf;
                    p = pbuf;
                }
            }
            if (ai->ai_family == AF_INET6) {
                printf("bind %d [%s]:%s\n", fd, h, p);
            } else {
                printf("bind %d %s:%s\n", fd, h, p);
            }
        }

        r = listen(fd, 1024);
        if (r == -1) {
            twarn("listen()");
            close(fd);
            continue;
        }

        {
            int qlen = 1024;
            setsockopt(fd, IPPROTO_TCP, TCP_FASTOPEN, &qlen, sizeof qlen);
        }

        // Defer accept until client sends data. Reduces empty accept
        // events — kernel holds the connection until data arrives.
        {
            int val = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &val, sizeof val);
        }

        break;
    }

    freeaddrinfo(airoot);

    if(ai == NULL)
        fd = -1;

    return fd;
}

static int
make_unix_socket(char *path)
{
    int fd = -1, r;
    struct stat st;
    struct sockaddr_un addr;
    const size_t maxlen = sizeof(addr.sun_path) - 1; // Reserve the last position for '\0'

    memset(&addr, 0, sizeof(struct sockaddr_un));
    addr.sun_family = AF_UNIX;
    if (strlen(path) > maxlen) {
        warnx("socket path %s is too long (%ld characters), where maximum allowed is %ld",
              path, strlen(path), maxlen);
        return -1;
    }
    strncpy(addr.sun_path, path, maxlen);

    r = stat(path, &st);
    if (r == 0) {
        if (S_ISSOCK(st.st_mode)) {
            warnx("removing existing local socket to replace it");
            r = unlink(path);
            // ENOENT here means another process raced us and already removed
            // the stale socket — fine, bind() will proceed. Any other error
            // is a real failure (permissions, bad mount, etc.).
            if (r == -1 && errno != ENOENT) {
                twarn("unlink");
                return -1;
            }
        } else {
            twarnx("another file already exists in the given path");
            return -1;
        }
    } else if (errno != ENOENT) {
        // stat() failed for a reason other than "path doesn't exist".
        // EACCES, ELOOP, etc. — don't proceed blindly to bind().
        twarn("stat(\"%s\")", path);
        return -1;
    }

    fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        twarn("socket()");
        return -1;
    }

    r = bind(fd, (struct sockaddr *) &addr, sizeof(struct sockaddr_un));
    if (r == -1) {
        twarn("bind()");
        close(fd);
        return -1;
    }
    if (verbose) {
        printf("bind %d %s\n", fd, path);
    }

    r = listen(fd, 1024);
    if (r == -1) {
        twarn("listen()");
        close(fd);
        return -1;
    }

    return fd;
}

int
make_server_socket(char *host, char *port)
{
#ifdef HAVE_LIBSYSTEMD
    int fd = -1, r;

    /* See if we got a listen fd from systemd. If so, all socket options etc
     * are already set, so we check that the fd is a TCP or UNIX listen socket
     * and return. */
    r = sd_listen_fds(1);
    if (r < 0) {
        twarn("sd_listen_fds");
        return -1;
    }
    if (r > 0) {
        if (r > 1) {
            twarnx("inherited more than one listen socket;"
                   " ignoring all but the first");
        }
        fd = SD_LISTEN_FDS_START;
        r = sd_is_socket_inet(fd, 0, SOCK_STREAM, 1, 0);
        if (r < 0) {
            twarn("sd_is_socket_inet");
            errno = -r;
            return -1;
        }
        if (r == 0) {
            r = sd_is_socket_unix(fd, SOCK_STREAM, 1, NULL, 0);
            if (r < 0) {
                twarn("sd_is_socket_unix");
                errno = -r;
                return -1;
            }
            if (r == 0) {
                twarnx("inherited fd is not a TCP or UNIX listening socket");
                return -1;
            }
        }
        return fd;
    }
#endif

    if (host && !strncmp(host, "unix:", 5)) {
        return make_unix_socket(&host[5]);
    } else {
        return make_inet_socket(host, port);
    }
}
