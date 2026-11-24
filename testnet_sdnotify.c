// Angry tests for sdnotify and sdwatchdog_interval (net.c) — the
// service-manager side of systemd's Type=notify and WatchdogSec.
//
// Contract (sd_notify(3), sd_watchdog_enabled(3)): the state string is
// sent as one datagram to the AF_UNIX socket named by NOTIFY_SOCKET — a
// filesystem path, or an abstract name when it starts with '@'. Without
// that variable nothing is sent and nothing fails. The watchdog is on
// when WATCHDOG_USEC is a positive number and WATCHDOG_PID, if present,
// is this process; keep-alives are due at half the timeout.

#include "dat.h"
#include "ct/ct.h"
#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

// Binds the receiving end the way a service manager does and returns
// its fd. name is a path, or "@..." for the abstract namespace.
static int
sn_listen(const char *name)
{
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof sa);
    sa.sun_family = AF_UNIX;
    size_t n = strlen(name);
    assertf(n < sizeof sa.sun_path, "setup: socket name too long");
    memcpy(sa.sun_path, name, n);
    if (name[0] == '@')
        sa.sun_path[0] = 0;
    int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK, 0);
    assertf(fd >= 0, "setup: socket: %s", strerror(errno));
    socklen_t len = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + n);
    assertf(bind(fd, (struct sockaddr *)&sa, len) == 0,
            "setup: bind %s: %s", name, strerror(errno));
    return fd;
}

// Reads one datagram into buf, NUL-terminated; -1 if none is waiting.
static ssize_t
sn_recv(int fd, char *buf, size_t cap)
{
    ssize_t r = recv(fd, buf, cap - 1, 0);
    if (r >= 0)
        buf[r] = 0;
    return r;
}

void
cttest_sdnotify_sends_the_state_as_one_datagram_to_a_path_socket(void)
{
    char path[256];
    snprintf(path, sizeof path, "%s/notify.sock", ctdir());
    int fd = sn_listen(path);
    setenv("NOTIFY_SOCKET", path, 1);

    int r = sdnotify("READY=1");

    char got[64];
    ssize_t n = sn_recv(fd, got, sizeof got);
    char more[64];
    ssize_t extra = sn_recv(fd, more, sizeof more);
    assertf(r == 1 && n == 7 && strcmp(got, "READY=1") == 0 && extra == -1,
            "the manager must receive exactly \"READY=1\" once: returned "
            "%d, got %zd bytes \"%s\", then %zd more",
            r, n, n >= 0 ? got : "", extra);
}

// systemd itself hands out abstract sockets ("@/org/freedesktop/...").
// The leading '@' stands for a NUL, and the address length must stop at
// the name: padding it to sizeof(sun_path) names a different socket.
void
cttest_sdnotify_reaches_an_abstract_socket(void)
{
    char name[64];
    snprintf(name, sizeof name, "@bs-sdnotify-test-%d", (int)getpid());
    int fd = sn_listen(name);
    setenv("NOTIFY_SOCKET", name, 1);

    int r = sdnotify("WATCHDOG=1");

    char got[64];
    ssize_t n = sn_recv(fd, got, sizeof got);
    assertf(r == 1 && n == 10 && strcmp(got, "WATCHDOG=1") == 0,
            "an abstract NOTIFY_SOCKET must receive the datagram: returned "
            "%d, got %zd bytes", r, n);
}

// Not under a service manager: the default, and it must stay silent.
void
cttest_sdnotify_does_nothing_without_a_notify_socket(void)
{
    unsetenv("NOTIFY_SOCKET");
    int unset = sdnotify("READY=1");
    setenv("NOTIFY_SOCKET", "", 1);
    int empty = sdnotify("READY=1");

    assertf(unset == 0 && empty == 0,
            "with no socket named there is nobody to tell: unset -> %d, "
            "empty -> %d", unset, empty);
}

// The variable is input from the environment. None of these may be
// dialled, overflow sun_path, or take the server down.
void
cttest_sdnotify_refuses_addresses_it_cannot_use(void)
{
    char longpath[400];
    memset(longpath, 'a', sizeof longpath - 1);
    longpath[0] = '/';
    longpath[sizeof longpath - 1] = 0;
    char gone[256];
    snprintf(gone, sizeof gone, "%s/nobody-listens.sock", ctdir());

    static const char *fixed[] = { "relative/path.sock", "@", "/" };
    int sent = 0;
    for (size_t i = 0; i < sizeof fixed / sizeof fixed[0]; i++) {
        setenv("NOTIFY_SOCKET", fixed[i], 1);
        sent += sdnotify("READY=1");
    }
    setenv("NOTIFY_SOCKET", longpath, 1);
    sent += sdnotify("READY=1");
    setenv("NOTIFY_SOCKET", gone, 1);
    sent += sdnotify("READY=1");

    assertf(sent == 0,
            "a relative, empty, overlong or dead address must report "
            "\"not sent\"; %d of 5 claimed success", sent);
}

void
cttest_sdwatchdog_interval_is_half_the_timeout_in_nanoseconds(void)
{
    char me[32], other[32];
    snprintf(me, sizeof me, "%d", (int)getpid());
    snprintf(other, sizeof other, "%d", (int)getpid() + 1);

    static const struct {
        const char *usec;
        int pid;            // 0 unset, 1 this process, 2 another one
        int64 want;
        const char *why;
    } tt[] = {
        { NULL,                  0, 0,            "no watchdog asked for" },
        { "",                    0, 0,            "empty value" },
        { "30000000",            0, 15000000000LL, "30s, no pid" },
        { "30000000",            1, 15000000000LL, "30s, for this process" },
        { "30000000",            2, 0,            "meant for another pid" },
        { "2",                   0, 1000,         "smallest usable timeout" },
        { "1",                   0, 0,            "no half of 1us to wait" },
        { "0",                   0, 0,            "zero disables" },
        { "-5",                  0, 0,            "negative" },
        { "30s",                 0, 0,            "trailing garbage" },
        { "99999999999999999999", 0, 0,           "does not fit" },
        { "9223372036854775807", 0, 0,            "would overflow in ns" },
    };
    for (size_t i = 0; i < sizeof tt / sizeof tt[0]; i++) {
        if (tt[i].usec) setenv("WATCHDOG_USEC", tt[i].usec, 1);
        else unsetenv("WATCHDOG_USEC");
        if (tt[i].pid == 0) unsetenv("WATCHDOG_PID");
        else setenv("WATCHDOG_PID", tt[i].pid == 1 ? me : other, 1);

        int64 got = sdwatchdog_interval();
        assertf(got == tt[i].want, "%s: got %" PRId64 ", want %" PRId64,
                tt[i].why, got, tt[i].want);
    }
}
