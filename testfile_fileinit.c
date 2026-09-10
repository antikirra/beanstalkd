// Angry tests for fileinit (file.c) — it names a binlog. The naming
// convention is a two-sided contract: whatever fileinit writes,
// walscandir has to be able to find again on the next start. A path the
// scanner will never match is a binlog the WAL writes to and the reader
// never looks at.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
fi_setup(void)
{
    fault_clear_all();
    progname = "testfile_fileinit";
}

// walscandir's acceptance rule, restated here so the round trip is
// checked against the reader's side and not against the writer's.
static int
fi_rediscoverable(const char *path)
{
    static const char base[] = "binlog.";
    size_t len = sizeof base - 1;
    const char *slash = strrchr(path, '/');
    const char *name = slash ? slash + 1 : path;
    if (strncmp(name, base, len) != 0)
        return 0;
    const char *start = name + len;
    char *end;
    errno = 0;
    long n = strtol(start, &end, 10);
    return end != start && *end == '\0' && !errno && n >= 1 && n < INT_MAX;
}


void
cttest_fileinit_builds_the_documented_binlog_path(void)
{
    fi_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    w.dir = "/var/lib/beanstalkd";

    int r = fileinit(&f, &w, 7);
    assertf(r != 0, "setup: the path allocation must succeed");

    assertf(strcmp(f.path, "/var/lib/beanstalkd/binlog.7") == 0,
            "the binlog path must be <dir>/binlog.<seq>, got %s", f.path);

    free(f.path);
}


// The largest sequence walscandir will still accept. A fixed-size
// buffer anywhere on this path would truncate it into a name that
// collides with a different binlog.
void
cttest_fileinit_does_not_truncate_the_largest_usable_sequence(void)
{
    fi_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    w.dir = "/d";

    int n = INT_MAX - 1;
    int r = fileinit(&f, &w, n);
    assertf(r != 0, "setup: the path allocation must succeed");

    char want[64];
    snprintf(want, sizeof want, "/d/binlog.%d", n);

    assertf(strcmp(f.path, want) == 0,
            "sequence %d must survive whole: expected %s, got %s",
            n, want, f.path);

    free(f.path);
}


// w->next is a plain int and nothing between the caller and fileinit
// bounds it. A negative sequence produces "binlog.-1", which the
// directory scan skips: the writer would fill a file the reader can
// never rediscover.
void
cttest_fileinit_must_not_name_a_binlog_the_scan_will_never_find(void)
{
    fi_setup();
    Wal w;
    File f;
    memset(&w, 0, sizeof w);
    memset(&f, 0, sizeof f);
    w.dir = "/d";

    fileinit(&f, &w, -1);

    assertf(!f.path || fi_rediscoverable(f.path),
            "either a negative sequence is refused, or the name it "
            "produces must be one walscandir can find again: got %s",
            f.path ? f.path : "(refused)");

    free(f.path);
}
