#include "ct/ct.h"
#include "dat.h"
#include <stdint.h>
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
