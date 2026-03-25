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
