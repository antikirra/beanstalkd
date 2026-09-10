#!/bin/sh
# bench/binlog/run.sh
#
# Does the binlog stay bounded when a few long-lived jobs sit buried and
# delayed while thousands of short-lived ones churn past them?
#
# That shape is what upstream reports as "binlogs never being compacted"
# (#599, #622): compaction cannot simply drop the oldest file while a
# pinned job's only copy still lives in it, so it has to MIGRATE that
# job forward. A migration that never happens, or one that leaves the
# reservation booked, turns a queue of 200 jobs into gigabytes of WAL.
#
# 3000 put/reserve/delete cycles past four pinned jobs, with -s 65536 so
# rotation happens every few hundred records. Expected: the directory
# holds one or two binlogs throughout, never a growing pile.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
make >/dev/null
python3 "$HERE/growth.py"
