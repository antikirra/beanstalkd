#!/bin/sh
# bench/shutdown/run.sh
#
# A clean SIGTERM must leave the WAL durable and the binlog trimmed to
# what it holds. This exercises the real main() shutdown path, which the
# unit tests cannot reach: their server is srvserve() called directly,
# with a SIGTERM handler that exits straight away.
#
# Expected: the binlog shrinks from its preallocated size to the bytes
# actually written, and every job replays on the next start.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
make >/dev/null
python3 "$HERE/check.py"
