#!/bin/sh
# bench/crash/run.sh
#
# SIGKILL mid-traffic, then restart and count. Under -D an ack means the
# record is on disk (invariant #14), so every acked job must come back.
# Without -D the promise is weaker on purpose — the page cache survives
# a killed process but not a power cut — so that run is a smoke test.
#
# What this does NOT simulate is losing the page cache. For that, run it
# in a VM you can hard-reset, or on a filesystem mounted so the cache is
# dropped; the unit tests cover the reader's side of that with corrupted
# and truncated binlogs (bench/fuzz/replay.py).
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
make >/dev/null
python3 "$HERE/check.py"
