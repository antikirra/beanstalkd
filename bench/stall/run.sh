#!/bin/sh
# bench/stall/run.sh [JOBS] [BODY] [SECONDS]
#
# Does memory upkeep stall the event loop? The -m trim used to run
# malloc_trim every interval whether or not anything had been freed:
# on a heap of 270MB that is a 34ms stop of the whole server once per
# interval, forever, returning nothing. See stall.py for the scenario.
#
# PASS needs both halves: at most one stall in the pinged window (the
# one trim that the mass delete has paid for), and the memory back once
# all but a few scattered jobs are deleted. A build that trims
# unconditionally fails the first; a build that never trims fails the
# second.
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
cd "$ROOT"
[ -x ./beanstalkd ] || make beanstalkd >/dev/null

OUT=$(python3 "$HERE/stall.py" ./beanstalkd "${PORT:-11820}" "${1:-600000}" "${2:-200}" "${3:-6}")
echo "$OUT"
python3 - "$OUT" <<'EOF'
import json, sys
r = json.loads(sys.argv[1])
bad = []
if r["stalls"] > 1:
    bad.append("%d replies slower than 5ms (max %.1fms)" % (r["stalls"], r["max_ms"]))
if r["rss_pinned_mb"] > max(16.0, r["rss_full_mb"] / 4):
    bad.append("RSS %.1fMB with a handful of jobs left (was %.1fMB full)" % (r["rss_pinned_mb"], r["rss_full_mb"]))
print("RESULT: FAIL - " + "; ".join(bad) if bad else "RESULT: PASS")
sys.exit(1 if bad else 0)
EOF
