#!/bin/bash
# S3-only A/B: 8 conns × 2K ops, pipeline=16, 16KB body, WAL on (-b -f 50).
# Usage: s3-only.sh [label] — prints one line per binary.
set -euo pipefail
BENCH=${BENCH:-/usr/local/bin/bench}
label=${1:-run}

run_s3() {
    local bin=$1 port=$2
    local w="/tmp/wal-s3-$$-$port"; rm -rf "$w"; mkdir -p "$w"
    $bin -p $port -b "$w" -f 50 >/dev/null 2>&1 &
    local pid=$!; sleep 1
    local out
    out=$($BENCH -p $port -c 8 -n 2000 -P 16 -B 16384 2>/dev/null)
    kill $pid 2>/dev/null; wait $pid 2>/dev/null || true
    local tries=0
    while fuser $port/tcp >/dev/null 2>&1 && [ $tries -lt 20 ]; do
        fuser -k $port/tcp >/dev/null 2>&1 || true; sleep 0.1; tries=$((tries+1))
    done
    rm -rf "$w"
    echo "$out" | awk -v n="$3" '/Rate:/{r=$2} END{printf "%s %s\n", n, r}'
}

run_s3 /usr/local/bin/beanstalkd-upstream 11600 upstream
run_s3 "${FORKBIN:-/usr/local/bin/beanstalkd-fork}" 11700 fork
