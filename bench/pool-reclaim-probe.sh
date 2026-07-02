#!/usr/bin/env bash
# bench/pool-reclaim-probe.sh — targeted Mode-D probe for the job-pool drain.
#
# The stock D-memory.sh only PUTs (jobs stay live), so the size-class pool —
# which fills on job_free/delete — is never exercised. This probe drives the
# exact path the drain optimization targets:
#   PUT N  → jobs live, pool empty            (RSS_live)
#   DELETE N → jobs freed into the pool       (RSS_pooled, pool holds <=8MB)
#   idle > -m → trim tick: drain + malloc_trim(RSS_idle)
# Reclaimed = RSS_pooled - RSS_idle. With the drain, idle should fall back
# toward baseline; without it, the pool's pages stay resident.
#
# Assumes the fork VM already has a freshly-built binary (run sync-fork.sh).
# Usage: bench/pool-reclaim-probe.sh <label> <out-dir>

set -euo pipefail

LABEL="${1:?usage: pool-reclaim-probe.sh <label> <out-dir>}"
OUT_DIR="${2:?usage: pool-reclaim-probe.sh <label> <out-dir>}"
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SELF_DIR/lib/common.sh"

N="${BENCH_N:-30000}"
BODY_SIZE="${BENCH_BODY_SIZE:-1024}"
TRIM_SEC="${BENCH_TRIM_SEC:-2}"
IDLE_SEC="${BENCH_IDLE_SEC:-6}"

mkdir -p "$OUT_DIR"
SAMPLES="$OUT_DIR/$LABEL.samples"
: > "$SAMPLES"

ensure_vm_running fork
ensure_vm_stopped upstream || true
vm_wipe_wal fork
vm_start_server fork "-m $TRIM_SEC"
sleep 0.5

baseline=$(vm_rss_kb fork)

# Host-side interpolation of N/BSZ/IDLE; escape VM-side $.
limactl shell "$(vm_name fork)" -- bash -s <<EOF >> "$SAMPLES"
set -e
N=${N}
BSZ=${BODY_SIZE}
IDLE=${IDLE_SEC}
BODY=\$(head -c "\$BSZ" /dev/urandom | base64 -w0 | head -c "\$BSZ")
PID=\$(cat /tmp/bsd.pid)
rss() { awk '/^VmRSS:/ { print \$2 }' /proc/\$PID/status; }

# Phase 1: PUT N jobs (Ready), pool stays empty.
{
  printf 'use reclaim\r\n'
  for i in \$(seq 1 "\$N"); do printf 'put 0 0 3600 %d\r\n%s\r\n' "\$BSZ" "\$BODY"; done
  printf 'quit\r\n'
} | nc -q1 127.0.0.1 11300 > /dev/null
printf 'live\t%s\n' "\$(rss)"

# Phase 2: DELETE every job by id → freed into the size-class pool.
{
  for i in \$(seq 1 "\$N"); do printf 'delete %d\r\n' "\$i"; done
  printf 'quit\r\n'
} | nc -q1 127.0.0.1 11300 > /dev/null
printf 'pooled\t%s\n' "\$(rss)"

# Phase 3: full quiescence past the -m trim interval. prottick feeds the
# trim deadline into its epoll timeout, so an idle server self-wakes at the
# -m cadence and drain + malloc_trim fire with zero client traffic. A plain
# sleep is the honest probe — the old stats trickle here was a workaround
# for the idle-never-trims bug and would mask its regression.
sleep "\$IDLE"
printf 'idle\t%s\n' "\$(rss)"
EOF

vm_stop_server fork

live=$(awk -F'\t'   '$1=="live"   {print $2}' "$SAMPLES")
pooled=$(awk -F'\t' '$1=="pooled" {print $2}' "$SAMPLES")
idle=$(awk -F'\t'   '$1=="idle"   {print $2}' "$SAMPLES")
reclaimed=$(( pooled - idle ))

{
  echo "=== pool-reclaim probe: $LABEL ==="
  echo "N=$N body=${BODY_SIZE}B -m=${TRIM_SEC}s idle=${IDLE_SEC}s"
  printf '%-10s %s kB\n' baseline "$baseline"
  printf '%-10s %s kB\n' live     "$live"
  printf '%-10s %s kB\n' pooled   "$pooled"
  printf '%-10s %s kB\n' idle     "$idle"
  printf '%-10s %s kB  (pooled - idle)\n' reclaimed "$reclaimed"
} | tee "$OUT_DIR/$LABEL.report"
