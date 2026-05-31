#!/usr/bin/env bash
# bench/modes/D-memory.sh — RSS growth under sustained PUT pressure.
# This is the mode that maps directly to the production pain from 2026-04-24:
# "third-party service floods beanstalkd with jobs, memory explodes".
#
# Protocol: start clean (no WAL), baseline RSS, pipeline N puts of a fixed
# body size in batches of SAMPLE_EVERY, sample RSS after each batch. Report
# baseline, peak, final, kb-per-1k-jobs.
# Expected: fork ≈ upstream (our memory pool is Job-struct reuse, not body
# offloading). Fork >> upstream is a regression; fork << upstream is worth
# investigating.

set -euo pipefail

OUT_DIR="${1:?usage: D-memory.sh <out-dir>}"
SELF_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$SELF_DIR/lib/common.sh"

N="${BENCH_N:-20000}"
BODY_SIZE="${BENCH_BODY_SIZE:-1024}"
SAMPLE_EVERY="${BENCH_SAMPLE:-2000}"

other_role() { [ "$1" = upstream ] && echo fork || echo upstream; }

run_one() {
  local role="$1"
  local samples="$OUT_DIR/$role.samples"
  : > "$samples"

  ensure_vm_running "$role"
  ensure_vm_stopped "$(other_role "$role")" || true
  vm_start_server "$role"
  sleep 0.5

  local baseline_rss
  baseline_rss=$(vm_rss_kb "$role")
  printf '0\t%s\n' "$baseline_rss" >> "$samples"

  # Host-side interpolation of N/BSZ/SMP.
  limactl shell "$(vm_name "$role")" -- bash -s >> "$samples" <<EOF
set -e
N=${N}
BSZ=${BODY_SIZE}
SMP=${SAMPLE_EVERY}
BODY=\$(head -c "\$BSZ" /dev/urandom | base64 -w0 | head -c "\$BSZ")

PID=\$(cat /tmp/bsd.pid)
rss() { awk '/^VmRSS:/ { print \$2 }' /proc/\$PID/status; }

put_batch() {
  local from=\$1 to=\$2
  {
    printf 'use memtest\r\n'
    for i in \$(seq "\$from" "\$to"); do
      printf 'put 0 0 3600 %d\r\n%s\r\n' "\$BSZ" "\$BODY"
    done
    printf 'quit\r\n'
  } | nc -q1 127.0.0.1 11300 > /dev/null
}

i=0
while [ "\$i" -lt "\$N" ]; do
  next=\$(( i + SMP ))
  [ "\$next" -gt "\$N" ] && next=\$N
  put_batch \$(( i + 1 )) "\$next"
  printf '%d\t%s\n' "\$next" "\$(rss)"
  i=\$next
done
EOF

  vm_stop_server "$role"
}

REPORT="$OUT_DIR/report.txt"
{
  echo "=== Mode D: RSS growth under PUT pressure ==="
  echo "N=$N body_size=${BODY_SIZE}B sample_every=$SAMPLE_EVERY"
  echo "Each line: jobs_in<TAB>RSS_kb"
  echo

  for r in upstream fork; do
    echo "-- $r --"
    run_one "$r"
    cat "$OUT_DIR/$r.samples"
    echo
  done

  echo "=== Summary ==="
  printf '%-10s %-14s %-14s %-14s %-14s\n' role baseline_kb peak_kb final_kb kb_per_1k_jobs
  for r in upstream fork; do
    local_file="$OUT_DIR/$r.samples"
    base=$(head -1 "$local_file" | cut -f2)
    peak=$(awk -F'\t' 'NR>1 {if ($2>m) m=$2} END{print m+0}' "$local_file")
    final=$(tail -1 "$local_file" | cut -f2)
    final_n=$(tail -1 "$local_file" | cut -f1)
    if [ "$final_n" -gt 0 ] 2>/dev/null; then
      dkpk=$(awk -v b="$base" -v f="$final" -v n="$final_n" 'BEGIN { printf "%.1f", (f-b)*1000/n }')
    else
      dkpk=0
    fi
    printf '%-10s %-14s %-14s %-14s %-14s\n' "$r" "$base" "$peak" "$final" "$dkpk"
  done
} | tee "$REPORT"
