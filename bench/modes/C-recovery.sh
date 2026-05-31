#!/usr/bin/env bash
# bench/modes/C-recovery.sh — WAL durability under SIGKILL.
# Starts server with -b /tmp/bsd-wal (async WAL, supported by both upstream
# and fork — fork-only -D intentionally NOT used here to keep apples-to-apples),
# puts N jobs, drains the fsync thread via a short settle pause, SIGKILLs,
# restarts, verifies current-jobs-ready after replay.
#
# Expected both: 0 ≤ loss ≤ N (async WAL has a best-effort window; with 500
# small jobs and ≥2s settle before kill, losses should be 0 on both). If
# fork loses MORE than upstream under identical conditions — regression.

set -euo pipefail

OUT_DIR="${1:?usage: C-recovery.sh <out-dir>}"
SELF_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$SELF_DIR/lib/common.sh"

N="${BENCH_N:-500}"
SETTLE="${BENCH_SETTLE:-2}"  # seconds to let async WAL flush before SIGKILL

other_role() { [ "$1" = upstream ] && echo fork || echo upstream; }

stats_count() {
  local role="$1" field="$2"
  printf 'stats\r\nquit\r\n' | wire_send "$role" \
    | awk -v f="$field" '$1 == f":" { gsub(/\r/,"",$2); print $2; exit }'
}

run_one() {
  local role="$1"
  local log="$OUT_DIR/$role.log"
  : > "$log"

  ensure_vm_running "$role"
  ensure_vm_stopped "$(other_role "$role")" || true
  vm_wipe_wal "$role"

  echo "-- $role: start with -b /tmp/bsd-wal (async WAL)" | tee -a "$log"
  vm_start_server "$role" -b /tmp/bsd-wal

  echo "-- $role: put $N jobs (body=1B)" | tee -a "$log"
  {
    printf 'use rec\r\n'
    for i in $(seq 1 "$N"); do
      printf 'put 0 0 60 1\r\nx\r\n'
    done
    printf 'quit\r\n'
  } | wire_send "$role" | tail -3 >> "$log"

  local before; before=$(stats_count "$role" current-jobs-ready)
  echo "-- $role: current-jobs-ready before kill = $before" | tee -a "$log"

  echo "-- $role: settle ${SETTLE}s (let async WAL flush)" | tee -a "$log"
  sleep "$SETTLE"

  echo "-- $role: SIGKILL" | tee -a "$log"
  vm_kill9_server "$role"
  sleep 1

  echo "-- $role: restart (WAL preserved), replay" | tee -a "$log"
  vm_start_server "$role" -b /tmp/bsd-wal
  sleep 1   # give replay time to complete on QEMU TCG

  local after; after=$(stats_count "$role" current-jobs-ready)
  echo "-- $role: current-jobs-ready after replay = $after" | tee -a "$log"

  vm_stop_server "$role"

  printf '%s\t%s\t%s\t%s\n' "$role" "$N" "${before:-0}" "${after:-0}"
}

REPORT="$OUT_DIR/report.txt"
{
  echo "=== Mode C: WAL durability under SIGKILL ==="
  echo "Both roles use -b /tmp/bsd-wal (async WAL, upstream-compatible)."
  echo "Fork-only -D not used here — tested in a dedicated fork-only mode if needed."
  echo "Settle = ${SETTLE}s before SIGKILL to let async fsync thread flush."
  echo
  printf '%-10s %-10s %-10s %-10s %s\n' role N before after status
  for r in upstream fork; do
    line=$(run_one "$r" | tail -1)
    role=$(echo "$line" | cut -f1)
    n=$(echo "$line" | cut -f2)
    b=$(echo "$line" | cut -f3)
    a=$(echo "$line" | cut -f4)
    if [ "$a" = "$n" ]; then
      s=OK
    else
      s="LOSS($((n - a)))"
    fi
    printf '%-10s %-10s %-10s %-10s %s\n' "$role" "$n" "$b" "$a" "$s"
  done
} | tee "$REPORT"
