#!/usr/bin/env bash
# bench/modes/A-functional.sh — wire-protocol parity diff.
# Runs the SAME fixed sequence of beanstalkd commands against each role,
# captures responses, normalizes time-dependent stats lines, diffs.
#
# Expected differences vs upstream (declared in README + CLAUDE.md):
#   - `cmd-truncate` line in stats (additive, fork only)
#   - `truncate <tube>` command (unknown in upstream)
#   - trailing-space variants: `stats \r\n` → UNKNOWN_COMMAND in fork, BAD_FORMAT upstream
#   - `-H` HTTP responses (not exercised here)
# This script labels those as EXPECTED-DIFF, everything else as UNEXPECTED.

set -euo pipefail

OUT_DIR="${1:?usage: A-functional.sh <out-dir>}"
SELF_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=../lib/common.sh
source "$SELF_DIR/lib/common.sh"

# --- The wire script we send. Uses \r\n termination via printf.
# Deterministic sequence — both fresh servers must produce identical IDs.
WIRE_FILE="$OUT_DIR/wire.txt"
{
  # Each line becomes one wire message. `put` needs body immediately after.
  printf 'use testtube\r\n'
  printf 'watch testtube\r\n'
  printf 'ignore default\r\n'
  printf 'list-tubes-watched\r\n'
  printf 'stats-tube testtube\r\n'
  # put job with body "hello" (5 bytes)
  printf 'put 0 0 60 5\r\nhello\r\n'
  printf 'put 100 0 60 5\r\nworld\r\n'
  printf 'put 50 1 60 3\r\nabc\r\n'
  printf 'peek-ready\r\n'
  printf 'peek-delayed\r\n'
  printf 'stats-tube testtube\r\n'
  printf 'reserve-with-timeout 1\r\n'
  printf 'stats-job 1\r\n'
  # release it so the next reserve gets it again (same id=1)
  printf 'release 1 0 0\r\n'
  printf 'reserve-with-timeout 1\r\n'
  printf 'bury 1 0\r\n'
  printf 'stats-tube testtube\r\n'
  printf 'kick-job 1\r\n'
  printf 'delete 1\r\n'
  printf 'delete 2\r\n'
  printf 'kick 5\r\n'
  # stats command (cpu times, counters — will be normalized out)
  printf 'stats\r\n'
  printf 'list-tubes\r\n'
  printf 'pause-tube testtube 0\r\n'
  # known EXPECTED-DIFF zone: these are behavior differences
  printf 'truncate testtube\r\n'
  printf 'cmd-no-such-command\r\n'
  printf 'quit\r\n'
} > "$WIRE_FILE"

run_one() {
  local role="$1"
  local raw="$OUT_DIR/$role.raw"
  local norm="$OUT_DIR/$role.norm"

  echo "[A] ensuring $role VM state" >&2
  ensure_vm_running "$role"
  ensure_vm_stopped "$(other_role "$role")" || true

  echo "[A] starting server in $role" >&2
  vm_start_server "$role"

  echo "[A] sending wire script" >&2
  # shove wire file over stdin into nc-in-VM, capture response
  wire_send "$role" < "$WIRE_FILE" > "$raw" || true

  echo "[A] stopping server in $role" >&2
  vm_stop_server "$role"

  # Normalize volatile lines before diff: stats counters, pids, timestamps.
  # Keep deterministic fields (cmd-put: 3, current-jobs-ready, etc) — but strip values that
  # MUST diverge between processes (id, pid, rusage, hostname, uptime, version).
  sed -E \
    -e 's/^(id):.*/\1: <ID>/' \
    -e 's/^(hostname):.*/\1: <HOST>/' \
    -e 's/^(pid):.*/\1: <PID>/' \
    -e 's/^(uptime):.*/\1: <UPTIME>/' \
    -e 's/^(rusage-utime):.*/\1: <RUTIME>/' \
    -e 's/^(rusage-stime):.*/\1: <RSTIME>/' \
    -e 's/^(version):.*/\1: <VER>/' \
    -e 's/^(binlog-current-index):.*/\1: <BIDX>/' \
    -e 's/^(max-job-size):.*/\1: <MAXJOB>/' \
    -e 's/^(time-left):.*/\1: <TLEFT>/' \
    -e 's/^OK [0-9]+$/OK <N>/' \
    "$raw" > "$norm"
}

other_role() { [ "$1" = upstream ] && echo fork || echo upstream; }

run_one upstream
run_one fork

# Diff. Label EXPECTED vs UNEXPECTED.
DIFF_RAW="$OUT_DIR/diff.raw"
diff -u "$OUT_DIR/upstream.norm" "$OUT_DIR/fork.norm" > "$DIFF_RAW" || true

REPORT="$OUT_DIR/report.txt"
{
  echo "=== Mode A: wire-protocol functional diff ==="
  echo "upstream responses: $(wc -l <"$OUT_DIR/upstream.raw") lines"
  echo "fork responses:     $(wc -l <"$OUT_DIR/fork.raw") lines"
  echo
  if [ ! -s "$DIFF_RAW" ]; then
    echo "VERDICT: IDENTICAL (after normalization)."
  else
    echo "--- Diff (after normalization) ---"
    cat "$DIFF_RAW"
    echo
    # Expected-diff markers: lines mentioning cmd-truncate, truncate verb, etc.
    if grep -qE '(cmd-truncate|^\+TRUNCATED|truncate)' "$DIFF_RAW"; then
      echo "NOTE: contains EXPECTED fork additions (truncate/cmd-truncate)."
    fi
    echo
    echo "VERDICT: DIFF PRESENT — inspect manually, mark expected vs regression."
  fi
} | tee "$REPORT"
