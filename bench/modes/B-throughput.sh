#!/usr/bin/env bash
# bench/modes/B-throughput.sh — PUT/DELETE throughput measurement.
# Generates N put+delete command pairs inside the VM, pipes into a single
# nc session, measures wall time. Single-connection, sequential, 1-byte bodies.
# Coarse number — NOT a scientific benchmark — 1:1 relative comparison only.

set -euo pipefail

OUT_DIR="${1:?usage: B-throughput.sh <out-dir>}"
SELF_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$SELF_DIR/lib/common.sh"

N="${BENCH_N:-5000}"
BODY="${BENCH_BODY:-x}"
BODY_LEN=${#BODY}

other_role() { [ "$1" = upstream ] && echo fork || echo upstream; }

run_one() {
  local role="$1"
  local out="$OUT_DIR/$role.txt"

  ensure_vm_running "$role"
  ensure_vm_stopped "$(other_role "$role")" || true
  vm_start_server "$role"

  # Host-side interpolation: N, BODY, LEN substituted at send time.
  limactl shell "$(vm_name "$role")" -- bash -s > "$out" <<EOF
set -e
N=${N}
BODY='${BODY}'
LEN=${BODY_LEN}

TMP=\$(mktemp)
{
  printf 'use bench\r\n'
  printf 'watch bench\r\n'
  printf 'ignore default\r\n'
  for i in \$(seq 1 \$N); do
    printf 'put 0 0 60 %d\r\n%s\r\n' "\$LEN" "\$BODY"
  done
  for i in \$(seq 1 \$N); do
    printf 'reserve\r\n'
    printf 'delete %d\r\n' "\$i"
  done
} > "\$TMP"

start_ns=\$(date +%s%N)
nc -q1 127.0.0.1 11300 < "\$TMP" > /dev/null
end_ns=\$(date +%s%N)
rm -f "\$TMP"

elapsed_ms=\$(( (end_ns - start_ns) / 1000000 ))
elapsed_s=\$(awk -v ms="\$elapsed_ms" 'BEGIN { printf "%.3f", ms/1000 }')
ops=\$(( N * 3 ))
rate=\$(awk -v n="\$ops" -v s="\$elapsed_s" 'BEGIN { printf "%.0f", n/s }')
printf 'n_pairs=%d ops=%d elapsed_s=%s ops_per_sec=%s\n' "\$N" "\$ops" "\$elapsed_s" "\$rate"
EOF

  vm_stop_server "$role"
  cat "$out"
}

REPORT="$OUT_DIR/report.txt"
{
  echo "=== Mode B: throughput (PUT+RESERVE+DELETE, 1 conn, ${N} pairs, body=${BODY_LEN}B) ==="
  echo "NOTE: QEMU TCG emulation — absolute ops/s are not meaningful."
  echo "      Only the upstream↔fork ratio on the same host is interpretable."
  echo
  echo -n "upstream: "; run_one upstream
  echo -n "fork:     "; run_one fork
} | tee "$REPORT"
