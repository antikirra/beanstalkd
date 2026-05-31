#!/usr/bin/env bash
# bench/investigate-memory2.sh — second-pass investigation.
# After 10K PUTs at body=1024, dump full /proc/PID/status and smaps_rollup
# for both roles so we can see WHERE the ~900 B/job delta lives (heap vs mmap,
# anon vs file, VmData vs VmLib, etc).
set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SELF_DIR/lib/common.sh"

N=10000
BSZ=1024

ensure_vm_running upstream
ensure_vm_running fork

run_and_dump() {
  local role="$1"
  local bin_rel; bin_rel="$(vm_binary_rel "$role")"

  limactl shell "$(vm_name "$role")" -- bash -s <<EOF
set -e
bin="\$HOME/${bin_rel}"
mkdir -p /tmp/bsd-wal
rm -f /tmp/bsd.pid /tmp/bsd.log
setsid "\$bin" -l 127.0.0.1 -p 11300 >/tmp/bsd.log 2>&1 < /dev/null &
echo \$! > /tmp/bsd.pid
disown
for i in \$(seq 1 50); do
  ss -ltn 2>/dev/null | grep -q ':11300 ' && break
  sleep 0.1
done

PID=\$(cat /tmp/bsd.pid)
echo "=== BEFORE ==="
grep -E '^(Vm|Rss)' /proc/\$PID/status
echo "---"
cat /proc/\$PID/smaps_rollup 2>/dev/null || echo "(no smaps_rollup)"
echo

# Put N jobs
N=${N}; BSZ=${BSZ}
BODY=\$(head -c "\$BSZ" /dev/urandom | base64 -w0 | head -c "\$BSZ")
{
  printf 'use memtest\r\n'
  for i in \$(seq 1 \$N); do
    printf 'put 0 0 3600 %d\r\n%s\r\n' "\$BSZ" "\$BODY"
  done
  printf 'quit\r\n'
} | nc -q1 127.0.0.1 11300 > /dev/null

echo "=== AFTER \$N PUT body=\$BSZ ==="
grep -E '^(Vm|Rss)' /proc/\$PID/status
echo "---"
cat /proc/\$PID/smaps_rollup 2>/dev/null || echo "(no smaps_rollup)"
echo

# Stats from beanstalkd
echo "=== beanstalkd stats ==="
printf 'stats\r\nquit\r\n' | nc -q1 127.0.0.1 11300 | head -50

# Cleanup
kill "\$PID" 2>/dev/null
wait 2>/dev/null || true
EOF
}

for r in upstream fork; do
  echo "################## $r ##################"
  run_and_dump "$r"
  echo
done
