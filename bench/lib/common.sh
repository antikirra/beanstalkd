#!/usr/bin/env bash
# bench/lib/common.sh — shared helpers for beanstalkd A/B harness.
# Sourced by mode scripts; never run directly.

set -euo pipefail

# Relative (to $HOME inside VM) path to each role's built binary.
vm_binary_rel() {
  case "$1" in
    upstream) echo "upstream/beanstalkd" ;;
    fork)     echo "fork/beanstalkd" ;;
    *) echo "unknown role: $1" >&2; return 1 ;;
  esac
}

vm_name() {
  case "$1" in
    upstream) echo "beanstalkd-upstream" ;;
    fork)     echo "beanstalkd-fork" ;;
    *) echo "unknown role: $1" >&2; return 1 ;;
  esac
}

# vm_exec <role> <cmd...> — run command inside VM.
vm_exec() {
  local role="$1"; shift
  limactl shell "$(vm_name "$role")" -- "$@"
}

# vm_run <role> — read bash script from stdin, run inside VM.
# Unlike a heredoc-inside-function, caller controls interpolation.
vm_run() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- bash -s
}

# vm_start_server <role> [extra beanstalkd args...]
# Starts beanstalkd inside the VM, PID → /tmp/bsd.pid, log → /tmp/bsd.log.
# Waits until :11300 listens.
vm_start_server() {
  local role="$1"; shift
  local extra="$*"
  local bin_rel
  bin_rel="$(vm_binary_rel "$role")"

  # Host-side interpolation of bin_rel and extra; escape $HOME/$! for VM.
  limactl shell "$(vm_name "$role")" -- bash -s <<EOF
set -e
bin="\$HOME/${bin_rel}"
# Do NOT wipe /tmp/bsd-wal here — caller owns WAL lifecycle.
# (Mode C needs WAL to survive across kill+restart for replay.)
mkdir -p /tmp/bsd-wal
rm -f /tmp/bsd.pid /tmp/bsd.log

setsid "\$bin" -l 127.0.0.1 -p 11300 ${extra} >/tmp/bsd.log 2>&1 < /dev/null &
echo \$! > /tmp/bsd.pid
disown

for i in \$(seq 1 50); do
  if ss -ltn 2>/dev/null | grep -q ':11300 '; then
    exit 0
  fi
  sleep 0.1
done
echo "timeout waiting for beanstalkd to listen" >&2
cat /tmp/bsd.log >&2 || true
exit 1
EOF
}

# vm_stop_server <role>
vm_stop_server() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- bash -s <<'EOF'
set +e
if [ -r /tmp/bsd.pid ]; then
  pid=$(cat /tmp/bsd.pid)
  kill "$pid" 2>/dev/null
  for i in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || exit 0
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null
fi
EOF
}

vm_kill9_server() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- bash -s <<'EOF'
set +e
[ -r /tmp/bsd.pid ] && kill -9 "$(cat /tmp/bsd.pid)" 2>/dev/null
EOF
}

vm_pid() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- bash -s <<'EOF'
cat /tmp/bsd.pid 2>/dev/null || true
EOF
}

vm_rss_kb() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- bash -s <<'EOF'
pid=$(cat /tmp/bsd.pid 2>/dev/null)
[ -z "$pid" ] && { echo 0; exit 0; }
awk '/^VmRSS:/ { print $2 }' /proc/$pid/status 2>/dev/null || echo 0
EOF
}

# wire_send <role> — stream stdin as wire commands to beanstalkd in VM, echo replies.
wire_send() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- nc -q1 127.0.0.1 11300
}

ensure_vm_running() {
  local role="$1"
  local name; name="$(vm_name "$role")"
  if ! limactl list 2>/dev/null | awk -v n="$name" '$1==n && $2=="Running" {f=1} END{exit !f}'; then
    echo "[harness] starting VM $name" >&2
    limactl start "$name" --tty=false >/dev/null
  fi
}

ensure_vm_stopped() {
  local role="$1"
  local name; name="$(vm_name "$role")"
  if limactl list 2>/dev/null | awk -v n="$name" '$1==n && $2=="Running" {f=1} END{exit !f}'; then
    echo "[harness] stopping VM $name for honest sequential run" >&2
    limactl stop "$name" >/dev/null 2>&1 || true
  fi
}

# vm_wipe_wal <role> — explicitly clear WAL dir inside the VM.
# Callers that need a fresh WAL call this before the first start;
# callers testing replay across kill must NOT call between kill and restart.
vm_wipe_wal() {
  local role="$1"
  limactl shell "$(vm_name "$role")" -- rm -rf /tmp/bsd-wal
}
