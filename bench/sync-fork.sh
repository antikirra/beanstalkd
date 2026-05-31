#!/usr/bin/env bash
# bench/sync-fork.sh — rsync current host repo into beanstalkd-fork VM and rebuild.
# Clean build every invocation: make clean && make inside the VM.

set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SELF_DIR/.." && pwd)"
VM=beanstalkd-fork

# Ensure VM is up.
if ! limactl list 2>/dev/null | awk -v n="$VM" '$1==n && $2=="Running" {f=1} END{exit !f}'; then
  echo "[sync-fork] VM $VM not running, starting..."
  limactl start "$VM" --tty=false >/dev/null
fi

SSH_CONFIG="$HOME/.lima/$VM/ssh.config"
if [ ! -r "$SSH_CONFIG" ]; then
  echo "[sync-fork] ssh.config missing at $SSH_CONFIG" >&2
  exit 1
fi

SSH_HOST="lima-${VM}"

# Prepare dest.
ssh -F "$SSH_CONFIG" "$SSH_HOST" 'mkdir -p ~/fork'

# Sync (exclude git, build artifacts, VM configs, bench results).
echo "[sync-fork] rsync $REPO_DIR -> $SSH_HOST:~/fork"
rsync -az --delete \
  --exclude='.git/' \
  --exclude='.claude/' \
  --exclude='*.o' \
  --exclude='beanstalkd' \
  --exclude='ct/_ctcheck*' \
  --exclude='lima*.yaml' \
  --exclude='bench/results/' \
  --exclude='*.log' \
  -e "ssh -F $SSH_CONFIG" \
  "$REPO_DIR/" "$SSH_HOST:fork/"

# Clean build inside VM.
echo "[sync-fork] make clean && make"
ssh -F "$SSH_CONFIG" "$SSH_HOST" 'cd ~/fork && make clean >/dev/null 2>&1; make 2>&1 | tail -5'
ssh -F "$SSH_CONFIG" "$SSH_HOST" 'ls -lh ~/fork/beanstalkd'
echo "[sync-fork] done"
