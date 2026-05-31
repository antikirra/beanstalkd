#!/usr/bin/env bash
# bench/ab.sh — master A/B orchestrator.
# Runs one or all test modes sequentially against upstream + fork VMs.
# Sequential by design: each run gets the host to itself for honest numbers.
# Usage:
#   bench/ab.sh A              # functional diff
#   bench/ab.sh B              # throughput
#   bench/ab.sh C              # durability/recovery
#   bench/ab.sh D              # memory footprint
#   bench/ab.sh all            # A then B then C then D
#   bench/ab.sh --no-sync A    # skip fork rebuild
#
# Before each run, by default: `sync-fork.sh` rsyncs the current host repo
# into the fork VM and does `make clean && make`.

set -euo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SELF_DIR/.." && pwd)"

SYNC=1
MODES=()
for arg in "$@"; do
  case "$arg" in
    --no-sync) SYNC=0 ;;
    A|B|C|D) MODES+=("$arg") ;;
    all) MODES=(A B C D) ;;
    *) echo "usage: $0 [--no-sync] {A|B|C|D|all}..." >&2; exit 2 ;;
  esac
done

[ ${#MODES[@]} -eq 0 ] && { echo "no modes" >&2; exit 2; }

TS=$(date +%Y%m%d-%H%M%S)
OUT_BASE="$SELF_DIR/results/$TS"
mkdir -p "$OUT_BASE"

# Record repo context.
(
  cd "$REPO_DIR"
  echo "=== Host context ==="
  git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo "no git"
  git -C "$REPO_DIR" status --short 2>/dev/null || true
  echo "--- Upstream VM SHA ---"
  limactl shell beanstalkd-upstream -- cat ~/upstream/VERSION.info 2>&1 || true
) > "$OUT_BASE/context.txt"

if [ "$SYNC" -eq 1 ]; then
  echo "[ab] syncing current repo -> fork VM"
  "$SELF_DIR/sync-fork.sh" 2>&1 | tee "$OUT_BASE/sync-fork.log"
fi

for m in "${MODES[@]}"; do
  mdir="$OUT_BASE/$m"
  mkdir -p "$mdir"
  echo
  echo "======================================================================"
  echo "[ab] mode $m → $mdir"
  echo "======================================================================"
  case "$m" in
    A) bash "$SELF_DIR/modes/A-functional.sh" "$mdir"  ;;
    B) bash "$SELF_DIR/modes/B-throughput.sh" "$mdir"  ;;
    C) bash "$SELF_DIR/modes/C-recovery.sh"   "$mdir"  ;;
    D) bash "$SELF_DIR/modes/D-memory.sh"     "$mdir"  ;;
  esac
done

echo
echo "======================================================================"
echo "[ab] all done. results: $OUT_BASE"
echo "======================================================================"
ls -la "$OUT_BASE"
