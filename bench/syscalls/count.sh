#!/bin/sh
# bench/syscalls/count.sh [port] [iters] [depth] [conns] [wal]
#
# Counts the syscalls the server makes per command under a fixed
# workload (put -> reserve -> delete). The server spends most of its
# time in the kernel, so this number is the one that moves throughput;
# some of the properties behind it are invisible from the wire and can
# only be seen here.
#
# Expected on this workload, one connection, no pipelining, no WAL:
#
#   epoll_pwait ~1.0/cmd   read ~1.0/cmd   write ~0.67   writev ~0.33
#   setsockopt   2 TOTAL (both at accept: TCP_NODELAY + TCP_QUICKACK)
#
# Depth 1 is the floor for a request/response server: wait, read, reply.
# What the depths above it measure is reply coalescing. A pipelined
# burst stages its replies in one buffer and leaves in ONE write, so
# write+writev per command should collapse roughly as 1/depth:
#
#   depth  8: total ~0.42/cmd   (write ~0.17, writev 0)
#   depth 32: total ~0.11/cmd   (write ~0.04, writev 0)
#
# With `durable` as the fifth argument the server runs under -D, and the
# number to watch is fdatasync. Invariant #16 promises ONE per event-loop
# tick, amortised across every WAL-dirty command in it, so the count must
# fall with depth even though the durability contract does not weaken:
#
#   depth  1: fdatasync ~0.67/cmd   (a put and a delete each own a tick)
#   depth 32: fdatasync ~0.02/cmd   (one sync per ~48 commands)
#
# A count that stays flat as depth grows means the group commit has
# collapsed to disk-sync rate — the exact failure the batching exists to
# prevent, and one that costs throughput without costing correctness, so
# nothing else in the suite would notice.
#
# writev stays at ~0.67/cmd under -D: one per WAL record. Batching those
# into a single gathered writev per tick was measured as worth roughly 8%
# and rejected — it would put a staging buffer, its rollback and its
# accounting into the one path where a mistake loses acked jobs, and #16
# already removed the expensive half.
#
# Depth 8 sits above depth 32 because of the other half of the story,
# the input buffer: eight short commands fit the old one-line buffer
# too, so only the deeper burst shows what CMD_BUF_SIZE bought. At
# depth 32 read and epoll_pwait should be ~0.03/cmd each — one of each
# per burst. If read climbs back toward 0.1, CMD_BUF_SIZE shrank.
#
# Two numbers are the regression alarms. setsockopt must stay at 2 TOTAL
# at every depth: it used to be TCP_CORK, two calls per burst, and the
# burst still paid one write per command — if setsockopt grows with the
# command count, the coalescing is gone and the cork is back. And writev
# must stay at 0 above depth 1: a job body small enough for the burst
# buffer is copied into it, so a pipelined reserve run needs no writev
# of its own; a non-zero count means job replies stopped joining the
# burst and each one is ending it instead.
#
# Needs strace: run the container with --cap-add=SYS_PTRACE.
#
# The cmd/s this prints is measured WITH strace attached and is NOT a
# throughput figure: a traced syscall costs about two orders of
# magnitude more than a real one, so any change that removes syscalls
# reads several times better here than it does in production (the
# reply-coalescing A/B read +642% at depth 32 under strace and +32%
# without it). Use the counts from this script and take timings from a
# run with no tracer: bench/syscalls/loadgen.c against a plain server.
set -e
PORT=${1:-11290}
ITERS=${2:-3000}
DEPTH=${3:-1}
CONNS=${4:-1}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)

cd "$ROOT"
make >/dev/null
cc -O2 -o /tmp/loadgen "$HERE/loadgen.c"

case "$5" in
wal)
    D=$(mktemp -d); ./beanstalkd -l 127.0.0.1 -p "$PORT" -b "$D" & B=$!
    ;;
durable)
    D=$(mktemp -d); ./beanstalkd -l 127.0.0.1 -p "$PORT" -b "$D" -D & B=$!
    ;;
*)
    ./beanstalkd -l 127.0.0.1 -p "$PORT" & B=$!
    ;;
esac
sleep 0.7
strace -c -f -p "$B" -o /tmp/syscalls.out & S=$!
sleep 0.4
"${LOADGEN:-/tmp/loadgen}" "$PORT" "$ITERS" "$DEPTH" "$CONNS"
kill -INT "$S"; sleep 1
kill -TERM "$B" 2>/dev/null || true

TOTAL=$((ITERS * 3 * CONNS))
echo "--- syscalls for $TOTAL commands (depth=$DEPTH conns=$CONNS)"
awk -v t="$TOTAL" '
    /^[ ]*[0-9]/ && NF >= 5 && $NF != "total" {
        name=$NF; calls=$(NF-1)+0
        if (calls > 0) {
            printf "  %-14s %8d  %6.3f/cmd\n", name, calls, calls/t
            sum += calls
        }
    }
    END { printf "  %-14s %8d  %6.3f/cmd\n", "TOTAL", sum, sum/t }' /tmp/syscalls.out
