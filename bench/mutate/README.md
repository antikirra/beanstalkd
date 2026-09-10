# Mutation testing

Coverage says a line ran. Mutation testing says a test would have
noticed if the line were wrong — which is the property this suite is
actually claiming.

    python3 bench/mutate/mutate.py conn.c,job.c 12 2
                                   ^files       ^per-file  ^seed

Each mutant flips one comparison or boolean operator, rebuilds only what
changed and runs the suite. A mutant the suite still passes is either a
hole in the tests or an *equivalent mutant* — a change with no
observable difference, which is a fact about the code, not a defect.
Judge every survivor by hand; do not chase the score.

Last run (2026-09-10, `conn.c,job.c`, 12 per file, seed 2):
20 compiled, 14 killed, 6 survived. Five of the six were equivalent
(both sides of the boundary produce the same result, or the bound is
unreachable); the sixth was real — `on_watch_remove` copying a hint
from one slot past the array — and is now covered by
`cttest_on_watch_remove_never_reads_past_the_hint_array`, which fails
under the ASan gate when the bound is loosened.

Second run (2026-09-10, `file.c,walg.c`, 10 per file, seed 3): 19
compiled, 12 killed, 7 survived. One survivor was real — the v7 WAL
reader's `namelen >= MAX_TUBE_NAME_LEN` bound, which nothing pinned at
the boundary; loosening it writes the NUL terminator one past the name
buffer. Both readers now have a pair of tests at exactly that edge
(`..._a_namelen_at_the_buffer_size_is_refused` and
`..._the_longest_legal_name_still_replays`), and the first fails under
ASan when the bound moves. The others were equivalent, and one was an
artefact of the mutator editing a trailing comment — it no longer does.

Third run (2026-09-10, `prot.c`, 14, seed 5): 14 compiled, 8 killed, 6
survived. All six are equivalent or defensive — a NULL guard on a path
where the caller has already checked, a write to an out-parameter the
caller ignores on failure, a boundary whose two sides produce the same
result — with one exception worth naming: loosening the TCP_CORK
heuristic survives because corking a lone command produces identical
bytes on the wire. It is only visible as two extra setsockopt calls, so
`bench/syscalls/count.sh` is where that property is pinned, not the
test suite.

Fourth run (2026-09-10, `heap.c,ms.c,tube.c`, 8 each, seed 7): 21
compiled, 13 killed, 8 survived. Two were real gaps, both now closed:

- `heapremove`'s `k < h->len` loosened to `<=` — the exact shape of
  upstream #670 — survived because the existing test looks for the
  removed element inside the heap, and it lands one slot past what that
  loop walks. What it does leave behind is a write through the element
  the caller now owns, so the new test poisons the recorded position and
  checks nothing wrote to it.
- `heapresift`'s bound at `len`, which callers reach with a cached index
  that can go stale.

The rest are equivalent: prefetch hints (a hint at a wrong address is
still just a hint), an unreachable overflow bound, and the branch
thresholds inside the tube-name hash, where a different-but-still-good
hash keeps every property the tests state.

Fifth run (2026-09-10, `util.c,serv.c,net.c`, 8 each, seed 11): 23
compiled, 17 killed, 6 survived — the best score so far, and the
survivors point at `net.c`, which is socket setup the unit tests barely
reach. One was worth acting on: a flipped `ai_family == AF_INET6`
survived because nothing bound an IPv6 listener at all, even though
README lists IPv6 as supported. There is a test now. The rest are the
systemd socket-activation branches (stubs without libsystemd) and a
boundary in the `-f` cap whose two sides give the same result.

Sixth run (2026-09-10, `prot.c,conn.c`, 16 each, seed 23): 30 compiled,
21 killed, 9 survived. Equivalent or unobservable, except one worth
recording as a known gap: OP_BURY's two WAL-failure branches (the
reservation failed before the transition vs. the write failed after it)
answer the client identically with INTERNAL_ERROR and differ only in
whether `restore_reserved_job` runs. Reaching the first from the wire
needs a binlog wound to exactly the point where the reservation cannot
grow, which is a lot of setup for a branch whose observable behaviour is
already covered; the accounting it protects is invariant #2, which has
its own end-to-end test.

It is slow (a rebuild and a full suite per mutant, minutes each), so it
is a deliberate exercise, not a CI gate.
