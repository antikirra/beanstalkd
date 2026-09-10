// Angry tests for fileadd (file.c) — the append that builds the Wal's
// file list. walgc, walscandir and usenext all walk that list and all
// assume the same three things: it is NULL-terminated, head is first and
// tail is last, and w->nfile equals the number of nodes reachable from
// head. Nothing else in the tree asserts any of them.

#include "dat.h"
#include "ct/ct.h"
#include "testinject.h"
#include <stdio.h>
#include <string.h>

static void
fa_setup(void)
{
    fault_clear_all();
    progname = "testfile_fileadd";
}

// Collects the seq of every file reachable from the head, stopping at a
// generous bound so a cycle cannot hang the run.
static int
fa_walk(Wal *w, int *out, int max)
{
    int n = 0;
    for (File *p = w->head; p && n < max; p = p->next)
        out[n++] = p->seq;
    return n;
}


void
cttest_fileadd_links_every_file_in_insertion_order(void)
{
    fa_setup();
    Wal w;
    File a, b, c, d;
    memset(&w, 0, sizeof w);
    memset(&a, 0, sizeof a);
    memset(&b, 0, sizeof b);
    memset(&c, 0, sizeof c);
    memset(&d, 0, sizeof d);
    a.seq = 11; b.seq = 22; c.seq = 33; d.seq = 44;

    fileadd(&a, &w);
    fileadd(&b, &w);
    fileadd(&c, &w);
    fileadd(&d, &w);

    int want[4] = {11, 22, 33, 44};
    int got[8];
    int n = fa_walk(&w, got, 8);

    assertf(n == 4 && memcmp(got, want, sizeof want) == 0,
            "the list must hold exactly the four files in insertion order "
            "and terminate: walked %d nodes", n);
}


void
cttest_fileadd_counts_exactly_the_files_reachable_from_the_head(void)
{
    fa_setup();
    Wal w;
    File a, b, c;
    memset(&w, 0, sizeof w);
    memset(&a, 0, sizeof a);
    memset(&b, 0, sizeof b);
    memset(&c, 0, sizeof c);
    a.seq = 1; b.seq = 2; c.seq = 3;

    fileadd(&a, &w);
    fileadd(&b, &w);
    fileadd(&c, &w);

    int got[8];
    int n = fa_walk(&w, got, 8);

    assertf(w.nfile == n,
            "nfile must equal the number of reachable files: counter says "
            "%d, the list holds %d", w.nfile, n);
}


void
cttest_fileadd_makes_the_first_file_both_head_and_tail(void)
{
    fa_setup();
    Wal w;
    File a;
    memset(&w, 0, sizeof w);
    memset(&a, 0, sizeof a);
    a.seq = 7;

    fileadd(&a, &w);

    assertf(w.head == &a && w.tail == &a,
            "the only file in the list is both its head and its tail");
}


// A File struct that carries a stale ->next (a reused or partially
// initialised node) is spliced in whole: the list then reaches files the
// Wal never adopted, while nfile counts only the one that was appended.
// walgc walks the list and frees what it finds there.
void
cttest_fileadd_must_not_splice_a_foreign_tail_into_the_list(void)
{
    fa_setup();
    Wal w;
    File stray, reused;
    memset(&w, 0, sizeof w);
    memset(&stray, 0, sizeof stray);
    memset(&reused, 0, sizeof reused);
    stray.seq = 999;
    reused.seq = 1;
    reused.next = &stray;        // left over from a previous life

    fileadd(&reused, &w);

    int got[8];
    int n = fa_walk(&w, got, 8);

    assertf(n == w.nfile,
            "one file was appended, so exactly one must be reachable: the "
            "list walks %d nodes while nfile says %d", n, w.nfile);
}
