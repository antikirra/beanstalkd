#!/usr/bin/env python3
"""Mutation testing: flip one operator in a product file, rebuild only what
changed, run the suite. A mutant the suite still passes is a hole."""
import os, random, re, subprocess, sys

SRC = os.environ.get("MUTATE_SRC", os.getcwd())
FILES = sys.argv[1].split(",")
N = int(sys.argv[2])
SEED = int(sys.argv[3]) if len(sys.argv) > 3 else 1
random.seed(SEED)

MUTS = [
    (r"(?<![<>=!+\-*/])<=(?!=)", "<"), (r"(?<![<>=!+\-*/])>=(?!=)", ">"),
    (r"(?<![<>=!+\-*/])<(?![=<])", "<="), (r"(?<![<>=!+\-*/])>(?![=>])", ">="),
    (r"==", "!="), (r"!=", "=="),
    (r"&&", "||"), (r"\|\|", "&&"),
]

def code_prefix(line):
    """The part of the line before any trailing comment.

    Mutating inside a comment produces a mutant identical to the
    original, which then shows up as a survivor and wastes a reviewer's
    time. This is a line-level approximation: it does not track block
    comments spanning lines, which the skip below covers well enough."""
    cut = len(line)
    for marker in ("//", "/*"):
        k = line.find(marker)
        if k != -1:
            cut = min(cut, k)
    return line[:cut]


def is_masked(code):
    """Per-column mask: True where the character is inside a string or
    character literal.

    Same reason as comments, one step subtler. `printf(">%d job ...")`
    holds a `>`; turning it into `>=` mutates a MESSAGE, so the suite
    passes and the line is reported as an uncovered survivor. It is not
    one — nothing about the program's behaviour changed. Two of the
    seven survivors in the first run after the pipeline work were this,
    and both cost a reviewer the trip."""
    mask = [False] * len(code)
    quote = None
    i = 0
    while i < len(code):
        ch = code[i]
        if quote:
            mask[i] = True
            if ch == "\\" and i + 1 < len(code):
                mask[i + 1] = True
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("\"", "'"):
            quote = ch
            mask[i] = True
        i += 1
    return mask


def candidates(text):
    out = []
    in_block = False
    for i, line in enumerate(text.splitlines()):
        st = line.strip()
        if in_block:
            if "*/" in line:
                in_block = False
            continue
        if "/*" in line and "*/" not in line:
            in_block = True
        if not st or st.startswith(("//", "*", "/*", "#")):
            continue
        code = code_prefix(line)
        mask = is_masked(code)
        for pat, rep in MUTS:
            for m in re.finditer(pat, code):
                if any(mask[m.start():m.end()]):
                    continue
                out.append((i, m.start(), m.end(), rep))
    return out

# MUTATE_ASAN=1 builds the mutants under AddressSanitizer. Worth the
# ~3x slowdown when the file under test does pointer arithmetic: a
# mutated loop bound ("k < len" -> "k <= len") reads one element past
# the array, which plain -O1 usually gets away with and ASan never
# does. Without it such mutants are reported as survivors and read as
# missing tests when what is missing is the detector.
_ASAN = os.environ.get("MUTATE_ASAN") == "1"
_CFLAGS = ("-O1 -g -Wall -Wformat=2"
           + (" -fsanitize=address -fno-omit-frame-pointer" if _ASAN else ""))
_LDFLAGS = "-fsanitize=address" if _ASAN else ""
BUILD = (f"make CFLAGS='{_CFLAGS}' LDFLAGS='{_LDFLAGS}' "
         f"LDLIBS='-lrt -lpthread' ct/_ctcheck")
if _ASAN:
    os.environ["ASAN_OPTIONS"] = "allocator_may_return_null=1:detect_leaks=0"
def sh(cmd, timeout=600):
    return subprocess.run(cmd, shell=True, cwd=SRC, capture_output=True,
                          text=True, timeout=timeout)

# baseline must be green
sh("make clean")
b = sh(BUILD)
assert b.returncode == 0, b.stdout[-800:] + b.stderr[-800:]
b = sh("ct/_ctcheck")
assert b.returncode == 0, "baseline suite is not green"

killed = survived = nobuild = 0
survivors = []
for f in FILES:
    path = os.path.join(SRC, f)
    orig = open(path).read()
    cands = candidates(orig)
    random.shuffle(cands)
    for (ln, a, bcol, rep) in cands[:N]:
        lines = orig.splitlines(keepends=True)
        line = lines[ln]
        lines[ln] = line[:a] + rep + line[bcol:]
        open(path, "w").write("".join(lines))
        try:
            r = sh(BUILD)
            if r.returncode != 0:
                nobuild += 1
                continue
            t = sh("ct/_ctcheck", timeout=600)
            if t.returncode == 0:
                survived += 1
                survivors.append(f"{f}:{ln+1}: {line.strip()[:66]}  ->  {rep}")
            else:
                killed += 1
        except subprocess.TimeoutExpired:
            killed += 1
        finally:
            open(path, "w").write(orig)
print(f"killed={killed} survived={survived} did-not-compile={nobuild}")
if killed + survived:
    print(f"mutation score = {100*killed/(killed+survived):.0f}%")
for s in survivors:
    print("SURVIVED", s)
