#!/usr/bin/env python3
"""verify_model.py ORIGINAL.cnf MODEL [--trail PREFIX.trail] [--all]

Independent check that a (partial) assignment satisfies the ORIGINAL formula.

MODEL is Dagster's -o output (one solution per line; the first is checked unless
--all) or any file of literals ('s'/'c' lines ignored, 'v' prefixes stripped).
Dagster prunes models to the literals that satisfy clauses, so an unassigned
variable satisfies nothing; tautologies are counted as satisfied.  The tdag trail
(literals fixed by unit propagation / pure literals) is merged in first.

Prints VALID or INVALID with the first falsified clause.  Exit 0/1, 2 on error.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cnf_io import read_cnf, is_tautology  # noqa: E402


def read_solution_lines(path):
    lines = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if not s or s[0] in "sc":
                continue
            if s[0] == "v":
                s = s[1:]
            lits = [int(t) for t in s.split() if t != "0"]
            if lits:
                lines.append(lits)
    return lines


def check(clauses, nvars, lits, trail):
    val = {}
    for l in list(trail) + list(lits):
        v = abs(l)
        if v > nvars:            # tdag's auxiliary gate variable
            continue
        s = l > 0
        if val.get(v, s) != s:
            return False, "model assigns variable %d both ways" % v
        val[v] = s
    for i, c in enumerate(clauses):
        if any(val.get(abs(l)) == (l > 0) for l in c):
            continue
        if is_tautology(c):
            continue
        return False, "clause %d not satisfied: %s" % (i, " ".join(map(str, c)))
    return True, "%d of %d variables assigned" % (len(val), nvars)


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("--")]
    trail_path = None
    if "--trail" in argv:
        trail_path = argv[argv.index("--trail") + 1]
        args.remove(trail_path)
    if len(args) != 2:
        print(__doc__)
        return 2
    nvars, clauses = read_cnf(args[0])
    sols = read_solution_lines(args[1])
    trail = []
    if trail_path and os.path.exists(trail_path):
        with open(trail_path) as f:
            trail = [int(t) for t in f.read().split()]
    if not sols:
        print("INVALID: no solution in %s" % args[1])
        return 1
    todo = sols if "--all" in argv else sols[:1]
    why = ""
    for k, lits in enumerate(todo):
        ok, why = check(clauses, nvars, lits, trail)
        if not ok:
            print("INVALID (solution %d): %s" % (k, why))
            return 1
    print("VALID: %d solution(s) checked; %s" % (len(todo), why))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
