"""Shared DIMACS reading for the demonstrator's Python tools (stdlib only).

Same rules as tdag: trust clauses over the header (sha1-sat overcounts), stop at
SATLIB's '%' marker, ignore comments.  Tautologies are kept and flagged.
"""


def read_cnf(path):
    """Return (nvars, clauses) with clauses as lists of ints."""
    clauses, cur, nv, maxv = [], [], 0, 0
    with open(path, "r", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s[0] == "c":
                continue
            if s[0] == "%":
                break
            if s[0] == "p":
                parts = s.split()
                nv = int(parts[2])
                continue
            for tok in s.split():
                v = int(tok)
                if v == 0:
                    clauses.append(cur)
                    cur = []
                else:
                    cur.append(v)
                    maxv = max(maxv, abs(v))
    if cur:
        clauses.append(cur)
    return max(nv, maxv), clauses


def is_tautology(clause):
    s = set(clause)
    return any(-l in s for l in s)
