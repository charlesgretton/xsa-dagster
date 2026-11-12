#!/usr/bin/env python3
"""cube_control.py META CNF K OUT.cnf -- the split-K DAG as a sequential cube.

Pins every episode outside the split's residual block to transcript 0 (an arbitrary
prefix, not the planted one) with unit clauses, so a flat solver gets exactly the
knowledge the split DAG exploits.  If this is as fast as the DAG, the DAG's win is the
cube, not distributed execution."""
import sys
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__import__("os").path.abspath(__file__)), ".."))
import xsa_dagster as xd

meta, cnf, k, out = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
x = xd.XSA(meta)
rb, path, siblings, a_nodes, b_nodes = x.split_parts(k)
pinned = sorted(i for s in siblings for i in x.leaves(s))
units = [x.var["sel[%d,0]" % i] for i in pinned]
lines = open(cnf).read().splitlines()
hdr = next(i for i, l in enumerate(lines) if l.startswith("p cnf"))
nv, nc = map(int, lines[hdr].split()[2:4])
lines[hdr] = "p cnf %d %d" % (nv, nc + len(units))
with open(out, "w") as f:
    f.write("\n".join(lines) + "\n")
    for u in units:
        f.write("%d 0\n" % u)
print("pinned %d of %d episodes to transcript 0; residual block of %d episodes" % (len(pinned), x.N, len(x.leaves(rb))))
