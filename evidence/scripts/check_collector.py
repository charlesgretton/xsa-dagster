#!/usr/bin/env python3
"""check_collector.py DAG [DAG ...]

Structural checks on executed Dagster DAG files, without the CNF:
  * node ids in range, graph acyclic, exactly one sink (the collector);
  * the collector's incoming interface-variable sets are pairwise disjoint -- the
    precondition under which the table master's consistency check (-g 0) can be skipped
    (-g 2) without changing which combinations are formed.
Prints one line per DAG: nodes, edges, sink, in-degree of the sink, total and distinct
variables on the sink's inputs, and PASS/FAIL.
"""
import sys

def ranges(spec):
    out = []
    for part in spec.strip().split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-"); out.append((int(a), int(b)))
        else:
            out.append((int(part), int(part)))
    return out

def check(path):
    n, edges, sec = None, [], None
    for line in open(path):
        line = line.strip()
        if line.startswith("NODES:"):
            n = int(line.split(":")[1]); continue
        if line in ("GRAPH:", "CLAUSES:", "REPORTING:"):
            sec = line; continue
        if sec == "GRAPH:" and "->" in line:
            uv, vars_ = line.split(":", 1)
            u, v = (int(x) for x in uv.split("->"))
            edges.append((u, v, ranges(vars_)))
    ok = n is not None and all(0 <= u < n and 0 <= v < n for u, v, _ in edges)
    outdeg = {u: 0 for u in range(n)}; indeg = {u: 0 for u in range(n)}
    for u, v, _ in edges:
        outdeg[u] += 1; indeg[v] += 1
    sinks = [u for u in range(n) if outdeg[u] == 0]
    # acyclicity (Kahn)
    deg = dict(indeg); frontier = [u for u in range(n) if deg[u] == 0]; seen = 0
    adj = {u: [] for u in range(n)}
    for u, v, _ in edges:
        adj[u].append(v)
    while frontier:
        u = frontier.pop(); seen += 1
        for v in adj[u]:
            deg[v] -= 1
            if deg[v] == 0:
                frontier.append(v)
    acyclic = seen == n
    sink = sinks[0] if len(sinks) == 1 else None
    total = distinct = 0
    if sink is not None:
        allv = set()
        for u, v, rr in edges:
            if v == sink:
                for a, b in rr:
                    total += b - a + 1
                    allv.update(range(a, b + 1))
        distinct = len(allv)
    disjoint = sink is not None and total == distinct
    verdict = "PASS" if (ok and acyclic and sink is not None and disjoint) else "FAIL"
    print("%-55s nodes=%5d edges=%5d sink=%5s in=%4d vars_in=%7d distinct=%7d acyclic=%s disjoint=%s %s" % (
        path, n, len(edges), sink, indeg.get(sink, 0) if sink is not None else -1, total, distinct, acyclic, disjoint, verdict))
    return verdict == "PASS"

if __name__ == "__main__":
    res = [check(p) for p in sys.argv[1:]]
    print("%d/%d DAGs pass" % (sum(res), len(res)))
    sys.exit(0 if all(res) else 1)
