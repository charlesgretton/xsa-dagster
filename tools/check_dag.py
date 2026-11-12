#!/usr/bin/env python3
"""check_dag.py DAG CNF [--report PREFIX.json --mode dp|jt]

Validate a DAG file exactly as Dagster's parser (dagster/Dag.cpp) will read it, and
check the invariants tdag's DAG families depend on.

Always: strict grammar (DAG-FILE / NODES / GRAPH / CLAUSES / REPORTING; ranges need
end>begin in CLAUSES and REPORTING; no empty clause list -- Dagster's fscanf would
silently read the next line; no trailing blank line), node ids in range, no
duplicate edge, no empty edge variable set (a non-terminal node with an empty
interface falls back to projecting on REPORTING and enumerates every model),
acyclic, exactly one sink (Dagster treats any sink solution as a full solution),
sink holds every clause, clause indices exist in the CNF.

With --report (tdag JSON written with --dump-pieces) for dp/jt: every clause is in
exactly one piece and inside that piece's variables, separators equal
vars(child) & vars(parent), running intersection holds, and the emitted edges carry
exactly the separators.  Exit 0 iff every check passes.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cnf_io import read_cnf  # noqa: E402


class Bad(Exception):
    pass


def ranges(s, strict, lo=0):
    if s == "":
        raise Bad("empty list")
    out = []
    for tok in s.split(","):
        if "-" in tok:
            a, b = (int(x) for x in tok.split("-"))
            if (strict and not b > a) or (not strict and not b >= a):
                raise Bad("range %s rejected by Dagster" % tok)
            out.extend(range(a, b + 1))
        else:
            out.append(int(tok))
    if any(x < lo for x in out):
        raise Bad("value below %d" % lo)
    return out


def parse_dag(path):
    text = open(path).read()
    if not text.endswith("\n") or text.endswith("\n\n"):
        raise Bad("file must end with exactly one newline")
    L = text.split("\n")[:-1]
    if L[0] != "DAG-FILE" or not L[1].startswith("NODES:") or L[2] != "GRAPH:":
        raise Bad("bad header")
    n = int(L[1][6:])
    i, edges = 3, []
    while i < len(L) and L[i][:1].isdigit():
        head, _, body = L[i].partition(":")
        a, _, b = head.partition("->")
        edges.append((int(a), int(b), ranges(body, strict=False, lo=1)))
        i += 1
    if L[i] != "CLAUSES:":
        raise Bad("missing CLAUSES:")
    i += 1
    clauses = {}
    while i < len(L) and L[i][:1].isdigit():
        head, _, body = L[i].partition(":")
        node = int(head)
        if node in clauses:
            raise Bad("node %d listed twice in CLAUSES" % node)
        clauses[node] = ranges(body, strict=True)
        i += 1
    if L[i] != "REPORTING:":
        raise Bad("missing REPORTING:")
    rep = []
    for line in L[i + 1:]:
        rep += ranges(line, strict=True, lo=1)
    return n, edges, clauses, rep


def main(argv):
    pos = [a for a in argv[1:] if not a.startswith("--")]
    rpt = argv[argv.index("--report") + 1] if "--report" in argv else None
    mode = argv[argv.index("--mode") + 1] if "--mode" in argv else None
    pos = [a for a in pos if a not in (rpt, mode)]
    if len(pos) != 2:
        print(__doc__)
        return 2
    dag_path, cnf_path = pos
    problems = []
    try:
        n, edges, ncl, rep = parse_dag(dag_path)
    except (Bad, ValueError, IndexError) as e:
        print("FAIL %s: grammar: %s" % (dag_path, e))
        return 1
    nv, cls = read_cnf(cnf_path)
    m = len(cls)
    seen = set()
    succ = {k: [] for k in range(n)}
    pred = {k: [] for k in range(n)}
    for a, b, vs in edges:
        if not (0 <= a < n and 0 <= b < n):
            problems.append("edge %d->%d out of range" % (a, b))
            continue
        if (a, b) in seen:
            problems.append("duplicate edge %d->%d" % (a, b))
        seen.add((a, b))
        succ[a].append(b)
        pred[b].append(a)
        if not vs:
            problems.append("empty edge %d->%d" % (a, b))
    for k in range(n):
        if k not in ncl:
            problems.append("node %d has no CLAUSES line" % k)
        elif any(c >= m for c in ncl[k]):
            problems.append("node %d refers to a clause beyond %d" % (k, m))
    indeg = {k: len(pred[k]) for k in range(n)}
    stack = [k for k in range(n) if indeg[k] == 0]
    done = 0
    while stack:
        k = stack.pop()
        done += 1
        for j in succ[k]:
            indeg[j] -= 1
            if indeg[j] == 0:
                stack.append(j)
    if done != n:
        problems.append("cycle")
    sinks = [k for k in range(n) if not succ[k]]
    if len(sinks) != 1:
        problems.append("expected exactly one sink, found %s" % sinks)
    elif "--partial-sink" not in argv and set(ncl.get(sinks[0], [])) != set(range(m)):
        problems.append("sink %d does not hold every clause (pass --partial-sink if models are "
                        "completed and verified outside Dagster)" % sinks[0])
    if "--partial-sink" in argv:
        covered = set().union(*ncl.values()) if ncl else set()
        if covered != set(range(m)):
            problems.append("%d clauses belong to no node" % (m - len(covered)))
    if rep and max(rep) > nv:
        problems.append("REPORTING beyond the CNF's variables")

    if rpt and mode in ("dp", "jt"):
        R = json.load(open(rpt))
        P = R["td"]["piece_table"]
        if not P or "var_list" not in P[0]:
            problems.append("report lacks var lists (run tdag with --dump-pieces)")
        else:
            vars_ = [set(p["var_list"]) for p in P]
            owner = {}
            for k, p in enumerate(P):
                for c in p["clause_list"]:
                    if c in owner:
                        problems.append("clause %d in pieces %d and %d" % (c, owner[c], k))
                    owner[c] = k
                    if not {abs(l) for l in cls[c]} <= vars_[k]:
                        problems.append("clause %d not inside piece %d" % (c, k))
            aux = R["preprocess"]["aux_clause"]
            missing = set(range(m)) - set(owner) - {aux}
            if missing:
                problems.append("%d clauses in no piece" % len(missing))
            adj = {k: set() for k in range(len(P))}
            for k, p in enumerate(P):
                if p["parent"] >= 0:
                    q = p["parent"]
                    adj[k].add(q)
                    adj[q].add(k)
                    if set(p["sep_list"]) != vars_[k] & vars_[q]:
                        problems.append("piece %d: separator != intersection with parent" % k)
            holders = {}
            for k, vs in enumerate(vars_):
                for v in vs:
                    holders.setdefault(v, []).append(k)
            for v, hs in holders.items():
                hs = set(hs)
                start = next(iter(hs))
                seen_p, st = {start}, [start]
                while st:
                    x = st.pop()
                    for y in adj[x]:
                        if y in hs and y not in seen_p:
                            seen_p.add(y)
                            st.append(y)
                if seen_p != hs:
                    problems.append("running intersection fails for variable %d" % v)
                    break
            emitted = {(a, b): set(vs) for a, b, vs in edges}
            roles = R["dags"][mode]["roles"]
            node = {r: i for i, r in enumerate(roles)}
            aux = {R["preprocess"]["aux_var"]}
            want = {}
            for k, p in enumerate(P):
                U, D = node["U%d" % k], node.get("D%d" % k)
                if p["parent"] >= 0:
                    sep = set(p["sep_list"])
                    want[(U, node["U%d" % p["parent"]])] = sep
                    if mode == "jt":
                        want[(U, node["D%d" % p["parent"]])] = sep
                        want[(node["D%d" % p["parent"]], D)] = sep
                else:
                    want[(U, node["GATE"])] = aux
                    if mode == "jt":
                        want[(node["GATE"], D)] = aux
                if mode == "jt":
                    want[(D, node["FINAL"])] = vars_[k]
            if mode == "dp":
                want[(node["GATE"], node["FINAL"])] = aux
            for e, vs in want.items():
                if emitted.get(e) != vs:
                    problems.append("edge %s->%s does not carry the expected variables" % (roles[e[0]], roles[e[1]]))
            if set(emitted) != set(want):
                problems.append("unexpected edges: %s" % sorted(set(emitted) - set(want))[:5])
            if mode == "jt" and R["dags"]["jt"]["single_shot"]:
                ss = set(ranges(R["dags"]["jt"]["single_shot"], strict=False))
                if ss != {node["D%d" % k] for k in range(len(P))}:
                    problems.append("single_shot list is not exactly the distribute nodes")
    if problems:
        for p in problems[:12]:
            print("FAIL %s: %s" % (dag_path, p))
        return 1
    print("OK   %s: %d nodes, %d edges, sink %d%s" % (os.path.basename(dag_path), n, len(edges), sinks[0],
                                                     ", TD invariants hold" if rpt and mode in ("dp", "jt") else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
