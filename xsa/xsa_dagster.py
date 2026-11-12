#!/usr/bin/env python3
"""xsa_dagster.py -- run the XSA-N benchmark through Dagster.

  xsa_dagster.py dags META OUTDIR [--block B ...] [--split K ...]
  xsa_dagster.py complete META DAGSTER_SOLUTION > MODEL
  xsa_dagster.py predict META [--split K ...]

DAG shapes, all over the benchmark's own CNF (clause sets taken from the metadata):

  one       a single node holding every clause (the sequential Dagster baseline)
  oracle    the semantic tree: one node per episode, per XOR join, and the goal
  block-B   maximal subtrees of <= B episodes fused into one node, joins above them, goal
  split-K   A = everything except the rightmost subtree of <= K episodes and its path to
            the root; B = that subtree + the path joins + the goal.  Any selection
            satisfies A; B must hit the residual target using its K episodes.

Every edge carries the producer's output summary bits AND the selectors of its subtree,
so the sink's message contains every selector.  `complete` rebuilds the full assignment
from those selectors using the benchmark's semantics (leaf summary = delta of the chosen
transcript; join = XOR of children), rejects any reported literal that disagrees, and
checks the attack semantics; the benchmark's own validator can then check the result.

`predict` gives the analytic acceptance of a split: B (k episodes, Q choices each) can
reach a given W-bit residual target with probability ~ 1 - exp(-Q^k / 2^W), so A must
offer ~1/p prefixes on average.  This is the quantity a probing DAG elicitor should
estimate; on XSA it is known in closed form.
"""
import argparse
import json
import math
import os
import sys


class XSA:
    def __init__(self, meta_path):
        m = json.load(open(meta_path))
        self.meta = m
        self.N = m["scale_parameter"]["value"]
        self.Q = m["derived"]["choices_per_episode_Q"]
        self.W = m["derived"]["interface_width_W"]
        self.nvars = m["derived"]["variables"]
        self.nclauses = m["derived"]["clauses"]
        self.var = {name: int(v) for v, name in m["variables"].items()}
        self.nodes = {n["id"]: n for n in m["nodes"]}
        self.root = self.nodes["attack_goal"]["children"][0]
        self.deltas = [[int(x, 16) for x in row] for row in m["episode_deltas_hex"]]
        self.target = int(m["target_hex"], 16)
        self._leaves = {}

    def kids(self, nid):
        return self.nodes[nid]["children"]

    def leaves(self, nid):
        if nid not in self._leaves:
            if nid.startswith("episode_"):
                self._leaves[nid] = [int(nid.split("_")[1])]
            else:
                self._leaves[nid] = [e for c in self.kids(nid) for e in self.leaves(c)]
        return self._leaves[nid]

    def out_bits(self, nid):
        return [self.var["summary[%s,%d]" % (nid, b)] for b in range(self.W)]

    def selectors(self, nid):
        return [self.var["sel[%d,%d]" % (i, j)] for i in self.leaves(nid) for j in range(self.Q)]

    def subtree(self, nid):
        out = [nid]
        for c in self.kids(nid):
            out += self.subtree(c)
        return out

    def clauses(self, nids):
        # Dagster is 0-based; clauses beyond nclauses (e.g. XSA-B strategy units when the
        # plain CNF is used) are dropped
        return sorted({c - 1 for n in nids for c in self.nodes[n]["clauses"] if c - 1 < self.nclauses})

    def use_prefix(self, nclauses):
        """Restrict to the first nclauses clauses (the XSA-B plain CNF)."""
        self.nclauses = nclauses
        return self

    # ---------------------------------------------------------------- DAG shapes
    def dag_one(self):
        return {"nodes": [list(range(self.nclauses))], "edges": [], "roles": ["ALL"], "single_shot": ""}

    def _tree_dag(self, fused_roots):
        """Nodes: each fused subtree root (all its clauses), every join above them, goal."""
        order, ids = [], {}

        def visit(nid):
            if nid in fused_roots:
                ids[nid] = len(order)
                order.append(("F", nid))
                return
            for c in self.kids(nid):
                visit(c)
            ids[nid] = len(order)
            order.append(("J", nid))
        visit(self.root)
        goal = len(order)
        nodes, roles, edges = [], [], []
        for kind, nid in order:
            nodes.append(self.clauses(self.subtree(nid) if kind == "F" else [nid]))
            roles.append(nid if kind == "J" else "block:" + nid)
            parent = next((p for p in self.nodes[nid]["parents"]), None)
            to = goal if parent == "attack_goal" else ids[parent]
            edges.append((ids[nid], to, self.out_bits(nid) + self.selectors(nid)))
        nodes.append(self.clauses(["attack_goal"]))
        roles.append("GOAL")
        return {"nodes": nodes, "edges": edges, "roles": roles, "single_shot": ""}

    def dag_oracle(self):
        return self._tree_dag({n for n in self.nodes if n.startswith("episode_")})

    def dag_block(self, B):
        fused = set()

        def cut(nid):
            if len(self.leaves(nid)) <= B:
                fused.add(nid)
            else:
                for c in self.kids(nid):
                    cut(c)
        cut(self.root)
        return self._tree_dag(fused)

    def split_parts(self, K):
        cur, path = self.root, []
        while len(self.leaves(cur)) > K:
            path.append(cur)
            cur = self.kids(cur)[-1]           # follow the right spine
        siblings = [self.kids(p)[0] for p in path]
        b_nodes = set(self.subtree(cur)) | set(path) | {"attack_goal"}
        a_nodes = [n for n in self.nodes if n not in b_nodes]
        return cur, path, siblings, a_nodes, sorted(b_nodes)

    def dag_split(self, K):
        rb, path, siblings, a_nodes, b_nodes = self.split_parts(K)
        if not path:
            return None
        a_sel = [v for s in siblings for v in self.selectors(s)]
        iface = [v for s in siblings for v in self.out_bits(s)]
        return {"nodes": [self.clauses(a_nodes), self.clauses(b_nodes)],
                "edges": [(0, 1, iface + a_sel)], "roles": ["A:prefix", "B:residual+goal"],
                "single_shot": "0", "k": len(self.leaves(rb)), "interface_bits": len(iface)}

    def frontier(self, d):
        """Subtree roots d levels below the root, and the join nodes above them."""
        front, top = [self.root], []
        for _ in range(d):
            nxt = []
            for nid in front:
                if nid.startswith("episode_"):
                    nxt.append(nid)
                else:
                    top.append(nid)
                    nxt += self.kids(nid)
            front = nxt
        return front, top

    def dag_topdown(self, d, splitter_single_shot=False, keyed=False, bound_bits=None):
        """Top-down target splitting.  S (top d levels + goal) chooses sub-targets for the
        2^d subtrees whose XOR is the target; each subtree C_i is an independent smaller
        XSA instance with a fixed target; K (top levels + goal again) checks the pieces
        and carries every selector.  C_i are single-shot-safe: under one split every
        child result is acceptable to K.  S is single-shot-safe only when every child
        reaches any sub-target with probability ~1 (see predict_topdown)."""
        front, top = self.frontier(d)
        if len(front) < 2:
            return None
        n = len(front)
        top_clauses = self.clauses(top + ["attack_goal"])
        nodes = [top_clauses] + [self.clauses(self.subtree(c)) for c in front] + [top_clauses]
        key = [v for c in front for v in self.out_bits(c)]   # the whole split: a join key
        edges = [(0, 1 + i, key if keyed else self.out_bits(c)) for i, c in enumerate(front)]
        edges += [(1 + i, n + 1, (key if keyed else self.out_bits(c)) + self.selectors(c))
                  for i, c in enumerate(front)]
        ss = "1-%d" % n if n > 1 else "1"
        if splitter_single_shot:
            ss = "0-%d" % n
        aux = []
        if bound_bits is not None:
            # bounded splitter: pin every sub-target except the last to its low bound_bits
            # bits, via auxiliary unit clauses only S holds (appended after the CNF), so S
            # can offer at most 2^(bound_bits*(n-1)) splits and its stream ends
            for c in front[:-1]:
                aux += [-v for b, v in enumerate(self.out_bits(c)) if b >= bound_bits]
            nodes[0] = nodes[0] + [self.nclauses + k for k in range(len(aux))]
        return {"nodes": nodes, "edges": edges, "single_shot": ss, "aux_units": aux,
                "roles": ["S:split"] + ["C:" + c for c in front] + ["K:collect"],
                "children_episodes": [len(self.leaves(c)) for c in front]}

    def dag_topdown2(self, d1, d2):
        """Two-stage top-down splitting.  S (top d1 levels + goal) chooses a sub-target for
        each of the 2^d1 block roots; mid-splitter M_j (the joins of block j down to depth
        d1+d2) splits its block's sub-target among its 2^d2 subtrees; each subtree C_i is an
        independent smaller XSA instance; K (top d1+d2 levels + goal) checks the pieces and
        carries every selector.  Subtrees, collector and the safety condition are those of
        topdown:(d1+d2); only the splitting is staged.  Why: at the start the splitter's job
        is the only work, so every idle worker runs a copy, and the master processes every
        copy's solution in full (~0.9 s each for a 512-way split on the HPC cluster).  Staging makes each
        splitter's solution 2^d1- or 2^d2-way instead of 2^(d1+d2)-way."""
        blocks, top1 = self.frontier(d1)
        front, top = self.frontier(d1 + d2)
        if len(blocks) < 2 or len(front) <= len(blocks):
            return None
        top_set = set(top)
        block_of = {}
        for j, b in enumerate(blocks):
            for nid in self.subtree(b):
                block_of[nid] = j
        mids = [[nid for nid in self.subtree(b) if nid in top_set] for b in blocks]
        assert all(mids), "every block root must be a join above the final frontier"
        kids_of = [[c for c in front if block_of[c] == j] for j in range(len(blocks))]
        B, n = len(blocks), len(front)
        order = [c for j in range(B) for c in kids_of[j]]          # subtree ids contiguous per block
        cid = {c: 1 + B + i for i, c in enumerate(order)}
        K = 1 + B + n
        nodes = ([self.clauses(top1 + ["attack_goal"])] + [self.clauses(m) for m in mids]
                 + [self.clauses(self.subtree(c)) for c in order] + [self.clauses(top + ["attack_goal"])])
        edges = [(0, 1 + j, self.out_bits(b)) for j, b in enumerate(blocks)]
        edges += [(1 + j, cid[c], self.out_bits(c)) for j in range(B) for c in kids_of[j]]
        edges += [(cid[c], K, self.out_bits(c) + self.selectors(c)) for c in order]
        return {"nodes": nodes, "edges": edges, "single_shot": "%d-%d" % (1 + B, B + n), "aux_units": [],
                "roles": ["S:split"] + ["M:" + b for b in blocks] + ["C:" + c for c in order] + ["K:collect"],
                "children_episodes": [len(self.leaves(c)) for c in order]}

    def dag_frontier(self, front, splitter_single_shot=True, keyed=False):
        """Top-down splitting over an arbitrary frontier: any set of subtree roots that
        partitions the episodes (e.g. uniform depth, or deeper where a probe found a slow
        child).  S holds every join above the frontier plus the goal."""
        cover = sorted(e for c in front for e in self.leaves(c))
        assert cover == list(range(self.N)), "frontier must partition the episodes"
        inside = set(n for c in front for n in self.subtree(c))
        top = [n for n in self.nodes if n not in inside and n != "attack_goal"]
        n = len(front)
        top_clauses = self.clauses(top + ["attack_goal"])
        nodes = [top_clauses] + [self.clauses(self.subtree(c)) for c in front] + [top_clauses]
        key = [v for c in front for v in self.out_bits(c)]
        edges = [(0, 1 + i, key if keyed else self.out_bits(c)) for i, c in enumerate(front)]
        edges += [(1 + i, n + 1, (key if keyed else self.out_bits(c)) + self.selectors(c))
                  for i, c in enumerate(front)]
        return {"nodes": nodes, "edges": edges,
                "single_shot": ("0-%d" if splitter_single_shot else "1-%d") % n,
                "roles": ["S:split"] + ["C:" + c for c in front] + ["K:collect"]}

    def predict_topdown(self, d):
        front, _ = self.frontier(d)
        sizes = [len(self.leaves(c)) for c in front]
        p_all = 1.0
        for k in sizes:
            p_all *= self.accept(k)[0]
        return {"d": d, "children": len(front), "episodes_per_child": sorted(set(sizes)),
                "p_every_child_reaches_its_subtarget": p_all}

    def accept(self, k):
        """P(a block of k episodes can reach a given W-bit target) ~ 1 - exp(-Q^k / 2^W),
        computed in log space (Q^k overflows a float: 16^512 at N=4096, depth 3)."""
        log_lam = k * math.log(self.Q) - self.W * math.log(2)
        if log_lam > 50:                 # lambda > 5e21: exp(-lambda) is 0 to double precision
            return 1.0, log_lam / math.log(2)
        return -math.expm1(-math.exp(log_lam)), log_lam / math.log(2)

    def predict(self, K):
        rb, path, siblings, _, _ = self.split_parts(K)
        k = len(self.leaves(rb))
        p, log2_lam = self.accept(k)
        return {"k": k, "Q": self.Q, "W": self.W, "log2(Q^k/2^W)": round(log2_lam, 2), "acceptance": p,
                "expected_prefixes": (1.0 / p) if p > 0 else float("inf"),
                "interface_bits": len(siblings) * self.W, "path_joins": len(path)}

    # ---------------------------------------------------------------- completion
    def complete(self, lits):
        """Full assignment from the selectors in a (pruned) Dagster model.
        Returns (literals, message); literals is None if the model is unusable."""
        pos = {l for l in lits if l > 0}
        neg = {-l for l in lits if l < 0}
        choice = []
        for i in range(self.N):
            chosen = [j for j in range(self.Q) if self.var["sel[%d,%d]" % (i, j)] in pos]
            if len(chosen) != 1:
                return None, "episode %d has %d selectors set" % (i, len(chosen))
            choice.append(chosen[0])
        val = {}
        for i, j in enumerate(choice):
            for jj in range(self.Q):
                val[self.var["sel[%d,%d]" % (i, jj)]] = (jj == j)
        summary = {}

        def summ(nid):
            if nid not in summary:
                if nid.startswith("episode_"):
                    i = int(nid.split("_")[1])
                    summary[nid] = self.deltas[i][choice[i]]
                else:
                    a, b = self.kids(nid)
                    summary[nid] = summ(a) ^ summ(b)
                for b, v in enumerate(self.out_bits(nid)):
                    val[v] = bool((summary[nid] >> b) & 1)
            return summary[nid]
        root = summ(self.root)
        if root != self.target:
            return None, "decoded attack summary 0x%x != target 0x%x" % (root, self.target)
        clash = [v for v in pos if v in val and val[v] is False] + [v for v in neg if v in val and val[v] is True]
        if clash:
            return None, "%d reported literals disagree with the semantics" % len(clash)
        missing = [v for v in range(1, self.nvars + 1) if v not in val]
        if missing:
            return None, "%d variables not covered by the semantics" % len(missing)
        return [v if val[v] else -v for v in range(1, self.nvars + 1)], "attack 0x%x matches target" % root


def parse_telemetry(log_path, roles):
    """Per-node jobs and busy seconds from a Dagster master log written at GLOG_v=2.
    A job runs from its dispatch to that worker's next request-for-assignment or next
    dispatch (a pre-emptive reassignment).  On an oversubscribed CPU busy times are
    time-sliced; job counts are not."""
    import re
    disp = re.compile(r"^I\d+ (\d+):(\d+):([\d.]+) .*sending message to worker (\d+) message about node (\d+)")
    done = re.compile(r"^I\d+ (\d+):(\d+):([\d.]+) .*received MPI_TAG_REQUEST_FOR_ASSIGNMENT from worker (\d+)")
    cur, jobs, busy = {}, {}, {}

    def ts(h, m, sec):
        return int(h) * 3600 + int(m) * 60 + float(sec)

    def close(w, t):
        if w in cur:
            node, t0 = cur.pop(w)
            busy[node] = busy.get(node, 0.0) + max(0.0, t - t0)
    for line in open(log_path, errors="replace"):
        m = disp.match(line)
        if m:
            t, w, node = ts(*m.group(1, 2, 3)), int(m.group(4)), int(m.group(5))
            close(w, t)
            cur[w] = (node, t)
            jobs[node] = jobs.get(node, 0) + 1
            continue
        m = done.match(line)
        if m:
            close(int(m.group(4)), ts(*m.group(1, 2, 3)))
    out = {}
    for node in sorted(set(jobs) | set(busy)):
        name = roles[node] if node < len(roles) else str(node)
        out[name] = {"jobs": jobs.get(node, 0), "busy_s": round(busy.get(node, 0.0), 3)}
    return out


def parse_builtin_stats(log_path, roles):
    """Dagster's own StatisticsModule output (VLOG 3, unpatched): the LAST
    'MASTER: Timings --' line, per node 'i:jobs,solutions:Tspm..%,S..spm..%,U..spm..%'.
    Times are MASTER CPU seconds (clock()): trustworthy only while the master busy-polls
    on a dedicated core (Open MPI's default), not on an oversubscribed CPU."""
    import re
    last = None
    for line in open(log_path, errors="replace"):
        if "MASTER: Timings --" in line:
            last = line.split("MASTER: Timings --", 1)[1]
    if last is None:
        return None
    out = {}
    for m in re.finditer(r"(\d+):(\d+),(\d+):([\d.#]+)spm[\d.#-]+%,S([\d.#]+)spm[^,]*,U([\d.#]+)spm", last):
        node, jobs, sols = int(m.group(1)), int(m.group(2)), int(m.group(3))
        name = roles[node] if node < len(roles) else str(node)
        f = lambda v: None if "#" in v else float(v)
        out[name] = {"jobs": jobs, "solutions": sols, "mean_job_cpu_s": f(m.group(4)),
                     "mean_between_solutions_cpu_s": f(m.group(5)), "mean_tail_cpu_s": f(m.group(6))}
    return out


def ranges(vals):
    v = sorted(set(vals))
    out, i = [], 0
    while i < len(v):
        j = i
        while j + 1 < len(v) and v[j + 1] == v[j] + 1:
            j += 1
        out.append(str(v[i]) if j == i else "%d-%d" % (v[i], v[j]))
        i = j + 1
    return ",".join(out)


def write_dag(path, dag, nvars):
    with open(path, "w") as f:
        f.write("DAG-FILE\nNODES:%d\nGRAPH:\n" % len(dag["nodes"]))
        for a, b, vs in dag["edges"]:
            assert vs, "empty edge"
            f.write("%d->%d:%s\n" % (a, b, ranges(vs)))
        f.write("CLAUSES:\n")
        for k, cl in enumerate(dag["nodes"]):
            assert cl, "node without clauses"
            f.write("%d:%s\n" % (k, ranges(cl)))
        f.write("REPORTING:\n1-%d\n" % nvars)


def read_first_solution(path):
    """First solution in a file: all 'v' lines if present (SAT-competition format),
    otherwise the first line of literals (Dagster's -o format, one solution per line)."""
    lines = open(path).read().splitlines()
    if any(l.startswith("v ") for l in lines):
        lits = []
        for l in lines:
            if l.startswith("v "):
                lits += [int(t) for t in l[2:].split() if t != "0"]
        return lits
    for line in lines:
        toks = [int(t) for t in line.split() if t.lstrip("-").isdigit() and t != "0"]
        if toks:
            return toks
    return []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("dags")
    d.add_argument("meta")
    d.add_argument("outdir")
    d.add_argument("--block", type=int, action="append", default=[])
    d.add_argument("--split", type=int, action="append", default=[])
    c = sub.add_parser("complete")
    c.add_argument("meta")
    c.add_argument("solution")
    p = sub.add_parser("predict")
    p.add_argument("meta")
    p.add_argument("--split", type=int, action="append", default=[])
    a = ap.parse_args()
    x = XSA(a.meta)
    if a.cmd == "dags":
        os.makedirs(a.outdir, exist_ok=True)
        shapes = {"one": x.dag_one(), "oracle": x.dag_oracle()}
        for B in a.block:
            shapes["block%d" % B] = x.dag_block(B)
        for K in a.split:
            dg = x.dag_split(K)
            if dg:
                shapes["split%d" % K] = dg
        index = {}
        for name, dg in shapes.items():
            path = os.path.join(a.outdir, "xsa_n%d.%s.dag" % (x.N, name))
            write_dag(path, dg, x.nvars)
            index[name] = {"file": path, "nodes": len(dg["nodes"]), "edges": len(dg["edges"]),
                           "single_shot": dg["single_shot"], "roles": dg["roles"],
                           "max_edge_vars": max((len(e[2]) for e in dg["edges"]), default=0)}
            print("%-8s %4d nodes  %4d edges  widest edge %6d vars  -> %s" % (
                name, index[name]["nodes"], index[name]["edges"], index[name]["max_edge_vars"], path))
        json.dump(index, open(os.path.join(a.outdir, "xsa_n%d.dags.json" % x.N), "w"), indent=1)
    elif a.cmd == "complete":
        lits, msg = x.complete(read_first_solution(a.solution))
        if lits is None:
            print("INVALID: " + msg, file=sys.stderr)
            return 1
        print("s SATISFIABLE")
        for k in range(0, len(lits), 20):
            print("v " + " ".join(map(str, lits[k:k + 20])) + " 0")
        print("c " + msg, file=sys.stderr)
    elif a.cmd == "predict":
        for K in a.split or [2, 4, 8, 16]:
            print(json.dumps(x.predict(K)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
