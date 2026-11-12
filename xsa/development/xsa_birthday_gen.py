#!/usr/bin/env python3
"""xsa_birthday_gen.py -- XSA in the generalized-birthday regime (prototype).

  xsa_birthday_gen.py M OUTDIR [--ell 4] [--no-shuffle]

N = 2**M episodes, Q = 2**ell transcript choices per episode, W = ell*(M+1) bits:
Wagner's balance for the k-list XOR problem (Wagner, "A Generalized Birthday Problem",
CRYPTO 2002).  The single scale knob stays N (ell is a fixed constant).

The CNF has exactly XSA-N's shape: exactly-one selection per episode, summary bits
fixed by the chosen delta, a balanced XOR tree, root = target.  Two files are written:

  xsab_mM.plain.cnf     the problem: root XOR = target
  xsab_mM.strategy.cnf  the same clauses (identical numbering) followed by Wagner's
                        filtration as unit clauses: a join at level j fixes bits
                        [(j-1)*ell, j*ell) of its summary (the root: its last 2*ell bits)
                        to 0, or to the target's bits on the right spine.  The strategy
                        prunes the search space; it is not part of the problem.

A planted selection satisfies the strategy, hence both CNFs are satisfiable.  The
metadata uses the XSA-N schema (variables, nodes, clauses, deltas, target), with the
strategy units owned by their join nodes, so xsa_dagster.py builds DAGs for either CNF:
its oracle tree over the strategy CNF is Wagner's k-tree algorithm run by Dagster.
"""
import argparse
import hashlib
import json
import os
import random


def word(tag, width):
    return int.from_bytes(hashlib.sha256(tag.encode()).digest(), "big") & ((1 << width) - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("m", type=int)
    ap.add_argument("outdir")
    ap.add_argument("--ell", type=int, default=4)
    ap.add_argument("--no-shuffle", action="store_true")
    a = ap.parse_args()
    m, ell = a.m, a.ell
    N, Q, W = 2 ** m, 2 ** ell, ell * (m + 1)
    rng = random.Random(word("XSAB/%d/%d" % (m, ell), 64))
    deltas = []
    for i in range(N):
        row = set()
        while len(row) < Q:
            row.add(rng.randrange(1, 1 << W))
        deltas.append(list(row))
    target = rng.randrange(1, 1 << W)
    choice = [rng.randrange(Q) for _ in range(N)]

    def bits_mask(lo, hi):
        return ((1 << hi) - 1) ^ ((1 << lo) - 1)

    # Wagner levels: level j joins cover 2**j leaves; spine nodes contain the last leaf.
    for j in range(1, m + 1):
        lo, hi = (j - 1) * ell, (j * ell if j < m else W)
        mask = bits_mask(lo, hi)
        span = 2 ** j
        for start in range(0, N, span):
            x = 0
            for i in range(start, start + span):
                x ^= deltas[i][choice[i]]
            spine = (start + span == N)
            want = (target & mask) if spine else 0
            diff = (x ^ want) & mask
            if diff:
                r = start + span - 1                       # rightmost leaf of the right subtree
                new = deltas[r][choice[r]] ^ diff
                others = set(deltas[r]) - {deltas[r][choice[r]]}
                while new in others or new == 0:           # keep the episode's deltas distinct
                    clash = new
                    k = deltas[r].index(clash) if clash in deltas[r] else None
                    if k is not None and k != choice[r]:
                        deltas[r][k] = rng.randrange(1, 1 << W)
                        others = set(deltas[r]) - {deltas[r][choice[r]]}
                    else:
                        break
                deltas[r][choice[r]] = new
    acc = 0
    for i in range(N):
        acc ^= deltas[i][choice[i]]
    assert acc == target, "planting failed"

    # ---- CNF (XSA-N layout) ----
    names, clauses, owner = {}, [], []
    nv = [0]

    def var(name):
        nv[0] += 1
        names[nv[0]] = name
        return nv[0]

    def add(lits, own):
        clauses.append(list(lits))
        owner.append(own)

    leaf_bits, children, level_of = [], {}, {}
    for i in range(N):
        own = "episode_%d" % i
        sels = [var("sel[%d,%d]" % (i, j)) for j in range(Q)]
        add(sels, own)
        for x in range(Q):
            for y in range(x + 1, Q):
                add([-sels[x], -sels[y]], own)
        bits = [var("summary[%s,%d]" % (own, b)) for b in range(W)]
        for b in range(W):
            for j in range(Q):
                add([-sels[j], bits[b] if (deltas[i][j] >> b) & 1 else -bits[b]], own)
        leaf_bits.append((own, bits, i, i + 1))
    cur, level, strategy = leaf_bits, 0, []
    while len(cur) > 1:
        nxt = []
        for k in range(0, len(cur), 2):
            (ln, lb, lo, _), (rn, rb, _, hi) = cur[k], cur[k + 1]
            own = "join_L%d_%d_%d" % (level, lo, hi)
            ob = [var("summary[%s,%d]" % (own, b)) for b in range(W)]
            for b in range(W):
                x_, y_, z_ = lb[b], rb[b], ob[b]
                add([-x_, -y_, -z_], own); add([x_, y_, -z_], own)
                add([x_, -y_, z_], own); add([-x_, y_, z_], own)
            children[own] = [ln, rn]
            j = level + 1
            blo, bhi = (j - 1) * ell, (j * ell if j < m else W)
            spine = (hi == N)
            for b in range(blo, bhi):
                want = ((target >> b) & 1) if spine else 0
                strategy.append(([ob[b] if want else -ob[b]], own))
            nxt.append((own, ob, lo, hi))
        cur, level = nxt, level + 1
    root, rbits = cur[0][0], cur[0][1]
    for b in range(W):
        add([rbits[b] if (target >> b) & 1 else -rbits[b]], "attack_goal")
    children["attack_goal"] = [root]
    n_plain = len(clauses)
    for lits, own in strategy:
        add(lits, own)

    # ---- shuffle variables (clause order kept: plain clauses first, strategy appended) ----
    ids = list(range(1, nv[0] + 1))
    if not a.no_shuffle:
        rng.shuffle(ids)
    new = dict(zip(range(1, nv[0] + 1), ids))
    order = list(range(n_plain))
    if not a.no_shuffle:
        rng.shuffle(order)
    order += list(range(n_plain, len(clauses)))
    pos = {old: k for k, old in enumerate(order)}
    os.makedirs(a.outdir, exist_ok=True)
    pre = os.path.join(a.outdir, "xsab_m%d" % m)
    for tag, count in (("plain", n_plain), ("strategy", len(clauses))):
        with open("%s.%s.cnf" % (pre, tag), "w") as f:
            f.write("c XSA birthday regime m=%d N=%d Q=%d W=%d (%s)\n" % (m, N, Q, W, tag))
            f.write("p cnf %d %d\n" % (nv[0], count))
            for old in order[:count]:
                f.write(" ".join(str(new[abs(l)] * (1 if l > 0 else -1)) for l in clauses[old]) + " 0\n")
    nodes = []
    parents = {c: p for p, cs in children.items() for c in cs}
    for own in sorted(set(owner)):
        cl = sorted(pos[k] + 1 for k in range(len(clauses)) if owner[k] == own)
        nodes.append({"id": own, "children": children.get(own, []),
                      "parents": [parents[own]] if own in parents else [],
                      "clauses": cl, "kind": "episode" if own.startswith("episode") else
                      ("goal" if own == "attack_goal" else "xor_join")})
    meta = {"benchmark": "XSA-B (birthday regime, prototype)", "scale_parameter": {"name": "N", "value": N},
            "derived": {"choices_per_episode_Q": Q, "interface_width_W": W, "variables": nv[0],
                        "clauses": len(clauses), "plain_clauses": n_plain, "ell": ell, "m": m},
            "variables": {str(new[v]): n for v, n in names.items()}, "nodes": nodes,
            "episode_deltas_hex": [["0x%x" % d for d in row] for row in deltas],
            "target_hex": "0x%x" % target, "planted_choice_by_episode": choice,
            "note": "metadata clause numbers index the strategy CNF; the plain CNF is its prefix"}
    json.dump(meta, open(pre + ".meta.json", "w"))
    print("m=%d N=%d Q=%d W=%d vars=%d plain=%d strategy=+%d" % (m, N, Q, W, nv[0], n_plain, len(strategy)))


if __name__ == "__main__":
    main()
