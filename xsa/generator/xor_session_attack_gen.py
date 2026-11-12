#!/usr/bin/env python3
"""Generate and validate the XSA-N Dagster elicitation benchmark.

XSA-N models a bounded symbolic XOR attack assembled from N protocol episodes.
Each episode contributes exactly one of Q(N) transcript-derived XOR deltas.  A
successful attack is a selection whose balanced-tree XOR summary equals a
planted target fingerprint.

No third-party Python packages are required.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

VERSION = "1.0"


@dataclass
class CNF:
    next_var: int = 1
    clauses: List[List[int]] = field(default_factory=list)
    names: Dict[int, str] = field(default_factory=dict)
    clause_owner: List[str] = field(default_factory=list)

    def var(self, name: str) -> int:
        v = self.next_var
        self.next_var += 1
        self.names[v] = name
        return v

    def add(self, lits: Iterable[int], owner: str) -> int:
        clause = list(lits)
        if not clause or any(x == 0 for x in clause):
            raise ValueError("clauses must be non-empty and contain no zero")
        self.clauses.append(clause)
        self.clause_owner.append(owner)
        return len(self.clauses)  # 1-based DIMACS clause number

    @property
    def nvars(self) -> int:
        return self.next_var - 1


def bit_width(n: int) -> int:
    # Derived, not independently configurable: enough interface growth to make
    # communication visible while remaining far below the number of leaves.
    return 8 + math.ceil(math.log2(max(2, n)))


def choices_per_episode(n: int) -> int:
    # Derived ambiguity.  It rises slowly with N so N is the only scaling knob.
    return 4 + math.ceil(math.log2(max(2, n)))


def stable_word(tag: str, width: int) -> int:
    digest = hashlib.sha256(tag.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") & ((1 << width) - 1)


def exactly_one(cnf: CNF, xs: Sequence[int], owner: str) -> None:
    cnf.add(xs, owner)
    for i in range(len(xs)):
        for j in range(i + 1, len(xs)):
            cnf.add([-xs[i], -xs[j]], owner)


def xor_gate(cnf: CNF, a: int, b: int, z: int, owner: str) -> None:
    """Add z <-> (a xor b), using four ternary clauses."""
    cnf.add([-a, -b, -z], owner)
    cnf.add([a, b, -z], owner)
    cnf.add([a, -b, z], owner)
    cnf.add([-a, b, z], owner)


def parse_solver_model(path: Path) -> Dict[int, bool]:
    assignment: Dict[int, bool] = {}
    status: Optional[str] = None
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("c"):
            continue
        upper = line.upper()
        if upper.startswith("S "):
            upper = upper[2:].strip()
        if upper in {"SAT", "SATISFIABLE"}:
            status = "SAT"
            continue
        if upper in {"UNSAT", "UNSATISFIABLE"}:
            raise ValueError("model file reports UNSAT")
        if line.startswith("v "):
            line = line[2:]
        for token in line.split():
            try:
                lit = int(token)
            except ValueError:
                continue
            if lit == 0:
                continue
            assignment[abs(lit)] = lit > 0
    if status not in {None, "SAT"}:
        raise ValueError(f"unsupported status {status}")
    if not assignment:
        raise ValueError("no assignment literals found")
    return assignment


def parse_dimacs(path: Path) -> Tuple[int, List[List[int]]]:
    nvars = None
    clauses: List[List[int]] = []
    current: List[int] = []
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("c"):
            continue
        if line.startswith("p "):
            parts = line.split()
            if len(parts) != 4 or parts[1] != "cnf":
                raise ValueError("bad DIMACS header")
            nvars = int(parts[2])
            continue
        for tok in line.split():
            lit = int(tok)
            if lit == 0:
                if not current:
                    raise ValueError("empty clause")
                clauses.append(current)
                current = []
            else:
                current.append(lit)
    if current:
        raise ValueError("unterminated DIMACS clause")
    if nvars is None:
        raise ValueError("missing DIMACS header")
    return nvars, clauses


def generate(n: int, out_dir: Path, shuffle: bool) -> Dict[str, Path]:
    if n < 2:
        raise ValueError("N must be at least 2")
    # The reference generator refused N > 4096 while its interface computation was quadratic in
    # the number of semantic nodes; with the linear version (below) larger N is practical.
    # Outputs for N <= 4096 are unchanged (byte-identical).
    if n > 65536:
        raise ValueError("N above 65536 is refused by this generator")

    q = choices_per_episode(n)
    w = bit_width(n)
    prefix = f"xsa_n{n}"
    out_dir.mkdir(parents=True, exist_ok=True)

    cnf = CNF()
    node_clauses: Dict[str, List[int]] = {}
    node_vars: Dict[str, set[int]] = {}
    node_children: Dict[str, List[str]] = {}
    selected: List[int] = []
    deltas: List[List[int]] = []
    leaf_bits: List[List[int]] = []
    planted_true: set[int] = set()

    def record_owner(owner: str, before: int) -> None:
        node_clauses.setdefault(owner, []).extend(range(before + 1, len(cnf.clauses) + 1))
        for ci in range(before, len(cnf.clauses)):
            node_vars.setdefault(owner, set()).update(abs(x) for x in cnf.clauses[ci])

    # Leaves: one symbolic transcript choice per protocol episode.
    for i in range(n):
        owner = f"episode_{i}"
        sels = [cnf.var(f"sel[{i},{j}]") for j in range(q)]
        selected.append(stable_word(f"XSA/{n}/planted/{i}", 32) % q)
        episode_deltas: List[int] = []
        used = set()
        for j in range(q):
            word = stable_word(f"XSA/{n}/delta/{i}/{j}", w)
            while word == 0 or word in used:
                word = ((word * 1103515245 + 12345) & ((1 << w) - 1)) or 1
            used.add(word)
            episode_deltas.append(word)
        deltas.append(episode_deltas)

        before = len(cnf.clauses)
        exactly_one(cnf, sels, owner)
        bits = [cnf.var(f"summary[episode_{i},{b}]") for b in range(w)]
        leaf_bits.append(bits)
        for b, bit_var in enumerate(bits):
            for j, sel in enumerate(sels):
                bit = (episode_deltas[j] >> b) & 1
                cnf.add([-sel, bit_var if bit else -bit_var], owner)
        record_owner(owner, before)

        planted_true.add(sels[selected[-1]])
        chosen = episode_deltas[selected[-1]]
        for b, bit_var in enumerate(bits):
            if (chosen >> b) & 1:
                planted_true.add(bit_var)

    # Balanced XOR tree.  Singleton nodes are represented by direct reuse, so
    # only genuine binary composition creates clauses.
    current: List[Tuple[str, List[int], int, int]] = [
        (f"episode_{i}", leaf_bits[i], i, i + 1) for i in range(n)
    ]
    level = 0
    while len(current) > 1:
        nxt: List[Tuple[str, List[int], int, int]] = []
        for k in range(0, len(current), 2):
            if k + 1 == len(current):
                nxt.append(current[k])
                continue
            left_name, left_bits, lo, mid = current[k]
            right_name, right_bits, mid2, hi = current[k + 1]
            assert mid == mid2
            owner = f"join_L{level}_{lo}_{hi}"
            out_bits = [cnf.var(f"summary[{owner},{b}]") for b in range(w)]
            before = len(cnf.clauses)
            for b in range(w):
                xor_gate(cnf, left_bits[b], right_bits[b], out_bits[b], owner)
            record_owner(owner, before)
            node_children[owner] = [left_name, right_name]
            nxt.append((owner, out_bits, lo, hi))

            for b in range(w):
                lv = left_bits[b] in planted_true
                rv = right_bits[b] in planted_true
                if lv ^ rv:
                    planted_true.add(out_bits[b])
        current = nxt
        level += 1

    root_name, root_bits, _, _ = current[0]
    target = 0
    for i, j in enumerate(selected):
        target ^= deltas[i][j]

    target_owner = "attack_goal"
    before = len(cnf.clauses)
    for b, bit_var in enumerate(root_bits):
        required = (target >> b) & 1
        cnf.add([bit_var if required else -bit_var], target_owner)
    record_owner(target_owner, before)
    node_children[target_owner] = [root_name]

    # Validate the planted model against the generated clauses before any
    # permutation.  Unlisted variables are false.
    def value(v: int) -> bool:
        return v in planted_true

    bad = []
    for ci, clause in enumerate(cnf.clauses, 1):
        if not any(value(abs(lit)) == (lit > 0) for lit in clause):
            bad.append(ci)
    if bad:
        raise AssertionError(f"internal planted model failed clauses {bad[:10]}")

    # Permute variable identifiers and clause order by default.  This avoids
    # leaking the hierarchy through consecutive DIMACS numbering while leaving
    # the semantic metadata available for controlled experiments.
    old_to_new = {v: v for v in range(1, cnf.nvars + 1)}
    clause_order = list(range(len(cnf.clauses)))
    if shuffle:
        rng = random.Random(stable_word(f"XSA/{n}/shuffle", 64))
        new_ids = list(range(1, cnf.nvars + 1))
        rng.shuffle(new_ids)
        old_to_new = dict(zip(range(1, cnf.nvars + 1), new_ids))
        rng.shuffle(clause_order)

    new_clause_number: Dict[int, int] = {}
    dimacs_clauses: List[List[int]] = []
    dimacs_owners: List[str] = []
    for new_no, old_idx in enumerate(clause_order, 1):
        new_clause_number[old_idx + 1] = new_no
        dimacs_clauses.append([
            old_to_new[abs(lit)] if lit > 0 else -old_to_new[abs(lit)]
            for lit in cnf.clauses[old_idx]
        ])
        dimacs_owners.append(cnf.clause_owner[old_idx])

    cnf_path = out_dir / f"{prefix}.cnf"
    with cnf_path.open("w", encoding="ascii") as f:
        f.write(f"c XSA-N symbolic XOR session attack benchmark, version {VERSION}\n")
        f.write(f"c scale N={n}; derived Q={q}, W={w}; shuffled={str(shuffle).lower()}\n")
        f.write(f"c sha256 metadata is in {prefix}.meta.json\n")
        f.write(f"p cnf {cnf.nvars} {len(dimacs_clauses)}\n")
        for clause in dimacs_clauses:
            f.write(" ".join(map(str, clause)) + " 0\n")

    # Metadata is intentionally rich: it provides ground truth for evaluating
    # DAG elicitation, but the CNF can be used independently in blind mode.
    nodes = []
    all_node_names = sorted(node_clauses)
    parents: Dict[str, List[str]] = {name: [] for name in all_node_names}
    for parent, children in node_children.items():
        for child in children:
            parents.setdefault(child, []).append(parent)
    # A variable is on a node's interface iff some other node also uses it.  Counting
    # owners once is linear; the pairwise intersection it replaces was quadratic in the
    # number of semantic nodes (N=4096: >6 min and >1 GB before this change).
    owner_count: Dict[int, int] = {}
    for name in all_node_names:
        for v in node_vars[name]:
            owner_count[v] = owner_count.get(v, 0) + 1
    for name in all_node_names:
        vars_here = {old_to_new[v] for v in node_vars[name]}
        interface = {old_to_new[v] for v in node_vars[name] if owner_count[v] > 1}
        nodes.append({
            "id": name,
            "kind": "episode" if name.startswith("episode_") else ("goal" if name == target_owner else "xor_join"),
            "children": node_children.get(name, []),
            "parents": parents.get(name, []),
            "clauses": sorted(new_clause_number[c] for c in node_clauses[name]),
            "variables": sorted(vars_here),
            "interface_variables": sorted(interface),
        })

    metadata = {
        "benchmark": "XSA-N",
        "version": VERSION,
        "purpose": "Dagster DAG elicitation and telemetry-guided refinement",
        "scale_parameter": {"name": "N", "value": n, "meaning": "number of bounded protocol episodes"},
        "derived": {
            "choices_per_episode_Q": q,
            "interface_width_W": w,
            "variables": cnf.nvars,
            "clauses": len(dimacs_clauses),
            "semantic_nodes": len(nodes),
            "shuffled_dimacs": shuffle,
        },
        "semantics": {
            "episode": "choose exactly one recorded transcript-derived XOR delta",
            "composition": "balanced XOR of episode summaries",
            "attack_goal": "root summary equals the planted target fingerprint",
            "single_model_interpretation": "one selected transcript per episode forming a successful bounded symbolic attack",
        },
        "target_hex": f"0x{target:0{(w + 3) // 4}x}",
        "planted_choice_by_episode": selected,
        "episode_deltas_hex": [[f"0x{x:0{(w + 3) // 4}x}" for x in row] for row in deltas],
        "variables": {str(old_to_new[v]): cnf.names[v] for v in sorted(cnf.names)},
        "nodes": nodes,
        "dimacs_clause_owner": {str(i + 1): owner for i, owner in enumerate(dimacs_owners)},
        "recommended_experiments": {
            "blind": "use only the CNF",
            "semantic": "use variable names and episode ownership, but not nodes",
            "oracle": "use the complete semantic node tree as an upper-bound reference",
        },
    }
    meta_path = out_dir / f"{prefix}.meta.json"
    meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    model_path = out_dir / f"{prefix}.planted.model"
    planted_new = {old_to_new[v] for v in planted_true}
    with model_path.open("w", encoding="ascii") as f:
        f.write("s SATISFIABLE\n")
        lits = [v if v in planted_new else -v for v in range(1, cnf.nvars + 1)]
        for k in range(0, len(lits), 20):
            f.write("v " + " ".join(map(str, lits[k:k + 20])) + " 0\n")

    manifest = {
        "cnf": cnf_path.name,
        "metadata": meta_path.name,
        "planted_model": model_path.name,
        "sha256": {},
    }
    for p in (cnf_path, meta_path, model_path):
        manifest["sha256"][p.name] = hashlib.sha256(p.read_bytes()).hexdigest()
    manifest_path = out_dir / f"{prefix}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {"cnf": cnf_path, "metadata": meta_path, "model": model_path, "manifest": manifest_path}


def validate(cnf_path: Path, model_path: Path, metadata_path: Optional[Path]) -> None:
    nvars, clauses = parse_dimacs(cnf_path)
    assignment = parse_solver_model(model_path)
    missing = sorted(v for v in range(1, nvars + 1) if v not in assignment)
    if missing:
        raise ValueError(f"partial model: {len(missing)} variables are unassigned; first are {missing[:10]}")
    bad = []
    for ci, clause in enumerate(clauses, 1):
        if not any(assignment[abs(lit)] == (lit > 0) for lit in clause):
            bad.append(ci)
    if bad:
        raise ValueError(f"model falsifies {len(bad)} clauses; first are {bad[:10]}")

    extra = ""
    if metadata_path:
        meta = json.loads(metadata_path.read_text(encoding="utf-8"))
        names = {int(k): v for k, v in meta["variables"].items()}
        selections: Dict[int, int] = {}
        for v, name in names.items():
            if assignment.get(v, False) and name.startswith("sel["):
                i, j = (int(x) for x in name[4:-1].split(","))
                selections[i] = j
        if len(selections) != meta["scale_parameter"]["value"]:
            raise ValueError("model does not select exactly one transcript in every episode")
        q = meta["derived"]["choices_per_episode_Q"]
        w = meta["derived"]["interface_width_W"]
        deltas = [[int(x, 16) for x in row] for row in meta["episode_deltas_hex"]]
        summary = 0
        for i in range(len(selections)):
            j = selections[i]
            if not 0 <= j < q:
                raise ValueError(f"episode {i} has invalid selection {j}")
            summary ^= deltas[i][j]
        target = int(meta["target_hex"], 16)
        if summary != target:
            raise ValueError(f"semantic XOR attack check failed: got {summary:#x}, expected {target:#x}")
        extra = f"; semantic attack summary {summary:#0{(w + 3)//4 + 2}x} matches target"
    print(f"VALID: {cnf_path.name} under {model_path.name}; {len(clauses)} clauses{extra}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="command", required=True)

    gp = sub.add_parser("generate", help="generate one XSA-N instance")
    gp.add_argument("N", type=int, help="the only scaling hyperparameter: number of protocol episodes")
    gp.add_argument("--out", type=Path, default=Path("."), help="output directory")
    gp.add_argument("--no-shuffle", action="store_true", help="retain semantic variable and clause order")

    vp = sub.add_parser("validate", help="validate a SAT-solver model")
    vp.add_argument("cnf", type=Path)
    vp.add_argument("model", type=Path)
    vp.add_argument("--metadata", type=Path)

    args = ap.parse_args(argv)
    if args.command == "generate":
        files = generate(args.N, args.out, not args.no_shuffle)
        for kind, path in files.items():
            print(f"{kind}: {path}")
        return 0
    validate(args.cnf, args.model, args.metadata)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
