#!/usr/bin/env python3
"""selftest.py -- every check must pass before anything in this repository is trusted.

Runs in about a minute on a laptop, needs only Python 3.7+, and runs no experiment:

  1. every Python file compiles;
  2. the generator reproduces the bundled N=8 and N=32 instances byte for byte;
  3. every DAG family builds on N=32 and passes Dagster's DAG-grammar check
     (tools/check_dag.py), and every top-down DAG passes the collector check
     (single sink, pairwise variable-disjoint collector inputs: the condition under
     which Dagster's -g 2 is exact);
  4. the acceptance predictor reproduces the proceedings' figure for N=32, depth 3;
  5. the evidence tables are complete and agree with the headline numbers of the
     proceedings (206 runs, 203 verified, 3 censored; 64.3 s, 22.5x at 207 workers).
"""
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "xsa"))
FAILS = []


def check(ok, what):
    print(("PASS  " if ok else "FAIL  ") + what)
    if not ok:
        FAILS.append(what)


def sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def main():
    # 1. compile
    bad = []
    for dirpath, _, files in os.walk(ROOT):
        if "/." in dirpath or "__pycache__" in dirpath:
            continue
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(dirpath, f)
                try:
                    compile(open(path, encoding="utf-8").read(), path, "exec")
                except SyntaxError as e:
                    bad.append("%s: %s" % (f, e))
    check(not bad, "all Python files compile" + ("" if not bad else ": " + "; ".join(bad)))

    ex = os.path.join(ROOT, "xsa", "examples")
    gen = os.path.join(ROOT, "xsa", "generator", "xor_session_attack_gen.py")
    with tempfile.TemporaryDirectory() as tmp:
        # 2. generator reproduces the examples
        for n in (8, 32):
            out = os.path.join(tmp, "n%d" % n)
            r = subprocess.run([sys.executable, gen, "generate", str(n), "--out", out],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, universal_newlines=True)
            same = r.returncode == 0 and all(
                sha(os.path.join(out, "xsa_n%d.%s" % (n, ext))) == sha(os.path.join(ex, "xsa_n%d.%s" % (n, ext)))
                for ext in ("cnf", "meta.json", "planted.model"))
            check(same, "generator reproduces the N=%d instance byte for byte" % n)

        # 3. DAG families on N=32
        import xsa_dagster as xd
        x = xd.XSA(os.path.join(ex, "xsa_n32.meta.json"))
        cnf = os.path.join(ex, "xsa_n32.cnf")
        fams = {"one": x.dag_one(), "split:8": x.dag_split(8), "tree": x.dag_oracle(), "block:4": x.dag_block(4),
                "topdown:2": x.dag_topdown(2), "topdown2:1:1": x.dag_topdown2(1, 1)}
        topdown = []
        for name, dag in fams.items():
            path = os.path.join(tmp, name.replace(":", "_") + ".dag")
            xd.write_dag(path, dag, x.nvars)
            r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "check_dag.py"), path, cnf, "--partial-sink"],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            check(r.returncode == 0, "DAG family %-13s passes the Dagster grammar check" % name)
            if name.startswith("topdown"):
                topdown.append(path)
        r = subprocess.run([sys.executable, os.path.join(ROOT, "evidence", "scripts", "check_collector.py")] + topdown,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        check(r.returncode == 0, "top-down DAGs: single sink, variable-disjoint collector inputs")

    # 4. predictor
    p = x.predict_topdown(3)["p_every_child_reaches_its_subtarget"]
    check(abs(p - 0.0085) < 0.0005, "acceptance predictor: N=32, depth 3 gives P_all = %.4f (proceedings: 0.85%%)" % p)

    # 5. evidence
    ev = os.path.join(ROOT, "evidence")
    man = json.load(open(os.path.join(ev, "derived", "MANIFEST.json")))
    check(man["runs"] == 206 and man["status_counts"] == {"SAT": 203, "TIMEOUT": 3},
          "evidence: 206 runs, 203 SAT, 3 TIMEOUT")
    runs = list(csv.DictReader(open(os.path.join(ev, "derived", "runs.csv"))))
    sat = [r for r in runs if r["status"] == "SAT"]
    check(len(runs) == 206 and all("matches target" in r["verified"] for r in sat),
          "evidence: every SAT run carries a verified-attack message")
    head = [r for r in csv.DictReader(open(os.path.join(ev, "derived", "scaling.csv")))
            if r["N"] == "16384" and r["dag"] == "topdown2:4:5" and r["ranks"] == "208"]
    ok = len(head) == 1 and abs(float(head[0]["t_median"]) - 64.27) < 0.01 and abs(float(head[0]["speedup"]) - 22.52) < 0.01 \
        and abs(float(head[0]["efficiency"]) - 0.109) < 0.001 and abs(float(head[0]["solve_speedup"]) - 33.1) < 0.05
    check(ok, "evidence: headline cell (N=16384, 207 workers) = 64.3 s, speedup 22.5, efficiency 10.9%, solve 33.1")
    inv = list(csv.DictReader(open(os.path.join(ev, "derived", "inventory.csv"))))
    tot = [sum(int(r[k]) for r in inv) for k in ("runs", "successful_verified", "timeouts", "launch_failures", "invalid_models")]
    check(tot == [206, 203, 3, 0, 0], "evidence: inventory over 13 batch jobs sums to 206 / 203 / 3 / 0 / 0")
    cc = open(os.path.join(ev, "validation", "collector_disjointness.txt")).read()
    check("55/55 DAGs pass" in cc, "evidence: all 55 executed top-down DAGs passed the collector check")
    check(sum(1 for l in open(os.path.join(ev, "validation", "executed_dag_sha256.txt")) if l.strip()) == 69,
          "evidence: SHA-256 recorded for all 69 executed DAG files")

    print("\n%d check(s) failed" % len(FAILS) if FAILS else "\nSELFTEST OK")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
