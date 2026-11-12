#!/usr/bin/env python3
"""run_xsab.py -- XSA-B (birthday regime) through Dagster, plain vs strategy CNF.

  run_xsab.py --meta M --dir D --m M --dagster BIN --shapes one,oracle --variants plain,strategy
              [--ranks 3] [--timeout 120] [--count-jobs] [--oversubscribe] --out OUT

'oracle' over the strategy CNF is Wagner's k-tree algorithm executed by Dagster: every
join holds its XOR gates plus Wagner's filtration units.  Over the plain CNF the same
tree has no filtration (the XSA-N situation).  Models are completed from the reported
selectors and checked against the semantics and the full CNF of that variant.
"""
import argparse
import json
import os
import shlex
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))                # xsa_dagster
sys.path.insert(0, os.path.join(HERE, "..", "..", "harness"))
import common  # noqa: E402
import xsa_dagster as xd  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--dir", required=True)
ap.add_argument("--m", required=True)
ap.add_argument("--dagster", required=True)
ap.add_argument("--shapes", default="one,oracle")
ap.add_argument("--variants", default="plain,strategy")
ap.add_argument("--ranks", default="3")
ap.add_argument("--timeout", type=float, default=120)
ap.add_argument("--dagster-args", default="-m 4 -e 0 -b 0 -i 1")
ap.add_argument("--count-jobs", action="store_true")
ap.add_argument("--oversubscribe", action="store_true")
ap.add_argument("--out", required=True)
a = ap.parse_args()
out = os.path.abspath(a.out)
os.makedirs(out, exist_ok=True)
jl = open(os.path.join(out, "xsab_results.jsonl"), "a")
for m in a.m.split(","):
    meta = os.path.join(a.dir, "xsab_m%s.meta.json" % m)
    for variant in a.variants.split(","):
        x = xd.XSA(meta)
        if variant == "plain":
            x.use_prefix(json.load(open(meta))["derived"]["plain_clauses"])
        cnf = os.path.join(a.dir, "xsab_m%s.%s.cnf" % (m, variant))
        for shape in a.shapes.split(","):
            dag = x.dag_one() if shape == "one" else x.dag_oracle()
            dpath = os.path.join(out, "xsab_m%s.%s.%s.dag" % (m, variant, shape))
            xd.write_dag(dpath, dag, x.nvars)
            env = {"GLOG_v": "2", "GLOG_minloglevel": "0"} if a.count_jobs else {}
            for r in [int(v) for v in a.ranks.split(",")]:
                wd = os.path.join(out, "runs", "m%s_%s_%s_r%d" % (m, variant, shape, r))
                res = common.run_dagster(os.path.abspath(a.dagster), dpath, cnf, wd, r, a.timeout, cnf,
                                         dagster_args=shlex.split(a.dagster_args),
                                         oversubscribe=a.oversubscribe, env_extra=env,
                                         complete_fn=x.complete)
                jobs = None
                if a.count_jobs:
                    jobs = sum(1 for line in open(os.path.join(wd, "dagster.log"), errors="replace")
                               if "sending message to worker" in line)
                row = dict(m=int(m), N=x.N, Q=x.Q, W=x.W, variant=variant, shape=shape, ranks=r,
                           status=res["status"], t=res["t_answer"], dag_nodes=len(dag["nodes"]),
                           jobs=jobs, verify=res.get("verify", ""), when=time.strftime("%H:%M:%S"))
                jl.write(json.dumps(row) + "\n")
                jl.flush()
                print("m=%s N=%-4d %-8s %-7s r=%d  %-12s %8s  jobs=%s  %s" % (
                    m, x.N, variant, shape, r, res["status"],
                    "%.2fs" % res["t_answer"] if res["t_answer"] else "-", jobs, res.get("verify", "")[:50]),
                    flush=True)
