#!/usr/bin/env python3
"""run_xsa.py -- XSA-N through Dagster: DAG shapes x N x ranks, every model checked.

  run_xsa.py --gen PATH/xor_session_attack_gen.py --dagster BIN [--dagster-ss BIN]
             --n 8,16 --shapes one,oracle,block4,split8,split8ss --ranks 3
             [--timeout 120] [--count-jobs] [--oversubscribe] --out DIR

Shapes are those of xsa_dagster.py; a trailing "ss" runs the shape on the single-shot
binary with node A single-shot (split only).  A SAT answer counts only after semantic
completion from the reported selectors (attack XOR = target) AND a full-CNF check.
--count-jobs logs Dagster at GLOG_v=2 and counts the jobs the master dispatched (slow on
large N: messages carry thousands of selector literals).
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))                # xsa_dagster
sys.path.insert(0, os.path.join(HERE, "..", "..", "harness"))
import common  # noqa: E402
import xsa_dagster as xd  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gen", required=True)
    ap.add_argument("--dagster", required=True)
    ap.add_argument("--dagster-ss", default=None)
    ap.add_argument("--n", default="8,16")
    ap.add_argument("--shapes", default="one,oracle,block4,split8")
    ap.add_argument("--ranks", default="3")
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--dagster-args", default="-m 4 -e 0 -b 0 -i 1")
    ap.add_argument("--count-jobs", action="store_true")
    ap.add_argument("--oversubscribe", action="store_true")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    jl = open(os.path.join(out, "xsa_results.jsonl"), "a")
    for n in [int(v) for v in a.n.split(",")]:
        idir = os.path.join(out, "inst", "n%d" % n)
        meta = os.path.join(idir, "xsa_n%d.meta.json" % n)
        if not os.path.exists(meta):
            subprocess.run([sys.executable, a.gen, "generate", str(n), "--out", idir], check=True,
                           stdout=subprocess.DEVNULL)
        cnf = os.path.join(idir, "xsa_n%d.cnf" % n)
        x = xd.XSA(meta)
        for shape in a.shapes.split(","):
            ss = shape.endswith("ss")
            base = shape[:-2] if ss else shape
            if base == "one":
                dag = x.dag_one()
            elif base == "oracle":
                dag = x.dag_oracle()
            elif base.startswith("block"):
                dag = x.dag_block(int(base[5:]))
            elif base.startswith("split"):
                dag = x.dag_split(int(base[5:]))
            else:
                sys.exit("unknown shape " + shape)
            if dag is None:
                continue
            dpath = os.path.join(idir, "xsa_n%d.%s.dag" % (n, base))
            xd.write_dag(dpath, dag, x.nvars)
            binary, env = a.dagster, {}
            if ss:
                if not a.dagster_ss:
                    continue
                binary, env = a.dagster_ss, {"DAGSTER_SINGLE_SHOT": dag["single_shot"]}
            if a.count_jobs:   # dispatches are INFO-level VLOG(2): both settings are needed
                env["GLOG_v"] = "2"
                env["GLOG_minloglevel"] = "0"
            for r in [int(v) for v in a.ranks.split(",")]:
                wd = os.path.join(out, "runs", "n%d" % n, "%s_r%d" % (shape, r))
                res = common.run_dagster(os.path.abspath(binary), dpath, cnf, wd, r, a.timeout, cnf,
                                         dagster_args=shlex.split(a.dagster_args),
                                         oversubscribe=a.oversubscribe, env_extra=env,
                                         complete_fn=x.complete)
                jobs = None
                if a.count_jobs:
                    jobs = sum(1 for line in open(os.path.join(wd, "dagster.log"), errors="replace")
                               if "sending message to worker" in line)
                row = dict(N=n, Q=x.Q, W=x.W, shape=shape, ranks=r, status=res["status"],
                           t=res["t_answer"], dag_nodes=len(dag["nodes"]), jobs=jobs,
                           verify=res.get("verify", ""), when=time.strftime("%H:%M:%S"))
                jl.write(json.dumps(row) + "\n")
                jl.flush()
                print("N=%-5d %-9s r=%d  %-12s %8s  jobs=%s  %s" % (
                    n, shape, r, res["status"], "%.2fs" % res["t_answer"] if res["t_answer"] else "-",
                    jobs, res.get("verify", "")[:60]), flush=True)


if __name__ == "__main__":
    main()
