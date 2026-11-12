#!/usr/bin/env python3
"""refine_topdown.py -- telemetry-guided refinement of a top-down DAG.

  refine_topdown.py --meta M --cnf C --start-depth 3 --rounds 4 --split-per-round 2
                    --dagster-ss BIN [--min-episodes 16] [--timeout 300] --out OUT

Round 0 runs the uniform depth-d frontier on ONE worker with GLOG_v=2, so each child's
busy time approximates its cost.  Each round then replaces the slowest children by their
two subtrees (never below --min-episodes, the acceptance floor from predict_topdown) and
reruns.  Reported per round: children, total work, slowest child (the critical path if
every child had its own worker), and the ratio slowest/median.  Sub-targets are re-chosen
each run, so per-child costs are re-drawn from a heavy-tailed distribution: read the
trend over rounds, not one round.
"""
import argparse
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))                # xsa_dagster
sys.path.insert(0, os.path.join(HERE, "..", "..", "harness"))
import common  # noqa: E402
import xsa_dagster as xd  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--meta", required=True)
ap.add_argument("--cnf", required=True)
ap.add_argument("--dagster-ss", required=True)
ap.add_argument("--start-depth", type=int, default=3)
ap.add_argument("--rounds", type=int, default=4)
ap.add_argument("--split-per-round", type=int, default=2)
ap.add_argument("--min-episodes", type=int, default=16)
ap.add_argument("--timeout", type=float, default=300)
ap.add_argument("--oversubscribe", action="store_true")
ap.add_argument("--out", required=True)
a = ap.parse_args()
out = os.path.abspath(a.out)
os.makedirs(out, exist_ok=True)
x = xd.XSA(a.meta)
front, _ = x.frontier(a.start_depth)
jl = open(os.path.join(out, "refine.jsonl"), "a")
for rnd in range(a.rounds + 1):
    dag = x.dag_frontier(front)
    dpath = os.path.join(out, "n%d_round%d.dag" % (x.N, rnd))
    xd.write_dag(dpath, dag, x.nvars)
    wd = os.path.join(out, "runs", "n%d_round%d" % (x.N, rnd))
    res = common.run_dagster(os.path.abspath(a.dagster_ss), dpath, a.cnf, wd, 2, a.timeout, a.cnf,
                             dagster_args=shlex.split("-m 4 -e 0 -b 0 -i 1"), oversubscribe=a.oversubscribe,
                             env_extra={"DAGSTER_SINGLE_SHOT": dag["single_shot"], "GLOG_v": "2",
                                        "GLOG_minloglevel": "0"}, complete_fn=x.complete)
    tel = xd.parse_telemetry(os.path.join(wd, "dagster.log"), dag["roles"])
    kids = {k[2:]: v["busy_s"] for k, v in tel.items() if k.startswith("C:")}
    costs = sorted(kids.values())
    row = dict(N=x.N, round=rnd, status=res["status"], t=res["t_answer"], children=len(front),
               work=round(sum(costs), 2), slowest=max(costs) if costs else None,
               median=costs[len(costs) // 2] if costs else None,
               sizes=sorted(set(len(x.leaves(c)) for c in front)))
    jl.write(json.dumps(row) + "\n")
    jl.flush()
    print("round %d: %-4s children=%-3d total %7.1fs  work %6.1fs  slowest %5.2fs  median %5.2fs  sizes %s" % (
        rnd, res["status"], len(front), res["t_answer"] or -1, row["work"], row["slowest"] or -1,
        row["median"] or -1, row["sizes"]), flush=True)
    if res["status"] != "SAT" or rnd == a.rounds:
        break
    slow = sorted(kids, key=lambda c: -kids[c])
    split_now = [c for c in slow if not c.startswith("episode_") and
                 min(len(x.leaves(k)) for k in x.kids(c)) >= a.min_episodes][:a.split_per_round]
    if not split_now:
        print("no child can be split above the acceptance floor; stopping")
        break
    front = [k for c in front for k in (x.kids(c) if c in split_now else [c])]
