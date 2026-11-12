#!/usr/bin/env python3
"""run_families.py -- sweep DAG families over XSA-N / XSA-B instances on Dagster.

  run_families.py --meta M --cnf C [--plain-prefix] --families one,tree,block:4,split:8,topdown:3
                  --dagster BIN [--dagster-ss BIN] [--ranks 3] [--timeout 120]
                  [--telemetry] [--oversubscribe] --tag NAME --out OUT

Families: one | tree | block:B | split:K | topdown:D | topdown1:D (splitter also
single-shot; only when predict_topdown says every child reaches its sub-target).
topdown* runs on the single-shot binary (children single-shot).  Every SAT answer is
completed from its selectors and checked against the semantics and the full CNF.
--telemetry logs at GLOG_v=2 and records per-node jobs and busy time.
"""
import argparse
import json
import os
import shlex
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "harness"))
import common  # noqa: E402
import xsa_dagster as xd  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--meta", required=True)
ap.add_argument("--cnf", required=True)
ap.add_argument("--plain-prefix", action="store_true", help="XSA-B: use the plain CNF prefix")
ap.add_argument("--families", default=None)
ap.add_argument("--plan", default=None,
                help="';'-separated FAMILY@RANKS[,RANKS..] entries, e.g. 'one@2;topdown:5@2,33' "
                     "(each family with its own rank counts; overrides --families/--ranks)")
ap.add_argument("--repeats", type=int, default=1)
ap.add_argument("--dagster", required=True)
ap.add_argument("--dagster-ss", default=None)
ap.add_argument("--ranks", default="3")
ap.add_argument("--timeout", type=float, default=120)
ap.add_argument("--dagster-args", default="-m 4 -e 0 -b 0 -i 1")
ap.add_argument("--telemetry", action="store_true")
ap.add_argument("--telemetry-reps", type=int, default=0,
                help="with --telemetry, log only the first K repeats (0 = all); later repeats run at "
                     "GLOG_v=0, so their timings carry no logging cost")
ap.add_argument("--fair", action="store_true", help="DAGSTER_FAIR_SCHEDULING=1 (patched binary)")
ap.add_argument("--e3", action="store_true",
                help="UNPATCHED showcase: upstream binary for every family, -m 4 -e 3 -b 0 "
                     "(one solution per node message, stop at the first terminal solution)")
ap.add_argument("--bound-bits", type=int, default=1, help="free bits per sub-target for bounded:D")
ap.add_argument("--e3-extra", default="",
                help="extra Dagster arguments in --e3 mode, e.g. '-g 2' (table master without the "
                     "pairwise join-consistency check; safe when a node's inputs share no variables)")
ap.add_argument("--oversubscribe", action="store_true")
ap.add_argument("--tag", required=True)
ap.add_argument("--variant", default="", help="free-form label kept apart in the report (e.g. btl=self,tcp)")
ap.add_argument("--out", required=True)
a = ap.parse_args()
# Dagster runs inside each run directory: every path handed to it must be absolute
a.meta, a.cnf, a.dagster = os.path.abspath(a.meta), os.path.abspath(a.cnf), os.path.abspath(a.dagster)
if a.dagster_ss:
    a.dagster_ss = os.path.abspath(a.dagster_ss)
out = os.path.abspath(a.out)
os.makedirs(out, exist_ok=True)
x = xd.XSA(a.meta)
if a.plain_prefix:
    x.use_prefix(json.load(open(a.meta))["derived"]["plain_clauses"])
jl = open(os.path.join(out, "families.jsonl"), "a")
plan = []
if a.plan:
    for item in a.plan.split(";"):
        item = item.strip()
        if item:
            fam_, _, rk = item.partition("@")
            plan.append((fam_.strip(), [int(v) for v in (rk or a.ranks).split(",")]))
elif a.families:
    plan = [(f_, [int(v) for v in a.ranks.split(",")]) for f_ in a.families.split(",")]
else:
    sys.exit("give --plan or --families")
for rep, fam, ranks_list in [(rp, f_, rl) for rp in range(a.repeats) for f_, rl in plan]:
    kind, _, par = fam.partition(":")
    if kind == "topdown2":                       # topdown2:D1:D2 -- two-stage splitting
        d1, d2 = (int(v) for v in par.split(":"))
        par = None
    else:
        par = int(par) if par else None
    ss_binary = False
    if kind == "topdown2":
        dag = x.dag_topdown2(d1, d2)
        ss_binary = True
    elif kind == "one":
        dag = x.dag_one()
    elif kind == "tree":
        dag = x.dag_oracle()
    elif kind == "block":
        dag = x.dag_block(par)
    elif kind == "split":
        dag = x.dag_split(par)
    elif kind in ("topdown", "topdown1", "keyed", "keyed1", "bounded", "bkeyed"):
        # keyed*: every edge also carries the whole split (a join key for the collector)
        # bounded/bkeyed: the splitter offers at most 2^(bound_bits*(children-1)) splits
        dag = x.dag_topdown(par, splitter_single_shot=kind.endswith("1"),
                            keyed=kind in ("keyed", "keyed1", "bkeyed"),
                            bound_bits=a.bound_bits if kind in ("bounded", "bkeyed") else None)
        ss_binary = True
    else:
        sys.exit("unknown family " + fam)
    if dag is None:
        continue
    dpath = os.path.join(out, "%s.%s.dag" % (a.tag, fam.replace(":", "")))
    xd.write_dag(dpath, dag, x.nvars)
    run_cnf = a.cnf
    if dag and dag.get("aux_units"):           # DAG-specific auxiliary clauses (splitter only)
        run_cnf = os.path.join(out, "%s.%s.cnf" % (a.tag, fam.replace(":", "")))
        lines = open(a.cnf).read().splitlines()
        h = next(i for i, l in enumerate(lines) if l.startswith("p cnf"))
        nv, nc = map(int, lines[h].split()[2:4])
        lines[h] = "p cnf %d %d" % (nv, nc + len(dag["aux_units"]))
        with open(run_cnf, "w") as f:
            f.write("\n".join(lines) + "\n" + "".join("%d 0\n" % u for u in dag["aux_units"]))
    binary, env = a.dagster, {}
    if a.fair:
        env["DAGSTER_FAIR_SCHEDULING"] = "1"
    if a.e3:
        ss_binary = False                     # -e 3 gives every node single-shot semantics
    if ss_binary:
        if not a.dagster_ss:
            continue
        binary = a.dagster_ss
        env["DAGSTER_SINGLE_SHOT"] = dag["single_shot"]   # add to, never replace, env (keeps --fair)
    tel_on = a.telemetry and (a.telemetry_reps == 0 or rep < a.telemetry_reps)
    if tel_on:
        env.update(GLOG_v="2", GLOG_minloglevel="0")
    else:
        env.update(GLOG_v="0", GLOG_minloglevel="1")
    for r in ranks_list:
        wd = os.path.join(out, "runs", a.tag, "%s_r%d_rep%d" % (fam.replace(":", ""), r, rep))
        dargs = ("-m 4 -e 3 -b 0 " + a.e3_extra).strip() if a.e3 else a.dagster_args
        if tel_on and a.e3:
            env.update(GLOG_v="3")            # also Dagster's own StatisticsModule lines
        res = common.run_dagster(os.path.abspath(binary), dpath, run_cnf, wd, r, a.timeout, a.cnf,
                                 dagster_args=shlex.split(dargs), oversubscribe=a.oversubscribe,
                                 env_extra=env, complete_fn=x.complete)
        tel = xd.parse_telemetry(os.path.join(wd, "dagster.log"), dag["roles"]) if tel_on else None
        builtin = xd.parse_builtin_stats(os.path.join(wd, "dagster.log"), dag["roles"]) if tel_on else None
        row = dict(tag=a.tag, N=x.N, Q=x.Q, W=x.W, family=fam, ranks=r, rep=rep, run_dir=wd, fair=a.fair,
                   bound_bits=a.bound_bits if kind in ("bounded", "bkeyed") else None, status=res["status"],
                   t=res["t_answer"], dag_nodes=len(dag["nodes"]),
                   jobs=sum(v["jobs"] for v in tel.values()) if tel else None, telemetry=tel,
                   builtin_stats=builtin, e3=a.e3, dagster_args=dargs, timeout=a.timeout, variant=a.variant, binary=os.path.basename(os.path.dirname(os.path.dirname(binary))),
                   verify=res.get("verify", "")[:80], when=time.strftime("%H:%M:%S"))
        jl.write(json.dumps(row) + "\n")
        jl.flush()
        hot = ""
        if tel:
            top = sorted(tel.items(), key=lambda kv: -kv[1]["jobs"])[:3]
            hot = " ".join("%s:%d" % (k, v["jobs"]) for k, v in top)
        print("%-10s %-11s r=%-4d rep=%d %-12s %8s jobs=%-6s %s" % (a.tag, fam, r, rep, res["status"],
              "%.2fs" % res["t_answer"] if res["t_answer"] else "-", row["jobs"], hot), flush=True)
