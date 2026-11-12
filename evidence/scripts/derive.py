#!/usr/bin/env python3
"""derive.py RESULTS_DIR OUT_DIR

Regenerate every derived table of the XSA-Dagster proceedings from the raw result
directories (one directory per batch job, each holding families.jsonl and runs/).  Writes CSV files and MANIFEST.json to OUT_DIR:

  runs.csv             one row per submitted run (206), including censored runs
  inventory.csv        per result directory: runs, successes, timeouts, failures
  scaling.csv          per (N, DAG, master mode, ranks): min / median / max time, speedup,
                       efficiency, core-second ratios, solve and start-up (logged repeat)
  phase_breakdown.csv  per logged run: pre, outside, splitter, subtree phase, join, collector
  latency.csv          per logged run: job compute / lag / response percentiles, utilisation
  master_load.csv      per logged run: master busy fraction; splitter-duplicate counts and cost

Standard library only.  The log parsers are xsa/showcase_report.py and xsa/latency_report.py.
Result-directory names are normalised by dropping a trailing scheduler host suffix
(e.g. '<name>_<jobid>.<host>-pbs' -> '<name>_<jobid>'), so released tables do not depend on it.
"""
import csv
import json
import math
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "xsa"))
from showcase_report import family_layout, log_of, phases, master   # noqa: E402
from latency_report import analyse                                   # noqa: E402

TS = re.compile(r"^[IWEF](?:\d{8}|\d{4}) (\d+):(\d+):([\d.]+) +(\d+) +([A-Za-z_]+\.(?:cpp|h)):(\d+)\] (.*)")
DISP = re.compile(r"sending message to worker (\d+) message about node (\d+)")
SOL = re.compile(r"received MPI_TAG_SOLUTION from worker (\d+)")


def master_profile(log, lay):
    """Master busy fraction, and master time per received solution by job role."""
    ev, mp = [], None
    for line in open(log, errors="replace"):
        m = TS.match(line[:400])
        if not m:
            continue
        t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
        if m.group(5).startswith("Master") and mp is None:
            mp = m.group(4)
        if m.group(4) == mp:
            ev.append((t, m.group(7)[:120]))
    rec = [k for k, e in enumerate(ev) if "receiving from workers" in e[1]]
    busy = wait = 0.0
    for a, b in zip(rec, rec[1:]):
        g = next((k for k in range(a + 1, b) if "received MPI_TAG" in ev[k][1]), None)
        if g is None:
            continue
        wait += ev[g][0] - ev[a][0]
        busy += ev[b][0] - ev[g][0]
    last, cost = {}, {"S": [], "M": [], "C": [], "K": []}
    for (t1, x1), (t2, _x2) in zip(ev, ev[1:]):
        d = DISP.search(x1)
        if d:
            last[int(d.group(1))] = int(d.group(2))
        s = SOL.search(x1)
        if s:
            node = last.get(int(s.group(1)))
            if node is None:
                continue
            role = ("S" if node == 0 else "K" if node == lay["K"] else
                    "M" if lay["mids"] and lay["mids"][0] <= node <= lay["mids"][1] else "C")
            cost[role].append(t2 - t1)
    if busy + wait == 0:
        return None
    out = {"rho": busy / (busy + wait), "master_busy_s": busy}
    for k, v in cost.items():
        out["%s_solutions" % k] = len(v)
        out["%s_master_ms_total" % k] = round(1000 * sum(v), 3)
    return out


def acceptance(N, Q, W, D):
    """Random-XOR acceptance predictor (a model, not a guarantee): lambda = Q^k / 2^W."""
    k = N // (2 ** D)
    log_lam = k * math.log(Q) - W * math.log(2)
    p = 1.0 if log_lam > 50 else -math.expm1(-math.exp(log_lam))
    if p <= 0:
        p_all = 0.0
    elif p >= 1:
        p_all = 1.0
    else:
        p_all = math.exp((2 ** D) * math.log(p))
    return k, p, p_all


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


def write(out, name, recs):
    if not recs:
        return
    keys = []
    for rr in recs:
        for kk in rr:
            if kk not in keys:
                keys.append(kk)
    with open(os.path.join(out, name), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, lineterminator="\n")
        w.writeheader()
        w.writerows(recs)


def main():
    res, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    rows = []
    for d in sorted(os.listdir(res)):
        f = os.path.join(res, d, "families.jsonl")
        if not os.path.exists(f):
            continue
        for line in open(f):
            if line.strip():
                r = json.loads(line)
                r["_root"], r["_dir"] = os.path.join(res, d), re.sub(r"\.[A-Za-z0-9_-]*pbs$", "", d)
                rows.append(r)
    runs = []
    for r in rows:
        args, src = r.get("dagster_args"), "recorded"
        if not args:
            args = "-m 4 -e 3 -b 0" if r.get("e3") else "unknown"
            src = "inferred: driver --e3, master -g left at its default 0"
        ver = r.get("verify")
        vok = (ver.get("ok") if isinstance(ver, dict) else ver) if r["status"] == "SAT" else ""
        D = family_layout(r["family"])["depth"] if r["family"].startswith("topdown") else None
        k = p = pall = ""
        if D:
            k, p, pall = acceptance(r["N"], r["Q"], r["W"], D)
        runs.append({
            "result_dir": r["_dir"], "pbs_job": r["_dir"].split("_")[-1],
            "experiment": r["_dir"].rsplit("_", 1)[0], "tag": r.get("tag"),
            "N": r["N"], "Q": r["Q"], "W": r["W"], "dag": r["family"], "ranks": r["ranks"],
            "workers": r["ranks"] - 1, "rep": r.get("rep", 0), "variant": r.get("variant", ""),
            "status": r["status"], "t_seconds": r["t"], "timeout_s": r.get("timeout", ""),
            "jobs": r.get("jobs"), "dagster_args": args, "args_source": src,
            "binary_build_dir": r.get("binary"), "verified": vok, "logged": bool(r.get("telemetry")),
            "depth_D": D or "", "episodes_per_subtree": k,
            "pred_single_subtree_acceptance": p, "pred_all_subtrees_acceptance": pall,
            "observed_failed_subtrees": (0 if (D and r["status"] == "SAT") else ("unknown" if D else "")),
        })
    write(out, "runs.csv", runs)

    inv = {}
    for x in runs:
        g = inv.setdefault(x["result_dir"], {"result_dir": x["result_dir"], "experiment": x["experiment"],
                                              "N": set(), "dags": set(), "ranks": set(), "repeats": 0,
                                              "runs": 0, "successful_verified": 0, "timeouts": 0,
                                              "launch_failures": 0, "invalid_models": 0})
        g["runs"] += 1
        g["N"].add(x["N"]); g["dags"].add(x["dag"]); g["ranks"].add(x["ranks"])
        g["repeats"] = max(g["repeats"], x["rep"] + 1)
        if x["status"] == "SAT":
            g["successful_verified" if x["verified"] not in (False, "False") else "invalid_models"] += 1
        elif x["status"] == "TIMEOUT":
            g["timeouts"] += 1
        else:
            g["launch_failures"] += 1
    invr = []
    for g in inv.values():
        g = dict(g)
        g["N"] = " ".join(map(str, sorted(g["N"])))
        g["dags"] = " ".join(sorted(g["dags"]))
        g["ranks"] = " ".join(map(str, sorted(g["ranks"])))
        invr.append(g)
    write(out, "inventory.csv", invr)

    ph, lat, ml = [], [], []
    for r in rows:
        if not r["family"].startswith("topdown") or r["status"] != "SAT":
            continue
        log = log_of(r)
        if not log or not os.path.exists(log):
            continue
        lay = family_layout(r["family"])
        base = {"result_dir": r["_dir"], "N": r["N"], "dag": r["family"], "ranks": r["ranks"],
                "rep": r.get("rep", 0), "master": master(r), "t_seconds": r["t"]}
        p = phases(log, lay)
        if p:
            ph.append({**base, "pre": round(p["pre"], 3), "outside": round(r["t"] - p["span_logged"], 3),
                       "splitter_S": round(p["S"], 3), "subtree_phase": round(p["kids"], 3),
                       "join": round(p["join"], 3), "collector_K": round(p["K"], 3),
                       "solve": round(p["span_logged"] - p["pre"], 3)})
        a = analyse(log, lay)
        if a and a["comp"]:
            rec = dict(base, jobs=a["jobs"])
            for nm in ("comp", "lag", "resp"):
                xs = a[nm]
                if xs:
                    rec[nm + "_median"] = round(statistics.median(xs), 4)
                    rec[nm + "_p90"] = round(pct(xs, 0.9), 4)
                    rec[nm + "_max"] = round(max(xs), 4)
            rec["subtree_phase"] = a["phase"] and round(a["phase"], 3)
            rec["utilisation"] = a["util"] and round(a["util"], 4)
            lat.append(rec)
        m = master_profile(log, lay)
        if m:
            ml.append({**base, **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in m.items()}})
    write(out, "phase_breakdown.csv", ph)
    write(out, "latency.csv", lat)
    write(out, "master_load.csv", ml)

    cells = {}
    for x, r in zip(runs, rows):
        if x["status"] == "SAT":
            cells.setdefault((x["N"], x["dag"], master(r), x["ranks"]), []).append(x["t_seconds"])
    phx = {}
    for p in ph:
        phx.setdefault((p["N"], p["dag"], p["master"], p["ranks"]), []).append(p["solve"])
    sc = []
    for (N, dag, mm, R), ts in sorted(cells.items()):
        plain = mm.split(" btl")[0]
        ref = cells.get((N, dag, plain, 2)) or cells.get((N, dag, mm, 2))
        T1 = statistics.median(ref) if ref else None
        T, P = statistics.median(ts), R - 1
        sv = phx.get((N, dag, mm, R))
        solve = statistics.median(sv) if sv else None
        sv1 = phx.get((N, dag, plain, 2))
        solve1 = statistics.median(sv1) if sv1 else None
        sc.append({
            "N": N, "dag": dag, "master": mm, "ranks": R, "workers": P, "n_runs": len(ts),
            "t_min": min(ts), "t_median": T, "t_max": max(ts),
            "spread_pct_of_median": round(100 * (max(ts) - min(ts)) / T, 2),
            "speedup": round(T1 / T, 3) if T1 else "", "efficiency": round(T1 / T / P, 4) if T1 else "",
            "solve_median": solve if solve is not None else "",
            "startup_median": round(T - solve, 3) if solve is not None else "",
            "solve_speedup": round(solve1 / solve, 3) if (solve and solve1) else "",
            "solve_efficiency": round(solve1 / solve / P, 4) if (solve and solve1) else "",
            "worker_core_seconds_ratio": round(P * T / T1, 3) if T1 else "",
            "allocated_core_seconds_ratio": round(R * T / T1, 3) if T1 else "",
        })
    write(out, "scaling.csv", sc)

    man = {
        "generated_by": "scripts/derive.py",
        "result_dirs": sorted({x["result_dir"] for x in runs}),
        "runs": len(runs),
        "status_counts": {s: sum(1 for x in runs if x["status"] == s) for s in sorted({x["status"] for x in runs})},
        "dagster_args": sorted({"%s  [%s]" % (x["dagster_args"], x["args_source"]) for x in runs}),
        "binary_build_dirs": sorted({str(x["binary_build_dir"]) for x in runs}),
        "logged_runs_analysed": len(ph),
        "files": ["runs.csv", "inventory.csv", "scaling.csv", "phase_breakdown.csv", "latency.csv", "master_load.csv"],
    }
    with open(os.path.join(out, "MANIFEST.json"), "w") as fh:
        json.dump(man, fh, indent=1)
    print(json.dumps(man, indent=1))


if __name__ == "__main__":
    main()
