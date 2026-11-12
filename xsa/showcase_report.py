#!/usr/bin/env python3
"""showcase_report.py RESULTS_DIR [RESULTS_DIR ...] [--md OUT.md] [--assume-timeout S]

Summarise the unpatched XSA showcase (run_families.py --e3 --telemetry): every
families.jsonl under the given directories, with each run's Dagster log.

Per instance (N), grouped by DAG family, master mode and rank count (-g 0 checks every
join combination literal by literal; -g 2 skips that check):
  * all runs: status, median time to a verified attack, jobs;
  * top-down runs: the phase timeline from the master log (GLOG_v=3) --
      pre      first log line -> splitter dispatched (master set-up, CNF and DAG loading)
      outside  time not covered by the log: MPI launch before the first line, plus the time
               from the final solution to the answer file (a shutdown lag on Dagster
               older than AFSAT-Integration 1c89951)
      S        splitter job
      kids     first to last subtree job finishing (the subtree phase)
      join     last subtree done -> collector dispatched
      K        collector job
    and subtree busy times (sum, slowest).  On a one-worker run, kids >> Σ busy is master
    time spent between jobs.
  * PREDICTION for one worker per subtree, from the same DAG's one-worker (2-rank) run:
        pre + outside + S + slowest subtree + join + K + 0.3 s
  * speedups against one-node Dagster and against the same DAG on one worker.
--assume-timeout gives the timeout of runs recorded before run_families stored it.
"""
import argparse
import json
import os
import re
import statistics
import sys

TS = re.compile(r"^[IWEF](?:\d{8}|\d{4}) (\d+):(\d+):([\d.]+)")
DISP = re.compile(r"sending message to worker (\d+) message about node (\d+)")
SOLW = re.compile(r"received MPI_TAG_SOLUTION from worker (\d+)")


def family_layout(fam):
    """Node ids of a top-down family's DAG: splitter 0, then the mid-splitters (topdown2
    only), the subtrees, and the collector last."""
    kind, _, par = fam.partition(":")
    if kind == "topdown2":
        d1, d2 = (int(v) for v in par.split(":"))
        B, n = 2 ** d1, 2 ** (d1 + d2)
        return {"mids": (1, B), "kids": (1 + B, B + n), "K": 1 + B + n, "depth": d1 + d2, "children": n}
    n = 2 ** int(par.split(":")[0])
    return {"mids": None, "kids": (1, n), "K": n + 1, "depth": int(par.split(":")[0]), "children": n}


def phases(log, layout):
    first, prev, offset, ev = None, None, 0.0, []
    try:
        fh = open(log, errors="replace")
    except OSError:
        return None
    for line in fh:
        m = TS.match(line)
        if not m:
            continue
        t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
        if prev is not None and t + offset < prev - 3600:      # midnight wrap
            offset += 86400
        t += offset
        prev = t
        if first is None:
            first = t
        head = line[:400]
        d = DISP.search(head)
        if d:
            ev.append(("D", t, int(d.group(2)), int(d.group(1))))
        elif "received MPI_TAG_REQUEST_FOR_ASSIGNMENT" in head:
            ev.append(("R", t, None, None))
        else:
            sw = SOLW.search(head)
            if sw:
                ev.append(("S", t, None, int(sw.group(1))))
    K = layout["K"]
    try:
        dS = next(e[1] for e in ev if e[0] == "D" and e[2] == 0)
        s_done = next(e[1] for e in ev if e[0] == "R" and e[1] > dS)
        dK = next(e[1] for e in ev if e[0] == "D" and e[2] == K)
        last_kid = max(e[1] for e in ev if e[0] == "R" and e[1] <= dK)
        # the collector's solution is the first one sent by a worker that was given the
        # collector's job (duplicate subtree jobs may report in between)
        k_workers = {}
        for e in ev:
            if e[0] == "D" and e[2] == K:
                k_workers.setdefault(e[3], e[1])
        k_sol = next(e[1] for e in ev if e[0] == "S" and e[3] in k_workers and e[1] >= k_workers[e[3]])
    except (StopIteration, ValueError):
        return None
    return {"pre": dS - first, "S": s_done - dS, "kids": last_kid - s_done, "join": dK - last_kid,
            "K": k_sol - dK, "span_logged": k_sol - first}


def log_of(r):
    fam = r["family"].replace(":", "")
    for c in (r.get("run_dir"),
              os.path.join(r.get("_root", ""), "runs", r.get("tag", ""), "%s_r%d_rep%d" % (fam, r["ranks"], r.get("rep", 0))),
              os.path.join(r.get("_root", ""), "runs", r.get("tag", ""), "%s_r%d" % (fam, r["ranks"]))):
        if c and os.path.exists(os.path.join(c, "dagster.log")):
            return os.path.join(c, "dagster.log")
    return ""


def master(r):
    m = re.search(r"-g\s*(\d+)", r.get("dagster_args", "") or "")
    lab = "-g %s" % (m.group(1) if m else "0")
    return lab + (" " + r["variant"] if r.get("variant") else "")


def med(xs):
    xs = [v for v in xs if v is not None]
    return statistics.median(xs) if xs else None


def f(v, unit="s"):
    return "—" if v is None else ("%.1f%s" % (v, unit) if v >= 10 else "%.2f%s" % (v, unit))


def kid_busy(r):
    return [v["busy_s"] for k, v in (r.get("telemetry") or {}).items() if k.startswith("C:")]


def scaling(rows, out_lines, plot_path=None):
    """Strong scaling of each top-down DAG: one table per (N, family, master mode) that has a
    one-worker (2-rank) run and at least two wider runs.  'solve' is S + subtrees + join + K
    from the master log of telemetry repeats (splitter dispatch -> collector answer); 'total'
    adds start-up.  Speedups are against the same DAG on one worker."""
    groups = {}
    for r in rows:
        if r["family"].startswith("topdown") and r["status"] == "SAT":
            groups.setdefault((r["N"], r["family"], master(r)), []).append(r)
    curves = []
    for (N, fam, mm), g in sorted(groups.items(), key=lambda kv: (kv[0][0], family_layout(kv[0][1])["depth"], kv[0][1], kv[0][2])):
        ranks = sorted({r["ranks"] for r in g})
        if 2 not in ranks or len(ranks) < 3:
            continue
        nk = family_layout(fam)
        pts = []
        for R in ranks:
            rr = [r for r in g if r["ranks"] == R]
            ph = [p for p in (phases(log_of(r), nk) for r in rr) if p]
            solve = med([p["span_logged"] - p["pre"] for p in ph])
            pts.append((R, med([r["t"] for r in rr]), solve))
        t1, s1 = pts[0][1], pts[0][2]
        out_lines += ["### Strong scaling: N = %d, %s, %s" % (N, fam, mm), "",
                      "| ranks | workers | total | solve | start-up | speedup (total) | speedup (solve) | "
                      "efficiency (solve) |", "|---|---|---|---|---|---|---|---|"]
        for R, t, sv in pts:
            w = R - 1
            out_lines.append("| %d | %d | %s | %s | %s | %s | %s | %s |" % (
                R, w, f(t), f(sv), f((t - sv) if (t is not None and sv is not None) else None),
                "%.1fx" % (t1 / t) if (t1 and t) else "—", "%.1fx" % (s1 / sv) if (s1 and sv) else "—",
                "%.0f%%" % (100.0 * s1 / sv / w) if (s1 and sv) else "—"))
        out_lines.append("")
        curves.append(("N=%d %s" % (N, fam), pts))
    if plot_path and curves:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            out_lines.append("(matplotlib not available: no figure)")
            return
        fig, ax = plt.subplots(figsize=(6.5, 4.5))
        for k, (lab, pts) in enumerate(curves):
            w = [R - 1 for R, _, _ in pts]
            col = "C%d" % k
            ax.plot(w, [t for _, t, _ in pts], "o:", color=col, label=lab + " total")
            if all(s is not None for _, _, s in pts):
                ax.plot(w, [s for _, _, s in pts], "s-", color=col, label=lab + " solve")
                ax.plot(w, [pts[0][2] / x for x in w], "--", color=col, alpha=0.35, linewidth=1)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("Dagster workers (ranks - 1)")
        ax.set_ylabel("seconds to a verified attack")
        ax.set_title("XSA top-down DAG, unpatched Dagster (-m 4 -e 3 -g 2); dashed = ideal")
        ax.legend(fontsize=7)
        fig.tight_layout()
        fig.savefig(plot_path, dpi=130)
        out_lines.append("Figure: %s" % plot_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--md", default=None)
    ap.add_argument("--plot", default=None, help="write a strong-scaling figure (PNG) if matplotlib exists")
    ap.add_argument("--assume-timeout", type=float, default=None,
                    help="timeout (s) for runs recorded without one (e.g. 3600 for the first seq.pbs)")
    a = ap.parse_args()
    rows = []
    for d in dict.fromkeys(os.path.realpath(x) for x in a.dirs):   # a directory given twice counts once
        for root, _, files in os.walk(d):
            if "families.jsonl" in files:
                for l in open(os.path.join(root, "families.jsonl")):
                    if l.strip():
                        r = json.loads(l)
                        r["_root"] = root
                        if not r.get("timeout") and a.assume_timeout:
                            r["timeout"] = a.assume_timeout
                        rows.append(r)
    if not rows:
        sys.exit("no families.jsonl found")
    out = ["# XSA showcase on unpatched Dagster (-m 4 -e 3 -b 0)", ""]
    bad = [r for r in rows if r["status"] not in ("SAT", "TIMEOUT")]
    out += ["**Problems:** " + "; ".join("N=%d %s r=%d: %s" % (r["N"], r["family"], r["ranks"], r["status"])
                                          for r in bad) if bad else
            "Every finished run returned a model that was verified against the CNF and the attack semantics.", ""]
    for N in sorted({r["N"] for r in rows}):
        rs = [r for r in rows if r["N"] == N]
        # the one-node baseline is the 2-rank run (wider one-node runs are start-up calibration)
        one = med([r["t"] for r in rs if r["family"] == "one" and r["ranks"] == 2 and r["status"] == "SAT"])
        one_to = [r.get("timeout") for r in rs if r["family"] == "one" and r["ranks"] == 2 and r["status"] == "TIMEOUT"]
        bound = max([v for v in one_to if v] or [0]) or None
        one_txt = f(one) if one else ("> %gs" % bound if bound else
                                      ("timed out (timeout unknown: use --assume-timeout)" if one_to else "not run"))

        def vs_one(t):
            if t is None:
                return "—"
            if one:
                return "%.1fx" % (one / t)
            return (">%.0fx" % (bound / t)) if bound else "—"
        out += ["## N = %d (one-node Dagster: %s)" % (N, one_txt), "",
                "| family | master | ranks | runs | status | median time | jobs |", "|---|---|---|---|---|---|---|"]
        keys = sorted({(r["family"], master(r), r["ranks"]) for r in rs})
        for fam, mm, rk in keys:
            g = [r for r in rs if (r["family"], master(r), r["ranks"]) == (fam, mm, rk)]
            st = "/".join(sorted({r["status"] for r in g}))
            out.append("| %s | %s | %d | %d | %s | %s | %s |" % (fam, mm, rk, len(g), st, f(med([r["t"] for r in g])),
                                                               med([r["jobs"] for r in g])))
        out += ["", "| family | master | ranks | pre | outside | S | subtrees | join | K | Σ subtree busy | "
                "slowest subtree | **predicted** (1 worker/subtree) | **measured** | vs one-node | vs same DAG, 1 worker |",
                "|" + "---|" * 15]
        for fam, mm, rk in sorted({k for k in keys if k[0].startswith("topdown")},
                                  key=lambda k: (family_layout(k[0])["depth"], k[0], k[1], k[2])):
            nk = family_layout(fam)
            g = [r for r in rs if (r["family"], master(r), r["ranks"]) == (fam, mm, rk) and r["status"] == "SAT"]
            if not g:
                out.append("| %s | %s | %d | | | | | | | | | | timed out | | |" % (fam, mm, rk))
                continue
            ph = [p for p in (phases(log_of(r), nk) for r in g) if p]
            t = med([r["t"] for r in g])
            P = {k: med([p[k] for p in ph]) for k in ("pre", "S", "kids", "join", "K", "span_logged")} if ph else {}
            outside = (t - P["span_logged"]) if P else None
            ksum = med([sum(k) for k in map(kid_busy, g) if k])
            kmax = med([max(k) for k in map(kid_busy, g) if k])
            base = [r for r in rs if r["family"] == fam and r["ranks"] == 2 and r["status"] == "SAT"]
            base = ([r for r in base if master(r) == mm] or
                    [r for r in base if master(r).split()[:2] == mm.split()[:2]] or base)  # same -g where possible
            pred, seq = None, med([r["t"] for r in base])
            if base:
                ph2 = [p for p in (phases(log_of(r), nk) for r in base) if p]
                k2 = [max(k) for k in map(kid_busy, base) if k]
                if ph2 and k2:
                    Q = {k: med([p[k] for p in ph2]) for k in ("pre", "S", "join", "K", "span_logged")}
                    pred = Q["pre"] + max(0.0, seq - Q["span_logged"]) + Q["S"] + med(k2) + Q["join"] + Q["K"] + 0.3
            out.append("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                fam, mm, rk, f(P.get("pre")), f(outside), f(P.get("S")), f(P.get("kids")), f(P.get("join")),
                f(P.get("K")), f(ksum), f(kmax), f(pred), f(t), vs_one(t),
                ("%.1fx" % (seq / t)) if (seq and rk > 2) else "—"))
        out.append("")
    sc = []
    scaling(rows, sc, a.plot)
    if sc:
        out += ["## Strong scaling", ""] + sc + [""]
    out += ["Times are seconds to a verified attack, medians over repeats. Predictions come from the same "
            "DAG's one-worker run (same master mode where available): pre + outside + S + slowest subtree + "
            "join + K + 0.3 s. On a one-worker run, a 'subtrees' phase much longer than 'Σ subtree busy' is "
            "master time spent between jobs (join searches under -g 0)."]
    text = "\n".join(out) + "\n"
    print(text)
    if a.md:
        open(a.md, "w").write(text)


if __name__ == "__main__":
    main()
