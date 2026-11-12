#!/usr/bin/env python3
"""latency_report.py RESULTS_DIR [RESULTS_DIR ...] [--md OUT.md]

Where does a job's time go when many workers share one Dagster master?  For every logged
run (GLOG_v=3: the first repeat of scale.pbs / scale2.pbs) this splits each job, from
Dagster's own log lines, into

  compute   master "sending message to worker r"  ->  worker r "finished ..., sending
            assignment request"   (message transit + CNF build + MiniSat solve)
  lag       worker r sends its request  ->  master logs "received ... from worker r"
            (the master is busy elsewhere: a backlog grows here)
  response  master received the request  ->  master dispatches r's next job
            (job allocation; zero-work gaps excluded)

and reports medians / 90th percentiles / maxima per run, plus worker utilisation over the
subtree phase (compute time / (workers x phase length)).

If compute stays near its one-worker value while lag and response grow with the number of
workers, the master is the bottleneck; if compute itself grows, the workers are (memory
bandwidth, duplicate jobs).  Runs over two nodes compare clocks of two hosts (NTP-synced,
typically within milliseconds).  Logs can be large (the collector's messages are long);
only the first 400 characters of each line are inspected.
"""
import argparse
import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from showcase_report import family_layout, log_of, master   # noqa: E402

TS = re.compile(r"^[IWEF](?:\d{8}|\d{4}) (\d+):(\d+):([\d.]+)")
DISP = re.compile(r"sending message to worker (\d+) message about node (\d+)")
REQ = re.compile(r"received MPI_TAG_REQUEST_FOR_ASSIGNMENT from worker (\d+)")
FIN = re.compile(r"WORKER (\d+): finished generating new solutions, sending assignment request")


def events(log):
    ev, prev, offset = [], None, 0.0
    with open(log, errors="replace") as fh:
        for line in fh:
            head = line[:400]
            m = TS.match(head)
            if not m:
                continue
            t = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
            if prev is not None and t + offset < prev - 3600:
                offset += 86400
            t += offset
            prev = t
            d = DISP.search(head)
            if d:
                ev.append((t, "D", int(d.group(1)), int(d.group(2))))
                continue
            r = REQ.search(head)
            if r:
                ev.append((t, "R", int(r.group(1)), None))
                continue
            f = FIN.search(head)
            if f:
                ev.append((t, "F", int(f.group(1)), None))
    ev.sort(key=lambda e: e[0])
    return ev


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


def analyse(log, lay):
    ev = events(log)
    if not any(e[1] == "F" for e in ev):
        return None                      # not a GLOG_v=3 log
    lo, hi = lay["kids"]
    per = {}
    for e in ev:
        per.setdefault(e[2], []).append(e)
    comp, lag, resp, child_comp = [], [], [], 0.0
    for w, es in per.items():
        i = 0
        while i < len(es):
            if es[i][1] != "D":
                i += 1
                continue
            t_d, node = es[i][0], es[i][3]
            j = next((k for k in range(i + 1, len(es)) if es[k][1] == "F"), None)
            if j is None:
                break
            comp.append(es[j][0] - t_d)
            if lo <= node <= hi:
                child_comp += es[j][0] - t_d
            k = next((k for k in range(j + 1, len(es)) if es[k][1] == "R"), None)
            if k is None:
                break
            lag.append(es[k][0] - es[j][0])
            n = next((n for n in range(k + 1, len(es)) if es[n][1] == "D"), None)
            if n is not None:
                resp.append(es[n][0] - es[k][0])
            i = k + 1
    workers = len(per)
    first_child = min((e[0] for e in ev if e[1] == "D" and lo <= (e[3] if e[3] is not None else -1) <= hi), default=None)
    k_disp = min((e[0] for e in ev if e[1] == "D" and e[3] == lay["K"]), default=None)
    phase = (k_disp - first_child) if (first_child is not None and k_disp is not None) else None
    util = (child_comp / (workers * phase)) if (phase and workers) else None
    return {"jobs": len(comp), "workers": workers, "comp": comp, "lag": lag, "resp": resp,
            "phase": phase, "util": util}


def f(v):
    return "—" if v is None else ("%.0f s" % v if v >= 100 else "%.1f s" % v if v >= 1 else "%.0f ms" % (1000 * v))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--md", default=None)
    a = ap.parse_args()
    rows = []
    for d in dict.fromkeys(os.path.realpath(x) for x in a.dirs):
        for root, _, files in os.walk(d):
            if "families.jsonl" in files:
                for l in open(os.path.join(root, "families.jsonl")):
                    if l.strip():
                        r = json.loads(l)
                        r["_root"] = root
                        rows.append(r)
    out = ["# Where job time goes: worker compute vs master latency", "",
           "| N | DAG | master | ranks | jobs | compute median / p90 / max | lag median / p90 / max | "
           "response median / p90 / max | subtree phase | worker utilisation |", "|" + "---|" * 10]
    seen = set()
    for r in sorted(rows, key=lambda r: (r["N"], r["family"], r["ranks"], r.get("rep", 0))):
        if not r["family"].startswith("topdown") or r["status"] != "SAT":
            continue
        key = (r["N"], r["family"], master(r), r["ranks"])
        if key in seen:
            continue
        log = log_of(r)
        if not log:
            continue
        res = analyse(log, family_layout(r["family"]))
        if not res:
            continue
        seen.add(key)
        trip = lambda xs: "%s / %s / %s" % (f(statistics.median(xs)) if xs else "—", f(pct(xs, 0.9)), f(max(xs) if xs else None))
        out.append("| %d | %s | %s | %d | %d | %s | %s | %s | %s | %s |" % (
            r["N"], r["family"], master(r), r["ranks"], res["jobs"], trip(res["comp"]), trip(res["lag"]),
            trip(res["resp"]), f(res["phase"]), "%.0f%%" % (100 * res["util"]) if res["util"] else "—"))
        print("analysed N=%d %s r=%d" % (r["N"], r["family"], r["ranks"]), file=sys.stderr)
    out += ["", "compute = dispatch -> worker finished; lag = worker's request -> master reads it; "
            "response = master reads request -> next dispatch to that worker. Utilisation = subtree "
            "compute / (workers x subtree phase)."]
    text = "\n".join(out) + "\n"
    print(text)
    if a.md:
        open(a.md, "w").write(text)


if __name__ == "__main__":
    main()
