"""Shared harness code: MPI flavour detection, launching Dagster or the portfolio
in its own session, watching for the first answer, verifying it, ending the job.

Lessons carried over from the sha1-dagster bundle, all enforced here:
  * With -e 0 a mode-4 worker cannot be interrupted mid-solve, so Dagster's exit
    can lag its first solution by minutes.  Never time SAT runs by exit.
  * On the AFSAT-Integration branch the master breaks out of its loop on the first
    terminal solution and writes -o immediately, so a complete line in the -o file
    is the earliest reliable signal; no verbose (V4) logging is needed.
  * End the whole session (TERM, then KILL), never a single pid of a wrapper.
  * '--oversubscribe' is an Open MPI option; MPICH/Hydra rejects it.
  * A launch that dies early is a LAUNCH FAILURE, not an answer.  UNSAT requires
    exit code 0 AND an (empty) -o file that Dagster itself created.
"""
import os
import shlex
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "tools"))
from cnf_io import read_cnf  # noqa: E402
import verify_model  # noqa: E402

_CNF_CACHE = {}


def original_clauses(path):
    if path not in _CNF_CACHE:
        _CNF_CACHE.clear()
        _CNF_CACHE[path] = read_cnf(path)
    return _CNF_CACHE[path]


def verify_lits(original_cnf, lits, trail_path=None):
    nv, cls = original_clauses(original_cnf)
    trail = []
    if trail_path and os.path.exists(trail_path):
        trail = [int(t) for t in open(trail_path).read().split()]
    return verify_model.check(cls, nv, lits, trail)


def mpi_flavour(mpirun="mpirun"):
    try:
        p = subprocess.run([mpirun, "--version"], capture_output=True, text=True, timeout=30)
        out = p.stdout + p.stderr
    except (OSError, subprocess.SubprocessError):
        return "missing"
    if "Open MPI" in out or "OpenRTE" in out:
        return "openmpi"
    if "HYDRA" in out or "MPICH" in out:
        return "mpich"
    if "Intel" in out:
        return "intel"
    return "unknown"


def mpi_command(ranks, flavour=None, oversubscribe=False):
    """Launcher prefix.  MPIRUN_CMD overrides everything, e.g.
    MPIRUN_CMD='mpirun -np {ranks} --map-by ppr:48:node --bind-to core'."""
    tmpl = os.environ.get("MPIRUN_CMD")
    if tmpl:
        return shlex.split(tmpl.format(ranks=ranks))
    mpirun = os.environ.get("MPIRUN", "mpirun")
    flavour = flavour or mpi_flavour(mpirun)
    cmd = [mpirun, "-n", str(ranks)]
    if flavour == "openmpi" and oversubscribe:
        cmd += ["--oversubscribe", "--bind-to", "none"]
    cmd += shlex.split(os.environ.get("MPIRUN_EXTRA", ""))
    return cmd


def mpi_env():
    env = dict(os.environ)
    env.setdefault("GLOG_logtostderr", "1")
    env.setdefault("GLOG_minloglevel", "1")   # warnings and above only; logging costs time
    env.setdefault("GLOG_v", "0")
    if os.geteuid() == 0:                       # sandbox/containers only
        env.setdefault("OMPI_ALLOW_RUN_AS_ROOT", "1")
        env.setdefault("OMPI_ALLOW_RUN_AS_ROOT_CONFIRM", "1")
    return env


def _group_alive(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def end_session(proc, grace=3.0):
    """TERM then KILL the launched session; returns True if it is gone."""
    pgid = proc.pid
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if proc.poll() is not None and not _group_alive(pgid):
            return True
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return True
        deadline = time.time() + grace
        while time.time() < deadline:
            proc.poll()
            if not _group_alive(pgid):
                return True
            time.sleep(0.05)
    proc.poll()
    return not _group_alive(pgid)


def _first_line(path):
    try:
        with open(path) as f:
            data = f.read()
    except OSError:
        return None
    if "\n" not in data:
        return None
    for line in data.split("\n")[:-1]:
        toks = [t for t in line.split() if t.lstrip("-").isdigit() and t != "0"]
        if toks:
            return [int(t) for t in toks]
    return None


def run_dagster(dagster, dag, cnf, workdir, ranks, timeout, original_cnf, trail=None,
                dagster_args=None, oversubscribe=False, poll=0.05, log_name="dagster.log",
                env_extra=None, complete_fn=None):
    """Run one Dagster job; return a result dict (status, times, verification)."""
    os.makedirs(workdir, exist_ok=True)
    out = os.path.join(workdir, "solution.txt")
    if os.path.exists(out):
        os.remove(out)
    args = dagster_args if dagster_args is not None else ["-m", "4", "-e", "0", "-b", "0"]
    cmd = mpi_command(ranks, oversubscribe=oversubscribe) + [dagster] + args + ["-o", out, dag, cnf]
    res = {"cmd": " ".join(shlex.quote(c) for c in cmd), "ranks": ranks}
    env = mpi_env()
    env.update(env_extra or {})
    if env_extra:
        res["env"] = dict(env_extra)
    with open(os.path.join(workdir, log_name), "w") as log:
        t0 = time.time()
        proc = subprocess.Popen(cmd, cwd=workdir, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True, env=env)
        status, t_ans, lits = None, None, None
        while status is None:
            rc = proc.poll()
            lits = _first_line(out)
            now = time.time() - t0
            if lits is not None:
                t_ans = now
                status = "SAT?"
            elif rc is not None:
                lits = _first_line(out)
                if lits is not None:
                    t_ans, status = now, "SAT?"
                elif rc == 0 and os.path.exists(out):
                    t_ans, status = now, "UNSAT"
                else:
                    status = "LAUNCH_FAILURE" if now < 5 else "ERROR"
                    res["exit_code"] = rc
            elif now > timeout:
                status = "TIMEOUT"
            else:
                time.sleep(poll)
        res["gone_after_kill"] = end_session(proc)
        res["t_end"] = time.time() - t0
    if status == "SAT?":
        why0 = ""
        if complete_fn is not None:     # case studies whose sink reports only part of a model
            lits, why0 = complete_fn(lits)
        if lits is None:
            status, res["verify"] = "SAT_INVALID", why0
        else:
            ok, why = verify_lits(original_cnf, lits, trail)
            status = "SAT" if ok else "SAT_INVALID"
            res["verify"] = (why0 + "; " if why0 else "") + why
    res["status"] = status
    res["t_answer"] = t_ans
    return res


def run_portfolio(minisat, cnf, workdir, ranks, timeout, original_cnf, trail=None,
                  oversubscribe=False, poll=0.05):
    """K seeded MiniSat runs of the full CNF under mpirun; first answer wins."""
    os.makedirs(workdir, exist_ok=True)
    resdir = os.path.join(workdir, "res")
    os.makedirs(resdir, exist_ok=True)
    for f in os.listdir(resdir):
        os.remove(os.path.join(resdir, f))
    script = os.path.join(HERE, "portfolio_rank.sh")
    cmd = mpi_command(ranks, oversubscribe=oversubscribe) + ["/bin/sh", script, minisat, cnf, resdir, "1"]
    res = {"cmd": " ".join(shlex.quote(c) for c in cmd), "ranks": ranks}
    with open(os.path.join(workdir, "portfolio.log"), "w") as log:
        t0 = time.time()
        proc = subprocess.Popen(cmd, cwd=workdir, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True, env=mpi_env())
        status, t_ans, winner, lits = None, None, None, None
        while status is None:
            done = sorted(f for f in os.listdir(resdir) if f.endswith(".res"))
            now = time.time() - t0
            for f in done:
                body = open(os.path.join(resdir, f)).read().split()
                if body[:1] == ["SAT"]:
                    status, t_ans, winner = "SAT?", now, f
                    lits = [int(t) for t in body[1:] if t != "0"]
                    break
                if body[:1] == ["UNSAT"]:
                    status, t_ans, winner = "UNSAT", now, f
                    break
            if status is None:
                if proc.poll() is not None:
                    status = "ERROR"
                elif now > timeout:
                    status = "TIMEOUT"
                else:
                    time.sleep(poll)
        res["gone_after_kill"] = end_session(proc)
        res["t_end"] = time.time() - t0
    if status == "SAT?":
        ok, why = verify_lits(original_cnf, lits, trail)
        status = "SAT" if ok else "SAT_INVALID"
        res["verify"] = why
    res.update(status=status, t_answer=t_ans, winner=winner)
    return res
