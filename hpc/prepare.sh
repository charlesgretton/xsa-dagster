#!/bin/bash
# hpc/prepare.sh -- run once on a login node (a few minutes) before submitting any job.
#   checks: python >= 3.7, your Dagster binary, -e 3 support, no cnfgraphs-hpc patches
#   makes : XSA instances for $NS (default "2048 4096") under $XSA_DATA/inst
#   prints: the depth-safety table (every subtree must be able to reach its sub-target)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd); export ROOT
source "$ROOT/hpc/env.sh"
python3 -c 'import sys; assert sys.version_info >= (3, 7), sys.version' \
  || { echo "need python >= 3.7 (module load python3/3.11.0)"; exit 1; }
[ -x "$DAGSTER_BIN" ] || { echo "no Dagster binary at $DAGSTER_BIN -- build it: cd $DAGSTER_DIR && make -j 8"; exit 1; }
grep -q "ENUMERATE_SOLUTIONS==3" "$DAGSTER_DIR/Worker.cpp" \
  || { echo "this Dagster source has no -e 3 mode (Worker.cpp); use the AFSAT-Integration branch"; exit 1; }
if grep -lq "DAGSTER_SINGLE_SHOT\|DAGSTER_FAIR_SCHEDULING" "$DAGSTER_DIR"/*.cpp 2>/dev/null; then
  echo "WARNING: $DAGSTER_DIR carries cnfgraphs-hpc patches; the showcase is meant for unpatched Dagster"
fi
if python3 - "$DAGSTER_DIR/Master.cpp" << 'PY'
import re, sys
src = open(sys.argv[1]).read()
sys.exit(0 if re.search(r"if \(i==organiser->num_workers\) break;\s*break;", src) else 1)
PY
then echo "termination: AFSAT-Integration behaviour (master stops at the first full solution)"
else
  echo "NOTE: this Dagster predates AFSAT-Integration commit 1c89951.  After the first full solution its"
  echo "      master waits for every busy worker to finish its job before writing the model, so timed"
  echo "      runs can include a shutdown lag.  For the showcase timings build the AFSAT-Integration head"
  echo "      alongside your checkout (hpc/README.md) and set DAGSTER_DIR to it."
fi
mkdir -p "$XSA_DATA/inst" "$XSA_DATA/results"
{ echo "binary: $DAGSTER_BIN"; ls -l "$DAGSTER_BIN";
  ( cd "$DAGSTER_DIR" && git log -1 --format='commit %H (%ad) %s' --date=iso && git status --short | head -20 ) 2>/dev/null || true
  mpirun --version 2>&1 | head -1; python3 --version; } > "$XSA_DATA/provenance.txt"
cat "$XSA_DATA/provenance.txt"
for N in ${NS:-2048 4096}; do
  I="$XSA_DATA/inst/n$N"
  if [ ! -f "$I/xsa_n$N.manifest.json" ]; then
    echo "generating XSA N=$N (N=4096 takes ~1-2 min) ..."
    python3 "$ROOT/xsa/generator/xor_session_attack_gen.py" generate "$N" --out "$I" > /dev/null
  fi
  echo "N=$N: $(grep '^p cnf' "$I/xsa_n$N.cnf")"
done
echo; echo "depth safety: P(every subtree reaches its sub-target) -- run only depths at ~1.0000"
python3 - "$XSA_DATA" ${NS:-2048 4096} << 'PY'
import os, sys
sys.path.insert(0, os.path.join(os.environ["ROOT"], "xsa"))
import xsa_dagster as xd
data, ns = sys.argv[1], sys.argv[2:]
for n in ns:
    x = xd.XSA(os.path.join(data, "inst", "n%s" % n, "xsa_n%s.meta.json" % n))
    cells = []
    for d in range(3, 11):
        if 2 ** d > x.N // 2:
            break
        cells.append("d=%d %.4f" % (d, x.predict_topdown(d)["p_every_child_reaches_its_subtarget"]))
    print("N=%s: %s" % (n, "  ".join(cells)))
PY
echo; echo "ready.  next: qsub hpc/smoke.pbs"
