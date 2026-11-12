# Running the campaign on a PBS cluster

The job scripts reproduce every cluster run in the proceedings. They were written for a PBS-managed
cluster with 104-core nodes, Open MPI 4.1 and a large parallel file system; any similar cluster
works after the site settings below. Submit every job from the repository root.

## 1. Site settings (once)

* `hpc/env.sh`, section **SITE**: module names (Open MPI, Python ≥ 3.7) and `DEPS_PREFIX`, the prefix
  holding glog and CUDD.
* `hpc/env.sh`: `DAGSTER_DIR`, the directory of an unmodified Dagster `AFSAT-Integration` build
  (top-level `README.md`), and `XSA_DATA`, where instances (~1 GB at N = 16384) and results go.
* Every `hpc/*.pbs`: replace `YOUR_PROJECT` and `YOUR_QUEUE`, and add any resource lines your site
  requires (for example file-system access) where marked. Core counts assume 104-core nodes; on
  other nodes adjust `ncpus` and the rank lists (`SCALE_RANKS`, `NODE_PLAN`, `WIDE_PLAN`).

## 2. Prepare (login node, a few minutes)

    bash hpc/prepare.sh

Checks Python, the Dagster binary and its `-e 3` mode, records provenance in `$XSA_DATA`, generates
N = 2048 and 4096, and prints the depth-safety table (the acceptance predictor; run only depths at
~1.0000). The instance hashes must match `evidence/checksums.txt`.

## 3. Jobs

| job | what it runs | resources (as in the campaign) |
|---|---|---|
| `smoke.pbs` | N = 32, every DAG shape; must end `SMOKE OK` | 8 cores, minutes |
| `seq.pbs` | one worker: one-node baseline, split, top-down depths 3–8 at N = 2048, 4096 | 4 cores, ≤ 4 h (`SEQ_ONE=0` skips the baselines) |
| `wide.pbs` | one worker per subtree, 9–257 ranks | 3 nodes, ≤ 40 min |
| `node.pbs` | one node: start-up calibration, more subtrees than workers, two transports | 1 node, ~20 min |
| `scale_seq.pbs` | generates N = 8192, 16384; one-worker reference of the scaling DAG | 8 cores, 64 GB, ~35 min |
| `scale.pbs` | strong scaling, 9–104 ranks, 3 repeats (first logged) | 1 node, 400 GB, ~1 h |
| `scale2.pbs` | strong scaling, 208 ranks | 2 nodes, ~20 min |

The scaling jobs default to the two-stage DAG `topdown2:4:5`; `qsub -v SCALE_FAMILY=topdown:9 ...`
runs the single-stage comparison. Each job writes `$XSA_DATA/results/<job>_<id>/` with one
`families.jsonl` line per run and each run's Dagster log, solution and DAG file. Every returned
answer is verified before it is recorded.

## 4. Analyse

    python3 xsa/showcase_report.py $XSA_DATA/results/scale_seq_* $XSA_DATA/results/scale_* --md scaling.md --plot scaling.png
    python3 xsa/latency_report.py  $XSA_DATA/results/scale_seq_* $XSA_DATA/results/scale_* --md latency.md
    python3 evidence/scripts/derive.py $XSA_DATA/results rederived

## Notes

* **Never use `-e 3` for an UNSAT search** on a multi-node DAG: it returns one solution per job,
  which is right for finding one attack and unsound for proving that none exists.
* `MASTER_ARGS="-g 2"` (no combination consistency check) is exact only when every multi-input
  node's inputs are variable-disjoint; `evidence/scripts/check_collector.py` checks this per DAG.
* Logging at verbosity 3 is limited to the first repeat of the scaling jobs, so later repeats time
  Dagster without logging cost.
