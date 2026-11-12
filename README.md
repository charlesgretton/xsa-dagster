# Distributed SAT solving with Dagster — investigation repository

How well does Dagster, a distributed SAT solver, used unmodified, distribute a structured
satisfiability problem on a high-performance computing (HPC) cluster, and what limits it? This
repository holds the complete software and data record of an investigation of that question: the
virtual proceedings, every tool that produced a result in them, the batch-job scripts for a PBS
cluster, and the derived evidence behind every reported number.

Start with `proceedings/xsa_dagster_proceedings.pdf` (38 pages). Everything here is referenced
from it.

> On a synthetic XOR session-attack benchmark (XSA-N), a fixed two-stage top-down decomposition
> scales from one to 207 MiniSat workers: 1,447.6 s to 64.3 s, a 22.5-fold speedup (10.9%
> efficiency; 33.1-fold for the solving phase), bought with about 9.2 times the core-seconds. Three
> effects centred on Dagster's master and its allocation policy limited earlier decompositions; all
> were diagnosed from Dagster's own logs and avoided without modifying Dagster. The benchmark admits
> a faster sequential shortcut, and no matched solver portfolio was run, so the results characterise
> Dagster's distribution machinery, not the best way to solve the benchmark.

## Version

See `VERSION`. `make manifest` regenerates `MANIFEST.sha256`, which every released archive should
carry.

## Quick start

```bash
make selftest      # 17 checks, about a minute; all must pass before trusting anything
make proceedings   # rebuild proceedings/xsa_dagster_proceedings.pdf (pdflatex, three passes)
```

`make selftest` runs no experiment. It checks that every Python file compiles; that the generator
reproduces the bundled N=8 and N=32 instances byte for byte; that every DAG family builds and passes
Dagster's DAG-grammar check; that every top-down DAG gives its collector variable-disjoint inputs
(the condition under which Dagster's `-g 2` is exact); that the acceptance predictor reproduces the
proceedings' figure; and that the evidence tables are complete and agree with the headline numbers.

## Results, and what produced them

### Strong scaling of one fixed DAG

| workers | 1 | 8 | 32 | 64 | 103 | 207 |
|---|---|---|---|---|---|---|
| time (s), N = 16384 | 1,447.6 | 209.1 | 82.7 | 70.3 | 70.7 | **64.3** |
| speedup | 1.0 | 6.9 | 17.5 | 20.6 | 20.5 | **22.5** |
| efficiency | 100% | 87% | 55% | 32% | 20% | **10.9%** |

Two-stage top-down DAG `topdown2:4:5` (512 independent subtrees) on an instance of about one million
variables and ten million clauses; medians of 2–3 repeats, spread at most 4.7%. Produced by
`hpc/scale_seq.pbs`, `hpc/scale.pbs` and `hpc/scale2.pbs`; tabulated in
`evidence/derived/scaling.csv`. N = 8192 reaches 14.2× at 32 workers and then flattens on start-up.

### What limited scaling, and how each limit was avoided

| effect | symptom in Dagster's logs | avoided by |
|---|---|---|
| literal-by-literal consistency check when combining partial results (`-g 0`) | master time between jobs grows with depth; runs at 129+ ranks time out | `-g 2`, exact here because the collector's inputs are variable-disjoint (checked on all 55 executed DAGs) |
| eager duplication of the only initial job | every idle worker runs a copy of the splitter | — (inherent; made cheap by the next row) |
| serial processing of every duplicate's result | ~0.9 s per copy of a 512-way split; master 87–94% busy | two-stage splitting: 0.22 ms per copy |

The remaining limits at 207 workers are start-up (about 0.15 s per rank), the serial collector
(23 s at N = 16384), and slower jobs on fully loaded nodes. Analyses: `xsa/showcase_report.py`
(phases, predictions, scaling) and `xsa/latency_report.py` (job compute against waiting).

### Algorithmic gain, which is not a scaling result

Top-down splitting changes the search, not only its distribution. At N = 2048 it takes 7.7 s on 32
workers against 174.3 s for the one-node formulation; at N = 4096 it is at least 313 times faster
than a one-node run that was stopped after an hour. Part of these gains is algorithmic, and a
sequential cube (fix almost every choice, solve a small residue) is faster still.

### Correctness

206 runs: 203 answers, every one independently verified against every clause of the CNF and against
the attack condition; 3 runs censored by their time limit; no launch failures and no wrong answers.
`evidence/derived/runs.csv` holds one record per run, including the censored ones.

## Where results come from

| in the proceedings | evidence | produced by |
|---|---|---|
| run inventory, correctness | `evidence/derived/inventory.csv`, `runs.csv` | `hpc/*.pbs` → `evidence/scripts/derive.py` |
| strong scaling, one-worker times, one-node runs | `evidence/derived/scaling.csv` | `xsa/run_families.py` via `hpc/*.pbs` |
| time breakdowns | `evidence/derived/phase_breakdown.csv` | `xsa/showcase_report.py` (parser) |
| lag, job compute, master load, duplicate cost | `evidence/derived/latency.csv`, `master_load.csv` | `xsa/latency_report.py` (parser) |
| DAG validity, `-g 2` precondition | `evidence/validation/` | `tools/check_dag.py`, `evidence/scripts/check_collector.py` |
| instances, binary, environment | `evidence/checksums.txt`, `provenance.txt` | `xsa/generator/xor_session_attack_gen.py` |

The raw per-run results (Dagster logs and solutions, about 540 MB compressed) are not distributed;
they are identified by SHA-256 in `evidence/checksums.txt`, and `derive.py` regenerates the derived
tables from them exactly. Rerunning the campaign with `hpc/` produces results of the same form on
another machine.

## Status

Established, for deterministic XSA-N instances on one cluster: the strong-scaling result above, the
three master-side effects and their remedies, and the correctness of every returned answer. Not
established: that structural distribution beats a matched portfolio of independent solvers (not
run); how much of the overhead is Dagster's rather than the decomposition's (no direct dispatcher
was run); transfer to real protocol encodings (XSA-N is a selection pattern with a protocol
narrative, not a protocol model). See the proceedings' threats to validity.

## Layout

```
proceedings/      the virtual proceedings: LaTeX source (self-contained, TikZ/pgfplots) and PDF
xsa/              benchmark generator, DAG families, run driver, phase and latency analyses
xsa/examples/     the N=8 and N=32 instances: CNF, metadata, planted model, manifest
xsa/development/  development-phase tools: early families, XSA-B, cube control, refinement
harness/          MPI launch, answer watch and model verification used by the driver
tools/            DAG checker, CNF reader, model checker, self-test
patches/          opt-in Dagster patches for the variant DAGs only (not used in the reported campaign)
hpc/              batch-job scripts for a PBS cluster, environment template, guide
evidence/         derived tables, validation outputs, checksums, provenance, regeneration scripts
```

## Software that is not bundled

| software | version used | where |
|---|---|---|
| Dagster | `AFSAT-Integration` branch, commit `1c8995131b009410226aa1207d2c622672368293`, unmodified | https://github.com/ANU-HPC/dagster (see *Related publications*) |
| MiniSat | as embedded in that Dagster commit (`-m 4`) | part of Dagster |
| glog, CUDD | as required by Dagster | https://github.com/google/glog, https://github.com/ivmai/cudd |
| Open MPI | 4.1.0 | https://www.open-mpi.org |
| Python | ≥ 3.7 (campaign: 3.11); standard library only | https://www.python.org |

Building Dagster (glog and CUDD installed under a prefix such as `$HOME/local`):

```bash
git clone --branch AFSAT-Integration https://github.com/ANU-HPC/dagster.git dagster-afsat
cd dagster-afsat && git checkout 1c8995131b009410226aa1207d2c622672368293 && cd dagster
export CPATH=$HOME/local/include LIBRARY_PATH=$HOME/local/lib
make -j 8 && sha256sum dagster            # record the binary's hash
```

## Reproducing the runs

On a PBS cluster with 104-core nodes, after setting the site section of `hpc/env.sh` and the
`#PBS -P` / `#PBS -q` lines of the job scripts (details in `hpc/README.md`):

```bash
bash hpc/prepare.sh            # checks Dagster, generates N=2048 and 4096, prints the depth-safety table
qsub hpc/smoke.pbs             # must end SMOKE OK
qsub hpc/seq.pbs               # one-worker runs: baselines and per-phase timings
qsub hpc/wide.pbs              # one worker per subtree
qsub hpc/node.pbs              # one node: start-up calibration, over-decomposition, transports
qsub hpc/scale_seq.pbs         # N=8192/16384: instances and one-worker reference
qsub hpc/scale.pbs             # strong scaling of topdown2:4:5, one node (9-104 ranks)
qsub hpc/scale2.pbs            # the same on two nodes (208 ranks)
qsub -v SCALE_FAMILY=topdown:9 hpc/scale.pbs        # single-stage comparison (likewise scale2, scale_seq)

python3 evidence/scripts/derive.py $XSA_DATA/results rederived     # the evidence tables
```

## Discipline

Five conventions are load-bearing. Each was adopted after a failure in this project, and each is
the reason a later error was caught.

**Verify every answer outside the solver.** Each returned model is completed from its transcript
choices and checked against every clause and against the attack condition, independently of
Dagster. An early single-shot experiment on nodes that must offer alternatives produced a false
UNSAT; only an external check could have caught it.

**Account for every run.** Every submitted run has a record, censored runs included, and arguments
are marked as recorded or inferred. The 26 earliest runs predate argument recording and say so.

**Predict before you spend.** A one-worker run and a critical-path model predict each parallel
time. Every large disagreement between prediction and measurement located a bottleneck: the
consistency check, then the duplicated splitter.

**Attribute by worker, not by order.** With duplicate jobs, the first solution after a dispatch is
not necessarily that job's. Attributing by arrival order once counted the collector's 20-second
solve as start-up.

**Compare with the flat solver given the same knowledge.** A pinned prefix or a filtration helps a
single solver as much as it helps a DAG. The split decomposition's apparent gain was exactly such a
cube.

## Status and what remains

In order of priority: a matched MiniSat portfolio on the same cores; a direct (non-Dagster)
implementation of the same decomposition; profiling the collector, whose cost grows about four-fold
per doubling of N; launch-time provenance (binary hash, compiler flags, library versions). Then
problems without shortcuts: scarce-solution instances, bounded protocol encodings, and weak scaling
beyond two nodes. Suggested Dagster improvements (dropping identical results before processing,
avoiding eager duplication, incremental consistency checks, hierarchical collectors) are listed in
the proceedings and are not applied here.

## Related publications

Dagster's design and case studies are described in the first two papers; the third, a
property-directed-reachability planner, is built on the Dagster skeleton. BibTeX entries are in
`proceedings/references.bib`.

* M. A. Burgess, C. Gretton, J. Milthorpe, L. Croak, T. Willingham and A. Tiu. Dagster: Parallel
  Structured Search with Case Studies. *PRICAI 2022*, LNCS 13629, pp. 75–89, Springer, 2022.
  https://doi.org/10.1007/978-3-031-20862-1_6
* M. A. Burgess, C. Gretton, J. Milthorpe, L. Croak, T. Willingham and A. Tiu. Dagster: Parallel
  Structured Search. *AAAI 2023, Student Papers and Demonstrations*, vol. 37, pp. 16404–16406,
  AAAI Press, 2023. https://doi.org/10.1609/aaai.v37i13.27060
* A. Clifton and C. Gretton. Property Directed Reachability for Planning Revisited. *KR 2023*,
  pp. 156–166, IJCAI Organization, 2023. https://doi.org/10.24963/kr.2023/16

## Licence

Source code is licensed under the Apache License, Version 2.0 (`LICENSE`). The proceedings,
documentation and project-generated data are licensed under Creative Commons Attribution 4.0
International (CC BY 4.0, `LICENSE-CC-BY-4.0.md`). `LICENSING.md` gives the mapping of files to
licences; material by others keeps its original terms.

## Errata

We welcome reports of errors, omissions and overlooked prior work. Please send them to the corresponding
author, Charles Gretton, at charles.gretton@anu.edu.au. Every report that leads to a correction will be
acknowledged in the errata and in any revised edition, cited as a private communication; where the point
was already made in a publication that appeared before the report's publication date, that publication
will be cited as well.
