# DAG families for XSA on Dagster: what the telemetry says

This compares families of DAG shapes for the same XSA instances, all executed by Dagster.
It compares them against each other and against a one-node Dagster run. Every SAT answer
was rebuilt from the reported transcript choices and checked against the full CNF and the
attack semantics.

**Telemetry.** It comes from Dagster's own master log at `GLOG_v=2`, parsed by
`xsa_dagster.parse_telemetry`:
- jobs dispatched per DAG node;
- each node's busy time, from a job's dispatch to that worker's next request.

**Platform.** Everything ran in a 1-CPU sandbox with one run per cell.
- **Job counts** are robust.
- **Wall times** with more than one worker are time-sliced, so they are indicative only.
- **"Ideal parallel"** means the critical path assuming a worker per node, built from
  per-node busy times measured on one worker.

## 1. The families

| family | shape | parameter |
|---|---|---|
| `one` | one node, every clause | — |
| `tree` | XSA's semantic tree: a node per episode and per XOR join, plus the goal | — |
| `block:B` | maximal subtrees of ≤ B episodes fused, joins above them | B |
| `split:K` | A = all but the rightmost ≤ K episodes, then B = those episodes + path joins + goal | K |
| `topdown:D` | **S** (top D levels + goal) picks sub-targets XOR-ing to the target; **C_i**, the 2^D subtrees, each solve an independent smaller XSA; **K** checks the pieces and carries the model | D |
| frontier | top-down over any partition into subtrees (deeper where a subtree is slow) | the frontier |
| Wagner tree (XSA-B) | tree over the strategy CNF: each join also carries Wagner's filtration units | block size |

Splitter variants:
- **single-shot (`topdown1`)**: S offers one split.
- **streaming (`topdown`)**: S offers alternative splits.
- **bounded (`bounded`)**: auxiliary unit clauses, held only by S, cap how many splits it
  can offer.
- **keyed (`keyed`)**: every edge also carries the whole split.

The subtrees C_i are always single-shot, which is safe: under one split, every subtree
result is acceptable to K.

## 2. Findings

### 2.1 A join whose inputs share no variables becomes a nested loop

XSA siblings share no variables and only the root carries a constraint. So every pair of
child messages is consistent, Dagster dispatches each pair as a job, and one join absorbs
the run:

| instance, family | jobs | on the worst node | result |
|---|---|---|---|
| N = 8, `block:4` | 11,519 | root join 7,234; goal 4,283 | SAT, 74.9 s |
| N = 32, `tree` | 9,206 | `join_L1_0_4`: 8,965 | timeout, 100 s |
| N = 32, `block:4` | 6,884 | `join_L2_0_8`: 6,882 | timeout, 100 s |
| N = 32, `one` | 2 | — | SAT, 0.61 s |

### 2.2 Nodes that never stop enumerating starve everything else

At N = 32, `block:8` ran **2 jobs in 100 s**. Each block node enumerates an unlimited
stream of distinct transcript selections, which it must carry for the model. Two such
nodes held both workers, and Dagster only reassigns a worker to correct load imbalance,
so the other two blocks never ran.

### 2.3 Asymmetric split: two nodes, few jobs

`split:4` and `split:8` need 4 jobs at N = 32. At large N, `split:8` on unmodified
Dagster took 6.9 s at N = 2048 and 32 s at N = 4096. One-node Dagster took 185 s and
over 300 s.

### 2.4 Top-down splitting

The table is for the single-shot splitter on one worker, so jobs ran one after another.

| N | D | subtrees | total, 1 worker | subtree work | slowest subtree | ideal parallel |
|---|---|---|---|---|---|---|
| 2048 | 3 | 8 | 28.6 s | 26.3 s | 6.3 s | ~6.3 s |
| 2048 | 5 | 32 | 35.1 s | 30.2 s | 3.0 s | ~3.0 s |
| 2048 | 7 | 128 | 60.2 s | 39.8 s | 1.0 s | ~1.1 s |
| 4096 | 3 | 8 | 138.8 s | 133 s | 33.6 s | ~34 s |
| 4096 | 5 | 32 | 123.0 s | 105 s | 9.2 s | ~9.2 s |

What the table shows:
- **Less total work than one node,** even run sequentially: 28.6 s against 185 s at N = 2048.
- **A shorter critical path with depth,** at a cost of roughly 0.3 s of overhead per job here.
- **Stragglers set the span:** the slowest subtree is 2.5–3× the median.
- **The ideal times exclude the collector's final job,** which these logs do not time.

**Acceptance floor.** Each subtree must be able to reach an arbitrary W-bit sub-target. It
does so with probability about 1 − exp(−Q^size / 2^W) (`predict_topdown`). At N = 32,
depth 3 gives 4-episode subtrees and a predicted 0.85% chance that all subtrees succeed. It
timed out, as predicted.

### 2.5 A streaming splitter starves sibling subtrees

When S may offer alternative splits, Dagster's table-mode master serves each depth by
scanning nodes from id 0 and giving the job to the first node with a new input
combination. The first subtree always has one, because S keeps streaming splits:
- at N = 32, depth 1, the first subtree got 4,295 jobs and its sibling **0**;
- the collector never ran;
- the keyed variant failed identically, because a join key cannot help a join that never
  forms.

The fixes:

| N = 32 | 2 subtrees | 4 subtrees |
|---|---|---|
| streaming S | timeout, 4,295 jobs on one subtree | SAT 3.5 s with skewed jobs (1/17/22/42); keyed: timeout, 3,390 on one |
| **bounded S** (1 free bit per sub-target, stock scheduler) | **SAT 0.56 s, 10 jobs** | **SAT 0.61 s, 13 jobs**; keyed 0.71 s, 29 jobs |
| **fair scheduling** (`patches/apply_fair_scheduling.py`), streaming S | **SAT 0.66 s, 6 jobs**; keyed 0.55 s, 6 jobs | keyed **SAT 0.61 s, 8 jobs** |

The two fixes behave differently, and each fails in its own way:
- **Fair scheduling removes the starvation, which exposes the collector's join.** At depth
  3, fair but unkeyed, subtrees ran under many splits (most splits fail below the
  acceptance floor). The collector then got **3,644 of 3,696 jobs**, pairing results from
  different splits, so a streaming splitter needs fair scheduling *and* a join key.
- **A bounded splitter works only while the bound is tight.** At N = 2048 the bound "1 free
  bit per sub-target" across 8 subtrees still allows 128 splits. On the stock scheduler the
  first subtree ran **128 jobs**, one per split, before its siblings got 3, and the run
  timed out. Bound the *total* number of alternatives, e.g. free bits on one sub-target
  only, or combine the bound with fair scheduling.
- **At scale, fair + keyed + streaming** solved N = 2048 at depth 3: SAT in 67.9 s with
  12 jobs on two time-sliced workers.

### 2.6 Telemetry-guided refinement: split the slowest subtree

`refine_topdown.py` starts at N = 2048 from depth 3. Each round it replaces the two slowest
subtrees by their halves, never going below the acceptance floor.

| round | subtrees | slowest | median | subtree work |
|---|---|---|---|---|
| 0 | 8 | 6.45 s | 2.46 s | 29.0 s |
| 1 | 10 | 5.94 s | 2.01 s | 22.1 s |
| 2 | 12 | 3.28 s | 1.74 s | 20.5 s |
| 3 | 14 | **2.97 s** | 1.70 s | 21.0 s |
| 4 | 16 | 4.09 s | 1.58 s | 23.4 s |

Three rounds halve the critical path with 14 subtrees, where a uniform split needs 32 for
the same 2.98 s. Round 4 regressed: each rerun picks new sub-targets and re-draws
heavy-tailed costs. A real elicitor needs repeated probes per decision, or seeded replicas
of the slowest subtrees.

### 2.7 Coarsening a Wagner tree (XSA-B, filtration units at every join)

| N | `tree` | `block:2` | `block:4` | `block:8` | `one` |
|---|---|---|---|---|---|
| 16 | 4,081 jobs, 19.7 s | 1,846, 6.4 s | 835, 1.9 s | 25, 0.66 s | 2, 0.71 s |
| 32 | 8,083 jobs, 100 s | 3,776, 34.9 s | 1,396, 9.1 s | 1,004, 14.3 s | 2, 1.2 s |

With filtration the joins do filter, and fusing lower levels into blocks cuts jobs by
orders of magnitude. The best block size is intermediate: small blocks leave many
pair-per-job joins, while large blocks enumerate their survivors in longer, fewer jobs.
Telemetry names the dominant join in every run, which is exactly where a merge or split
step should act.

## 3. Design rules for Dagster DAGs (so far)

1. **Every join must filter.** Either its inputs share variables, or the join node rejects
   most combinations. Otherwise Dagster runs a nested loop of jobs (§2.1).
2. **Never let enumerating nodes outnumber workers.** Dagster only reassigns a worker to
   correct load imbalance (§2.2).
3. **A node streaming alternatives into same-depth siblings needs two things.** Either a
   single-shot splitter (when downstream acceptance is ≈ 1) or a tight bound on its
   alternatives, or else fair scheduling. *And* a join key carried to the collector, so
   results from different alternatives never pair (§2.5).
4. **Size subtrees above the acceptance floor** (`predict_topdown`; §2.4).
5. **Coarsen until a job's work outweighs its overhead** (§2.4, §2.7).
6. **Split stragglers,** with repeated probes: one run per decision is noise (§2.6).
7. **Single-shot only where every output is acceptable downstream:** top-down subtrees,
   and the distribute nodes of `tdag`'s `jt` DAG. Never on generators (CASESTUDY.md: a
   false UNSAT).

## 4. What only HPC can show

- **Parallel times against the ideal:** top-down at depths 3–7 with P ≥ number of subtrees.
- **The refinement loop at scale,** with repeated probes per round.
- **Straggler replicas.** Dagster's replicas are identical deterministic runs, so this needs
  per-replica solver seeds, another small opt-in patch.

## 5. Running it

```sh
python3 casestudies/xsa/run_families.py --meta M --cnf C --tag T --families one,split:8,topdown1:5 \
   --dagster build/dagster-upstream/dagster/dagster --dagster-ss build/dagster-ss/dagster/dagster \
   --ranks 33 --timeout 600 --telemetry --out runs/
python3 casestudies/xsa/run_families.py ... --families bounded:3 --bound-bits 1      # stock scheduler
python3 casestudies/xsa/run_families.py ... --families topdown:3 --fair               # fair-scheduling patch
python3 casestudies/xsa/refine_topdown.py --meta M --cnf C --dagster-ss build/dagster-ss/dagster/dagster \
   --start-depth 3 --rounds 4 --out refine/
```
Raw data: `results/families.jsonl`, `results/refine.jsonl`.
