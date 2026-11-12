# XSA as a Dagster case study: findings and the bar it sets

**Verdict.** XSA-N is a well-engineered benchmark. It is deterministic (regeneration
is byte-identical), its DIMACS is shuffled, its metadata is rich, and its validator checks
both the CNF and the attack semantics. As a Dagster case study, though, it shows the
opposite of what was hoped:
- The semantic tree is the wrong DAG.
- The right DAG, an asymmetric split, wins only because it amounts to a cube. A flat
  solver given that same cube is faster still.

The birthday-regime variant prototyped here, XSA-B, repeats the pattern one level up.
The filtration that makes Wagner's tree efficient also makes a flat solver efficient once
it is added as clauses.

The general lesson is the bar any Dagster case study must clear. Beating the flat solver
is not enough. It must beat **the flat solver given the same knowledge** that the DAG
encodes, as cubes, assumptions or extra clauses.

All numbers below come from a 1-CPU sandbox: single runs, multi-rank runs time-sliced. They
show behaviour and orders of magnitude, not parallel speedup. Every SAT answer was rebuilt
from the reported transcript choices, then checked against the full CNF and the attack
semantics.

## 1. XSA-N hardness for a sequential solver (MiniSat 2.2, Dagster's core)

| N | Q | W | time (default) | other seeds |
|---|---|---|---|---|
| 8–256 | 7–12 | 11–16 | ≤ 0.23 s | |
| 512 | 13 | 17 | 1.3 s | |
| 1024 | 14 | 18 | 3.9 s | 7.5 / 21.1 / 19.6 s |
| 2048 | 15 | 19 | 156 s | 103 / 35 s |
| 4096 | 16 | 20 | > 400 s | |

The recommended ladder (N = 8–512) takes about as long as Dagster's own start-up. Real
difficulty begins around N = 1024 and varies widely between seeds.

## 2. Why the semantic tree fails under Dagster

- **Nothing to filter at the joins.** Sibling subtrees share no variables, so every pair
  of child summaries is mutually consistent. The only constraint in the formula is at the
  root.
- **Dagster joins one pair per job.** It forms each input combination as a separate job,
  so the root finds the target only after about 2^W tries, each needing work upstream.
- **Jobs counted at N = 8**, where MiniSat takes 8 ms (3 ranks, from master logs at
  `GLOG_v=2`):

| DAG | jobs | time |
|---|---|---|
| one node | 2 | 0.55 s |
| split, k = 4 | 8 | 0.56 s |
| semantic tree (oracle) | 7,432 | 19.6 s (another run: timeout at 120 s) |
| blocks of 4 episodes | 11,519 | 74.9 s (another run: 65.7 s) |

- **Model extraction.** A tree DAG returns a model only if edges carry the subtree's
  transcript choices. The root alone knows only the fingerprint. Those choices make every
  partial selection a distinct message.

## 3. The right DAG for XSA-N, and what its win really is

**The split.** Solutions are astronomically dense: Q^N / 2^W. So the best DAG is a split:
- **A:** every episode but the rightmost subtree of k episodes; any selection works;
- **B:** those k episodes, the joins on their path to the root, and the goal.

**Acceptance.** B reaches a random residual target with probability about
1 − exp(−Q^k / 2^W); `xsa_dagster.py predict` computes it. At N = 32 that is:

| k | B accepts a random prefix |
|---|---|
| 2 | 1% |
| 4 | 55% |
| 8 | ~100% |

A probing elicitor should recover this threshold.

**Results at large N** (split on unmodified Dagster, 3 ranks; one node, 2 ranks):

| N | Dagster, one node | Dagster, split k = 8 | flat MiniSat | **flat MiniSat + same prefix pinned** |
|---|---|---|---|---|
| 1024 | 5.1 s | 3.8 s | 3.9–21 s | — |
| 2048 | 185 s | 6.9 s | 35–156 s | **4.1 s** |
| 4096 | > 300 s | 32 s | > 400 s | **3.5 s** |

**The cube control.** The last column pins every episode outside B to transcript 0, an
arbitrary prefix and not the planted one, using unit clauses (`cube_control.py`). The flat
solver is then faster than the DAG. The split's 27× gain over one-node Dagster is the cube,
not distributed execution.

**Single-shot is unsafe on generator nodes.** With the opt-in single-shot patch on A,
N = 8 reported **UNSAT on a satisfiable instance**. A offered one prefix, B rejected it
(about a 30% chance at k = 4), and A had no alternative left. Single-shot is only sound
where downstream acceptance is guaranteed, as on the distribute nodes of `tdag`'s
junction-tree DAG. Generator nodes need alternatives on demand.

## 4. Prototype: XSA-B, the generalized-birthday regime (`xsa_birthday_gen.py`)

**Parameters.** N = 2^m episodes with Q = 2^ℓ choices each and W = ℓ(m+1) bits:
Wagner's balance for the k-list XOR problem (CRYPTO 2002). ℓ is fixed at 4, so N stays
the only knob.

**Two CNFs with identical numbering.** The planted selection satisfies both.
- **plain:** the problem itself;
- **strategy:** plain plus Wagner's filtration, as unit clauses on each join's next ℓ
  summary bits.

The oracle tree run over the strategy CNF is Wagner's algorithm executed by Dagster.

| m (N) | flat, plain | flat, + strategy | Dagster one node, plain / strategy | Dagster Wagner tree |
|---|---|---|---|---|
| 4 (16) | 0.45 s | 0.07 s | 0.91 / 0.55 s | 19.0 s, 4,083 jobs |
| 5 (32) | 4.0 s | 0.16 s | 4.9 / 0.71 s | 98.8 s, 8,083 jobs |
| 6 (64) | > 90 s | 0.71 s | 67 / 1.1 s | — |
| 7 (128) | > 90 s | 1.7 s | — | — |

Without filtration, the same tree on the plain CNF timed out at m = 4: 90 s and 7,006
jobs.

What the table shows:
- The plain problem grows exponentially for flat CDCL (about 10× per level).
- Filtration removes that growth **for the flat solver too**.
- Dagster's job counts match one pair per job: (N−1)·Q² plus the leaves. Wagner's merge
  is a hash join costing about Q per list; Dagster's costs Q² jobs. Executed this way, the
  tree is about 100× slower than a flat solver given the same clauses.

## 5. What a passing case study needs

Knowledge that compiles into clauses or assumptions does not need distribution. A Dagster
case study therefore needs at least one property a single solver process cannot borrow:

1. **Genuinely parallel heavy work.** Many independent subproblems, each expensive, whose
   total exceeds one core's budget. One candidate: XSA-B scaled far up (larger m or ℓ),
   with the lower levels fused into blocks solved by CDCL plus strategy in parallel, and
   only the top levels as Dagster joins. It must be compared against flat + strategy
   sequential, and a flat + strategy portfolio.
2. **Memory beyond one node.** This needs per-node CNF loading (`-h`) and no node that
   holds every clause.
3. **Workflows a single solver cannot host.** For example, the repository's multi-fidelity
   protocol chain, where different encodings are linked by abstraction-invariant variables.

And Dagster needs a cheaper join for algebraic interfaces. A semi-join or hash join on
interface values would turn Q² jobs per merge into Q.

## 6. Recommendations for the XSA specification

- **Ladder:** run timings at N ≥ 1024, now possible up to 4096 with the linear generator.
  Keep 8–16 for correctness only.
- **DAG families and controls:**
  - add asymmetric splits;
  - treat the semantic tree as a negative reference;
  - always pair a decomposition result with the "flat + same cube/strategy" control.
- **Models:** document that tree DAGs must carry transcript choices on edges. Accept
  pruned partial models via semantic completion (`xsa_dagster.py complete`), since that is
  what Dagster reports.
- **Generator:** adopt the linear interface computation (`generator/`). Its output is
  byte-identical at N = 8, 32 and 128, and N = 4096 generates in 69 s instead of failing
  after more than 6 minutes and 1.2 GB.

## 7. Files and commands

```
casestudies/xsa/
  xsa_dagster.py        DAG shapes (one, oracle, block-B, split-K), semantic completion, predictor
  run_xsa.py            XSA-N driver: shapes x N x ranks, verified models, optional job counts
  xsa_birthday_gen.py   XSA-B prototype generator (plain + strategy CNFs, XSA metadata schema)
  run_xsab.py           XSA-B driver
  cube_control.py       the split as a flat cube (the decisive control)
  generator/            the XSA-N generator with the linear interface computation
  results/              sandbox JSONL behind every table above
```

```sh
python3 casestudies/xsa/xsa_dagster.py dags inst/n64/xsa_n64.meta.json dags/ --block 8 --split 8
python3 casestudies/xsa/xsa_dagster.py predict inst/n64/xsa_n64.meta.json --split 4 --split 8
python3 casestudies/xsa/run_xsa.py --gen casestudies/xsa/generator/xor_session_attack_gen.py \
   --dagster build/dagster-upstream/dagster/dagster --n 1024,2048 --shapes one,split8 --ranks 3 --out runs/
python3 casestudies/xsa/xsa_birthday_gen.py 6 xsab/
python3 casestudies/xsa/run_xsab.py --dir xsab --m 4,5 --dagster build/dagster-upstream/dagster/dagster \
   --shapes one,oracle --variants plain,strategy --out xsab_runs/
```
