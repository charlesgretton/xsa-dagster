# Development-phase tools

These tools produced the development-phase findings of the proceedings (early DAG families, the
birthday regime XSA-B, the cube control and telemetry-guided refinement). Those findings come from
one development core and are qualitative; the cluster campaign uses `xsa/run_families.py` and
`hpc/`.

| tool | what it does |
|---|---|
| `run_xsa.py` | XSA-N under the early families (one, split, tree, block) with model completion |
| `xsa_birthday_gen.py` | XSA-B generator: plain and Wagner-strategy CNFs in the birthday regime |
| `run_xsab.py` | XSA-B under flat solving, Wagner tree and fused blocks |
| `cube_control.py` | the same-knowledge control: flat MiniSat with the split's prefix pinned |
| `refine_topdown.py` | telemetry-guided re-splitting of the slowest subtrees |
| `CASESTUDY.md`, `FAMILIES.md` | development notes recording these experiments as they were run |

The variant DAGs (keyed edges, bounded splitter) also need the opt-in Dagster patches in
`../../patches/`; the reported campaign uses unmodified Dagster.
