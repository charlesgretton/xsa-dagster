# Licensing

| Material | Licence | Files |
|---|---|---|
| Source code, build and job scripts | Apache License 2.0 (`LICENSE`) | `*.py`, `*.sh`, `*.pbs`, `Makefile`, and other program text |
| Proceedings, documentation | CC BY 4.0 (`LICENSE-CC-BY-4.0.md`) | `*.tex`, `*.pdf`, `*.md`, `*.bib`, and the proceedings generally |
| Data produced by the project | CC BY 4.0 | benchmark instances, results, summaries and checksums generated here (`*.cnf`, `*.model`, `*.json`, `*.jsonl`, `*.txt`, `*.csv`, `*.sha256`) |

Machine-readable form: `REUSE.toml` (REUSE specification 3.2).

**Third-party software.** Dagster, MiniSat (embedded in Dagster), glog, CUDD and Open MPI are not
bundled; they are obtained from their own repositories and keep their own licences. The scripts in
`patches/` are this project's, but they edit a local Dagster checkout, and the patched source remains
under Dagster's licence. Material by others that is cited here keeps its original terms.

**Provenance note.** Licence notices are declared at repository level rather than in file headers,
because the benchmark generator (`xsa/generator/xor_session_attack_gen.py`) is bound by SHA-256 to the
instances reported in the proceedings, and any edit, even to a comment, would break that binding.

**Errata.** Reports of errors, omissions and overlooked prior work are welcome: please write to the
corresponding author, Charles Gretton, at charles.gretton@anu.edu.au. Every report that leads to a
correction will be acknowledged in the errata and in any revised edition, cited as a private
communication; where the point was already made in a publication that appeared before the report's
publication date, that publication will be cited as well.
