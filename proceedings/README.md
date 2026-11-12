# Virtual proceedings

`xsa_dagster_proceedings.tex` is the complete, self-contained source of the volume: every diagram is
TikZ and every plot is pgfplots with its data embedded, so it needs no external files.

Rebuild from the repository root with `make proceedings`, or here with `pdflatex` run three times.
Latin Modern is used where installed, otherwise Computer Modern.

Every number in the results is taken from `../evidence/derived/`; the appendix "Evidence and
checksums" identifies the raw results and instances by SHA-256.
