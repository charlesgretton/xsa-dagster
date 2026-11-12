PYTHON ?= python3

# every check in the repository; all must pass before any result is trusted
selftest: check-files
	$(PYTHON) tools/selftest.py

# the virtual proceedings (self-contained; three passes for references)
proceedings:
	cd proceedings && for i in 1 2 3; do pdflatex -interaction=nonstopmode -halt-on-error xsa_dagster_proceedings.tex > /dev/null || exit 1; done
	@echo "built proceedings/xsa_dagster_proceedings.pdf"

clean:
	rm -f proceedings/*.aux proceedings/*.log proceedings/*.out proceedings/*.toc
	find . -name __pycache__ -prune -exec rm -rf {} +

.PHONY: selftest proceedings clean

# reproducibility manifest: checksums of every tracked artefact plus toolchain
manifest:
	@echo "# xsa-dagster manifest"                         >  MANIFEST.sha256
	@echo "# version: $$(cat VERSION)"                    >> MANIFEST.sha256
	@echo "# generated: $$(date -u +%Y-%m-%dT%H:%M:%SZ)"  >> MANIFEST.sha256
	@echo "# python:   $$($(PYTHON) --version)"           >> MANIFEST.sha256
	@echo "# platform: $$(uname -srm)"                     >> MANIFEST.sha256
	@find . -type f ! -name MANIFEST.sha256 ! -path './.git/*' \
	      ! -name '*.pyc' ! -name '*.aux' ! -name '*.log' ! -name '*.out' ! -name '*.toc' -print0 \
	  | sort -z | xargs -0 sha256sum                    >> MANIFEST.sha256
	@echo "wrote MANIFEST.sha256 ($$(grep -c '^[0-9a-f]' MANIFEST.sha256) files)"

# refuse to proceed if any expected artefact is missing
check-files:
	@for f in proceedings/xsa_dagster_proceedings.tex proceedings/xsa_dagster_proceedings.pdf \
	          xsa/generator/xor_session_attack_gen.py xsa/xsa_dagster.py xsa/run_families.py \
	          xsa/examples/xsa_n32.cnf xsa/examples/xsa_n32.meta.json tools/check_dag.py \
	          evidence/derived/runs.csv evidence/derived/scaling.csv evidence/checksums.txt \
	          hpc/env.sh hpc/scale.pbs VERSION README.md LICENSE LICENSE-CC-BY-4.0.md; do \
	    test -f $$f || { echo "MISSING: $$f"; exit 1; }; done
	@echo "all expected artefacts present"

.PHONY: manifest check-files
