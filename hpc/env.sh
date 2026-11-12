# hpc/env.sh -- environment for every job script in hpc/.  Edit the SITE section once.
#
# The reported campaign ran on a PBS-managed cluster with 104-core nodes, Open MPI 4.1.0
# and Python 3.11; see proceedings/ for the full environment record.

# ---- SITE: modules, library locations ------------------------------------------------
if command -v module >/dev/null 2>&1; then
  module load openmpi 2>/dev/null || true          # e.g. openmpi/4.1.0
  module load python3 2>/dev/null || true          # Python >= 3.7
fi
# glog and CUDD, if installed under a private prefix (Dagster links against both)
export DEPS_PREFIX=${DEPS_PREFIX:-$HOME/local}
export PATH=$DEPS_PREFIX/bin:$PATH
export CPATH=$DEPS_PREFIX/include${CPATH:+:$CPATH}
export LIBRARY_PATH=$DEPS_PREFIX/lib${LIBRARY_PATH:+:$LIBRARY_PATH}
export LD_LIBRARY_PATH=$DEPS_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# ---- Dagster (AFSAT-Integration branch, unmodified) ----------------------------------
export DAGSTER_DIR=${DAGSTER_DIR:-$HOME/dagster-afsat/dagster}
export DAGSTER_BIN=${DAGSTER_BIN:-$DAGSTER_DIR/dagster}

# ---- where instances and results go (a large, fast file system) -----------------------
export XSA_DATA=${XSA_DATA:-$PWD/xsa-data}

# ---- run settings used in the reported campaign ---------------------------------------
# table master without the literal-by-literal join check (safe for top-down DAGs: the
# collector's inputs are variable-disjoint; see proceedings, "the cost of checking combinations")
export MASTER_ARGS=${MASTER_ARGS:--g 2}
# one rank per core; the campaign used TCP transport (shared memory made no difference)
export MPIRUN_EXTRA=${MPIRUN_EXTRA:---map-by core --bind-to core}
export OMPI_MCA_btl=${OMPI_MCA_btl:-self,tcp}
export GLOG_logtostderr=1
unset GLOG_v                                       # the driver sets verbosity per run
