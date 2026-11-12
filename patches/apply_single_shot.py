#!/usr/bin/env python3
"""apply_single_shot.py DAGSTER_SRC_DIR  -- opt-in per-node single-solution semantics.

Adds one environment variable to Dagster's worker: DAGSTER_SINGLE_SHOT, a range list
of DAG node ids (e.g. "0-31,65").  A worker on a listed node requests new work after
its FIRST solution for the assigned message -- what -e 3 does for every node.

Why: a junction-tree DAG's distribute (D*) nodes must hand a whole local assignment
to the collector.  Under Dagster's semantics a node enumerates every distinct
projection of its models onto its outgoing interface before releasing the worker,
and the organiser never preempts a worker unless load is unbalanced, so a D* node
would hold its worker enumerating every local model.  One solution per input
combination keeps the distribute pass complete (each combination is still its own
job) and makes it terminate.  Unlisted nodes are untouched; with the variable unset
the binary behaves exactly like upstream.  Idempotent; edits dagster/Worker.cpp only.
"""
import os
import sys

MARK = "DAGSTER_SINGLE_SHOT"
HELPER = r'''
// --- cnfgraphs-hpc patch: opt-in single-solution nodes (see DAGSTER_SINGLE_SHOT) ---
#include <cstdlib>
#include <string>
#include <vector>
static bool single_shot_node(int node) {
  static std::vector<char> table;
  static bool loaded = false;
  if (!loaded) {
    loaded = true;
    const char* s = getenv("DAGSTER_SINGLE_SHOT");
    std::string spec = s ? s : "";
    size_t p = 0;
    while (p < spec.size()) {
      size_t q = spec.find(',', p);
      if (q == std::string::npos) q = spec.size();
      std::string tok = spec.substr(p, q - p);
      if (!tok.empty()) {
        size_t dash = tok.find('-');
        int a = atoi(tok.c_str());
        int b = (dash == std::string::npos) ? a : atoi(tok.c_str() + dash + 1);
        for (int i = a; i >= 0 && i <= b; ++i) {
          if ((int)table.size() <= i) table.resize(i + 1, 0);
          table[i] = 1;
        }
      }
      p = q + 1;
    }
  }
  return node >= 0 && node < (int)table.size() && table[node];
}
// --- end patch ---
'''
OLD = "if ((!reset_solver_for_next_solution(m2)) || (command_line_arguments.ENUMERATE_SOLUTIONS==3)) {"
NEW = ("if ((!reset_solver_for_next_solution(m2)) || (command_line_arguments.ENUMERATE_SOLUTIONS==3)"
       " || single_shot_node(m2->from)) {")


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    path = os.path.join(sys.argv[1], "Worker.cpp")
    src = open(path).read()
    if MARK in src:
        print("already patched:", path)
        return 0
    if OLD not in src:
        print("ERROR: expected line not found in", path, "-- upstream changed; patch by hand")
        return 1
    anchor = 'extern Arguments command_line_arguments;'
    if anchor not in src:
        print("ERROR: anchor not found in", path)
        return 1
    src = src.replace(anchor, anchor + "\n" + HELPER, 1).replace(OLD, NEW, 1)
    open(path, "w").write(src)
    print("patched:", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
