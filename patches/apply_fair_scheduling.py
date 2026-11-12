#!/usr/bin/env python3
"""apply_fair_scheduling.py DAGSTER_SRC_DIR -- opt-in round-robin among same-depth nodes.

With DAGSTER_FAIR_SCHEDULING=1 the table-mode master (-g 0) starts its search for the
next job, at each DAG depth, just after the node that received the previous job at
that depth, instead of always at node 0.

Why: TableSolutions::get_new_message_combination scans nodes from id 0 and hands the job
to the first node at the requested depth that has a new input combination.  When an
upstream node streams alternatives (e.g. a splitter offering many target splits), the
lowest-numbered child always has a new combination and its siblings are never scheduled
-- observed as 4,295 jobs on one subtree and none on its sibling.  Unset, behaviour is
identical to upstream.  Idempotent; edits dagster/TableSolutions.cpp only.
"""
import os
import sys

MARK = "DAGSTER_FAIR_SCHEDULING"
HELPER = r'''
// --- cnfgraphs-hpc patch: opt-in round-robin scheduling (DAGSTER_FAIR_SCHEDULING=1) ---
#include <cstdlib>
static bool fair_scheduling_enabled() {
  static int state = -1;
  if (state < 0) { const char* s = getenv("DAGSTER_FAIR_SCHEDULING"); state = (s && s[0] == '1') ? 1 : 0; }
  return state == 1;
}
// --- end patch ---
'''
OLD_LOOP = '''  for (int i = 0; i < dag->no_nodes; i++) {
    if (dag->depth_for_each_node[i] == depth) {
      VLOG(5) << "inspecting node " << i;'''
NEW_LOOP = '''  static std::vector<int> rr_start;                       // patch: per-depth round-robin start
  if ((int)rr_start.size() <= dag->max_depth) rr_start.assign(dag->max_depth + 1, 0);
  const bool fair = fair_scheduling_enabled();
  for (int k = 0; k < dag->no_nodes; k++) {
    int i = fair ? (rr_start[depth] + k) % dag->no_nodes : k;
    if (dag->depth_for_each_node[i] == depth) {
      VLOG(5) << "inspecting node " << i;'''
OLD_HIT = '''      if (ret == true) {
        for (int reverse_connection_index=0;'''
NEW_HIT = '''      if (ret == true) {
        if (fair) rr_start[depth] = (i + 1) % dag->no_nodes;   // patch: next search starts after i
        for (int reverse_connection_index=0;'''


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    path = os.path.join(sys.argv[1], "TableSolutions.cpp")
    src = open(path).read()
    if MARK in src:
        print("already patched:", path)
        return 0
    for old in (OLD_LOOP, OLD_HIT):
        if src.count(old) != 1:
            print("ERROR: expected code not found exactly once in", path, "-- patch by hand")
            return 1
    anchor = "Message *TableSolutions::get_new_message_combination(int depth) {"
    src = src.replace(anchor, HELPER + "\n" + anchor, 1).replace(OLD_LOOP, NEW_LOOP, 1).replace(OLD_HIT, NEW_HIT, 1)
    open(path, "w").write(src)
    print("patched:", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
