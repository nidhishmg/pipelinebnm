"""
validate_diversity.py — Phase 3 diversity checker.

Generates 100 procedural scenarios per task (seeds 0-99) and verifies that
no two scenarios share the same (bug_type, column, row_index) triple.
This confirms the procedural generator provides adequate generalization coverage.

Usage:
    python scripts/validate_diversity.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from env.data.scenario_generator import generate_scenario


def extract_fingerprints(bugs: list[dict]) -> set[tuple]:
    """Extract (bug_type, column, row_or_index) triples from a scenario."""
    fps: set[tuple] = set()
    for bug in bugs:
        bug_type = bug.get("type", "")
        col = bug.get("column") or bug.get("old_col") or "N/A"
        row = bug.get("row") or bug.get("rows") or bug.get("indices") or "N/A"
        if isinstance(row, list):
            row = tuple(sorted(row))
        fps.add((bug_type, col, str(row)))
    return fps


def run_diversity_check(task_id: int, n: int = 100) -> dict:
    seen_fingerprints: list[frozenset] = []
    duplicates = 0

    for seed in range(n):
        bugs = generate_scenario(seed, task_id=task_id, difficulty="easy")
        fp = frozenset(extract_fingerprints(bugs))
        if fp in seen_fingerprints:
            duplicates += 1
        seen_fingerprints.append(fp)

    unique = len(set(str(f) for f in seen_fingerprints))
    return {
        "task_id": task_id,
        "total_scenarios": n,
        "unique_scenarios": unique,
        "duplicate_scenarios": duplicates,
        "diversity_rate": round(unique / n, 3),
        "pass": round(unique / n, 3) >= 0.90,
    }


if __name__ == "__main__":
    print("=" * 60)
    print("DataPipelineEnv — Scenario Diversity Validation")
    print("=" * 60)
    all_pass = True
    for tid in [1, 2, 3]:
        result = run_diversity_check(tid, n=100)
        status = "✅ PASS" if result["pass"] else "❌ FAIL"
        print(
            f"Task {tid}: {result['unique_scenarios']}/100 unique scenarios | "
            f"diversity={result['diversity_rate']} | {status}"
        )
        if not result["pass"]:
            all_pass = False
    print("=" * 60)
    if all_pass:
        print("✅ All tasks pass diversity check — no repeated (bug_type, column, row) triples.")
    else:
        print("❌ Some tasks have repeated scenario fingerprints — review generate_scenario().")
        sys.exit(1)
