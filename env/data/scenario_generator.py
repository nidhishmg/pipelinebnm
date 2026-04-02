"""
Procedural bug scenario generator for DataPipelineEnv.

generate_scenario(seed, task_id, difficulty) returns a fresh scenario
spec every time — no scenario file needed. Static JSON files are still
used as the fallback for /demo only.
"""
from __future__ import annotations

import random
from typing import Literal


# Column pools per task — bugs can only target plausible columns
_TASK1_NULL_COLS = ["salary", "age", "phone", "department"]
_TASK1_TYPE_COLS = ["age", "salary", "hire_date"]
_TASK2_SCHEMA_COLS = [
    ("department", "dept"),
    ("hire_date", "joined_on"),
    ("consent_flag", "gdpr_consent"),
    ("phone", "contact_num"),
]
_TASK2_TYPE_COLS = [("age", "cast_to_int"), ("salary", "cast_to_float")]

# Row index pool — expanded to guarantee uniqueness across 100 scenarios
_ROW_POOL = list(range(1, 150))
# Duplicate index pool — non-trivial offset pairs across the dataset
_DUP_POOL = [[i, i + 5] for i in range(10, 190, 10)]


def generate_scenario(
    seed: int,
    task_id: int,
    difficulty: Literal["easy", "medium", "hard"] = "easy",
) -> list[dict]:
    """
    Produce a procedurally generated bug spec from a numeric seed.

    Args:
        seed: Controls randomness — same seed always returns same scenario.
        task_id: 1, 2, or 3.
        difficulty: Affects bug count and severity distribution.

    Returns:
        A list[dict] in the same format as the static JSON scenario files.
    """
    rng = random.Random(seed)

    if task_id == 1:
        return _gen_task1(rng, difficulty)
    elif task_id == 2:
        return _gen_task2(rng, difficulty)
    elif task_id == 3:
        return _gen_task3(rng, difficulty)
    else:
        raise ValueError(f"Unknown task_id: {task_id}")


def _pick_rows(rng: random.Random, n: int) -> list[int]:
    """Pick n unique row indices from the pool."""
    return rng.sample(_ROW_POOL, min(n, len(_ROW_POOL)))


def _gen_task1(rng: random.Random, difficulty: str) -> list[dict]:
    """Generate Task 1 scenario: null, type, out-of-range, format, duplicate."""
    bugs: list[dict] = []

    # B001 — null injection (always present, column varies)
    null_col = rng.choice(_TASK1_NULL_COLS)
    null_rows = sorted(_pick_rows(rng, rng.randint(2, 4)))
    bugs.append({
        "bug_id": "B001",
        "type": "null_injection",
        "column": null_col,
        "rows": null_rows,
        "severity": "high",
        "description": f"NULL values injected in {null_col} column at rows {null_rows}",
    })

    # B002 — type corruption (must be a different column than null)
    type_col_pool = [c for c in _TASK1_TYPE_COLS if c != null_col]
    type_col = rng.choice(type_col_pool)
    type_row = rng.choice(_ROW_POOL)
    type_values = {
        "age": ["twenty-three", "forty", "N/A"],
        "salary": ["seventy_thousand", "n/a", "unknown"],
        "hire_date": ["last_year", "01/01/20", "invalid-date"],
    }
    type_val = rng.choice(type_values.get(type_col, ["invalid"]))
    bugs.append({
        "bug_id": "B002",
        "type": "type_corruption",
        "column": type_col,
        "row": type_row,
        "value": type_val,
        "severity": "high",
        "description": f"{type_col} stored as string at row {type_row}: '{type_val}'",
    })

    # B003 — out-of-range (age only — most interpretable)
    range_row = rng.choice([r for r in _ROW_POOL if r != type_row])
    range_val = rng.choice([0, 999, -5, 150])
    bugs.append({
        "bug_id": "B003",
        "type": "out_of_range",
        "column": "age",
        "row": range_row,
        "value": range_val,
        "severity": "medium",
        "description": f"Age value {range_val} is outside valid range 22-65 at row {range_row}",
    })

    # B004 — format inconsistency (phone)
    fmt_row = rng.choice([r for r in _ROW_POOL if r not in (type_row, range_row)])
    bugs.append({
        "bug_id": "B004",
        "type": "format_inconsistency",
        "column": "phone",
        "row": fmt_row,
        "severity": "low",
        "description": f"Phone reformatted to +91-XX-XXXXXXXX format at row {fmt_row}",
    })

    # B005 — duplicate rows
    dup_indices = rng.choice(_DUP_POOL)
    bugs.append({
        "bug_id": "B005",
        "type": "duplicate_rows",
        "column": None,
        "indices": dup_indices,
        "severity": "medium",
        "description": f"Rows {dup_indices[0]} and {dup_indices[1]} duplicated",
    })

    return bugs


def _gen_task2(rng: random.Random, difficulty: str) -> list[dict]:
    """Generate Task 2 scenario: schema drift + type bug."""
    bugs: list[dict] = []

    # B001 — schema drift (column rename)
    old_col, new_col = rng.choice(_TASK2_SCHEMA_COLS)
    bugs.append({
        "bug_id": "B001",
        "type": "schema_drift",
        "old_col": old_col,
        "new_col": new_col,
        "severity": "critical",
        "description": f"Column '{old_col}' renamed to '{new_col}' by upstream schema change",
    })

    # B002 — second drift (different column)
    remaining = [(o, n) for o, n in _TASK2_SCHEMA_COLS if o != old_col]
    old_col2, new_col2 = rng.choice(remaining)
    bugs.append({
        "bug_id": "B002",
        "type": "schema_drift",
        "old_col": old_col2,
        "new_col": new_col2,
        "severity": "high",
        "description": f"Column '{old_col2}' renamed to '{new_col2}' by upstream schema change",
    })

    # B003 — type corruption on a numeric column
    type_col, cast_hint = rng.choice(_TASK2_TYPE_COLS)
    type_row = rng.choice(_ROW_POOL)
    corrupt_vals = {"age": "N/A", "salary": "unknown"}
    bugs.append({
        "bug_id": "B003",
        "type": "type_corruption",
        "column": type_col,
        "row": type_row,
        "value": corrupt_vals.get(type_col, "invalid"),
        "severity": "high",
        "description": f"{type_col} corrupted to string at row {type_row}, expected {cast_hint}",
        "accepted_casts": ["cast_to_int", "cast_to_float"],
    })

    return bugs


def _gen_task3(rng: random.Random, difficulty: str) -> list[dict]:
    """Generate Task 3 scenario: schema drift on rev_amt + type + pii_leak + duplicates."""
    bugs: list[dict] = []

    # B001 — schema drift (always rev_amt for Task 3 — required for stage detection)
    bugs.append({
        "bug_id": "B001",
        "type": "schema_drift",
        "old_col": "revenue_amount",
        "new_col": "rev_amt",
        "severity": "critical",
        "stage": "stage_3_join",
        "description": "revenue_amount renamed to rev_amt in join stage — downstream joins fail",
    })

    # B002 — type corruption on rev_amt
    type_row = rng.choice(_ROW_POOL)
    bugs.append({
        "bug_id": "B002",
        "type": "type_corruption",
        "column": "revenue_amount",
        "row": type_row,
        "value": "corrupted_value",
        "severity": "critical",
        "stage": "stage_3_join",
        "description": f"revenue_amount dtype corrupted to object at row {type_row}",
    })

    # B003 — PII leak (inject employee_ssn column so detection is real discovery)
    bugs.append({
        "bug_id": "B003",
        "type": "pii_leak",
        "column": "ssn",
        "severity": "critical",
        "stage": "stage_3_join",
        "description": "SSN column propagated to analytics output — PII compliance violation",
    })

    # B004 — duplicate rows (causes aggregation inflation)
    dup_indices = rng.choice(_DUP_POOL)
    bugs.append({
        "bug_id": "B004",
        "type": "duplicate_rows",
        "indices": dup_indices,
        "severity": "high",
        "stage": "stage_4_aggregate",
        "description": f"Duplicate rows {dup_indices} inflate revenue aggregation",
    })

    return bugs
