# Reward Design

This document explains every reward signal in DataPipelineEnv. Each value was chosen to create a learnable gradient that prevents reward farming while ensuring a well-behaved agent scores 0.6+.

## Reward Table

| Action | Condition | Reward | Rationale |
|--------|-----------|--------|-----------|
| `INSPECT <signal>` | Broad scan (metrics/logs/dag/pii) — first time | `+0.05` | Reward breadth; encourage environmental scanning |
| `INSPECT <column>` | Column-level INSPECT reveals a bug — first time | `+0.15` | Reward targeted discovery |
| `INSPECT <schema>` | Schema inspection reveals schema_drift bug | `+0.15` | Same logic; schema is a key signal type |
| `INSPECT <any>` | Target already inspected (re-inspection) | `-0.10` | Deter repetition farming |
| `INSPECT <column>` | No bug on that column | `-0.05` | Deter random exploration |
| `FILL_DEFAULT` | Bug not yet discovered | `-0.10` | Enforce investigate-first protocol |
| `FILL_DEFAULT fill_median` | Correct fix on discovered null bug | `+0.20` | Correct targeted fix |
| `CAST_TYPE cast_to_int/float` | Correct fix on discovered type/range bug | `+0.20` | Correct targeted fix |
| `RENAME_COLUMN` | Correct schema fix on discovered drift bug | `+0.20` | Correct targeted fix |
| `MASK_PII` | Correct PII masking on discovered PII bug | `+0.20` | Correct targeted fix |
| `VALIDATE` | Fixes applied, validation passes | `+0.25` | Validate signal |
| Completion | All bugs fixed (all tasks) | `+0.30` | Terminal reward for full resolution |
| Unknown/unsupported action | Any | `-0.10` | Deter hallucinated actions |
| `DROP_COLUMN` | Any | `-0.10` | Destructive; discouraged |
| `NOOP` | Any | `0.0` | Neutral — no progress but no penalty |

> **Reward clamp**: All step rewards are clamped to `[-0.5, 1.0]` per OpenEnv spec.

## Shaped Completion Bonus (Phase 4)

The `+0.30` completion bonus is awarded at the VALIDATE step when all required bugs are fixed. This creates a spike at completion. In a future iteration, this can be distributed as:

```
progress_bonus = 0.30 * (bugs_fixed / total_bugs)   # per-step
terminal_adjustment = 0.30 - sum(progress_bonuses)   # at VALIDATE
```

This potential-based shaping is mathematically equivalent per the PBR theorem and eliminates the terminal spike.

## Anti-Gaming Design

1. **Re-inspect penalty** (`-0.10`): Eliminates free random exploration strategy. An agent cannot earn `+0.05 * N` by scanning the same targets repeatedly.
2. **Undiscovered fix penalty** (`-0.10`): Forces the agent to INSPECT before FIX. Graders verify `discovered_bugs` set.
3. **Contextual reasoning bonus** (`+0.05 max`): Grader 3 PII keywords require prior `stage_3` inspection. NOOP-only episodes get `0.0`.
4. **Diminishing discovery**: Subsequent inspections of the same target yield `0` (not negative, since `-0.10` covers it).

## Grader Sub-Score Weights

| Task | Sub-Score | Weight |
|------|-----------|--------|
| Task 1 | Identification | 0.40 |
| Task 1 | Remediation | 0.60 |
| Task 2 | Rows passing | 0.60 |
| Task 2 | Column recovery | 0.25 |
| Task 2 | Type correctness | 0.15 |
| Task 3 | Diagnosis | 0.25 |
| Task 3 | Fix | 0.35 |
| Task 3 | PII sweep | 0.20 |
| Task 3 | Validation | 0.20 |
