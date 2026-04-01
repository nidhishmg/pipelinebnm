---
title: Broken Pipeline Env
emoji: 🔧
colorFrom: red
colorTo: yellow
sdk: docker
pinned: false
---

# DataPipelineEnv

An OpenEnv environment where an AI agent acts as an on-call data engineer
diagnosing and remediating a broken enterprise ETL pipeline before the
morning job runs.

## Why This Environment Exists

Every company with data has broken pipelines. Gartner estimates poor data quality
costs organizations an average of \$12.9M per year. When a pipeline breaks at 2 AM,
an engineer must triage alerts, trace root causes through multi-stage DAGs, apply
targeted fixes, and validate output — all under time pressure. This environment
captures that workflow as an agentic benchmark.

No OpenEnv environment previously existed for this domain. DataPipelineEnv fills
the gap by testing whether an AI agent can systematically investigate, diagnose,
and fix real-world data quality issues — NULL injection, type corruption, schema
drift, PII leaks, and duplicate aggregation — rather than pattern-matching on
pre-revealed bug lists.

## Environment Design

### Key Mechanic: Progressive Discovery

Bugs are **hidden** at reset. `validation_report` starts empty.
The agent must use `INSPECT` on specific columns and pipeline stages to reveal
issues before it can fix them — exactly like a real on-call engineer.

```
Step 0: validation_report = []          ← agent sees nothing
Step 1: INSPECT salary                  ← reveals null_injection bug
Step 2: INSPECT age                     ← reveals type_corruption bug
Step 3: FILL_DEFAULT salary fill_median ← fixes it, reward +0.20
```

### Blast Radius Penalty

Wrong actions (e.g., dropping a column with dependents) cascade to downstream
stages, reducing `downstream_health`. Forces the agent to investigate before acting.

### AER Reasoning Trace

Every step logs an `AERRecord` with the agent's justification.
Grader 3 awards partial credit for correct diagnostic language even if
the final fix action fails.

### Investigation Tools

The agent can call structured tools via INSPECT:
- `run_null_check` — scans all columns for NULL values
- `run_type_check` — checks column dtypes against expected schema
- `run_duplicate_check` — counts duplicate rows
- `run_pii_scan` — scans for SSN/email patterns
- `run_schema_diff` — compares current vs expected schema

Each tool call costs one step. Agent must choose wisely within 8 steps.

## Observation Space

| Field | Type | Description |
|---|---|---|
| `dataset_preview` | `List[dict]` | First 10 rows of working dataset |
| `schema` | `dict` | Column names, dtypes, nullable flags |
| `pipeline_stage` | `str` | Current ETL stage |
| `validation_report` | `List[DetectedIssue]` | Only discovered, unfixed bugs |
| `time_remaining` | `int` | Steps left (starts at 8) |
| `downstream_health` | `float` | Cascade health score (0.0–1.0) |
| `step_count` | `int` | Current step number |
| `task_id` | `int` | Active task identifier |
| `pipeline_stage_health` | `dict` | Per-stage health (Task 3) |
| `agent_context` | `dict` | Inspected columns, bugs found/fixed, hints |

## Action Space

| `action_type` | Parameters | Effect |
|---|---|---|
| `INSPECT` | `target_column` | Reveals bugs in that column/facet/stage |
| `FILL_DEFAULT` | `target_column`, `transformation` | Fill nulls (e.g., `fill_median`) |
| `CAST_TYPE` | `target_column`, `transformation` | Fix type errors (e.g., `cast_to_int`) |
| `RENAME_COLUMN` | `target_column`, `transformation` | Fix schema drift |
| `MASK_PII` | `target_column` | Redact sensitive data |
| `VALIDATE` | — | Score current pipeline state |
| `DROP_COLUMN` | `target_column` | ⚠️ Triggers blast radius penalty |
| `NOOP` | — | No-op, zero reward |

## Tasks

### Task 1 — Data Quality Audit (Easy, max_steps=8)
Find and fix 5 planted bugs: nulls in salary, type errors in age, outlier age
values, phone format inconsistency, and duplicate rows.
**Grader:** `0.4 × identification + 0.6 × remediation + efficiency_bonus`

### Task 2 — Schema Drift Remediation (Medium, max_steps=8)
Upstream renamed `employee_id` → `customer_uuid`, `hire_date` → `dob_date`, and
nulled out `consent_flag`. Fix the schema before downstream joins fail.
Wrong column drops trigger cascade penalties.
**Grader:** `0.60 × rows_passing + 0.25 × column_recovery + 0.15 × type_correctness − blast_penalty`

### Task 3 — Full Incident Response (Hard, max_steps=8)
Production revenue numbers are wrong. Trace back through 5 pipeline stages:
stage_5 → stage_4 → stage_3 to find the corruption entry point. Fix `rev_amt`
type, mask SSN PII, validate output.
**Grader:** `0.25×diagnosis + 0.35×fix + 0.20×pii_sweep + 0.20×validation + bonuses`

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/ping` | Health check |
| `GET` | `/tasks` | List available tasks with action schema |
| `POST` | `/reset?task_id=N` | Reset environment, returns `DataObservation` |
| `POST` | `/step?task_id=N` | Submit action, returns `StepResult` |
| `GET` | `/state?task_id=N` | Current observation + action history |
| `GET` | `/grader?task_id=N` | Grade current episode |
| `GET` | `/baseline` | Runs NOOP agent, returns reproducible scores |
| `GET` | `/tools` | Tool registry with descriptions and costs |
| `GET` | `/replay?task_id=N` | Full AER episode history |

## Setup

### Docker (recommended)
```bash
docker build -t broken-pipeline-env .
docker run -p 7860:7860 broken-pipeline-env
```

### Local
```bash
pip install -r requirements.txt
uvicorn env.server:app --host 0.0.0.0 --port 7860
```

### Running the Agent
```bash
# Using OpenAI API (spec-compliant)
export OPENAI_API_KEY="sk-..."
export API_BASE_URL="http://localhost:7860"
python inference.py

# Using HuggingFace (fallback)
export HF_TOKEN="hf_..."
export MODEL_NAME="Qwen/Qwen2.5-72B-Instruct"
export API_BASE_URL="http://localhost:7860"
python inference.py
```

### Running Tests
```bash
python -m pytest tests/test_env.py -v
```

## Baseline Scores

| Task | Agent | Score |
|---|---|---|
| Task 1 | NOOP | ~0.0 |
| Task 2 | NOOP | ~0.0 |
| Task 3 | NOOP | ~0.0 |

Run `GET /baseline` to reproduce exact scores.