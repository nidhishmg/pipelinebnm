title: Broken Pipeline Env
emoji: 🔧
colorFrom: red
colorTo: yellow
colorTo: blue
sdk: docker
pinned: false
---

# DataPipelineEnv

An OpenEnv environment where an AI agent acts as an on-call data engineer
diagnosing and remediating a broken enterprise ETL pipeline before the
morning job runs.
Every company's revenue dashboard has been wrong at 2 AM.
A data engineer gets the call. We built the environment to
benchmark whether an AI agent can do their job.

## Why This Environment Exists
## The Problem

Every company with data has broken pipelines. Gartner estimates poor data quality
costs organizations an average of \$12.9M per year. When a pipeline breaks at 2 AM,
an engineer must triage alerts, trace root causes through multi-stage DAGs, apply
targeted fixes, and validate output — all under time pressure. This environment
captures that workflow as an agentic benchmark.
Bad data costs companies $12.9M/year on average (IBM, 2022).
Every data team faces broken pipelines. Until now, there was
no standardized benchmark to evaluate whether an AI agent
can diagnose and fix them. DataPipelineEnv fills that gap.

No OpenEnv environment previously existed for this domain. DataPipelineEnv fills
the gap by testing whether an AI agent can systematically investigate, diagnose,
and fix real-world data quality issues — NULL injection, type corruption, schema
drift, PII leaks, and duplicate aggregation — rather than pattern-matching on
pre-revealed bug lists.
## What Makes This Different

## Environment Design
**Progressive Discovery**: The agent cannot see bugs until it inspects.
Unlike static environments, DataPipelineEnv requires the agent to
investigate before it can act — just like a real engineer would.

### Key Mechanic: Progressive Discovery
**Blast Radius**: Wrong actions cascade. Drop the wrong column
and downstream tables break too. The agent must understand
data dependencies, not just column names.

Bugs are **hidden** at reset. `validation_report` starts empty.
The agent must use `INSPECT` on specific columns and pipeline stages to reveal
issues before it can fix them — exactly like a real on-call engineer.
**Curriculum Design**: Three tasks form a strict skill progression.
Skills learned in Task 1 transfer directly to Tasks 2 and 3.
This enables meaningful RL training signal across difficulty levels.

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
**Reproducible Grading**: Bugs are injected from fixed scenario files.
The grader compares agent output against the exact ground truth
we planted. Scores are 100% deterministic.

Every step logs an `AERRecord` with the agent's justification.
Grader 3 awards partial credit for correct diagnostic language even if
the final fix action fails.
## The 3 Tasks

### Investigation Tools
| Task | Difficulty | What the Agent Does | Smart Agent Score |
|------|-----------|---------------------|-------------------|
| 1 — Data Quality Audit | Easy | Find and fix nulls, type errors, duplicates | ~0.75 |
| 2 — Schema Drift | Medium | Fix renamed columns, type changes, missing fields | ~0.60 |
| 3 — Incident Response | Hard | Trace 5-stage pipeline, fix, PII sweep, validate | ~0.55 |

The agent can call structured tools via INSPECT:
- `run_null_check` — scans all columns for NULL values
- `run_type_check` — checks column dtypes against expected schema
- `run_duplicate_check` — counts duplicate rows
- `run_pii_scan` — scans for SSN/email patterns
- `run_schema_diff` — compares current vs expected schema
## Action Space

Each tool call costs one step. Agent must choose wisely within 8 steps.
| Action | Description | When to Use |
|--------|-------------|-------------|
| `INSPECT` | Reveals bugs in a column or facet | Always first |
| `FILL_DEFAULT` | Fill NULLs with median/zero | After inspecting null column |
| `CAST_TYPE` | Fix column data type | After finding type corruption |
| `RENAME_COLUMN` | Fix schema drift | After schema_diff reveals rename |
| `MASK_PII` | Redact sensitive data | Immediately on SSN detection |
| `VALIDATE` | Confirm all fixes and close episode | After all fixes applied |
| `DROP_COLUMN` | Remove column (triggers blast radius) | Avoid unless necessary |
| `NOOP` | No operation | Fallback only |

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

### Task 3 — Full Incident Response (Hard, max_steps=20)
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
|-------|------|-------------|
| `dataset_preview` | list[dict] | First 10 rows |
| `schema` | dict | Column types and nullable flags |
| `pipeline_stage` | str | Current ETL stage |
| `validation_report` | list | Bugs discovered so far (empty at reset) |
| `time_remaining` | int | Steps left |
| `downstream_health` | float | 0.0=broken, 1.0=fixed |
| `agent_context` | dict | Investigation state, recommendations |
| `pipeline_stage_health` | dict | Per-stage health (Task 3 only) |

## Reward Function

| Event | Reward |
|-------|--------|
| Broad scan (metrics/logs/pii/schema) | +0.05 |
| Discover real bug via INSPECT | +0.15 |
| Correct fix applied | +0.20 |
| VALIDATE after all fixes | +0.25 |
| All bugs fixed (completion) | +0.30 |
| Re-inspect same target | -0.05 |
| Fix before discovering | -0.10 |
| PII not masked | -0.20 |

## Quickstart
```bash
git clone https://github.com/Nithesh1109/broken-pipeline-env
cd broken-pipeline-env
pip install -r requirements.txt
uvicorn env.server:app --host 0.0.0.0 --port 7860
uvicorn env.server:app --port 7860
```

### Running the Agent
## Docker
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
docker build -t pipeline-env .
docker run -p 7860:7860 pipeline-env
curl http://localhost:7860/ping
```

### Running Tests
## Run the Agent
```bash
python -m pytest tests/test_env.py -v
export API_BASE_URL=http://localhost:7860
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
export HF_TOKEN=your_token
python inference.py
```

## Baseline Scores

| Task | Agent | Score |
|---|---|---|
| Task 1 | NOOP | ~0.0 |
| Task 2 | NOOP | ~0.0 |
| Task 3 | NOOP | ~0.0 |

Run `GET /baseline` to reproduce exact scores.
## Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ping` | GET | Health check |
| `/tasks` | GET | Task list with curriculum info |
| `/reset` | POST | Start episode |
| `/step` | POST | Submit action |
| `/grader` | GET | Get score 0.0–1.0 |
| `/tools` | GET | Available investigation tools |
| `/demo` | GET | Watch optimal agent solve Task 1 |
| `/replay` | GET | Replay any episode step by step |
| `/leaderboard` | GET | Score history across runs |
| `/baseline` | GET | NOOP agent scores |
| `/mcp` | POST | MCP tool discovery (JSON-RPC 2.0) |
