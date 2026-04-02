# DataPipelineEnv Architecture

## System Overview

```mermaid
graph TD
    A["inference.py / External Agent"] -->|"POST /reset?task_id=N"| B["server.py (FastAPI)"]
    A -->|"POST /step"| B
    A -->|"GET /grader"| B
    B -->|"task_id=1"| C["tasks/task1_audit.py"]
    B -->|"task_id=2"| D["tasks/task2_schema.py"]
    B -->|"task_id=3"| E["tasks/task3_incident.py"]
    C --> F["data/generator.py"]
    C --> G["data/scenario_generator.py"]
    D --> F
    D --> G
    E --> F
    E --> G
    G -->|"fallback for /demo"| H["data/scenarios/*.json"]
    C --> I["data/bug_injector.py"]
    D --> I
    E --> I
    B -->|"grade_task1/2/3"| J["graders/grader1-3.py"]
    J --> K["leaderboard.json (file-backed)"]
    B --> K
    B --> L["/.well-known/env-info"]
```

## Component Responsibilities

| Component | Role |
|-----------|------|
| `server.py` | FastAPI app, routes, leaderboard persistence |
| `tasks/task{1,2,3}_*.py` | Environment state machine, step logic, reward shaping |
| `data/generator.py` | Seed-parameterized clean employee dataset |
| `data/scenario_generator.py` | Procedural bug injection spec from seed + task_id |
| `data/bug_injector.py` | Applies bug spec to DataFrame, builds ground_truth |
| `graders/grader{1,2,3}.py` | Stateless episode scorer, reads env attributes |
| `models.py` | Pydantic models for API I/O |
| `inference.py` | LLM agent loop (OpenAI-compatible) |

## Key Design Decisions

### Progressive Discovery
Agents start with an empty `discovered_bugs` set. `validation_report` only shows bugs the agent has explicitly surfaced via INSPECT. This enforces the investigation-first protocol.

### Procedural Generation
All live `/reset` calls use `generate_scenario(seed, task_id)`. Static JSON files are reserved for `/demo` only. This prevents memorization of a fixed 7-scenario lookup table.

### State Isolation
`_envs` is a module-level dict. The server runs with `--workers 1`. For multi-worker support, replace with Redis-backed sessions.

### Leaderboard Persistence
`leaderboard.json` is written on every `/grader` and `/record_score` call, protected by `threading.Lock()`. Survives soft restarts.
