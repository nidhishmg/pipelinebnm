from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from env.graders.grader1 import grade_task1
from env.graders.grader2 import grade_task2
from env.graders.grader3 import grade_task3
from env.models import ActionType, DataAction, DataObservation, GraderResult, StepResult
from env.tasks.task1_audit import Task1AuditEnv
from env.tasks.task2_schema import Task2SchemaEnv
from env.tasks.task3_incident import Task3IncidentEnv


_envs: dict[int, object] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    _envs[1] = Task1AuditEnv()
    _envs[2] = Task2SchemaEnv()
    _envs[3] = Task3IncidentEnv()
    yield
    _envs.clear()


app = FastAPI(title="DataPipelineEnv", version="1.0.0", lifespan=lifespan)


def _get_env(task_id: int):
    if task_id not in _envs:
        raise HTTPException(status_code=404, detail=f"task_id {task_id} not found")
    return _envs[task_id]


@app.get("/ping")
def ping() -> dict[str, str]:
    try:
        return {"status": "ok"}
    except Exception:
        return {"status": "ok"}


@app.get("/health")
def health() -> dict[str, str]:
    """OpenEnv spec: health check endpoint."""
    return {"status": "healthy"}


@app.get("/metadata")
def metadata() -> dict[str, Any]:
    """OpenEnv spec: environment metadata."""
    return {
        "name": "broken-pipeline-env",
        "description": (
            "Enterprise Data Pipeline Remediation Environment — "
            "an AI agent acts as an on-call data engineer diagnosing and "
            "remediating a broken ETL pipeline with progressive discovery."
        ),
        "version": "1.0.0",
        "author": "Team BrokenPipeline",
        "tasks": 3,
        "max_steps": 8,
    }


@app.get("/schema")
def schema() -> dict[str, Any]:
    """OpenEnv spec: action, observation, and state schemas."""
    return {
        "action": DataAction.model_json_schema(),
        "observation": DataObservation.model_json_schema(),
        "state": DataObservation.model_json_schema(),
    }


@app.post("/mcp")
async def mcp(request: dict[str, Any] | None = None) -> dict[str, Any]:
    """OpenEnv spec: MCP JSON-RPC 2.0 endpoint for tool discovery."""
    req = request or {}
    method = req.get("method", "")
    req_id = req.get("id", 1)

    if method == "tools/list":
        tool_list = [
            {
                "name": "run_null_check",
                "description": "Scans all columns for NULL values.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "run_type_check",
                "description": "Checks column dtypes against expected schema.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "run_duplicate_check",
                "description": "Counts duplicate rows in dataset.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "run_pii_scan",
                "description": "Scans for PII patterns (SSN, email).",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "run_schema_diff",
                "description": "Compares current schema against expected contract.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "trace_pipeline_stage",
                "description": "Inspects a specific pipeline stage.",
                "inputSchema": {"type": "object", "properties": {}},
            },
        ]
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tool_list}}

    return {"jsonrpc": "2.0", "id": req_id, "result": {}}


@app.get("/tasks")
def list_tasks() -> dict[str, Any]:
    return {
        "tasks": [
            {"id": 1, "name": "Data Quality Audit", "difficulty": "easy", "max_steps": 8},
            {"id": 2, "name": "Schema Drift Remediation", "difficulty": "medium", "max_steps": 8},
            {"id": 3, "name": "Full Data Incident Response", "difficulty": "hard", "max_steps": 8},
        ],
        "action_schema": DataAction.model_json_schema(),
    }


@app.post("/reset", response_model=DataObservation)
def reset(task_id: int = 1) -> DataObservation:
    env = _get_env(task_id)
    obs = env.reset()
    return obs


@app.post("/step", response_model=StepResult)
def step(action: DataAction, task_id: int = 1) -> StepResult:
    env = _get_env(task_id)
    return env.step(action)


@app.get("/state")
def state(task_id: int = 1) -> dict[str, Any]:
    env = _get_env(task_id)
    obs = env.state()
    obs_dict = obs.model_dump(by_alias=True)

    # Episode replay: embed action history so judges can replay step-by-step
    aer_history = getattr(env, "aer_history", [])
    obs_dict["action_history"] = [
        {
            "step": r.step_id,
            "action": r.action_type,
            "target": r.target,
            "justification": r.justification,
            "reward": r.reward_earned,
            "bugs_discovered": r.issues_identified,
            "bugs_fixed": r.issues_fixed,
        }
        for r in aer_history
    ]
    return obs_dict


@app.get("/grader", response_model=GraderResult)
def grader(task_id: int = 1) -> GraderResult:
    if task_id == 1:
        return grade_task1(_envs[1])
    if task_id == 2:
        return grade_task2(_envs[2])
    if task_id == 3:
        return grade_task3(_envs[3])
    raise HTTPException(status_code=404, detail=f"task_id {task_id} not found")


@app.get("/baseline")
def baseline() -> dict[str, Any]:
    """Run a deterministic NOOP agent against all 3 tasks and return scores."""
    _graders = {1: grade_task1, 2: grade_task2, 3: grade_task3}
    results = {}
    for task_id in [1, 2, 3]:
        env = _get_env(task_id)
        env.reset()
        for _ in range(8):
            env.step(DataAction(action_type=ActionType.NOOP, justification="baseline"))
        results[f"task_{task_id}"] = _graders[task_id](env).score

    return {
        "agent": "noop_baseline",
        "description": "Deterministic NOOP agent — establishes lower bound score.",
        "scores": results,
    }


@app.get("/tools")
def tools() -> dict[str, Any]:
    """Return available investigation tools the agent can invoke via INSPECT."""
    return {
        "tools": [
            {
                "name": "run_null_check",
                "description": "Scans all columns for NULL values. Returns column names and null counts.",
                "action": {"action_type": "INSPECT", "target_column": "metrics"},
                "cost_steps": 1,
            },
            {
                "name": "run_type_check",
                "description": "Checks column dtypes against expected schema. Returns type mismatches.",
                "action": {"action_type": "INSPECT", "target_column": "schema"},
                "cost_steps": 1,
            },
            {
                "name": "run_duplicate_check",
                "description": "Counts duplicate rows in dataset.",
                "action": {"action_type": "INSPECT", "target_column": "metrics"},
                "cost_steps": 1,
            },
            {
                "name": "run_pii_scan",
                "description": "Scans for PII patterns (SSN, email) in all columns.",
                "action": {"action_type": "INSPECT", "target_column": "pii"},
                "cost_steps": 1,
            },
            {
                "name": "run_schema_diff",
                "description": "Compares current schema against expected contract. Shows renamed/missing columns.",
                "action": {"action_type": "INSPECT", "target_column": "schema"},
                "cost_steps": 1,
            },
            {
                "name": "trace_pipeline_stage",
                "description": "Inspects a specific pipeline stage for corruption entry point.",
                "action": {"action_type": "INSPECT", "target_column": "dag"},
                "cost_steps": 1,
            },
        ]
    }


@app.get("/replay")
def replay(task_id: int = 1) -> dict[str, Any]:
    """Return the full AER episode history for a given task so judges can replay agent decisions."""
    env = _get_env(task_id)
    aer_history = getattr(env, "aer_history", [])
    episode = []
    for record in aer_history:
        r = record.model_dump() if hasattr(record, "model_dump") else record
        episode.append({
            "step": r.get("step_id", 0),
            "action": r.get("action_type", ""),
            "target": r.get("target"),
            "justification": r.get("justification", ""),
            "reward": r.get("reward_earned", 0.0),
            "bugs_discovered": r.get("issues_identified", []),
            "bugs_fixed": r.get("issues_fixed", []),
            "health_after": getattr(env, "downstream_health", 0.0),
        })

    return {
        "task_id": task_id,
        "total_steps": len(episode),
        "final_score": getattr(env, "downstream_health", 0.0),
        "episode": episode,
    }