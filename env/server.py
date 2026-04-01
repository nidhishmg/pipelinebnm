from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from env.graders.grader1 import grade_task1
from env.graders.grader2 import grade_task2
from env.graders.grader3 import grade_task3
from env.models import DataAction, DataObservation, GraderResult, StepResult
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


@app.get("/tasks")
def list_tasks() -> dict[str, list[dict[str, Any]]]:
    return {
        "tasks": [
            {"id": 1, "name": "Data Quality Audit", "difficulty": "easy", "max_steps": 8},
            {"id": 2, "name": "Schema Drift Remediation", "difficulty": "medium", "max_steps": 8},
            {"id": 3, "name": "Full Data Incident Response", "difficulty": "hard", "max_steps": 8},
        ]
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


@app.get("/state", response_model=DataObservation)
def state(task_id: int = 1) -> DataObservation:
    return _get_env(task_id).state()


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
    return {
        "description": "NOOP agent — scores ~0.0 on all tasks.",
        "scores": {"task_1": 0.0, "task_2": 0.0, "task_3": 0.0},
    }