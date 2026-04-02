"""
Tests for Grader 2 - Schema Remediation Task
"""
import pytest
from env.tasks.task2_schema import Task2SchemaEnv
from env.graders.grader2 import grade_task2
from env.models import DataAction, ActionType

def test_grader2_baseline_noop():
    env = Task2SchemaEnv()
    env.reset()
    for _ in range(8):
        env.step(DataAction(action_type=ActionType.NOOP, justification="test"))
    result = grade_task2(env)
    assert result.score <= 0.2  # Should score poorly

def test_grader2_blast_radius_penalty():
    env = Task2SchemaEnv()
    env.reset()
    env.step(DataAction(action_type=ActionType.DROP_COLUMN, target_column="salary", justification="Drop to penalize"))
    result = grade_task2(env)
    assert result.breakdown["blast_penalty"] < 0.0

def test_grader2_perfect_run():
    env = Task2SchemaEnv()
    # Override with known scenario to ensure deterministic correct answers
    import os
    from pathlib import Path
    scenario = str(Path(__file__).parent.parent / "env" / "data" / "scenarios" / "task2_scenario.json")
    env.reset(scenario_override=scenario)
    
    # 1. Discover Schema
    env.step(DataAction(action_type=ActionType.INSPECT, target_column="schema"))
    # 2. Rename column (e.g. jnd_dt -> hire_date)
    # The scenario has: jnd_dt -> hire_date, depart -> department, type bug on age
    env.step(DataAction(action_type=ActionType.RENAME_COLUMN, target_column="jnd_dt", transformation="hire_date"))
    env.step(DataAction(action_type=ActionType.RENAME_COLUMN, target_column="depart", transformation="department"))
    env.step(DataAction(action_type=ActionType.CAST_TYPE, target_column="age", transformation="cast_to_int"))
    env.step(DataAction(action_type=ActionType.VALIDATE))
    
    result = grade_task2(env)
    assert result.score > 0.8  # Should score highly
