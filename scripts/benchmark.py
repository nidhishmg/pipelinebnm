"""
benchmark.py - Run the deterministic NOOP baseline across 100 seeds.
Useful for validating that the environment does not throw exceptions
on procedural states, and scores stay within expected bounds for 0-effort.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from env.tasks.task1_audit import Task1AuditEnv
from env.tasks.task2_schema import Task2SchemaEnv
from env.tasks.task3_incident import Task3IncidentEnv
from env.graders.grader1 import grade_task1
from env.graders.grader2 import grade_task2
from env.graders.grader3 import grade_task3
from env.models import DataAction, ActionType


def run_benchmark():
    envs = [Task1AuditEnv, Task2SchemaEnv, Task3IncidentEnv]
    graders = [grade_task1, grade_task2, grade_task3]

    print("Running Baseline Benchmark on 25 episodes per task...")
    
    for i, (env_cls, grader) in enumerate(zip(envs, graders)):
        task_id = i + 1
        scores = []
        for seed in range(25):
            env = env_cls()
            import random
            random.seed(seed)  # procedural seed inside reset will pick up random state
            env.reset()
            max_s = getattr(env, "MAX_STEPS", 8)
            for _ in range(max_s):
                env.step(DataAction(action_type=ActionType.NOOP, justification="baseline test"))
            result = grader(env)
            scores.append(result.score)
        
        avg = sum(scores) / len(scores)
        mx = max(scores)
        print(f"Task {task_id}: Avg Score = {avg:.3f}, Max = {mx:.3f}  (Passes threshold check: {mx <= 0.3})")

if __name__ == "__main__":
    run_benchmark()
