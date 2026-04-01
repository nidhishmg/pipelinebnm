from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from env.data.bug_injector import get_failure_signature, inject_bugs, load_scenario
from env.data.generator import generate_employee_dataset
from env.models import (
    AERRecord,
    ActionType,
    AlertSignal,
    DagOverview,
    DataAction,
    DataObservation,
    DetectedIssue,
    StepResult,
    VisibleSignals,
)


class Task2SchemaEnv:
    """Task 2 environment for schema drift diagnosis and remediation."""

    MAX_STEPS = 8
    TOTAL_BUGS = 3
    SCENARIO_PATH = Path(__file__).parent.parent / "data" / "scenarios" / "task2_scenario.json"

    def __init__(self) -> None:
        """Initialize dependency graph and task state containers."""
        with self.SCENARIO_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        self.COLUMN_DEPENDENCIES: dict[str, list[str]] = payload.get("column_dependencies", {})

        self.df: pd.DataFrame = pd.DataFrame()
        self.ground_truth: list[dict] = []
        self.step_count: int = 0
        self.fixed_bug_ids: set[str] = set()
        self.downstream_health: float = 0.0
        self.blast_events: int = 0
        self.visible_signals: VisibleSignals | None = None
        self.signals_unlocked: set[str] = set()
        self.aer_history: list[AERRecord] = []

    def reset(self) -> DataObservation:
        """Reset state and initialize a fresh corrupted Task2 dataframe."""
        scenario_bugs = load_scenario(str(self.SCENARIO_PATH))
        clean_df = generate_employee_dataset(seed=42)
        self.df, self.ground_truth = inject_bugs(clean_df, scenario_bugs)
        self.step_count = 0
        self.fixed_bug_ids = set()
        self.downstream_health = 0.0
        self.blast_events: int = 0

        failure_sig = get_failure_signature(self.ground_truth)
        initial_alert = AlertSignal(
            severity="high",
            message=f"Schema drift detected: {failure_sig.detection_hint}",
            risk_score=0.78,
        )
        self.visible_signals = VisibleSignals(
            alert=initial_alert,
            dag=DagOverview(
                current_node="stage_2_schema_validation",
                upstream_nodes=["stage_1_ingest"],
                downstream_nodes=["stage_3_join", "stage_4_aggregate"],
            ),
        )
        self.signals_unlocked = {"dag"}
        self.aer_history = []

        return self._build_observation()

    def _rows_passing(self) -> int:
        """Count rows passing essential schema conditions in current dataframe."""
        if self.df.empty:
            return 0

        expected_columns = {
            "employee_id",
            "name",
            "age",
            "salary",
            "department",
            "phone",
            "ssn",
            "hire_date",
            "consent_flag",
        }
        if not expected_columns.issubset(set(self.df.columns)):
            return 0

        salary_ok = pd.to_numeric(self.df["salary"], errors="coerce").notna()
        age_ok = pd.to_numeric(self.df["age"], errors="coerce").between(0, 120, inclusive="both")
        hire_ok = pd.to_datetime(self.df["hire_date"], errors="coerce").notna()
        consent_ok = self.df["consent_flag"].notna()
        return int((salary_ok & age_ok & hire_ok & consent_ok).sum())

    def step(self, action: DataAction) -> StepResult:
        """Apply one schema remediation step and return transition output."""
        reward = 0.0
        done = False

        scenario_bugs = load_scenario(str(self.SCENARIO_PATH))
        expected_renames = {
            b["new_col"]: b["old_col"]
            for b in scenario_bugs
            if b.get("type") == "schema_drift"
        }

        if action.action_type == ActionType.DROP_COLUMN:
            dependents = self.COLUMN_DEPENDENCIES.get(action.target_column, [])
            if dependents:
                penalty = -0.10 * len(dependents)
                self.downstream_health = max(0.0, self.downstream_health - 0.15 * len(dependents))
                self.blast_events += 1
                reward += penalty
            else:
                reward -= 0.10

        elif action.action_type == ActionType.RENAME_COLUMN:
            if action.target_column in expected_renames and action.transformation == expected_renames[action.target_column]:
                src = action.target_column
                dst = action.transformation
                if src in self.df.columns and dst not in self.df.columns:
                    self.df.rename(columns={src: dst}, inplace=True)
                for bug in self.ground_truth:
                    if bug["type"] == "schema_drift" and bug.get("column") == dst:
                        self.fixed_bug_ids.add(bug["bug_id"])
                reward += 0.20
            else:
                reward -= 0.10

        elif action.action_type == ActionType.CAST_TYPE:
            if action.target_column in {"hire_date", "dob_date"} and action.transformation == "cast_to_date":
                col = "hire_date" if "hire_date" in self.df.columns else "dob_date"
                if col in self.df.columns:
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce").dt.strftime("%Y-%m-%d")
                    self.fixed_bug_ids.add("B002")
                    reward += 0.20
                else:
                    reward -= 0.10
            else:
                reward -= 0.10

        elif action.action_type == ActionType.FILL_DEFAULT:
            if action.target_column == "consent_flag" and "consent_flag" in self.df.columns:
                self.df["consent_flag"] = self.df["consent_flag"].fillna(False)
                self.fixed_bug_ids.add("B003")
                reward += 0.20
            else:
                reward -= 0.10

        elif action.action_type == ActionType.VALIDATE:
            rows_passing = self._rows_passing()
            reward += 0.25 * (rows_passing / max(len(self.df), 1))
            if len(self.fixed_bug_ids) == self.TOTAL_BUGS:
                done = True
                reward += 0.05

        elif action.action_type == ActionType.NOOP:
            reward = 0.0

        else:
            reward -= 0.10

        reward = max(-0.5, min(1.0, reward))
        self.step_count += 1
        self.downstream_health = len(self.fixed_bug_ids) / self.TOTAL_BUGS
        done = done or (self.step_count >= self.MAX_STEPS)

        return StepResult(
            observation=self._build_observation(),
            reward=round(reward, 4),
            done=done,
            info={"blast_events": self.blast_events, "fixed": list(self.fixed_bug_ids)},
        )

    def _build_observation(self) -> DataObservation:
        """Construct DataObservation from current dataframe and unresolved bugs."""
        unresolved = [t for t in self.ground_truth if t["bug_id"] not in self.fixed_bug_ids]
        validation_report = [
            DetectedIssue(
                issue_type=b["type"],
                column=b.get("column"),
                description=b["description"],
                severity=b["severity"],
            )
            for b in unresolved
        ]

        schema_dict = {
            col: {"type": str(dtype), "nullable": bool(self.df[col].isna().any())}
            for col, dtype in self.df.dtypes.items()
        }

        return DataObservation(
            dataset_preview=self.df.head(10).to_dict(orient="records"),
            column_schema=schema_dict,
            pipeline_stage="SCHEMA_REMEDIATION",
            validation_report=validation_report,
            time_remaining=self.MAX_STEPS - self.step_count,
            downstream_health=self.downstream_health,
            step_count=self.step_count,
            task_id=2,
            pipeline_stage_health=None,
        )

    def state(self) -> DataObservation:
        """Return current state without changing environment variables."""
        return self._build_observation()
