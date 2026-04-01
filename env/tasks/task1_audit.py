from __future__ import annotations

from pathlib import Path

import pandas as pd

from env.data.bug_injector import (
    build_logs_facet,
    build_metrics_facet,
    get_failure_signature,
    inject_bugs,
    load_scenario,
    matches_ground_truth,
)
from env.data.generator import generate_employee_dataset
from env.models import (
    AERRecord,
    ActionType,
    AlertSignal,
    ComplianceFacet,
    DagOverview,
    DataAction,
    DataObservation,
    DetectedIssue,
    StepResult,
    VisibleSignals,
)


class Task1AuditEnv:
    """Task 1 environment for data quality audit and direct remediation."""

    MAX_STEPS = 8
    TOTAL_BUGS = 5
    SCENARIO_PATH = Path(__file__).parent.parent / "data" / "scenarios" / "task1_scenario.json"

    def __init__(self) -> None:
        """Initialize Task1 mutable state containers."""
        self.df: pd.DataFrame = pd.DataFrame()
        self.ground_truth: list[dict] = []
        self.step_count: int = 0
        self.identified_bug_ids: set[str] = set()
        self.fixed_bug_ids: set[str] = set()
        self.discovered_bugs: set[str] = set()          # NEW: progressive discovery
        self.downstream_health: float = 0.0
        self.visible_signals: VisibleSignals | None = None
        self.signals_unlocked: set[str] = set()
        self.aer_history: list[AERRecord] = []
        self.step_errors: list[str] = []

    def reset(self) -> DataObservation:
        """Reset state, generate deterministic data, inject bugs, and return observation."""
        scenario = load_scenario(str(self.SCENARIO_PATH))
        clean_df = generate_employee_dataset(seed=42)
        self.df, self.ground_truth = inject_bugs(clean_df, scenario)
        self.step_count = 0
        self.identified_bug_ids = set()
        self.fixed_bug_ids = set()
        self.discovered_bugs = set()                     # NEW: starts empty
        self.downstream_health = 0.0

        failure_sig = get_failure_signature(self.ground_truth)
        initial_alert = AlertSignal(
            severity="high",
            message=f"Pipeline anomaly: {failure_sig.detection_hint}",
            risk_score=0.65,
        )
        self.visible_signals = VisibleSignals(alert=initial_alert)
        self.signals_unlocked = set()
        self.aer_history = []
        self.step_errors = []

        return self._build_observation()

    def step(self, action: DataAction) -> StepResult:
        """Apply an agent action and return the resulting transition tuple."""
        reward = 0.0
        done = False

        if action.action_type == ActionType.INSPECT:
            target = (action.target_column or "").lower()

            # --- Tool target aliases (callable tool names → facet targets) ---
            _TOOL_ALIASES = {
                "run_null_check": "metrics",
                "run_type_check": "schema",
                "run_duplicate_check": "metrics",
                "run_pii_scan": "pii",
                "run_schema_diff": "schema",
                "trace_pipeline_stage": "dag",
            }
            target = _TOOL_ALIASES.get(target, target)

            # --- Facet-level inspection (unlock observability signals) ---
            if target == "metrics" and "metrics" not in self.signals_unlocked:
                self.visible_signals.metrics = build_metrics_facet(self.df)
                self.signals_unlocked.add("metrics")
                reward += 0.05
            elif target == "logs" and "logs" not in self.signals_unlocked:
                self.visible_signals.logs = build_logs_facet(self.step_errors or ["No errors logged"])
                self.signals_unlocked.add("logs")
                reward += 0.05
            elif target == "dag" and "dag" not in self.signals_unlocked:
                self.visible_signals.dag = DagOverview(
                    current_node="stage_1_audit",
                    upstream_nodes=["ingestion"],
                    downstream_nodes=["reporting"],
                )
                self.signals_unlocked.add("dag")
                reward += 0.03
            elif target in ["pii", "ssn", "compliance"] and "compliance" not in self.signals_unlocked:
                pii_cols = [col for col in self.df.columns if "ssn" in col.lower()]
                self.visible_signals.compliance = ComplianceFacet(
                    pii_detected=len(pii_cols) > 0,
                    risky_columns=pii_cols,
                )
                self.signals_unlocked.add("compliance")
                reward += 0.05
            elif target == "schema" and "schema" not in self.signals_unlocked:
                # Reveal schema-drift bugs
                for bug in self.ground_truth:
                    if bug["type"] == "schema_drift" and bug["bug_id"] not in self.discovered_bugs:
                        self.discovered_bugs.add(bug["bug_id"])
                        reward += 0.10
                self.signals_unlocked.add("schema")
                reward += 0.05
            else:
                # --- Column-specific inspection (progressive bug discovery) ---
                found_any = False
                for bug in self.ground_truth:
                    bug_col = (bug.get("column") or "").lower()
                    if bug_col and bug_col == target and bug["bug_id"] not in self.discovered_bugs:
                        self.discovered_bugs.add(bug["bug_id"])
                        self.identified_bug_ids.add(bug["bug_id"])
                        reward += 0.15
                        found_any = True
                if not found_any:
                    reward -= 0.03  # wasted inspection step

            # Legacy: also credit inline identified_issues from agent
            for issue in (action.identified_issues or []):
                for truth in self.ground_truth:
                    if matches_ground_truth(issue, truth) and truth["bug_id"] not in self.identified_bug_ids:
                        reward += 0.15
                        self.identified_bug_ids.add(truth["bug_id"])
                        break
                else:
                    reward -= 0.05

        elif (
            action.action_type == ActionType.FILL_DEFAULT
            and action.target_column == "salary"
            and action.transformation == "fill_median"
        ):
            if "B001" not in self.fixed_bug_ids:
                median_val = self.df["salary"].median()
                self.df["salary"] = self.df["salary"].fillna(median_val)
                reward += 0.20
                self.fixed_bug_ids.add("B001")
            else:
                reward -= 0.05

        elif (
            action.action_type == ActionType.CAST_TYPE
            and action.target_column == "age"
            and action.transformation == "cast_to_int"
        ):
            action_fixed_any = False
            if "B002" not in self.fixed_bug_ids:
                self.df.loc[5, "age"] = 23
                self.fixed_bug_ids.add("B002")
                reward += 0.20
                action_fixed_any = True

            numeric_age = pd.to_numeric(self.df["age"], errors="coerce")
            invalid_mask = (numeric_age > 150) | (numeric_age < 0)
            if invalid_mask.any() and "B003" not in self.fixed_bug_ids:
                median_age = int(numeric_age[(numeric_age >= 0) & (numeric_age <= 150)].median())
                self.df.loc[invalid_mask, "age"] = median_age
                self.fixed_bug_ids.add("B003")
                reward += 0.20
                action_fixed_any = True

            if not action_fixed_any:
                reward -= 0.05

        elif action.action_type == ActionType.VALIDATE:
            action_fixed_any = False
            if "B004" not in self.fixed_bug_ids:
                current = str(self.df.loc[10, "phone"])
                digits = "".join(ch for ch in current if ch.isdigit())
                if len(digits) >= 10:
                    self.df.loc[10, "phone"] = digits[-10:]
                self.fixed_bug_ids.add("B004")
                action_fixed_any = True

            if "B005" not in self.fixed_bug_ids:
                self.df = self.df.drop_duplicates().reset_index(drop=True)
                self.fixed_bug_ids.add("B005")
                action_fixed_any = True

            if not action_fixed_any:
                reward -= 0.05

            fixed_ratio = len(self.fixed_bug_ids) / self.TOTAL_BUGS
            if len(self.fixed_bug_ids) == self.TOTAL_BUGS:
                reward += 0.30
                done = True
            else:
                reward += 0.10 * fixed_ratio

        elif action.action_type == ActionType.DROP_COLUMN:
            reward -= 0.10

        elif action.action_type == ActionType.NOOP:
            reward = 0.0

        reward = max(-0.5, min(1.0, reward))
        self.step_count += 1
        self.downstream_health = len(self.fixed_bug_ids) / self.TOTAL_BUGS
        done = done or (self.step_count >= self.MAX_STEPS)

        aer = AERRecord(
            step_id=self.step_count,
            action_type=action.action_type.value,
            target=action.target_column,
            justification=action.justification,
            reward_earned=round(reward, 4),
            issues_identified=list(self.identified_bug_ids),
            issues_fixed=list(self.fixed_bug_ids),
        )
        self.aer_history.append(aer)

        return StepResult(
            observation=self._build_observation(),
            reward=round(reward, 4),
            done=done,
            info={
                "fixed": list(self.fixed_bug_ids),
                "identified": list(self.identified_bug_ids),
                "signals_unlocked": list(self.signals_unlocked),
                "visible_signals": self.visible_signals.model_dump() if self.visible_signals else {},
                "aer_last": aer.model_dump(),
                "step": self.step_count,
            },
        )

    def _build_observation(self) -> DataObservation:
        """Construct a DataObservation from current in-memory state.

        Progressive discovery: only bugs in `discovered_bugs` AND not yet
        fixed are shown in validation_report.  At reset this is empty.
        """
        # Only show bugs the agent has actually discovered via INSPECT
        visible_bugs = [
            t for t in self.ground_truth
            if t["bug_id"] in self.discovered_bugs and t["bug_id"] not in self.fixed_bug_ids
        ]
        validation_report = [
            DetectedIssue(
                issue_type=b["type"],
                column=b.get("column"),
                description=b["description"],
                severity=b["severity"],
            )
            for b in visible_bugs
        ]

        schema_dict = {
            col: {
                "type": str(dtype),
                "nullable": bool(self.df[col].isna().any()),
            }
            for col, dtype in self.df.dtypes.items()
        }

        # Build agent_context for belief tracking
        agent_context = {
            "inspected_columns": sorted(
                {(b.get("column") or "").lower() for b in self.ground_truth if b["bug_id"] in self.discovered_bugs}
                | self.signals_unlocked
            ),
            "bugs_found": [
                f"{b['type']}:{b.get('column', 'N/A')}"
                for b in self.ground_truth
                if b["bug_id"] in self.discovered_bugs
            ],
            "bugs_fixed": [
                f"{b['type']}:{b.get('column', 'N/A')}"
                for b in self.ground_truth
                if b["bug_id"] in self.fixed_bug_ids
            ],
            "tools_available": ["metrics", "logs", "dag", "pii", "schema"],
        }

        return DataObservation(
            dataset_preview=self.df.head(10).to_dict(orient="records"),
            column_schema=schema_dict,
            pipeline_stage="AUDIT",
            validation_report=validation_report,
            time_remaining=self.MAX_STEPS - self.step_count,
            downstream_health=self.downstream_health,
            step_count=self.step_count,
            task_id=1,
            pipeline_stage_health=None,
            agent_context=agent_context,
        )

    def state(self) -> DataObservation:
        """Return current observation snapshot without side effects."""
        return self._build_observation()
