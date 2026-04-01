from __future__ import annotations

from pathlib import Path

import pandas as pd

from env.data.bug_injector import (
    build_logs_facet,
    build_metrics_facet,
    get_failure_signature,
    inject_bugs,
    load_scenario,
)
from env.data.generator import generate_employee_dataset
from env.models import (
    AERRecord,
    ActionType,
    AlertSignal,
    ComplianceFacet,
    DataAction,
    DataObservation,
    DetectedIssue,
    MetricsFacet,
    StepResult,
    VisibleSignals,
)


class Task3IncidentEnv:
    """Task 3 environment for full data incident response handling."""

    MAX_STEPS = 8
    SCENARIO_PATH = Path(__file__).parent.parent / "data" / "scenarios" / "task3_scenario.json"
    CORRECT_DIAGNOSIS_KEYWORDS = [
        "stage 3",
        "join stage",
        "schema drift",
        "ssn",
        "pii",
        "type mismatch",
        "revenue",
        "aggregation",
    ]

    # ----- stage inspection results (progressive clues) -----
    STAGE_CLUES: dict[str, dict] = {
        "stage_5": {
            "message": "Output revenue: 2.3M. Expected: 1.8M. Anomaly: +28%",
            "reward": 0.05,
            "bugs": [],
        },
        "stage_4": {
            "message": (
                "Aggregation layer: 203 rows input, 203 rows output. "
                "Duplicate rows detected: 3 extra rows."
            ),
            "reward": 0.10,
            "bugs": ["B004"],
        },
        "stage_3": {
            "message": (
                "Join stage: schema mismatch detected. "
                "revenue_amount renamed to rev_amt. "
                "rev_amt dtype: object (should be float64). "
                "SSN column present in output (PII violation)"
            ),
            "reward": 0.15,
            "bugs": ["B001", "B002", "B003"],
        },
        "stage_2": {
            "message": "Clean stage: no anomalies detected.",
            "reward": 0.02,
            "bugs": [],
        },
        "stage_1": {
            "message": "Ingest stage: data loaded successfully. No anomalies.",
            "reward": 0.02,
            "bugs": [],
        },
    }

    def __init__(self) -> None:
        """Initialize mutable state containers for incident simulation."""
        self.df: pd.DataFrame = pd.DataFrame()
        self.ground_truth: list[dict] = []
        self.step_count: int = 0

        self.diagnosis_correct: bool = False
        self.fix_applied: bool = False
        self.pii_masked: bool = False
        self.validation_passed: bool = False

        self.pipeline_stage_health: dict[str, float] = {}
        self.downstream_health: float = 0.0
        self.zombie_partition_active: bool = False
        self.silent_drop_active: bool = False
        self.visible_signals: VisibleSignals | None = None
        self.signals_unlocked: set[str] = set()
        self.discovered_bugs: set[str] = set()          # NEW: progressive discovery
        self.stages_inspected: set[str] = set()         # NEW: track stage investigation
        self.aer_history: list[AERRecord] = []

    def reset(self) -> DataObservation:
        """Reset state, build deterministic dataset, inject bugs, and return observation."""
        clean_df = generate_employee_dataset(seed=42)
        clean_df["revenue_amount"] = (clean_df["salary"].astype(float) * 1.35).round(2)
        scenario_bugs = load_scenario(str(self.SCENARIO_PATH))
        self.df, self.ground_truth = inject_bugs(clean_df, scenario_bugs)

        self.step_count = 0
        self.diagnosis_correct = False
        self.fix_applied = False
        self.pii_masked = False
        self.validation_passed = False
        self.zombie_partition_active = False
        self.silent_drop_active = False
        self.discovered_bugs = set()                     # NEW: starts empty
        self.stages_inspected = set()

        for bug in self.ground_truth:
            if bug["type"] == "duplicate_rows":
                self.silent_drop_active = True
            if bug["type"] == "pii_leak":
                self.zombie_partition_active = True

        failure_sig = get_failure_signature(self.ground_truth)
        initial_alert = AlertSignal(
            severity="critical",
            message=f"PRODUCTION INCIDENT: {failure_sig.detection_hint}. Revenue figures anomalous. CEO asking questions.",
            risk_score=0.91,
        )
        self.visible_signals = VisibleSignals(alert=initial_alert)
        self.signals_unlocked = set()
        self.aer_history = []

        self.pipeline_stage_health = {
            "stage_1_ingest": 1.0,
            "stage_2_clean": 1.0,
            "stage_3_join": 0.0,
            "stage_4_aggregate": 0.3,
            "stage_5_output": 0.0,
        }
        self.downstream_health = sum(self.pipeline_stage_health.values()) / 5
        return self._build_observation()

    def step(self, action: DataAction) -> StepResult:
        """Apply one incident response action and return the resulting transition."""
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
            }
            # Only apply alias if target is not a stage reference
            if target in _TOOL_ALIASES:
                target = _TOOL_ALIASES[target]

            # --- Stage-by-stage investigation (multi-stage tracing) ---
            stage_key = None
            for sk in self.STAGE_CLUES:
                # Match "stage_5", "stage_5_output", "stage5", etc.
                if target.replace("_", "").replace(" ", "").startswith(
                    sk.replace("_", "")
                ):
                    stage_key = sk
                    break

            if stage_key and stage_key not in self.stages_inspected:
                clue = self.STAGE_CLUES[stage_key]
                self.stages_inspected.add(stage_key)
                reward += clue["reward"]
                # Discover bugs associated with this stage
                for bug_id in clue["bugs"]:
                    if bug_id not in self.discovered_bugs:
                        self.discovered_bugs.add(bug_id)
                # If agent reached stage_3, mark diagnosis correct
                if stage_key == "stage_3":
                    self.diagnosis_correct = True
                    self.pipeline_stage_health["stage_3_join"] = 0.5

            # --- Facet-level inspection ---
            elif target in ["metrics", "row_count", "revenue"] and "metrics" not in self.signals_unlocked:
                metrics = build_metrics_facet(self.df)
                if self.zombie_partition_active:
                    metrics = MetricsFacet(
                        row_count=metrics.row_count,
                        historical_avg=metrics.historical_avg,
                        null_ratio=metrics.null_ratio,
                        storage_bytes=0,
                    )
                self.visible_signals.metrics = metrics
                self.signals_unlocked.add("metrics")
                reward += 0.05

            elif target in ["logs", "join"] and "logs" not in self.signals_unlocked:
                self.visible_signals.logs = build_logs_facet(
                    [
                        "JoinError: column rev_amt not found in right table",
                        "TypeError: cannot convert str to float64",
                        "Warning: SSN column propagated to output",
                    ],
                    status="failed",
                )
                self.signals_unlocked.add("logs")
                reward += 0.05

            elif target in ["pii", "ssn", "compliance"] and "compliance" not in self.signals_unlocked:
                self.visible_signals.compliance = ComplianceFacet(
                    pii_detected=not self.pii_masked,
                    risky_columns=["ssn"] if not self.pii_masked else [],
                )
                self.signals_unlocked.add("compliance")
                reward += 0.03

            elif target == "dag" and "dag" not in self.signals_unlocked:
                from env.models import DagOverview
                self.visible_signals.dag = DagOverview(
                    current_node="stage_5_output",
                    upstream_nodes=["stage_4_aggregate", "stage_3_join", "stage_2_clean", "stage_1_ingest"],
                    downstream_nodes=[],
                )
                self.signals_unlocked.add("dag")
                reward += 0.03

            else:
                # Column-specific / keyword-based diagnosis (legacy support)
                justification_lower = action.justification.lower()
                keyword_hits = sum(1 for kw in self.CORRECT_DIAGNOSIS_KEYWORDS if kw in justification_lower)
                target_relevant = target in ["rev_amt", "revenue_amount", "ssn", ""]

                if keyword_hits >= 2 and target_relevant:
                    self.diagnosis_correct = True
                    self.pipeline_stage_health["stage_3_join"] = 0.5
                    reward += 0.25
                elif keyword_hits >= 1:
                    reward += min(0.05 * keyword_hits, 0.15)
                else:
                    # Column-specific bug discovery
                    found_any = False
                    for bug in self.ground_truth:
                        bug_col = (bug.get("column") or "").lower()
                        if bug_col and bug_col == target and bug["bug_id"] not in self.discovered_bugs:
                            self.discovered_bugs.add(bug["bug_id"])
                            reward += 0.15
                            found_any = True
                    if not found_any:
                        reward -= 0.03

        elif action.action_type == ActionType.RENAME_COLUMN:
            if action.target_column == "rev_amt":
                if "rev_amt" in self.df.columns and "revenue_amount" not in self.df.columns:
                    self.df.rename(columns={"rev_amt": "revenue_amount"}, inplace=True)
                reward += 0.15
                if self.diagnosis_correct:
                    reward += 0.05
            else:
                reward -= 0.10

        elif action.action_type == ActionType.CAST_TYPE:
            if action.target_column in ["rev_amt", "revenue_amount"] and action.transformation == "cast_to_float":
                col = "revenue_amount" if "revenue_amount" in self.df.columns else "rev_amt"
                if col in self.df.columns:
                    self.df[col] = pd.to_numeric(self.df[col], errors="coerce").astype(float)
                    self.fix_applied = True
                    reward += 0.20
                    self.pipeline_stage_health["stage_3_join"] = 1.0
                    self.pipeline_stage_health["stage_4_aggregate"] = 0.8
                else:
                    reward -= 0.10
            else:
                reward -= 0.10

        elif action.action_type == ActionType.MASK_PII:
            if action.target_column == "ssn" and "ssn" in self.df.columns:
                self.df["ssn"] = self.df["ssn"].astype(str).str.replace(r"\d", "X", regex=True)
                self.pii_masked = True
                reward += 0.20
            else:
                reward -= 0.10

        elif action.action_type == ActionType.VALIDATE:
            if self.fix_applied and self.pii_masked:
                self.validation_passed = True
                self.pipeline_stage_health["stage_4_aggregate"] = 1.0
                self.pipeline_stage_health["stage_5_output"] = 1.0
                reward += 0.30
                done = True
            else:
                stages_above_half = sum(1 for v in self.pipeline_stage_health.values() if v >= 0.5)
                reward += 0.05 * (stages_above_half / 5)

        elif action.action_type == ActionType.NOOP:
            reward = 0.0

        else:
            reward -= 0.10

        if len(self.signals_unlocked) >= 2 and action.action_type in [
            ActionType.CAST_TYPE,
            ActionType.MASK_PII,
            ActionType.VALIDATE,
        ]:
            process_bonus = 0.05 * (len(self.signals_unlocked) / 4)
            reward += process_bonus

        self.downstream_health = sum(self.pipeline_stage_health.values()) / 5
        reward = max(-0.5, min(1.0, reward))
        self.step_count += 1
        done = done or (self.step_count >= self.MAX_STEPS)

        aer = AERRecord(
            step_id=self.step_count,
            action_type=action.action_type.value,
            target=action.target_column,
            justification=action.justification,
            reward_earned=round(reward, 4),
            issues_identified=[bug["bug_id"] for bug in self.ground_truth if bug["bug_id"] in self.discovered_bugs],
            issues_fixed=[
                bug["bug_id"]
                for bug in self.ground_truth
                if (bug["type"] == "type_corruption" and self.fix_applied)
                or (bug["type"] == "pii_leak" and self.pii_masked)
            ],
        )
        self.aer_history.append(aer)

        return StepResult(
            observation=self._build_observation(),
            reward=round(reward, 4),
            done=done,
            info={
                "diagnosis_correct": self.diagnosis_correct,
                "fix_applied": self.fix_applied,
                "pii_masked": self.pii_masked,
                "validation_passed": self.validation_passed,
                "signals_unlocked": list(self.signals_unlocked),
                "stages_inspected": list(self.stages_inspected),
                "visible_signals": self.visible_signals.model_dump() if self.visible_signals else {},
                "aer_last": aer.model_dump(),
            },
        )

    def _build_observation(self) -> DataObservation:
        """Build observation for current task state.

        Progressive discovery: only bugs in `discovered_bugs` are shown.
        At reset this is empty — agent sees only the alert.
        """
        schema_dict = {
            col: {"type": str(dtype), "nullable": bool(self.df[col].isna().any())}
            for col, dtype in self.df.dtypes.items()
        }

        # Only show bugs the agent has discovered through stage inspection
        visible_bugs = [
            truth for truth in self.ground_truth
            if truth["bug_id"] in self.discovered_bugs
        ]
        validation_report = [
            DetectedIssue(
                issue_type=truth["type"],
                column=truth.get("column"),
                description=truth["description"],
                severity=truth["severity"],
            )
            for truth in visible_bugs
        ]

        # Determine which stages are classified
        stages_corrupted = []
        stages_cleared = []
        for stage in self.stages_inspected:
            if self.STAGE_CLUES[stage]["bugs"]:
                stages_corrupted.append(stage)
            else:
                stages_cleared.append(stage)

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
            "stages_inspected": sorted(self.stages_inspected),
            "stages_cleared": sorted(stages_cleared),
            "stages_corrupted": sorted(stages_corrupted),
            "recommended_next": self._recommend_next(),
            "tools_available": ["metrics", "logs", "dag", "pii", "stage_5", "stage_4", "stage_3", "stage_2", "stage_1"],
        }

        return DataObservation(
            dataset_preview=self.df.head(10).to_dict(orient="records"),
            column_schema=schema_dict,
            pipeline_stage="stage_5_output",
            validation_report=validation_report,
            time_remaining=self.MAX_STEPS - self.step_count,
            downstream_health=self.downstream_health,
            step_count=self.step_count,
            task_id=3,
            pipeline_stage_health=dict(self.pipeline_stage_health),
            agent_context=agent_context,
        )

    def _recommend_next(self) -> str:
        """Generate a contextual hint for what the agent should do next."""
        if not self.stages_inspected:
            return "Start by inspecting stage_5 to see the output anomaly."
        if "stage_5" in self.stages_inspected and "stage_4" not in self.stages_inspected:
            return "Trace back: inspect stage_4 to check aggregation."
        if "stage_4" in self.stages_inspected and "stage_3" not in self.stages_inspected:
            return "Trace back: inspect stage_3 to find corruption entry."
        if self.discovered_bugs and not self.fix_applied:
            return "Bugs discovered. Apply fixes: CAST_TYPE rev_amt, MASK_PII ssn."
        if self.fix_applied and not self.pii_masked:
            return "Fix applied. MASK_PII on ssn column next."
        if self.fix_applied and self.pii_masked and not self.validation_passed:
            return "All fixes applied. Run VALIDATE to confirm."
        return "Investigation complete."

    def state(self) -> DataObservation:
        """Return current observation snapshot without side effects."""
        return self._build_observation()
