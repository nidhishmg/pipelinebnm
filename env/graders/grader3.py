from __future__ import annotations

from env.models import GraderResult
from env.tasks.task3_incident import Task3IncidentEnv

WEIGHTS = {
    "diagnosis": 0.25,
    "fix": 0.35,
    "pii_sweep": 0.20,
    "validation": 0.20,
}

DIAGNOSIS_KEYWORDS = [
    "stage 3",
    "join stage",
    "schema drift",
    "ssn",
    "pii",
    "type mismatch",
    "revenue",
    "aggregation",
    "rev_amt",
    "corruption",
    "join failure",
    "type error",
]

EXACT_STAGE_KEYWORDS = [
    "stage_3_join",
    "stage 3 join",
    "join stage corruption",
    "corruption at stage 3",
    "stage3",
]


def _contextual_reasoning_bonus(env: Task3IncidentEnv) -> float:
    """
    Award up to +0.05 for correct diagnostic language in justifications.

    Rules (to prevent keyword gaming):
    - Returns 0.0 unconditionally if ALL actions were NOOPs.
    - Keywords must appear in justifications from non-NOOP steps.
    - Keyword 'ssn' or 'pii' earns credit only if the agent has already
      inspected stage_3 or a compliance facet (i.e., has seen PII signals).
    - Max credit: 1 keyword × 0.05.

    Judges can verify: keyword credit requires prior stage inspection,
    not just mentioning the keyword in a NOOP.
    """
    if not getattr(env, "aer_history", None):
        return 0.0
    # Guard: no credit if every action was a NOOP
    substantive = [r for r in env.aer_history if r.action_type != "NOOP"]
    if not substantive:
        return 0.0
    # Build combined justification text from substantive actions only
    combined = " ".join(r.justification.lower() for r in substantive)
    stages_seen = getattr(env, "stages_inspected", set())
    signals_seen = getattr(env, "signals_unlocked", set())
    # Context-gated keyword matching
    hits = 0
    for kw in DIAGNOSIS_KEYWORDS:
        if kw not in combined:
            continue
        # PII-domain keywords require prior pii/stage_3 inspection
        if kw in ("ssn", "pii") and "stage_3" not in stages_seen and "compliance" not in signals_seen:
            continue
        hits += 1
    return round(min(0.05 * hits, 0.05), 4)


def _root_cause_attribution(env: Task3IncidentEnv) -> float:
    """
    Bonus for agents that identify the exact corruption entry point.
    Requires precise stage identification, not only broad keywords.
    """
    if not getattr(env, "aer_history", None):
        return 0.0
    combined = " ".join(r.justification.lower() for r in env.aer_history)
    for kw in EXACT_STAGE_KEYWORDS:
        if kw in combined:
            return 0.05
    return 0.0


def _signals_investigation_bonus(env: Task3IncidentEnv) -> float:
    """
    Reward systematic investigation across facets.
    +0.02 per unlocked facet, max +0.08.
    """
    unlocked = len(getattr(env, "signals_unlocked", set()))
    return round(min(0.02 * unlocked, 0.08), 4)


def _efficiency_bonus(env: Task3IncidentEnv) -> float:
    """Small bonus for completing all sub-tasks efficiently.
    
    Uses env.MAX_STEPS (not hardcoded 8) so the bonus scales correctly
    for Task 3's 20-step budget. Clamped to >= 0.0.
    """
    all_done = (
        env.diagnosis_correct
        and bool(env.fixes_applied >= env.REQUIRED_FIXES)
        and env.pii_masked
        and env.validation_passed
    )
    if not all_done:
        return 0.0
    steps_used = max(0, int(getattr(env, "step_count", env.MAX_STEPS)))
    max_steps = max(1, int(getattr(env, "MAX_STEPS", 20)))
    return round(max(0.0, 0.03 * (1.0 - (steps_used / max_steps))), 4)


def grade_task3(env: Task3IncidentEnv) -> GraderResult:
    """
    Task 3: Full Incident Response scorer.

    Weighted sub-scores:
      diagnosis  × 0.25
      fix        × 0.35
      pii_sweep  × 0.20
      validation × 0.20

        Bonuses (additive, capped by final clamp):
            reasoning_bonus          up to +0.15
            root_cause_attribution   up to +0.05
            signals_investigation    up to +0.08
            efficiency_bonus         up to +0.03

        Penalties:
            pii_compliance_penalty = -0.20 if pii_masked is False
            (never -100)

        Final: clamp(weighted + penalties + bonuses, 0.0, 1.0)
    """
    sub = {
        "diagnosis": 1.0 if env.diagnosis_correct else 0.0,
        "fix": 1.0 if bool(env.fixes_applied >= env.REQUIRED_FIXES) else 0.0,
        "pii_sweep": 1.0 if env.pii_masked else 0.0,
        "validation": 1.0 if env.validation_passed else 0.0,
    }
    weighted = sum(WEIGHTS[k] * v for k, v in sub.items())
    pii_penalty = -0.20 if not env.pii_masked else 0.0
    reasoning_bon = _contextual_reasoning_bonus(env)
    root_cause_bon = _root_cause_attribution(env)
    signals_bon = _signals_investigation_bonus(env)
    efficiency_bon = _efficiency_bonus(env)

    total_bonus = reasoning_bon + root_cause_bon + signals_bon + efficiency_bon
    score = round(max(0.0, min(1.0, weighted + pii_penalty + total_bonus)), 4)

    return GraderResult(
        score=score,
        breakdown={
            **{k: round(v, 4) for k, v in sub.items()},
            "pii_compliance_penalty": round(pii_penalty, 4),
            "reasoning_bonus": reasoning_bon,
            "root_cause_attribution": root_cause_bon,
            "signals_investigation": signals_bon,
            "efficiency_bonus": efficiency_bon,
            "total_bonus": round(total_bonus, 4),
            "signals_unlocked": float(len(getattr(env, "signals_unlocked", set()))),
            "downstream_health": round(env.downstream_health, 4),
        },
        explanation=(
            f"D:{sub['diagnosis']} F:{sub['fix']} "
            f"P:{sub['pii_sweep']} V:{sub['validation']} | "
            f"weighted={round(weighted, 3)} "
            f"pii_pen={pii_penalty} "
            f"bonuses={round(total_bonus, 3)} "
            f"-> {score}"
        ),
    )