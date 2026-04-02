# DataOpsEnv — Production Grade Update (Phase 1-9)

DataOpsEnv has been upgraded to a robust, dynamic, robust 10/10 AI Data Engineer training environment. All hardcodes have been removed in favor of procedural generation, strict state gating, and precise multi-action evaluations.

## 🚀 Key Improvements

1. **State Isolation & Deployment Safe**: Server now warns of module-level caching and persists scores safely via multithreaded-locked `leaderboard.json` rather than memory. Added `/.well-known/env-info` for transparency.
2. **Procedural Generation**: `env/data/scenario_generator.py` replaces hardcoded traces. Tasks dynamically generate distinct permutations of column names, error types, duplicate distributions, and PII leaks, enabling robust LLM training logic free from memorization.
3. **Reward Rebalancing & Strict Scoring**:
    * Imposed strict -0.10 NOOP & Re-Inspect penalties to penalize hallucination strings or random search behavior.
    * Gated Reasoning Bonus (`+0.05`): Agent CANNOT earn reasoning bonuses for keywords (like `ssn/pii`) unless they have tangibly investigated and discovered them through facets.
    * Changed Boolean Task Gates: Task 3 replaced the singular `fix_applied` trick with a complex constraint gate `fixes_applied >= REQUIRED_FIXES` forcing actual completion logic.
4. **Dynamic Inference Bounds**: Removed fixed 8-step `MAX_STEPS`. `DataObservation` exposes task boundaries so Agents know their precise budget constraint per environment dynamically.
5. **Diversity & Benchmark Validated**: Automated tests validating >90% procedural uniqueness alongside rigorous bounded thresholds for deterministic validation NOOP agents.

## 📂 Documentation

* `docs/architecture.md`: Overview of request pathways and pipeline state.
* `docs/reward_design.md`: Explains reasoning sub-scores, scaling gates, and exact metric criteria for Judges.

## 🛠 Usage

**Run Unit Tests:**
```bash
python -m pytest tests/ -v
```

**Run Diversity Validation:**
```bash
python scripts/validate_diversity.py
```

**Run NOOP Benchmarks:**
```bash
python scripts/benchmark.py
```