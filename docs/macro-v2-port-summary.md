# macro-v2 Credit-REGIME Port — Summary & Comparison

**Date:** 2026-06-07
**Status:** Stage 1 + Stage 2 shipped to production, real-data validated. Awaiting matured audit data (~21 trading days) for empirical predictive-power verdict.

---

## 1. Why this happened

Question raised: *is the other project's macro-v2 (3-layer credit-regime early-warning) more predictive than Josan's current macro system?*

**Key discovery:** Josan's macro is not unrelated to macro-v2 — `src/services/macro-regime/types.ts` states it was *"sourced from a Portfolio Optimization project that went through 9 rounds of audit."* macro-v2 (in `portfolio_tool/`) is almost certainly the **newer generation of the same lineage** Josan forked earlier. So this was an **incremental upgrade of a shared ancestor**, not a paradigm replacement.

---

## 2. Josan's macro system (pre-port)

- **Data:** FRED (HY OAS `BAMLH0A0HYM2`, DGS10/2/3, VIXCLS) + Massive (QQQ/NDX breadth, SOX, AI cohort, concentration).
- **Logic:** 9 indicators (HY_OAS, YIELD_CURVE, VIX, DGS10 level, DGS10 4w shock, CONCENTRATION, AI_BREADTH, SOX_200DMA, BROAD_BREADTH) → per-indicator severity → **aggregated single overall severity** (Healthy/Neutral/Warning/Critical) + escalation/guardrail/persistence; side monitors (credit_funding = KBE + HY-accel + funding-proxy; ai_cloud_stress) + fundamental_modifier.
- **Output:** `MacroRegimeSnapshot` → (a) mobile Markets tab, (b) FCN gates (`MACRO_CREDIT_CRISIS` HARD_FAIL / `MACRO_REGIME_CRITICAL_CAP` / `MACRO_AI_CAPEX_STRESS` cap GO grades).
- **Note:** Josan caps FCN grades; it does NOT trim portfolios. macro-v2's "trim authority" layer therefore does not apply.

## 3. Gap analysis vs macro-v2

| Capability | Josan (pre-port) | macro-v2 | Gap |
|---|---|---|---|
| HY OAS | ✅ | ✅ | — |
| **CCC OAS + CCC-leads-HY** | ❌ | ✅ core | **missing** (earliest credit-quiet break signal) |
| **credit-equity divergence** | ❌ | ✅ | **missing** (equity highs while credit leaks) |
| breadth / concentration / AI breadth | ✅ own layer | ✅ TOP-ZONE | rough parity; TOP-ZONE mostly redundant |
| rates / VIX / curve | ✅ | ✅ | — |
| **authority separation / noise filter** | ❌ single aggregate severity | ✅ only credit-break acts; ≥2-3 corroboration | **missing** (Josan over-fires: any indicator → Warning bumps overall) |
| **forward self-audit** (backfill drawdown to validate) | ❌ | ✅ | **missing** (no feedback loop to prove predictive power) |

## 4. What was ported (AUGMENT, not replace)

Decision: **do not replace** the mature 9-indicator trunk (same lineage, deeply wired to FCN gates + mobile). Graft only the credit-break detection core + the self-audit loop.

### Stage 1 — credit-break leading signals (commit on 2026-06-07)
- Added CCC OAS fetch (FRED `BAMLH0A3HYC`, free).
- `evaluateCccLeadsHy` — CCC 4w delta vs HY 4w delta; thresholds ~25/50/100bp.
- `evaluateCreditEquityDivergence` — QQQ near 60d high + HY/CCC widening.
- `classifyCreditRegimeState` — NOISE / BREAK_FORMING / BREAK. **Noise filter: single signal → max BREAK_FORMING; BREAK requires ≥2 corroborating credit signals.**
- Folded into `credit_funding_stress.overall_score` so the existing `MACRO_CREDIT_CRISIS` gate benefits. Additive fields only; no schema/gate/mobile change.

### Stage 2 — forward self-audit (the loop Josan never had)
- `macro_regime_audit_log` table (via `ensureSchemaGuards` + `ensureRowLevelSecurity`). Each snapshot appends regime_state + leading_flags + severity + credit sub-scores.
- Backfill matures records after 21 trading days, pulls QQQ forward path, computes forward max-drawdown / realized vol, classifies TP/FP/TN/FN (stress = ≥5% drawdown OR ≥25% vol).
- `check-macro-credit-model-health` runner + admin endpoint + GitHub Actions cron → Telegram advisory on FP clusters (≥2). **Never auto-downgrades the model.**

## 5. Real-data validation (production, 2026-06-05/06-07)

- ✅ CCC fetch works: CCC 947bp / HY 275bp; CCC +32bp/4w while HY −4bp.
- ✅ Calibration not over-firing: faint divergence → score 1.
- ✅ **Noise filter validated live:** single signal → NOISE; two corroborating (CCC-leads 1 + divergence 1, QQQ −0.7% from high) → **BREAK_FORMING**. As equity later pulled off its high, divergence → 0 → back to NOISE — correct dynamic behavior.
- ✅ Gate-safe: credit_funding overall = watch (1), regime_state is advisory (does not drive the FCN gate, which keys on score-based status). No FCN blocking.
- ✅ Stage 2 loop end-to-end in prod: snapshot appended an audit record; backfill correctly defers immature record (insufficient_data); model-health runner clean.
- 🔑 **Substantive win:** the system surfaced a real, faint credit-equity divergence (equity at highs, CCC leaking while HY calm, against a SOX +71%-vs-200DMA frothy backdrop) that the old HY-only view (275bp "Healthy") was completely blind to.

## 6. The honest verdict on "more predictive?"

macro-v2's **design** is more likely predictive on two concrete axes Josan lacked: (1) earlier credit-break detection (CCC leads HY + credit-equity divergence), (2) fewer false positives (authority separation + ≥2-signal noise filter vs single-aggregate severity).

But **predictive power is empirical** and neither system had ever been forward-validated. The port now installs the machine that can answer it (Stage 2 self-audit). **The answer requires letting the audit log accrue ~21 trading days for the first matured TP/FP cohort.**

## 7. Outstanding / optional (none blocking)

- Revisit stress drawdown threshold 5% → ~8–10% once matured data accrues (5% flatters the model).
- Calibrate CCC-leads (25/50/100bp) + divergence thresholds using the audit data once available.
- TOP-ZONE / non-credit-severe-review: skipped — Josan has near-equivalents (BROAD_BREADTH/AI_BREADTH/CONCENTRATION; guardrail/escalation).
- Portfolio trim mapping: N/A — Josan caps FCN grades, does not trim.

## 8. Next checkpoint

In ~3 weeks, read the first `macro-credit-model-health` Telegram report (matured TP/FP). That is when Josan's own real data — not design intuition — answers whether the credit-break early-warning is predictive.
