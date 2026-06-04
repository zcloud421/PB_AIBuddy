# Phase 5.4 — Ranking & Explain Layer (Codex Implementation Prompt)

## Context

This is **Josan**, a rule-based FCN (sell-put) recommendation tool for HK private-bank RM/IC users; end clients are HNW individuals assigned the stock on knock-in. Recommendations must be explainable and rule-based.

Phase 5.3 (shipped, in production) added: buffer-first combo picker, buffer as a first-class suitability dimension, VRP (volatility risk premium), a computed high-beta cap for the search universe, and demoted PATH_RISK to a display-only signal. `FCN_ENGINE_MODE=gated_shadow` in prod; gates are a downgrade-only overlay (`final_grade = min(weighted, gate)` — never regress this).

5.4 separates **suitability** (can the client hold it on KI → the GO/CAUTION/AVOID grade) from **attractiveness/ranking** (is this a good vol sale → ordering among suitable names), adds an RM custom-strike re-score endpoint, enforces narrative↔grade consistency, and pays down two 5.3 debts.

## ⚠️ Safety protocol

Production is `gated_shadow`. Keep it there. Do NOT flip `FCN_ENGINE_MODE` to gated_live. Validate every change with a 53-universe shadow run + the adversarial search basket (RIVN/LCID/SNAP/AFRM/UPST/CHPT/ROKU). Preserve the downgrade-only invariant. `npx tsc --noEmit` and existing tests must pass; new logic needs unit tests.

## SCOPED OUT — do NOT do the composite_score rename

The originally-planned `composite_score → explanatory_score` rename is **explicitly out of scope**. It is 142 occurrences across 26 files (API contract, DB column, mobile UI, pitch/narrative system) with zero behavior change and would force a mobile rebuild + DB migration. Keep the field name `composite_score`. Express the "this is a ranking/explanatory score, not a suitability verdict" framing in reasoning text and code comments only. If you believe a rename is unavoidable for a specific new field, introduce a NEW field rather than renaming the existing one.

## Workstream A — RM custom-strike re-score endpoint (highest value, genuinely new)

Today `getSymbolIdea` picks the engine's preferred strike. RMs quote their own strike and need to see the grade/coupon/buffer for THAT strike. There is currently no endpoint that accepts a custom strike.

- Add an endpoint (e.g. `POST /ideas/:symbol/rescore` body `{ strike, tenor_days? }`) that fetches live symbol+chain data, locates the matching listed strike (or nearest), runs the SAME scoring path with that strike, and returns the full scored result (grade, composite_score, buffer, coupon, VRP, gate_decisions, reasoning).
- Reuse the existing scoring engine — do NOT fork scoring logic. Factor a shared internal `scoreSymbolWithStrike(...)` if needed so the daily path and the rescore path share one code path.
- Respect eligibility (restricted / FORCE_AVOID return not-recommendable) exactly like getSymbolIdea.
- Mark the run/candidate as ad-hoc (`triggered_by` must NOT be 'scheduled') so it never pollutes distribution/narrative health metrics.
- Unit-test the strike-matching (exact, nearest, out-of-chain → clear error).

## Workstream B — explicit ranking layer (suitability vs attractiveness separation)

The grade (GO/CAUTION/AVOID) is suitability. Ranking among suitable names is attractiveness. Today VRP/RSI/diversification are tangled into the suitability composite.

- Introduce an explicit **ranking score** (new field on ScoringResult, e.g. `ranking_score`, nullable; do NOT rename composite_score) that orders names WITHIN a grade. It composes attractiveness signals: VRP (rich vol), premium/coupon efficiency, and diversification — NOT path/suitability gates.
- The daily showcase / `selectDailyBest` / `selectDailyRecommendationShowcase` should order by grade first, then ranking_score, then existing diversification de-dup.
- VRP should drive ranking_score, and its weight in the suitability composite should be reduced (it currently nudges the grade; suitability shouldn't depend on whether vol is a good deal). Keep the change small and validate the 53 matrix doesn't shift grades unexpectedly.

## Workstream C — RSI neutralization

`scoreRsiStrength` (scoring-engine.ts ~2284) feeds `rsiScore` into the composite (~1118). For FCN suitability, RSI should be at most a minor modifier, not a primary grade driver. Reduce its weight / role so it doesn't materially move the grade; if it carries any signal, route it to ranking_score (B), not suitability. Document the reduced role explicitly.

## Workstream D — narrative ↔ grade consistency check

Ensure the generated narrative can never contradict the grade (e.g. an AVOID with bullish GO-style pitch language). Add a validation step in the narrative pipeline that cross-checks narrative sentiment/source_quality against overall_grade and falls back to the deterministic template on mismatch. Log mismatches for observability. Extend narrative tests.

## Workstream E — pay down 5.3 debt

1. **High-beta reasoning mislabel**: the computed speculative cap co-fires on quality industrials (e.g. ETN) and the reasoning text calls them "high-beta speculative," which reads wrong to IC. Either exempt quality names or soften the reasoning copy so the label is defensible. It must not change grades, only the explanation.
2. **Static exemption list**: the high-beta flagship exemption (NVDA/AMD/AVGO/TSM/LITE/VRT) is a static allowlist. Acceptable for now — but add a code comment documenting it as known debt and the conservative false-positive behavior on unenumerated high-vol search names, so it is not silently load-bearing.

## Deliverable

PR-sized changes per workstream (A first — it's the only net-new user capability). For each: diff, unit tests, and a 53-universe shadow matrix + adversarial-search matrix showing no unintended grade shifts. Stop after A for review before continuing.
