# Phase 5.3 — Suitability Scoring Layer (Codex Implementation Prompt)

## Context you must internalize first

This is **Josan**, a rule-based FCN (sell-put / Fixed Coupon Note) recommendation tool for HK private-bank RM/IC users. End clients are HNW individuals. On knock-in the client is **assigned the falling stock**, so suitability ≠ S&T edge — a structurally falling but "cheap-vol" name is *unsuitable* even if vol is rich. Recommendations are **explainable and rule-based**, never a black box.

Two universes (do NOT conflate):
- **推荐池**: ~53 curated tickers (`underlyings` where `status='active'`) — drives daily screener.
- **全美股 search**: any US stock, fetched live on RM search.

Current engine architecture (post Phase 5.2b):
- `FCN_ENGINE_MODE` env: `weighted | gated_shadow | gated_live`. **Currently `gated_live` in production.**
- In gated_live, `overallGrade = min(weightedGrade, gateGrade)` (gates are a **downgrade-only** overlay — they can pull a grade down GO→CAUTION→AVOID, never up). This invariant was just fixed (commit fec6e3e) — **do not regress it.**
- 6 gates in `src/utils/fcn-gates/gates/`: buffer-floor, bearish-structure, earnings-window, fundamental-deterioration, distribution-falling-knife, path-risk. Orchestrated by `gate-orchestrator.ts → runAllGates → resolveGateGrade`. Gate grade taxonomy: `HARD_FAIL→AVOID`, `TIMING_FAIL→AVOID+wait_reason`, `SUITABILITY_FAIL→CAUTION cap`.
- Diagnostic finding that motivates 5.3: the post-fix live→shadow matrix is **perfectly diagonal** — gates currently never pull a name below where the weighted engine already put it. The gates are near-redundant. **5.3's job is to give them genuine, calibrated discrimination so they catch weighted-GO names that are actually unsuitable.**

## ⚠️ Safety protocol (MANDATORY — we are live to clients)

Because production is `gated_live`, gate-logic changes hit real recommendations immediately. Therefore:

1. **Before starting**, set `FCN_ENGINE_MODE=gated_shadow` in production (`fly secrets set FCN_ENGINE_MODE=gated_shadow -a josan-backend`). All 5.3 work observes in shadow.
2. Each workstream lands behind validation: after deploy, trigger a screener/search run and inspect the live→shadow confusion matrix + gate distribution. New gate firings must be **explainable per-symbol** (no mystery downgrades).
3. **Acceptance before re-flipping to gated_live**: ≥2 clean shadow runs where every new GO→CAUTION/AVOID gate catch is manually justified (the name IS genuinely unsuitable), and zero spurious upgrades (matrix stays downgrade-only).
4. Do NOT re-enable gated_live yourself — leave it gated_shadow and report the matrices; the human makes the flip call.

## Data availability (confirmed — build on these, don't re-plumb)

- `SymbolData.price_history: Array<{date, close}>` — sufficient for realized vol (RV) and rolling-window drawdown. Compute inside gates/scoring; **no new fetcher fields needed.**
- `StrikeData.iv: number` — per-strike annualized IV (decimal). `SymbolData.iv_rank: number | null`.
- `SymbolData.rsi_14`, `ma20/50/200`, `pct_from_52w_high`, `high_52w/low_52w` present.
- `GateInput` (`gates/shared.ts`) already carries `symbolData`, `strikeData`, `tenorDays`. **Add derived metrics inside the gate from these — do not extend the fetch layer.**

---

## Workstream A — PATH_RISK: rolling-window breach frequency (HIGHEST PRIORITY)

**Problem**: `path-risk.ts` is fully dormant — it only fires when `input.historicalMaxDrawdownPct` is passed, which nothing passes. The old raw-180d-max-drawdown approach over-fires (every volatile name has a 30%+ drawdown vs a 10–20% buffer), which is why it was disabled.

**Implement**: compute breach frequency from `symbolData.price_history` *inside the gate*:
- Lookback = last ~252 trading days (cap to available history; require ≥ ~120 points or skip gate → return null).
- Slide a window of length = `tenorDays` (the actual FCN tenor) across the lookback.
- For each window, compute peak-to-trough drawdown from the window's entry price (mimics "if FCN struck here, did spot breach the buffer during the tenor"). Count fraction of windows where intra-window drawdown from entry exceeded the **current buffer** (`bufferPct(current_price, strike)`).
- `breach_freq = breaching_windows / total_windows`.
- Fire as **SUITABILITY_FAIL (CAUTION cap)**, NOT HARD_FAIL, when `breach_freq > THRESHOLD` (start `THRESHOLD = 0.20`, i.e. buffer was historically breached in >20% of tenor-length windows). Tune against the 53-universe so it fires on genuinely path-risky names (high-beta, China ADR) but not on steady blue chips.
- Include `breach_freq`, `windows_evaluated`, `buffer_pct`, `tenor_days` in `decision.details` for explainability.
- Keep the function pure + unit-testable. Add unit tests with synthetic price paths (a steadily-rising series → 0 breaches; a sawtooth breaching series → high freq).

**Acceptance**: on the 53-universe shadow run, PATH_RISK fires on a defensible subset (expect China ADR / high-beta names), not on COST/JNJ-type blue chips. Every fire explainable from `breach_freq`.

## Workstream B — VRP (Volatility Risk Premium) replaces raw IV

**Problem**: the engine currently treats raw IV / iv_rank as "is vol attractive." That's wrong — high IV on a structurally cracking name is a *trap*, not an opportunity. The correct signal is **VRP = IV − RV** (how much the option market overprices vol vs what the stock actually realizes).

**Implement**:
- Add `computeRealizedVol(price_history, lookbackDays)` helper: annualized stdev of daily log returns over ~20–30 trading days. Pure function, unit-tested.
- `VRP = strikeData.iv − RV` (both annualized decimals).
- In `scoring-engine.ts`, introduce a VRP-based component to replace/augment the existing `iv_rank_score` / `iv_premium_score` as the "vol richness" signal. Low or negative VRP → vol is NOT genuinely rich → reduce attractiveness. Keep `iv_rank` as a secondary input, not the primary.
- Surface VRP in `ScoringResult` (new field, nullable) and in `reasoning_text` so RMs see "IV X% vs RV Y% → VRP Z%".
- **Do not** let VRP alone upgrade anything (respect downgrade-only invariant for the gate path; VRP affects the weighted score / ranking, not a gate upgrade).

**Acceptance**: names with rich genuine VRP rank above same-IV names with poor VRP. Negative-VRP names visibly penalized in score + reasoning.

## Workstream C — Buffer as a first-class citizen + buffer-first combo picker

**Problem**: `buffer-floor.ts` only hard-triggers at buffer < 10%; buffer is otherwise not a graded dimension. The strike/combo picker is not buffer-first.

**Implement**:
- Make buffer a graded scoring dimension (more buffer = more downside protection = more suitable for HNW assignment risk), not just a binary floor. Keep the <10% hard floor.
- Combo/strike picker: prefer the strike that maximizes buffer while still reaching the coupon target (current target 15% annualized, no-knockout quote per house convention). Buffer-first, coupon-satisfied — not coupon-maximizing.
- Keep changes consistent with the existing `target_coupon_pct` / `max_achievable_coupon_pct` / `target_unreachable` fields.

**Acceptance**: for two strikes that both hit the coupon target, the picker selects the higher-buffer one. `target_unreachable` still set correctly when 15% can't be reached at an acceptable buffer.

## Workstream D — RSI neutralization + diversification into score

- **RSI**: today RSI likely contributes a directional tilt. For FCN suitability, extreme-overbought is a mild caution (mean-reversion → assignment risk) but RSI should be **neutralized as a primary driver** — at most a secondary modifier, not a main score component. Make the role explicit and small.
- **Diversification**: when assembling the daily showcase / selectDailyBest, fold a diversification term into ranking so the top recommendations aren't clustered in one sector/theme/cycle-family. (Check existing `selectDailyBest` / `selectDailyRecommendationShowcase` in `ideas-service.ts` — there's already some theme/cycle-family de-dup; extend it into the score rather than post-hoc filtering if cleaner.)

## Cross-cutting requirements

- TypeScript, `npx tsc --noEmit` must pass. Follow existing code style.
- New pure helpers (RV, breach-freq, buffer grade) must have unit tests.
- Every new gate fire / score penalty must be **explainable** via `decision.details` or `reasoning_text` — no opaque numbers. This is a rule-based tool for bank IC review.
- Respect the **downgrade-only gate invariant** (`final_grade = min(weighted, gate)` in `shadow-evaluator.ts`). Gates and VRP penalties may lower grades/scores; nothing here may upgrade a grade.
- Do NOT touch the fetch layer (Massive/Finnhub/FRED) — all new metrics derive from existing `SymbolData`/`StrikeData`.
- Do NOT change `FCN_ENGINE_MODE` to gated_live — leave production in gated_shadow for the human to flip after validation.
- **Security**: if you add ANY new table, the startup guard `ensureRowLevelSecurity()` (runs last in `ensureSchemaGuards`, `src/app.ts`) auto-enables RLS on it — so new tables are secured automatically. Do NOT add policies that grant `anon`/`authenticated` access; all DB access goes through the backend's `postgres` role. If a table is created outside the startup path (e.g. a one-off script), add `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` there too. Never expose tables to Supabase's public PostgREST API.

## Deliverable

PR-sized changes per workstream (A→B→C→D, A first since it's the only fully-dormant gate). For each: the diff, the unit tests, and a short note on threshold choices + how you validated against the 53-universe shadow run.
