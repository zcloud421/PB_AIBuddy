# Focused Port: macro-v2 Credit-REGIME signal → Josan credit side-monitor

## Goal & scope

Josan already runs a mature 9-indicator macro system (sourced from an earlier
generation of the same Portfolio Optimization lineage as macro-v2). It has HY
OAS but **lacks the macro-v2 credit-break early-warning core**: CCC-leads-HY,
credit-equity divergence, an explicit credit REGIME state machine, and a
forward self-audit loop.

**This is an AUGMENTATION, not a replacement.** Do NOT rewrite the 9-indicator
trunk, the aggregate severity, the FCN gate wiring, or the mobile contract.
Graft the credit-break leading signals into the EXISTING
`credit_funding_stress` side-monitor so the existing `MACRO_CREDIT_CRISIS` /
credit gate automatically benefits, then add a self-audit log.

Josan does NOT trim portfolios — it caps FCN GO grades. So macro-v2's
"trim authority" layer does NOT apply. We only port the **detection** core.

## Integration points (already in the repo — build on these)

- `src/services/macro-regime/side-monitors.ts` → `computeCreditFundingStress(massiveFetcher, hyOasSeriesBp)` computes KBE + HY-acceleration + funding-proxy sub-signals and takes max score. **Extend this** with the new credit-break sub-signals.
- `src/services/macro-regime/types.ts` → `CreditFundingStressReport` (extend), `SideSubSignal` (reuse), `MacroRegimeSnapshot` (add credit regime state + leading flags).
- `src/data/fred-series-fetcher.ts` / `fred-fetcher.ts` → FRED fetch pattern, `FRED_API_KEY` env. HY OAS = `BAMLH0A0HYM2` already fetched.
- `src/services/macro-regime/snapshot-builder.ts` → already fetches HY OAS series and passes `hyOasSeriesBp`; also pass a new `cccOasSeriesBp`.
- `src/utils/fcn-gates/macro-context.ts` + `gate-orchestrator.ts` → consume `credit_funding_status`; raising `overall_score` already flows into `MACRO_CREDIT_CRISIS`. Do not change the gate logic — just let the richer signal drive it.
- Snapshots persist to `macro_regime_snapshots` (DB) — the self-audit backfills against these.
- Massive already fetches NDX/QQQ for breadth — reuse for the equity leg of credit-equity divergence (no new data source).

## Data dependency (FRED, free — Josan already has the key)

Add ONE new series: **CCC OAS = `BAMLH0A3HYC`**. Optionally `T10Y2Y`, `DFII10`
if cheap, but CCC is the core. No new paid dependency.

---

## Stage 1 — Credit-break leading signals (highest value, ~1–2 days)

Extend `computeCreditFundingStress` with two new sub-signals + a derived state:

1. **`ccc_leads_hy` sub-signal**: fetch CCC OAS series (`BAMLH0A3HYC`). Compute CCC 4w delta and HY 4w delta (HY series already available). Fire when **CCC widens materially faster than HY** (CCC is the junk-of-junk; in a credit-quiet break it moves first). Score 0–3 on the spread of (CCC Δ4w − HY Δ4w) and CCC absolute level. Pure function, unit-tested with synthetic series.

2. **`credit_equity_divergence` sub-signal**: equity near highs (NDX/QQQ within X% of its trailing high — reuse the Massive data already pulled) **while** credit (HY or CCC OAS) is widening over the same window. This is the "price makes new highs but credit leaks" tell. Score 0–3. Pure function, unit-tested.

3. **Derived credit REGIME state** on the report: `NOISE | BREAK_FORMING | BREAK` as a pure function of the credit sub-signals (classify_regime in the spec). Keep it a pure classifier:
   - `BREAK` = credit genuinely breaking (high CCC-leads + widening + divergence corroborated).
   - `BREAK_FORMING` = leading signals firing but not yet corroborated.
   - `NOISE` = isolated / unconfirmed.
   - **Noise filter**: a single signal never escalates to BREAK — require ≥2 corroborating credit signals (CCC-leads, divergence, HY acceleration). This is the macro-v2 discipline that prevents the over-firing Josan's single-aggregate severity is prone to.

Fold the new sub-signals into `credit_funding_stress.overall_score` (extend the max-score, or a corroboration-aware combine). Expose `ccc_leads_hy`, `credit_equity_divergence`, and `credit_regime_state` on `CreditFundingStressReport` and surface the leading flags on `MacroRegimeSnapshot.leading_flags` (new optional field).

**Acceptance**: the existing `MACRO_CREDIT_CRISIS` / credit cap now fires earlier when CCC leads + divergence corroborate, but does NOT fire on isolated noise (single signal). Run the daily macro snapshot in a dry run and show: the new sub-signal values, the regime state, and confirm the 9-indicator overall severity path is unchanged when credit is quiet.

## Stage 2 — Forward self-audit (the thing Josan is missing most, ~1–2 days)

Josan has NO feedback loop to validate whether macro warnings were right. Port the macro-v2 self-audit:

- On each macro snapshot, append a record to a `macro_v2_audit_log` (JSONL or a DB table): as_of, credit_regime_state, leading_flags, overall severity.
- A backfill job: for each past record, after N trading days, fill in the realized forward drawdown / realized vol of a market proxy (SPY/QQQ — Massive). Compute whether a BREAK/BREAK_FORMING was followed by real stress (true positive) or not (false positive).
- A `model-health` report (Telegram, like the existing grade/gate distribution crons): when false positives cluster, flag it — **never auto-downgrade the model**, advisory only.

**Acceptance**: audit log accrues; backfill computes forward drawdown for matured records; a model-health summary can be produced. Reuse the existing Telegram cron pattern (`check-*-distribution` style + GitHub Actions HTTP trigger).

## Out of scope (Josan already has near-equivalents)

- TOP-ZONE / broad-breadth layer — Josan has BROAD_BREADTH + AI_BREADTH + CONCENTRATION already. Skip.
- non-credit severe review — Josan has guardrail/escalation. Skip for now.
- Any portfolio trim mapping — N/A, Josan caps FCN grades, doesn't trim.

## Cross-cutting

- TypeScript, `npx tsc --noEmit` clean. New pure functions (CCC-leads, divergence, classify_regime) must have unit tests mirroring the existing `test_macro_regime*` style.
- Persistence: new state files / tables must survive restarts (the snapshot already persists to `macro_regime_snapshots`; audit log needs its own durable store). Any new table goes through `ensureSchemaGuards` and is covered by `ensureRowLevelSecurity()`.
- Do NOT touch the 9-indicator aggregate, the FCN gate logic, or the mobile contract. Additive only.
- Stage 1 first; stop for review before Stage 2.

## Deliverable

Stage 1: diff + unit tests + a dry-run macro snapshot showing the new credit
sub-signals, regime state, and that the 9-indicator path is unchanged when
credit is quiet. Then stop.
