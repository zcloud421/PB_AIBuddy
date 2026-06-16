# Markets Regime Verdict — the NOISE vs CONFIRMED BREAK engine

## What this is

The single highest-value output of the Markets feature: an intuitive top-level verdict telling an IC/RM, in one glance, **whether the current market is STABLE, a mechanism is breaking early, a falling market is just NOISE, or it's a CONFIRMED BREAK.** Validated empirically: ~75% of corrections never become bears (so default conservative), and credit spreads are the proven leading separator (widen 3–9mo before recession; HY >1000bp in 2000/2008/2020), but credit false-alarms on sector stress (2015–16 energy HY>8% no recession) so corroboration is required.

This is an **AUGMENTATION / reframe** on top of the existing macro engine — NOT a replacement. The 9-indicator snapshot, credit-break signals (already ported), and fundamental_modifier stay as the **evidence layer**. This adds a synthesizer that reads them and emits one verdict, plus one missing brake (real-rate).

**No client-facing script.** The output is an objective regime assessment for IC/RM to read; it does NOT generate wording to relay to clients (compliance: regime assessment, not investment advice). No "sell / reduce exposure" directives. Conservative NOISE default.

## The 4-state model (mechanism-as-axis, NOT price-as-axis)

Critical design point: the axis is **"is any mechanism confirming?"**, NOT "is price falling?" — because the crown-jewel case is a mechanism breaking *while price is still high* (credit led equity ~20d in COVID; the QQQ-near-high + CCC-leaking divergence Josan already flags). A price-first axis would mislabel that as Stable and bury the lead time.

| State | Definition | Meaning |
|---|---|---|
| 🟢 STABLE | price calm AND all mechanisms quiet | normal |
| 🟡 BREAK_FORMING | a mechanism starting to confirm, price not yet falling (credit leaking / real-rate repricing) | **the most valuable cell — leading window** |
| 🔵 NOISE | price falling / vol up BUT all mechanisms quiet | just a dip, don't panic |
| 🔴 CONFIRMED_BREAK | a mechanism confirmed (corroborated) | systemic |

The three **mechanism brakes** that can confirm a break (each keyed to its own evidence, never cross-counted):
- **Credit** (2008/2020 type): use the existing `credit_funding_stress.credit_regime_state` — already NOISE/BREAK_FORMING/BREAK with the ≥2-signal noise filter. CONFIRMED when state == BREAK.
- **Rates/duration** (2022 type): **NEW — add this.** Real rate (DFII10) repricing persistently over ~8 weeks (e.g. ≥ +50bp/8wk) AND NDX in drawdown / below MA, AND credit still calm (if credit breaks too it's the credit brake, not this). This is the brake RMs most often miss.
- **Fundamental/bubble** (2000 type): use existing `fundamental_modifier.state` — CONFIRMED when `cracking` (escalation ≥1).

Context brakes (raise caution, NEVER alone confirm a break): crowding/positioning (concentration, F&G, breadth), and the general macro severity. These can push STABLE→(caution annotation) but cannot produce CONFIRMED_BREAK.

Price/vol stress (the gate into NOISE): NDX/SPX below MA50 or drawdown > ~5% from recent high, or VIX elevated.

### Synthesis logic (pure function)
1. Any mechanism brake **CONFIRMED** → `CONFIRMED_BREAK`, name the mechanism. (Holds even if price hasn't fallen — that's the lead-time value.)
2. Else any mechanism brake **FORMING** (credit BREAK_FORMING, or real-rate repricing not yet 8wk-confirmed, or fundamental weakening) → `BREAK_FORMING`.
3. Else price/vol under stress but all mechanisms quiet → `NOISE`.
4. Else → `STABLE`.

## Output shape (additive field on MacroRegimeSnapshot)

```
regime_verdict: {
  state: 'STABLE' | 'BREAK_FORMING' | 'NOISE' | 'CONFIRMED_BREAK';
  mechanism: 'credit' | 'rates' | 'fundamental' | null;   // which brake drives it
  one_line: string;        // objective why, e.g. "信用利差确认走阔加速 (CCC 领先 HY)，系统性风险确认"
  confidence: 'low' | 'medium' | 'high';   // from corroboration count + persistence
  watch: string;           // the nearest-to-firing datum, e.g. "盯 CCC OAS 4w 加速 / 实际利率 8w 变化"
  brakes: {                // per-brake status for the evidence layer
    credit: 'quiet'|'forming'|'confirmed';
    rates: 'quiet'|'forming'|'confirmed';
    fundamental: 'quiet'|'forming'|'confirmed';
    crowding: 'quiet'|'elevated';
  };
}
```

`one_line` / `watch` must be **objective market observations** (what is happening in credit/rates/fundamentals + historical base-rate framing), never directives. Conservative: when price falls but mechanisms are quiet, `one_line` should affirm NOISE explicitly ("信用/利率/基本面均平静，历史上此类回调多数数周收复").

## Calibration anchors (from validation)
- HY OAS: <3.5% compressed/over-optimistic, >6% elevated stress, >1000bp = historical bear. (Josan currently ~275bp = compressed — a late-cycle context, not safety.)
- Credit requires corroboration (≥2 signals — already enforced) + VIX cross-check (credit widening without VIX = sector-specific, not systemic → keep NOISE).
- Default conservative: ~75% of corrections are noise. Bias toward NOISE/STABLE; require genuine mechanism confirmation for BREAK.

## Integration points
- New pure synthesizer: `src/services/macro-regime/regime-verdict.ts` (pure fn `computeRegimeVerdict(snapshot, realRateBrake)`), unit-tested with synthetic fixtures for all 4 states + the price-high/credit-breaking lead case.
- Real-rate brake: add DFII10 fetch (FRED, free) via `fred-series-fetcher`, compute 8-week change + the rates-brake status; wire into the snapshot builder like CCC was.
- Expose `regime_verdict` on `MacroRegimeSnapshot` (additive); `snapshot-builder.ts` calls the synthesizer last.
- Do NOT change the 9-indicator trunk, credit-break logic, FCN gates, or mobile contract beyond adding the field. New tables (none expected) would go through `ensureSchemaGuards` + `ensureRowLevelSecurity`.

## Stage A (this task — backend only, then STOP for review)
Build the verdict synthesizer + real-rate brake + expose `regime_verdict`. Deliver: diff, unit tests (all 4 states incl. the lead-time case), and a dry-run real-data snapshot (use prod FRED+Massive keys) showing the computed verdict + per-brake status today. Do NOT touch mobile yet. Do NOT deploy.

## Stage B (later) — mobile verdict card
A prominent card at the top of the Markets tab rendering `regime_verdict` (state color + one_line + confidence + watch + the 4 brake chips), with the existing 9-indicator detail below it. No client script. Separate task after Stage A review.

## Cross-cutting
TypeScript, `tsc --noEmit` clean. Pure synthesizer + real-rate fn unit-tested. Additive only. Conservative NOISE default. No client-facing directives — objective regime assessment only.
