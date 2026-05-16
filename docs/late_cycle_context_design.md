# Market Internals — Breadth Signals Design Rationale

## Intentional MA Mismatch

Two breadth signals deliberately use different moving averages:

| Signal       | Universe              | MA      | Weight | Purpose                               |
|--------------|-----------------------|---------|--------|---------------------------------------|
| AI Breadth   | 16 mega-cap AI names  | 50DMA   | Tier   | leadership momentum tripwire          |
| Broad Breadth| NDX-100 components    | 200DMA  | Equal  | regime participation thermometer      |

**They serve different decision cadences** for a private-banking RM:

- **Weekly FCN pricing** needs 50DMA sensitivity — a DeepSeek-style sentiment
  shock can break AI-themed FCN strikes within days. 200DMA is too slow.
- **Quarterly framework review** needs 200DMA noise rejection — short-term
  momentum swings are not regime signals.

## Why Not Normalize to Apples-to-Apples?

Earlier experiment (commit 530a9a3) changed AI Breadth from MA50 → MA200 to
enable a "Narrow Rally Gap" derived metric. **This was reverted** because:

1. Forcing uniform MA destroys the Layer 1 (weekly) signal utility
2. The "gap" is an emergent observation, not a tracked sub-signal
3. AI Breadth's value lies in catching leadership *failure*, which only the
   short MA can do timely

This is a classic quant trap: **uniformity for its own sake sacrifices signal
utility**. The Druckenmiller principle applies — *observe aggressively,
codify selectively*.

## Structural Bias Caveat for AI Breadth

AI Breadth is **structurally bullish-biased** in normal regimes:

- Curated universe (16 conviction-picked names)
- Tier-weighted (heavier weight on highest-conviction)
- 50DMA easier to stay above than 200DMA

**Implication:** treat AI Breadth as a *momentum survival monitor*, not a
risk-level monitor. Its information value concentrates in the moments it
*fails* (drops below threshold), not in its absolute level.

## Narrow Rally Gap — Documented but Not Formalized

Empirical observation:

`Gap = AI Breadth − Broad Breadth`

| Gap     | Regime hint                                    |
|---------|------------------------------------------------|
| < +10pp | Broad participation (healthy)                  |
| +10-25pp| Normal leadership market                       |
| +25-35pp| Narrow rally warning                           |
| > +35pp | Reflexive narrow market (mag7 -10% → SPX -X%)  |

**Persistence matters more than level:**
- High gap for 1-2 weeks = normal leadership phase
- High gap for 3+ months = reflexive fragility accumulation

**Status:** documented observation only. Frame audit 2026-Q4 will revisit
whether to formalize as a tracked sub-signal. Reasons to hold off:

- Signal proliferation risk (already 13 metrics)
- Correlated redundancy with CONCENTRATION + AI Cloud Stress
- No out-of-sample validation yet

## Last Reviewed

- 2026-05-17 — initial document; reverted MA200 change
- Next audit: 2026-11-16 (quarterly)
