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

## Why No Composite Score

The system intentionally uses **discrete severity ladder + voting count** as
its aggregation, not a weighted 0-100 composite. Decision rationale:

1. **No calibration data** — we have ~18 months of production history; any
   weight choice is fabricated precision masquerading as research-anchored.

2. **PM gold standard is discrete** — Druckenmiller, Soros, Tudor Jones all
   operate on rule-based triggers ("if 3 of 4 break, de-risk"), not
   weighted scores. Weighted composites are mainly sell-side marketing or
   quant systems with 50+ year backtests.

3. **Composite scores hide single-signal failures** — when one indicator's
   data feed breaks or threshold drifts, a weighted score silently absorbs
   the bias. Discrete voting surfaces the problem indicator immediately.

4. **Round-number pillar weights aren't real research** — even if anchored
   in published frameworks (NY Fed, GS FCI), those weights come from
   regression-fit GDP impact models we don't replicate. Borrowing the
   numbers without the model is cargo-cult.

The Hero anchor is therefore "**N / M signals breached**" (Druckenmiller
voting metric), not "Josan Risk Score 47/100" (fake composite).

Variables can be added (e.g., DXY for FX channel), but always as discrete
peers in the MAX + escalation aggregator, never as weighted contributors
to a composite.

## Last Reviewed

- 2026-05-17 — initial document; reverted MA200 change
- Next audit: 2026-11-16 (quarterly)
