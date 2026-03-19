# FCN Decision Engine Scoring Spec

## 1. Objective

This document defines a deterministic first-version scoring engine for the PB FCN idea app.

The engine must produce, for each symbol on each run:

- symbol eligibility
- approved tenor buckets
- approved strike zones
- final grade
- machine-readable reason codes

This spec is intentionally rules-based. It is designed for explainability, internal governance, and fast MVP implementation.

## 2. Decision hierarchy

The engine should run in this order:

1. Universe selection
2. Symbol eligibility check
3. Tenor approval check
4. Strike approval check
5. Score calculation
6. Grade mapping
7. Post-score hard overrides

Each later stage may only evaluate candidates that pass the earlier stage.

## 3. Input data model

Each daily run requires the following inputs per symbol.

### Market inputs

- `spot`
- `market_cap` if available
- `avg_daily_value_traded_30d`
- `hist_close_series_1y`
- `realized_vol_20d`
- `realized_vol_60d`
- `realized_vol_120d`
- `max_drawdown_1m`
- `max_drawdown_3m`
- `max_drawdown_6m`
- `gap_down_p95_1y`
- `gap_down_worst_1y`
- `distance_to_50dma`
- `distance_to_200dma`
- `earnings_date` if available
- `sector`

### Option-chain inputs by expiry

- `days_to_expiry`
- `put_strikes`
- `put_bid`
- `put_ask`
- `put_mid`
- `put_delta`
- `put_iv`
- `put_open_interest`
- `put_volume`
- `atm_iv`
- `iv_percentile_1y`
- `put_call_skew_25d` if derivable

### Internal controls

- `house_blocked`
- `house_capped_grade`
- `house_preferred`
- `house_notes`

## 4. Universe selection

Only symbols inside the approved FCN universe should be evaluated.

Suggested default universe rules:

- listed equity or ETF only
- spot price greater than or equal to `5`
- minimum 6 months of price history
- minimum one valid put expiry inside `14` to `95` calendar days

Reason codes:

- `OUTSIDE_UNIVERSE`
- `INSUFFICIENT_HISTORY`
- `NO_SUPPORTED_EXPIRY`

## 5. Symbol eligibility

Eligibility is a hard gate. If a symbol fails, it is `Blocked` and no tenor or strike should be recommended.

### 5.1 Hard blocks

Block the symbol if any of the following is true:

#### Liquidity failure

- `avg_daily_value_traded_30d < 10,000,000`
- no put line with:
  - `open_interest >= 500`
  - `volume >= 50`
  - bid-ask spread less than or equal to `8%` of option mid

Reason code:

- `LIQUIDITY_FAIL`

#### Drawdown failure

- `max_drawdown_6m <= -45%`

Reason code:

- `SEVERE_DRAWDOWN_6M`

#### Gap-risk failure

- `gap_down_worst_1y <= -18%`

Reason code:

- `EXTREME_GAP_RISK`

#### Trend-break failure

- `distance_to_200dma <= -25%`
- and `distance_to_50dma <= -15%`

Reason code:

- `STRUCTURAL_TREND_BREAK`

#### Event-risk failure

- earnings date exists and is within `7` calendar days

Reason code:

- `NEAR_EARNINGS`

#### House restriction

- `house_blocked = true`

Reason code:

- `HOUSE_BLOCK`

### 5.2 Watchlist downgrade flags

These do not block the symbol, but cap quality and reduce score.

- `max_drawdown_6m <= -30%` -> `WATCH_DRAWDOWN`
- `gap_down_p95_1y <= -6%` -> `WATCH_GAP_RISK`
- `realized_vol_20d / realized_vol_120d >= 1.8` -> `WATCH_VOL_REGIME_SPIKE`
- `put_call_skew_25d` above universe 80th percentile -> `WATCH_STEEP_SKEW`
- `distance_to_50dma <= -8%` -> `WATCH_WEAK_NEAR_TERM_TREND`

## 6. Tenor buckets

Supported tenor buckets:

- `2W`: 10 to 20 DTE
- `1M`: 21 to 40 DTE
- `2M`: 41 to 70 DTE
- `3M`: 71 to 95 DTE

Choose the listed expiry nearest to the center of each bucket.

If no expiry exists in a bucket, that bucket is `Unavailable`, not rejected.

## 7. Tenor approval rules

Tenor approval is evaluated per symbol per bucket.

### 7.1 Automatic tenor rejection

Reject a tenor bucket if any of the following is true:

- earnings date falls before expiry plus `2` business days buffer -> `TENOR_EVENT_CLASH`
- no strike in approved delta band has valid liquidity -> `TENOR_NO_LIQUID_STRIKE`
- option spread for all candidate strikes is greater than `10%` of option mid -> `TENOR_WIDE_MARKET`
- bucket is `2M` or `3M` and `realized_vol_20d / realized_vol_120d >= 1.8` -> `TENOR_TOO_LONG_FOR_CURRENT_REGIME`
- bucket is `3M` and `max_drawdown_3m <= -20%` -> `TENOR_TOO_LONG_FOR_DRAWDOWN_PROFILE`

### 7.2 Tenor caps by regime

If any of the following is true:

- `iv_percentile_1y >= 85`
- `WATCH_VOL_REGIME_SPIKE`
- `WATCH_DRAWDOWN`

Then:

- `3M` cannot be above `C`
- `2M` cannot be above `B`

If `gap_down_p95_1y <= -8%`, then:

- `2M` and `3M` are rejected
- reason code `TENOR_EXCESS_GAP_RISK`

## 8. Strike candidate generation

For each approved tenor, generate strike candidates from delta buckets.

### 8.1 Delta bands

- Conservative: absolute delta `0.10` to `0.15`
- Balanced: absolute delta `0.15` to `0.20`
- Aggressive: absolute delta `0.20` to `0.25`

MVP default behavior:

- try Conservative first
- if annualized premium is below minimum threshold, try Balanced
- Aggressive may only be used if symbol provisional score is at least `80`

### 8.2 Minimum premium threshold

A strike is only eligible if:

- annualized premium reference is at least `8%`

This threshold should be configurable by desk.

## 9. Strike approval rules

Each candidate strike is evaluated independently.

### 9.1 Strike hard rejection

Reject strike if any condition is true:

- strike / spot greater than `0.96` -> `STRIKE_TOO_CLOSE_TO_SPOT`
- break-even / spot greater than `0.93` for symbols with `WATCH_DRAWDOWN` -> `BREAKEVEN_TOO_HIGH_FOR_WEAK_NAME`
- strike breach exceeds historical stress thresholds:
  - strike distance from spot is less than absolute `gap_down_p95_1y` for `2W` -> `INSUFFICIENT_GAP_BUFFER`
  - strike distance from spot is less than `1.25 * abs(gap_down_p95_1y)` for `1M+` -> `INSUFFICIENT_GAP_BUFFER`
- strike is above 3-month support proxy if support model is available -> `ABOVE_SUPPORT_ZONE`
- option line liquidity fails:
  - `open_interest < 500`
  - `volume < 50`
  - spread greater than `8%` of mid
  - any of these -> `STRIKE_LIQUIDITY_FAIL`

### 9.2 Strike quality scoring

For each surviving strike, calculate:

- `buffer_pct = 1 - strike / spot`
- `breakeven_buffer_pct = 1 - breakeven / spot`
- `premium_pa`
- `delta_abs`

Then compute:

- buffer quality
- premium efficiency
- stress coverage

Recommended formula:

`strike_score = 40% buffer_quality + 30% stress_coverage + 30% premium_efficiency`

Component definitions:

- `buffer_quality`: scaled 0 to 100, full score at `12%` downside buffer or better, zero at `4%`
- `stress_coverage`: scaled 0 to 100, based on how much buffer exceeds required stress buffer
- `premium_efficiency`: scaled 0 to 100, based on premium per 1% of downside buffer

Select the highest strike score among approved strikes, subject to:

- choose lower delta if two strikes are within `5` points
- choose lower tenor if two candidates are within `5` points

This bias keeps output conservative.

## 10. Symbol scoring model

The symbol score is computed before final candidate selection, then adjusted with tenor and strike results.

### 10.1 Base risk score: 70 points

#### Price stability: 15

- `realized_vol_20d <= 25%` -> 15
- `25% < rv20 <= 35%` -> 12
- `35% < rv20 <= 45%` -> 8
- `45% < rv20 <= 60%` -> 4
- `rv20 > 60%` -> 0

#### Drawdown profile: 15

- `max_drawdown_6m > -10%` -> 15
- `-10% to -20%` -> 12
- `-20% to -30%` -> 8
- `-30% to -45%` -> 3
- less than `-45%` -> blocked earlier

#### Gap risk: 10

- `gap_down_p95_1y > -2%` -> 10
- `-2% to -4%` -> 8
- `-4% to -6%` -> 5
- `-6% to -8%` -> 2
- less than `-8%` -> 0

#### Volatility regime: 10

- `rv20 / rv120 < 1.1` -> 10
- `1.1 to 1.3` -> 8
- `1.3 to 1.5` -> 5
- `1.5 to 1.8` -> 2
- greater than `1.8` -> 0

#### Options liquidity: 10

- deep chain, tight spreads, high OI -> 10
- moderate chain quality -> 7
- passable but uneven -> 4
- barely passing -> 1

Implementation note:

Map this from a composite of:

- count of liquid strikes
- median spread percent
- sum of OI in target delta range

#### Event cleanliness: 5

- no event in next 30 days -> 5
- earnings in 15 to 30 days -> 2
- earnings in 8 to 14 days -> 1
- within 7 days -> blocked earlier

#### Skew warning: 5

- skew below universe 50th percentile -> 5
- 50th to 70th -> 4
- 70th to 80th -> 2
- above 80th -> 0

### 10.2 Yield score: 20 points

Evaluate best approved strike per approved tenor.

#### Premium attractiveness: 10

- `premium_pa >= 20%` -> 10
- `16% to 20%` -> 8
- `12% to 16%` -> 6
- `8% to 12%` -> 3
- below `8%` -> 0

#### Premium efficiency: 10

Use:

`premium_efficiency_ratio = premium_pa / max(buffer_pct, 0.01)`

Suggested mapping:

- ratio `>= 1.8` -> 10
- `1.4 to 1.8` -> 8
- `1.0 to 1.4` -> 5
- `0.7 to 1.0` -> 2
- below `0.7` -> 0

### 10.3 Suitability score: 10 points

This is a policy overlay on whether assignment would be acceptable for PB clients.

Suggested first-version proxies:

- sector allowed by house view
- market-cap and trading liquidity strong enough
- not a meme / story-stock profile
- not in structural breakdown

Mapping:

- high ownership suitability -> 10
- acceptable -> 7
- debatable -> 3
- poor -> 0

If this score is below `4`, final grade cannot exceed `C`.

## 11. Final candidate selection

For each symbol:

1. Compute base symbol score
2. Evaluate approved tenors
3. Evaluate approved strikes inside approved tenors
4. Select the best candidate using:
   - highest total score
   - then lower delta
   - then shorter tenor

Final candidate score:

`final_score = 70% base_symbol_score + 30% candidate_specific_score`

Where candidate-specific score is:

- `50% strike_score`
- `30% premium attractiveness`
- `20% tenor suitability`

Tenor suitability mapping:

- `2W` -> 90 during stressed regime, 70 during normal regime
- `1M` -> 85 during normal regime, 75 during stressed regime
- `2M` -> 80 only for stable names, else 50
- `3M` -> 85 only for top-quality names, else 30

## 12. Grade mapping

- `85 to 100` -> `A`
- `70 to 84.99` -> `B`
- `55 to 69.99` -> `C`
- below `55` -> `Blocked`

## 13. Post-score overrides

Apply after grade mapping.

### Downgrade rules

- any `WATCH_DRAWDOWN` and tenor `2M` or `3M` -> max `C`
- any `WATCH_STEEP_SKEW` and delta above `0.18` -> downgrade one grade
- any `WATCH_WEAK_NEAR_TERM_TREND` and strike above `90%` spot -> downgrade one grade
- if no conservative strike passes and recommendation comes from Balanced band -> max `B`

### House caps

- if `house_capped_grade` exists, apply min of model grade and house cap

## 14. Output schema

Each recommended idea should output:

- `symbol`
- `run_date`
- `spot`
- `grade`
- `final_score`
- `approved_tenor`
- `approved_expiry`
- `delta_band`
- `suggested_strike`
- `strike_pct_of_spot`
- `breakeven`
- `premium_pa`
- `risk_flags[]`
- `reason_codes[]`
- `blocked_tenors[]`
- `rejected_strikes[]`
- `summary_rationale`

## 15. Reason-code taxonomy

Use short stable codes.

### Symbol-level

- `LIQUIDITY_FAIL`
- `SEVERE_DRAWDOWN_6M`
- `EXTREME_GAP_RISK`
- `STRUCTURAL_TREND_BREAK`
- `NEAR_EARNINGS`
- `HOUSE_BLOCK`

### Tenor-level

- `TENOR_EVENT_CLASH`
- `TENOR_NO_LIQUID_STRIKE`
- `TENOR_WIDE_MARKET`
- `TENOR_TOO_LONG_FOR_CURRENT_REGIME`
- `TENOR_TOO_LONG_FOR_DRAWDOWN_PROFILE`
- `TENOR_EXCESS_GAP_RISK`

### Strike-level

- `STRIKE_TOO_CLOSE_TO_SPOT`
- `BREAKEVEN_TOO_HIGH_FOR_WEAK_NAME`
- `INSUFFICIENT_GAP_BUFFER`
- `ABOVE_SUPPORT_ZONE`
- `STRIKE_LIQUIDITY_FAIL`

## 16. CRCL-style handling policy

For names showing collapse-then-rebound behavior, the engine should prioritize structural risk over rebound momentum.

Default policy:

- if `max_drawdown_6m <= -45%`, block regardless of current premium
- if `max_drawdown_6m <= -30%` and `rv20 / rv120 >= 1.5`, max grade `C`
- if worst 1-day gap in 1 year is below `-12%`, reject all but `2W`

This prevents the engine from mistaking elevated premium for safe FCN carry.

## 17. Configuration knobs

The following values should be stored in config, not hardcoded:

- minimum premium threshold
- minimum OI and volume
- max bid-ask spread percent
- drawdown block threshold
- gap-risk block threshold
- grade cutoffs
- house-specific allowed sectors

## 18. MVP implementation notes

Keep V1 deterministic and transparent.

Do not start with machine learning.

The first production version should allow:

- daily recomputation
- replay by historical date
- full audit trail of input metrics and rule outcomes
- manual override by internal desk users

## 19. Next coding deliverable

The next step after this spec should be to implement:

1. database schema for daily symbol metrics and idea outputs
2. scoring-engine interfaces
3. first-pass calculation service
4. sample API response payloads

