# FCN Idea App MVP

## 1. Product goal

Build an app for PB practitioners, mainly RM and IC, that provides daily FCN idea candidates based on listed options data as a reference for OTC short put style structures.

The app should answer four practical questions every day:

1. Which underlyings are still suitable to pitch today?
2. What tenor is more appropriate?
3. What strike range is acceptable?
4. Has market regime changed enough that previously acceptable names should now be blocked or downgraded?

## 2. Core product principle

The app should not behave like a pure yield screener. In PB, a high coupon with a poor entry path creates client dissatisfaction, concentration risk, and RM credibility damage.

So the ranking logic should be:

`survivability first -> suitability second -> coupon attractiveness third`

That means:

- Avoid names that can gap through any reasonable strike during stress
- Penalize unstable names even when implied yield is attractive
- Provide a reason code for every recommendation
- Support hard blocks, not only soft scores

## 3. Daily output format

Each idea card should include:

- Underlying
- Spot price
- Suggested direction: `FCN / short put reference`
- Preferred tenor bucket: `2w / 1m / 2m / 3m`
- Suggested strike range: for example `88% - 92% spot`
- Estimated listed-option annualized premium reference
- Risk grade: `A / B / C / Blocked`
- Main rationale
- Main risk flags
- Suitability note for client discussion

Example:

```text
AAPL
Spot: 212
Tenor: 1M
Suggested strike zone: 190 - 195
Premium reference: 16% - 19% p.a.
Risk grade: A
Rationale: liquid chain, moderate skew, healthy drawdown profile, earnings not inside tenor
Flags: macro event sensitivity
```

## 4. User roles

### RM

Needs:

- Simple ranked ideas
- Clear explanation in plain business language
- Confidence that today's idea is still safe enough to discuss

### IC

Needs:

- More detail on strike logic and risk
- Ability to compare tenors
- Ability to challenge or override system recommendations

### Desk / product specialist

Needs:

- Central monitoring
- Ability to set blocked names and house views
- Audit trail for why an idea was shown

## 5. Must-have datasets

### From Massive options API

- Underlying spot
- Full option chain
- Implied volatility
- Delta
- Open interest
- Volume
- Expiry dates
- Put-call skew if derivable

### Additional market data recommended

- Realized volatility
- Historical prices
- Earnings / corporate event calendar
- Sector classification
- Index and macro stress indicators
- Borrow / liquidity proxy if available

Without these, the app can still run, but risk control will be materially weaker.

## 6. Core engine design

Use three layers:

### Layer A: Eligibility filter

Remove names that should not be pitched regardless of premium.

Hard block examples:

- Too illiquid in listed options
- Corporate action or earnings inside selected tenor
- Extreme single-name drawdown behavior
- Very high realized gap frequency
- Recent abnormal regime break
- House restricted list

### Layer B: Risk score

Estimate whether the underlying can survive a short put style structure at a reasonable strike.

Suggested sub-scores:

- Trend stability
- Drawdown severity
- Gap risk
- Volatility regime
- Options liquidity
- Skew stress
- Event risk
- Concentration / correlation risk

### Layer C: Yield attractiveness

After the name passes risk controls, rank by premium efficiency:

- Annualized premium at target delta
- Premium per unit of historical downside risk
- Premium relative to implied volatility percentile

## 7. Strike and tenor framework

Do not let the model optimize strike and tenor only for coupon.

Instead, evaluate by target delta bands plus stress tolerance.

### Suggested tenor buckets

- `2W`: suitable during unstable markets when visibility is low
- `1M`: default PB tenor for most stable names
- `2M`: only for better quality names and cleaner event calendar
- `3M`: selective only, should have very strong stability profile

### Suggested strike logic

For each tenor bucket, test multiple delta bands:

- Conservative: `10d - 15d put`
- Balanced: `15d - 20d put`
- Aggressive: `20d - 25d put`

Convert the chosen delta band into candidate strike levels using the listed chain.

Then apply rejection rules:

- Reject strike if historical stressed move would breach it too often
- Reject strike if downside-to-coupon tradeoff is poor
- Reject strike if skew implies abnormal crash pricing

The final recommendation should be the lowest-risk strike that still delivers acceptable coupon.

## 8. Risk control framework

This is the most important part of the app.

### A. Permanent reject profile

Names that can collapse structurally should almost never rank high, even after rebound.

Examples of red flags:

- Peak-to-trough drawdown beyond policy threshold
- Repeated breakdowns below long-term support
- High frequency of single-day large drops
- Story-stock behavior driven by sentiment rather than cash flow

In your CRCL example, the right system behavior is not "find a lower strike". The right behavior is often:

`Blocked or maximum C rating until stability regime is re-established`

### B. Regime change detection

The app should distinguish between:

- Normal volatility
- Market-wide stress
- Single-name stress
- Post-shock rebound with still-elevated tail risk

Useful signals:

- 20d realized vol vs 6m realized vol
- 1m implied vol percentile
- Put skew steepening
- Distance from 50d and 200d moving averages
- Recent max drawdown over 1m and 3m

### C. Entry quality score

Even if the strike is below spot, ask:

If assigned, is this a level the PB client would still be willing to own?

That means the score should include:

- valuation proxy if available
- technical support proximity
- medium-term trend quality
- whether the name is suitable for cash equity ownership after assignment

This is critical because FCN is not just an options trade. It is a contingent stock acquisition path.

## 9. Proposed scoring model

Use a 100-point framework.

### Risk score: 70 points

- Price stability: 15
- Historical drawdown profile: 15
- Gap risk: 10
- Volatility regime: 10
- Options liquidity: 10
- Event cleanliness: 5
- Skew / crash premium warning: 5

### Yield score: 20 points

- Annualized premium attractiveness at approved delta: 10
- Premium efficiency versus downside risk: 10

### Suitability score: 10 points

- PB ownership suitability if assigned: 10

### Output mapping

- `85 - 100`: Grade A
- `70 - 84`: Grade B
- `55 - 69`: Grade C
- `<55`: Blocked

Also add hard overrides:

- Any name with hard block flag becomes `Blocked`
- Any name with event-risk breach cannot be above `C`

## 10. Recommended MVP features

### Phase 1

- Daily top ideas list
- Idea detail page
- Basic filters by market, sector, tenor, grade
- Manual house-view override
- Blocklist management
- Daily refresh job

### Phase 2

- Watchlist
- Push alerts when a name changes from eligible to blocked
- Compare tenor and strike scenarios
- RM talking points generator
- IC deep-dive analytics

### Phase 3

- Client suitability profiling
- Portfolio concentration check
- Backtest of assignment outcomes
- Internal notes and audit trail

## 11. Suggested system architecture

### Frontend

- Next.js app
- Role-based views for RM and IC
- Mobile-first design because users will check ideas during the day

### Backend

- Python or Node service for data ingestion and scoring
- Scheduled daily pipeline
- REST API or tRPC for app consumption

### Storage

- PostgreSQL for normalized market snapshots and idea outputs
- Redis optional for caching

### Jobs

- Pre-market chain ingestion
- Midday refresh
- Event-risk refresh

## 12. Suggested database entities

Minimum tables:

- `underlyings`
- `price_history`
- `option_snapshots`
- `option_contracts`
- `idea_runs`
- `idea_candidates`
- `risk_flags`
- `house_overrides`
- `blocked_underlyings`

## 13. API contract for the app layer

### `GET /ideas/today`

Returns ranked daily ideas.

### `GET /ideas/:symbol`

Returns:

- score breakdown
- approved tenors
- approved strike zones
- risk flags
- change since previous day

### `POST /overrides/:symbol`

House-view override for internal users.

### `GET /underlyings/:symbol/regime`

Returns regime summary and whether the name is pitchable.

## 14. UX principles

- Show the recommendation first
- Show the reason second
- Show the math third

RM users should not need to interpret raw Greeks before knowing whether a name is safe to discuss.

Use simple status language:

- `Approved`
- `Approved with caution`
- `High risk`
- `Blocked`

## 15. What not to do

- Do not rank purely by annualized coupon
- Do not recommend long tenors by default during unstable periods
- Do not show a strike without a reason code
- Do not treat a violent rebound as proof of safety
- Do not ignore assignment quality

## 16. First implementation recommendation

If starting now, build the first production-capable MVP in this order:

1. Ingestion of option chain and historical price data
2. Daily eligibility and risk scoring engine
3. Top ideas API
4. RM-facing mobile UI
5. IC detail analytics
6. Alerting and overrides

## 17. Immediate next build step

The first concrete deliverable should be a deterministic scoring spec for:

- symbol eligibility
- tenor approval
- strike approval
- final grade

That spec should be written before coding the UI, because this app's value is the decision engine, not the frontend shell.

