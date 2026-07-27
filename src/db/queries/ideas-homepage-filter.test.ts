import assert from 'node:assert/strict';
import { mapTodayIdeasResponse } from './ideas';
import type { DailyBestCard } from '../../types/api';
import type { RiskFlagRow, TodayIdeaRow } from './ideas';

const baseIdea = {
    run_id: 'run-1',
    run_date: '2026-07-27',
    exchange: 'NASDAQ',
    company_name: null,
    sector: null,
    themes: ['AI Infrastructure'],
    tier: 1,
    overall_grade: 'GO',
    composite_score: 0.8,
    ranking_score: 0.8,
    risk_reward_score: null,
    trend_score: 0.8,
    event_risk_score: 0.8,
    iv_premium_score: 0.8,
    recommended_strike: 100,
    recommended_tenor_days: 90,
    expiry_date: null,
    ref_coupon_pct: 15,
    moneyness_pct: 85,
    why_now: null,
    risk_note: null,
    sentiment_score: null,
    source_quality: null,
    narrative_engine_version: null,
    gate_decisions: [],
    shadow_grade: null,
    engine_mode: 'weighted',
    target_coupon_pct: 15,
    achieved_coupon_pct: 15,
    max_achievable_coupon_pct: 15,
    target_unreachable: false,
    generated_under_regime: null,
    macro_overrides_applied: [],
    key_events: [],
    news_items: [],
    reasoning_text: 'fixture',
    current_price: 120,
    ma20: 118,
    ma50: 115,
    ma200: 100,
    pct_from_52w_high: -5,
    selected_implied_volatility: 0.3,
    realized_volatility: 0.25,
    volatility_risk_premium: 0.05
} satisfies Omit<TodayIdeaRow, 'symbol'>;

const dailyBest = {
    symbol: 'ANET',
    company_name: 'Arista Networks',
    theme: 'AI Infrastructure',
    theme_narrative: '',
    grade: 'GO',
    recommended_strike: 100,
    recommended_tenor_days: 90,
    recommended_expiry_date: null,
    estimated_coupon_range: '12%-16%',
    moneyness_pct: 85,
    reasoning_text: 'fixture',
    narrative: null,
    news_items: [],
    flags: [],
    sentiment_score: null
} satisfies DailyBestCard;

const flags: RiskFlagRow[] = [
    {
        run_id: 'run-1',
        symbol: 'ANET',
        flag_type: 'EARNINGS_PROXIMITY',
        severity: 'WARN',
        detail_text: 'Earnings are approaching'
    }
];

const result = mapTodayIdeasResponse(
    { run_id: 'run-1', run_date: '2026-07-27' },
    [
        { ...baseIdea, symbol: 'ANET' },
        { ...baseIdea, symbol: 'JPM', themes: ['Financials'] }
    ],
    flags,
    dailyBest
);

assert.deepEqual(
    result.recommended.map((idea) => idea.symbol),
    ['JPM'],
    'a GO ticker with WAIT context must be excluded while ordinary GO remains'
);
assert.equal(
    result.daily_best,
    null,
    'a cached daily-best ticker must be hidden once it has WAIT context'
);

console.log('ideas homepage WAIT filter tests passed');

