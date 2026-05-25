import assert from 'node:assert/strict';

import { buildNarrativeInput, type NarrativeInputBuilderArgs } from './narrative-input-builder';

const base: NarrativeInputBuilderArgs = {
    symbol: 'NVDA',
    companyName: 'NVIDIA',
    theme: 'AI',
    grade: 'GO',
    compositeScore: 0.82,
    recommendedStrike: 155,
    estimatedCouponRange: '12%-16%',
    currentPrice: 180,
    change1dPct: 1.2,
    change5dPct: 3.4,
    changeYtdPct: 24.5,
    pctFrom52wHigh: -6.2,
    ma20: 170,
    ma50: 160,
    ma200: 120,
    impliedVolatility: 0.42,
    flags: [],
    tenorDays: 90,
    newsItems: [{ title: 'NVIDIA reports earnings', source: 'Test', url: '', published_at: '2026-05-25' }],
    hasRecentEarnings: true,
    earningsWeight: 0.8,
    daysToEarnings: null,
    daysSinceEarnings: 3,
    extendedMovePct: 2.1,
    refreshReason: 'first_gen',
    activeAttributionRules: [{ id: 'rule-1', reason_zh: 'test', driver_type: 'company', family: 'earnings' }]
};

const fromDaily = buildNarrativeInput(base);
const fromSearch = buildNarrativeInput({ ...base });
const fromRefresh = buildNarrativeInput({ ...base });

assert.deepStrictEqual(fromDaily, fromSearch);
assert.deepStrictEqual(fromSearch, fromRefresh);
assert.strictEqual(fromDaily.change_5d_pct, 3.4);
assert.strictEqual(fromDaily.news_headlines[0], 'NVIDIA reports earnings');
assert.strictEqual(fromDaily.active_attribution_rules?.length, 1);

console.log('narrative-input-builder tests passed');
