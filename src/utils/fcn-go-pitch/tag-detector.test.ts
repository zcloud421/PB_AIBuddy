import assert from 'node:assert/strict';
import { detectLitTags, hasMinimumTagsForPitch } from './tag-detector';

const base = {
    symbol: 'NVDA',
    current_price: 120,
    ma50: 105,
    ma200: 90,
    change_5d_pct: 2,
    pct_from_52w_high: -8,
    days_since_earnings: null,
    earnings_beat: null,
    sector: 'Technology',
    industry: 'Semiconductors',
    is_high_iv: true,
    news_headlines: []
};

assert.ok(detectLitTags({ ...base, days_since_earnings: 7, earnings_beat: true }).holding.includes('guide_raise'));
assert.ok(detectLitTags(base).holding.includes('super_cycle'));
assert.ok(detectLitTags(base).holding.includes('backlog'));
assert.ok(
    detectLitTags({
        ...base,
        earnings_surprise: { eps_actual: 1.12, eps_estimate: 1, eps_surprise_pct: 12 }
    }).holding.includes('earnings_strong_beat')
);
assert.ok(
    detectLitTags({
        ...base,
        earnings_surprise: { eps_actual: 1.03, eps_estimate: 1, eps_surprise_pct: 3 }
    }).holding.includes('earnings_modest_beat')
);
assert.ok(
    !detectLitTags({
        ...base,
        earnings_surprise: { eps_actual: 1.03, eps_estimate: 1, eps_surprise_pct: 3, revenue_surprise_pct: -4 }
    }).holding.includes('earnings_modest_beat')
);
assert.ok(
    detectLitTags({
        ...base,
        earnings_surprise: { eps_actual: 1.12, eps_estimate: 1, eps_surprise_pct: 12, revenue_surprise_pct: -4 }
    }).holding.includes('earnings_modest_beat')
);
assert.ok(detectLitTags({ ...base, news_headlines: ['Company raises full-year guidance after strong quarter'] }).holding.includes('guidance_reaffirmed_or_raised'));
assert.ok(detectLitTags({ ...base, news_headlines: ['公司上调全年指引'] }).holding.includes('guidance_reaffirmed_or_raised'));
assert.ok(!detectLitTags({ ...base, news_headlines: ['Analysts raise concerns about Apple supply chain'] }).holding.includes('guidance_reaffirmed_or_raised'));
assert.ok(!detectLitTags({ ...base, news_headlines: ['Rate raise fears pressure technology shares'] }).holding.includes('guidance_reaffirmed_or_raised'));
assert.ok(detectLitTags({ ...base, news_headlines: ['Company will join the Nasdaq-100 Index next month'] }).holding.includes('index_inclusion'));
assert.ok(detectLitTags({ ...base, news_headlines: ['公司纳入标普500指数'] }).holding.includes('index_inclusion'));
assert.ok(!detectLitTags({ ...base, symbol: 'GOOG', news_headlines: ['S&P 500 hits new high as GOOG leads megacaps'] }).holding.includes('index_inclusion'));
assert.ok(!detectLitTags({ ...base, symbol: 'GOOG', news_headlines: ['GOOG underweight in Nasdaq-100 ETF portfolios'] }).holding.includes('index_inclusion'));
assert.ok(detectLitTags({ ...base, symbol: 'VRT', industry: 'Electrical Equipment' }).holding.includes('infrastructure_capacity_cycle'));
assert.ok(!detectLitTags({ ...base, symbol: 'VRT', industry: 'Electrical Equipment' }).holding.includes('super_cycle'));
assert.ok(
    detectLitTags({
        ...base,
        pct_from_52w_high: -15,
        composite_score: 0.85
    }).timing.includes('quality_pullback')
);
assert.ok(detectLitTags(base).timing.includes('momentum_intact'));
assert.equal(hasMinimumTagsForPitch({ holding: ['super_cycle'], timing: [] }), false);
assert.equal(hasMinimumTagsForPitch({ holding: ['super_cycle'], timing: ['momentum_intact'] }), true);
console.log('fcn-go-pitch tag-detector tests passed');
