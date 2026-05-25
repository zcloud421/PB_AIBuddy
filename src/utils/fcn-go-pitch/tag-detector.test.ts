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
assert.ok(detectLitTags({ ...base, news_headlines: ['Company Q3 revenue beats estimates'] }).holding.includes('post_earnings_beat'));
assert.ok(detectLitTags({ ...base, news_headlines: ['Company reaffirms 2026 guidance'] }).holding.includes('guidance_reaffirmed_or_raised'));
assert.ok(detectLitTags({ ...base, news_headlines: ['Company added to Nasdaq-100 index'] }).holding.includes('index_inclusion'));
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
