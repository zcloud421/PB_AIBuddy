import assert from 'node:assert/strict';
import type { NarrativeInput } from '../narrative-generator';
import { detectConcernTags } from './tag-detector';

function input(overrides: Partial<NarrativeInput> = {}): NarrativeInput {
    return {
        symbol: 'TEST',
        company_name: 'Test Corp',
        theme: 'AI',
        grade: 'CAUTION',
        composite_score: 0.6,
        recommended_strike: 85,
        estimated_coupon_range: '12-16%',
        current_price: 100,
        change_5d_pct: 0,
        change_ytd_pct: 5,
        pct_from_52w_high: -8,
        ma20: 98,
        ma50: 95,
        ma200: 90,
        iv_level: 'high',
        flags: [],
        tenor_days: 90,
        news_headlines: [],
        news_items: [],
        has_recent_earnings: false,
        days_to_earnings: null,
        days_since_earnings: null,
        ...overrides
    };
}

let result = detectConcernTags(input({ days_to_earnings: 5 }));
assert.ok(result.caution_tags.includes('earnings_window_imminent'));
assert.equal(result.eligible_mode, 'CAUTION');

result = detectConcernTags(input({ iv_level: 'low' }));
assert.ok(result.caution_tags.includes('iv_too_low'));

result = detectConcernTags(input({ composite_score: 0.46 }));
assert.ok(result.caution_tags.includes('composite_score_borderline'));

result = detectConcernTags(input({ news_headlines: ['Company lowers guidance after Q4 miss'], has_recent_earnings: true }));
assert.ok(result.avoid_tags.includes('guide_cut'));
assert.ok(result.avoid_tags.includes('earnings_miss_recent'));
assert.equal(result.eligible_mode, 'AVOID');

result = detectConcernTags(
    input({
        current_price: 80,
        ma20: 90,
        ma50: 95,
        ma200: 100,
        news_headlines: ['Company faces antitrust investigation']
    })
);
assert.ok(result.avoid_tags.includes('breakdown_below_ma'));
assert.ok(result.avoid_tags.includes('regulatory_overhang'));
assert.equal(result.eligible_mode, 'AVOID');

result = detectConcernTags(input({ days_since_earnings: 3, change_5d_pct: -6 }));
assert.ok(result.avoid_tags.includes('post_earnings_gap_down'));
assert.equal(result.eligible_mode, 'CAUTION');

result = detectConcernTags(input({ current_price: 96, ma20: 98, change_5d_pct: -1 }));
assert.ok(result.caution_tags.includes('distribution_pattern'));

result = detectConcernTags(input({ pct_from_52w_high: -18, change_5d_pct: -2, days_since_earnings: 45 }));
assert.ok(result.caution_tags.includes('failed_rebound'));

result = detectConcernTags(
    input({
        news_items: [
            { title: 'Probe weighs on shares', published_at: '' },
            { title: 'Lawsuit expands', published_at: '' },
            { title: 'Guidance cut worries investors', published_at: '' }
        ]
    })
);
assert.ok(result.caution_tags.includes('single_name_news_overhang'));

result = detectConcernTags(input({ days_to_earnings: 10, iv_level: 'high' }));
assert.ok(result.caution_tags.includes('high_vol_event_risk'));

result = detectConcernTags(input({ flags: [{ type: 'LOW_LIQUIDITY', severity: 'WARN', message: 'thin tape' }] }));
assert.ok(result.caution_tags.includes('liquidity_or_gap_risk'));

result = detectConcernTags(input({ change_5d_pct: -4, change_ytd_pct: -2 }));
assert.ok(result.caution_tags.includes('relative_underperformance_5d_20d'));

result = detectConcernTags(input({ current_price: null, ma20: null, ma50: null, ma200: null, news_headlines: [], flags: [] }));
assert.equal(result.eligible_mode, null);

console.log('fcn-concern-pitch tag-detector tests passed');
