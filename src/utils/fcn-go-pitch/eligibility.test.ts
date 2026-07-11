import assert from 'node:assert/strict';
import type { PitchInputs } from './llm-stitcher';
import { hasSafePitchSpecificity } from './eligibility';

const base: PitchInputs = {
    symbol: 'GOOG',
    company_short_desc: 'Alphabet 是搜索广告与 AI 云平台龙头',
    display_description: 'Alphabet 是搜索广告与 AI 云平台龙头',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    lit_tags: { holding: [], timing: [] },
    change_5d_pct: null,
    pct_from_52w_high: null,
    days_since_earnings: null
};

assert.equal(hasSafePitchSpecificity(base), false);
assert.equal(hasSafePitchSpecificity({ ...base, change_5d_pct: 3.5 }), true);
assert.equal(hasSafePitchSpecificity({ ...base, pct_from_52w_high: -10.1 }), true);
assert.equal(hasSafePitchSpecificity({ ...base, recent_news_titles: ['Alphabet launches a verified cloud product'] }), true);
assert.equal(hasSafePitchSpecificity({ ...base, lit_tags: { holding: ['backlog'], timing: [] } }), true);
assert.equal(hasSafePitchSpecificity({ ...base, revenue_yoy_pct: 12.3 }), true);

console.log('fcn-go-pitch eligibility tests passed');
