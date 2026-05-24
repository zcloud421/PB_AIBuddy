import assert from 'node:assert/strict';
import { inferEarningsBeat, isHighIVString, parseCouponRange, parseTenorMonths } from './input-adapter';
import type { NarrativeInput } from '../narrative-generator';

assert.deepEqual(parseCouponRange('12%-16%'), { low: 12, high: 16 });
assert.deepEqual(parseCouponRange('12-16%'), { low: 12, high: 16 });
assert.deepEqual(parseCouponRange('12-16'), { low: 12, high: 16 });
assert.equal(parseTenorMonths(90), '3 个月');
assert.equal(isHighIVString('extreme high'), true);
assert.equal(isHighIVString('高'), true);

const baseInput: NarrativeInput = {
    symbol: 'NVDA',
    theme: 'AI',
    grade: 'GO',
    recommended_strike: 100,
    estimated_coupon_range: '12-16%',
    current_price: 120,
    pct_from_52w_high: -8,
    ma20: 110,
    ma50: 105,
    ma200: 90,
    iv_level: '高',
    flags: [],
    tenor_days: 90,
    news_headlines: [],
    has_recent_earnings: true
};

assert.equal(inferEarningsBeat({ ...baseInput, news_headlines: ['Nvidia earnings beat expectations'] }), true);
assert.equal(inferEarningsBeat({ ...baseInput, news_headlines: ['Company miss disappoints investors'] }), false);

console.log('fcn-go-pitch input-adapter tests passed');
