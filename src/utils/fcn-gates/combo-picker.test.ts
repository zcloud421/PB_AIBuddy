import assert from 'node:assert/strict';
import { selectStrikeAtTargetCoupon } from './combo-picker';

const base = { iv: 0.5, delta: -0.25, volume: 10, expiry_date: '2026-08-21', mid_price_source: 'last_quote' as const };

const lite = selectStrikeAtTargetCoupon({
    currentPrice: 100,
    tenorDays: 90,
    strikes: [
        { ...base, strike: 95, open_interest: 100, mid_price: 5 },
        { ...base, strike: 85, open_interest: 100, mid_price: 3.15 },
        { ...base, strike: 75, open_interest: 100, mid_price: 1.5 }
    ]
});

assert.ok(lite);
assert.equal(lite?.strike.strike, 85);
assert.equal(lite?.target_unreachable, false);

const bufferFirst = selectStrikeAtTargetCoupon({
    currentPrice: 100,
    tenorDays: 90,
    strikes: [
        { ...base, strike: 90, open_interest: 100, mid_price: 3.4 },
        { ...base, strike: 80, open_interest: 100, mid_price: 3.0 },
        { ...base, strike: 70, open_interest: 100, mid_price: 2.6 }
    ]
});

assert.ok(bufferFirst);
assert.equal(bufferFirst?.strike.strike, 70);
assert.equal(bufferFirst?.buffer_pct, 30);

const unreachable = selectStrikeAtTargetCoupon({
    currentPrice: 200,
    tenorDays: 90,
    strikes: [
        { ...base, strike: 170, open_interest: 100, mid_price: 3.5 },
        { ...base, strike: 160, open_interest: 100, mid_price: 2.5 }
    ]
});

assert.ok(unreachable);
assert.equal(unreachable?.target_unreachable, true);

const noBuffer = selectStrikeAtTargetCoupon({
    currentPrice: 100,
    tenorDays: 90,
    strikes: [{ ...base, strike: 95, open_interest: 100, mid_price: 4 }]
},);

assert.equal(noBuffer, null);

console.log('fcn-gates combo-picker tests passed');
