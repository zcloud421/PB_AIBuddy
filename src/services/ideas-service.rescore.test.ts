import assert from 'node:assert';

import { selectCustomStrikeMatch } from './ideas-service';
import type { TenorWindow } from '../scoring-engine';

function strike(value: number, openInterest = 100) {
    return {
        strike: value,
        iv: 0.35,
        delta: -0.2,
        volume: 10,
        open_interest: openInterest,
        mid_price: 2,
        expiry_date: '2026-09-18'
    };
}

function tenor(input: Partial<TenorWindow> & Pick<TenorWindow, 'tenor_days' | 'preferred_tenor_days' | 'expiry_date'>): TenorWindow {
    return {
        strikes: [strike(90), strike(95), strike(100)],
        ...input
    };
}

const ninetyDay = tenor({
    tenor_days: 91,
    preferred_tenor_days: 90,
    expiry_date: '2026-09-18'
});

const sixMonth = tenor({
    tenor_days: 181,
    preferred_tenor_days: 180,
    expiry_date: '2026-12-18',
    strikes: [strike(85), strike(90), strike(95)]
});

let match = selectCustomStrikeMatch({
    tenors: [ninetyDay],
    requestedStrike: 95
});
assert.equal(match?.strikeData.strike, 95);
assert.equal(match?.metadata.match_type, 'exact');
assert.equal(match?.metadata.strike_distance, 0);

match = selectCustomStrikeMatch({
    tenors: [ninetyDay],
    requestedStrike: 96
});
assert.equal(match?.strikeData.strike, 95);
assert.equal(match?.metadata.match_type, 'nearest');
assert.equal(match?.metadata.strike_distance, 1);

match = selectCustomStrikeMatch({
    tenors: [ninetyDay, sixMonth],
    requestedStrike: 90,
    requestedTenorDays: 180
});
assert.equal(match?.tenorData.expiry_date, '2026-12-18');
assert.equal(match?.metadata.matched_tenor_days, 181);

match = selectCustomStrikeMatch({
    tenors: [ninetyDay],
    requestedStrike: 60
});
assert.equal(match, null);

console.log('ideas-service rescore strike matcher tests passed');
