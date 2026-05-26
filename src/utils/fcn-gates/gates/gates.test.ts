import assert from 'node:assert/strict';
import { evaluateBearishStructureGate } from './bearish-structure';
import { evaluateBufferFloorGate } from './buffer-floor';
import { evaluateDistributionFallingKnifeGate } from './distribution-falling-knife';
import { evaluateEarningsWindowGate } from './earnings-window';
import { evaluateFundamentalDeteriorationGate } from './fundamental-deterioration';
import { evaluatePathRiskGate } from './path-risk';
import { DEFAULT_MACRO_CONTEXT } from '../macro-context';
import type { GateInput } from './shared';

function base(overrides: Partial<GateInput> = {}): GateInput {
    return {
        symbol: 'TEST',
        symbolData: {
            symbol: 'TEST',
            current_price: 100,
            ma50: 95,
            ma200: 90,
            days_to_earnings: 30
        } as any,
        strikeData: {
            strike: 85,
            iv: 0.4,
            delta: -0.2,
            volume: 10,
            open_interest: 100,
            mid_price: 2,
            mid_price_source: 'last_quote',
            expiry_date: '2026-08-21'
        },
        tenorDays: 90,
        macro: DEFAULT_MACRO_CONTEXT,
        ...overrides
    };
}

assert.equal(evaluateBufferFloorGate(base({ strikeData: { ...base().strikeData, strike: 91 } }))?.failType, 'HARD_FAIL');
assert.equal(evaluateBufferFloorGate(base({ strikeData: { ...base().strikeData, strike: 89 } })), null);

assert.equal(evaluateBearishStructureGate(base({ symbolData: { ...base().symbolData, current_price: 80, ma50: 85, ma200: 90 } as any }))?.failType, 'SUITABILITY_FAIL');
assert.equal(evaluateBearishStructureGate(base()), null);

assert.equal(evaluateEarningsWindowGate(base({ symbolData: { ...base().symbolData, days_to_earnings: 2 } as any }))?.failType, 'HARD_FAIL');
assert.equal(evaluateEarningsWindowGate(base({
    symbolData: { ...base().symbolData, days_to_earnings: 8 } as any,
    macro: { ...DEFAULT_MACRO_CONTEXT, overall: 'Warning' }
}))?.failType, 'TIMING_FAIL');
assert.equal(evaluateEarningsWindowGate(base({ symbolData: { ...base().symbolData, days_to_earnings: 20 } as any })), null);

assert.equal(evaluateFundamentalDeteriorationGate(base({ earningsMiss: true, guideCut: true }))?.failType, 'HARD_FAIL');
assert.equal(evaluateFundamentalDeteriorationGate(base({ earningsMiss: true }))?.failType, 'SUITABILITY_FAIL');
assert.equal(evaluateFundamentalDeteriorationGate(base()), null);

assert.equal(evaluateDistributionFallingKnifeGate(base({ symbolData: { ...base().symbolData, current_price: 84, ma50: 85, ma200: 90 } as any, change5dPct: -4 }))?.failType, 'SUITABILITY_FAIL');
assert.equal(evaluateDistributionFallingKnifeGate(base({
    symbolData: { ...base().symbolData, current_price: 84, ma50: 85, ma200: 90 } as any,
    change5dPct: -4,
    todayVolume: 200,
    averageVolume60d: 100,
    previousClose: 90
}))?.failType, 'HARD_FAIL');
assert.equal(evaluateDistributionFallingKnifeGate(base({ change5dPct: -4 })), null);

assert.equal(evaluatePathRiskGate(base({ historicalMaxDrawdownPct: 20 }))?.failType, 'HARD_FAIL');
assert.equal(evaluatePathRiskGate(base({ historicalMaxDrawdownPct: 8 })), null);
assert.equal(evaluatePathRiskGate(base()), null);

console.log('fcn-gates individual gate tests passed');
