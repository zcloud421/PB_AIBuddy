import assert from 'node:assert/strict';
import { evaluateBearishStructureGate } from './bearish-structure';
import { evaluateBufferFloorGate } from './buffer-floor';
import { evaluateDistributionFallingKnifeGate } from './distribution-falling-knife';
import { evaluateEarningsWindowGate } from './earnings-window';
import { evaluateFundamentalDeteriorationGate } from './fundamental-deterioration';
import { computePathRiskTrendConfirmation, computeRollingBreachFrequency, evaluatePathRiskGate } from './path-risk';
import { DEFAULT_MACRO_CONTEXT } from '../macro-context';
import type { GateInput } from './shared';

function risingHistory(length: number): Array<{ date: string; close: number }> {
    return Array.from({ length }, (_, index) => ({
        date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
        close: 100 + index * 0.1
    }));
}

function sawtoothHistory(length: number): Array<{ date: string; close: number }> {
    return Array.from({ length }, (_, index) => {
        const phase = index % 30;
        return {
            date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
            close: phase < 5 ? 100 : phase < 18 ? 78 : 95
        };
    });
}

function fallingHistory(length: number): Array<{ date: string; close: number }> {
    return Array.from({ length }, (_, index) => ({
        date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
        close: 130 - index * 0.4
    }));
}

function recoveredBreachHistory(length: number): Array<{ date: string; close: number }> {
    return Array.from({ length }, (_, index) => {
        if (index > length - 25) {
            return {
                date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
                close: 95 + (index - (length - 24)) * 0.4
            };
        }
        const phase = index % 35;
        return {
            date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
            close: phase < 31 ? 110 : phase < 34 ? 78 : 100
        };
    });
}

function base(overrides: Partial<GateInput> = {}): GateInput {
    return {
        symbol: 'TEST',
        symbolData: {
            symbol: 'TEST',
            current_price: 100,
            ma50: 95,
            ma200: 90,
            price_history: risingHistory(150),
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

const risingBreach = computeRollingBreachFrequency({
    priceHistory: risingHistory(150),
    currentPrice: 100,
    strike: 85,
    tenorDays: 30
});
assert.equal(risingBreach?.breach_frequency, 0);
assert.equal(evaluatePathRiskGate(base()), null);

assert.equal(computeRollingBreachFrequency({
    priceHistory: risingHistory(60),
    currentPrice: 100,
    strike: 85,
    tenorDays: 30
}), null);

const sawtoothBreach = computeRollingBreachFrequency({
    priceHistory: sawtoothHistory(150),
    currentPrice: 100,
    strike: 85,
    tenorDays: 30
});
assert.ok((sawtoothBreach?.breach_frequency ?? 0) > 0.2);
const severeBreach = computeRollingBreachFrequency({
    priceHistory: fallingHistory(150),
    currentPrice: 100,
    strike: 85,
    tenorDays: 90
});
assert.ok((severeBreach?.breach_frequency ?? 0) > 0.8);
assert.equal(computePathRiskTrendConfirmation({
    priceHistory: fallingHistory(150),
    currentPrice: 100,
    ma50: 95,
    ma200: 90
}).confirmed, true);
const recoveredBreach = computeRollingBreachFrequency({
    priceHistory: recoveredBreachHistory(180),
    currentPrice: 100,
    strike: 85,
    tenorDays: 90
});
assert.ok((recoveredBreach?.breach_frequency ?? 0) > 0.8);
assert.equal(evaluatePathRiskGate(base({
    symbolData: {
        ...base().symbolData,
        current_price: 104,
        ma50: 101,
        ma200: 95,
        price_history: recoveredBreachHistory(180)
    } as any
})), null);
const pathRisk = evaluatePathRiskGate(base({ symbolData: { ...base().symbolData, price_history: fallingHistory(150) } as any }));
assert.equal(pathRisk?.failType, 'SUITABILITY_FAIL');
assert.equal(pathRisk?.severity, 'WARN');
assert.equal(typeof pathRisk?.details?.breach_freq, 'number');

console.log('fcn-gates individual gate tests passed');
