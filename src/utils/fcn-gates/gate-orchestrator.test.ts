import assert from 'node:assert/strict';
import { runAllGates } from './gate-orchestrator';
import { DEFAULT_MACRO_CONTEXT } from './macro-context';
import type { GateInput } from './gates/shared';

function input(overrides: Partial<GateInput> = {}): GateInput {
    return {
        symbol: 'NVDA',
        symbolData: {
            symbol: 'NVDA',
            current_price: 100,
            ma20: 98,
            ma50: 95,
            ma200: 90,
            pct_from_52w_high: -6,
            price_history: [
                { date: '2026-05-20', close: 98 },
                { date: '2026-05-21', close: 99 },
                { date: '2026-05-22', close: 100 }
            ],
            days_to_earnings: 30
        } as any,
        strikeData: {
            strike: 85,
            iv: 0.45,
            delta: -0.25,
            volume: 50,
            open_interest: 200,
            mid_price: 3,
            mid_price_source: 'last_quote',
            expiry_date: '2026-08-21'
        },
        tenorDays: 90,
        macro: DEFAULT_MACRO_CONTEXT,
        ...overrides
    };
}

assert.equal(runAllGates(input()).grade, 'GO');

const bufferFail = runAllGates(input({ strikeData: { ...input().strikeData, strike: 92 } }));
assert.equal(bufferFail.grade, 'AVOID');
assert.equal(bufferFail.decisions[0]?.type, 'BUFFER_FLOOR');

const bearish = runAllGates(input({
    symbolData: { ...input().symbolData, current_price: 80, ma50: 85, ma200: 90 } as any,
    strikeData: { ...input().strikeData, strike: 65 }
}));
assert.equal(bearish.grade, 'CAUTION');
assert.equal(bearish.decisions.some((d) => d.type === 'BEARISH_STRUCTURE'), true);

const earningsHard = runAllGates(input({ symbolData: { ...input().symbolData, days_to_earnings: 2 } as any }));
assert.equal(earningsHard.grade, 'AVOID');
assert.equal(earningsHard.decisions.some((d) => d.type === 'EARNINGS_IMMINENT'), true);

const earningsTiming = runAllGates(input({ symbolData: { ...input().symbolData, days_to_earnings: 5 } as any }));
assert.equal(earningsTiming.grade, 'AVOID');
assert.equal(earningsTiming.wait_reason, 'WAIT_EARNINGS_RISK');

const fallingKnife = runAllGates(input({
    symbolData: { ...input().symbolData, current_price: 82, ma50: 85, ma200: 90 } as any,
    strikeData: { ...input().strikeData, strike: 65 },
    change5dPct: -4
}));
assert.equal(fallingKnife.grade, 'CAUTION');
assert.equal(fallingKnife.decisions.some((d) => d.type === 'DISTRIBUTION_FALLING_KNIFE'), true);

const macroCritical = runAllGates(input({
    macro: {
        ...DEFAULT_MACRO_CONTEXT,
        snapshot_stale: false,
        overall: 'Critical',
        regime_verdict_state: 'CONFIRMED_BREAK'
    }
}));
assert.equal(macroCritical.grade, 'CAUTION');
assert.equal(macroCritical.decisions.some((d) => d.type === 'MACRO_REGIME_CRITICAL_CAP'), true);

const aggregateCriticalButMechanismsStable = runAllGates(input({
    macro: {
        ...DEFAULT_MACRO_CONTEXT,
        snapshot_stale: false,
        overall: 'Critical',
        regime_verdict_state: 'STABLE'
    }
}));
assert.equal(aggregateCriticalButMechanismsStable.grade, 'GO');
assert.equal(
    aggregateCriticalButMechanismsStable.decisions.some((d) => d.type === 'MACRO_REGIME_CRITICAL_CAP'),
    false
);

const formingBreak = runAllGates(input({
    macro: {
        ...DEFAULT_MACRO_CONTEXT,
        snapshot_stale: false,
        overall: 'Critical',
        regime_verdict_state: 'BREAK_FORMING'
    }
}));
assert.equal(formingBreak.grade, 'GO');

const creditCrisis = runAllGates(input({
    macro: { ...DEFAULT_MACRO_CONTEXT, snapshot_stale: false, credit_funding_status: 'crisis' }
}));
assert.equal(creditCrisis.grade, 'AVOID');
assert.equal(creditCrisis.decisions.some((d) => d.type === 'MACRO_CREDIT_CRISIS'), true);

const aiStress = runAllGates(input({
    symbol: 'LITE',
    macro: { ...DEFAULT_MACRO_CONTEXT, snapshot_stale: false, ai_cloud_status: 'Stress' }
}));
assert.equal(aiStress.grade, 'CAUTION');
assert.equal(aiStress.decisions.some((d) => d.type === 'MACRO_AI_CAPEX_CAP'), true);

const stale = runAllGates(input({ macro: { ...DEFAULT_MACRO_CONTEXT, snapshot_stale: true } }));
assert.equal(stale.decisions.some((d) => d.type === 'MACRO_CONTEXT_UNAVAILABLE'), true);

console.log('fcn-gates orchestrator tests passed');
