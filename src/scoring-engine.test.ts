import assert from 'node:assert/strict';
import {
    computeRealizedVol,
    evaluateComputedSpeculative,
    scoreBufferSuitability,
    scoreAndGrade,
    type StrikeData,
    type SymbolData,
    type TenorWindow
} from './scoring-engine';

function historyFromCloses(closes: number[]): Array<{ date: string; close: number }> {
    return closes.map((close, index) => ({
        date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
        close
    }));
}

const calmHistory = historyFromCloses(Array.from({ length: 60 }, (_, index) => 100 + index * 0.05));
const choppyHistory = historyFromCloses(Array.from({ length: 60 }, (_, index) => 100 + (index % 2 === 0 ? 12 : -12)));

assert.equal(computeRealizedVol(historyFromCloses([100, 101, 102]), 30), null);
assert.equal(computeRealizedVol(historyFromCloses(Array.from({ length: 30 }, () => 100)), 30), 0);
assert.ok((computeRealizedVol(choppyHistory, 30) ?? 0) > (computeRealizedVol(calmHistory, 30) ?? 0));

function symbolData(priceHistory: Array<{ date: string; close: number }>): SymbolData {
    return {
        price_history: priceHistory,
        high_52w: 105,
        low_52w: 80,
        current_price: 100,
        ma20: 99,
        ma50: 97,
        ma200: 92,
        days_since_52w_high: 10,
        pct_from_52w_high: -5,
        iv_rank: null,
        macd_line: 1,
        macd_signal: 0,
        macd_histogram: 0.5,
        rsi_14: 55,
        earnings_date: null,
        days_to_earnings: 45,
        days_since_earnings: null,
        earnings_within_tenor_count: 1
    };
}

const strike: StrikeData = {
    strike: 85,
    iv: 0.5,
    delta: -0.25,
    volume: 100,
    open_interest: 100,
    mid_price: 3,
    mid_price_source: 'last_quote',
    expiry_date: '2026-09-18',
    skew: 0.2
};

const tenor: TenorWindow = {
    tenor_days: 90,
    preferred_tenor_days: 90,
    expiry_date: '2026-09-18',
    strikes: [strike]
};

const calmResult = scoreAndGrade({
    symbol: 'CALM',
    symbolData: symbolData(calmHistory),
    tenorData: tenor,
    strikeData: strike
});
const choppyResult = scoreAndGrade({
    symbol: 'CHOP',
    symbolData: symbolData(choppyHistory),
    tenorData: tenor,
    strikeData: strike
});

assert.ok((calmResult.volatility_risk_premium ?? 0) > (choppyResult.volatility_risk_premium ?? 0));
assert.ok(calmResult.iv_premium_score > choppyResult.iv_premium_score);
assert.equal(typeof calmResult.composite_score, 'number');
assert.equal(typeof calmResult.ranking_score, 'number');
assert.ok((calmResult.ranking_score ?? 0) > (choppyResult.ranking_score ?? 0));
assert.ok(calmResult.reasoning_text.includes('IV 50.0% vs 30d RV'));

const lowCouponRichVolResult = scoreAndGrade({
    symbol: 'LOWCOUPON',
    symbolData: symbolData(calmHistory),
    tenorData: {
        ...tenor,
        strikes: [{ ...strike, iv: 0.6, mid_price: 1 }]
    },
    strikeData: { ...strike, iv: 0.6, mid_price: 1 }
});
const highCouponModestVrpResult = scoreAndGrade({
    symbol: 'HIGHCOUPON',
    symbolData: symbolData(calmHistory),
    tenorData: {
        ...tenor,
        strikes: [{ ...strike, iv: 0.45, mid_price: 3.5 }]
    },
    strikeData: { ...strike, iv: 0.45, mid_price: 3.5 }
});
assert.ok((highCouponModestVrpResult.ranking_score ?? 0) > (lowCouponRichVolResult.ranking_score ?? 0));

const lowRsiResult = scoreAndGrade({
    symbol: 'LOWRSI',
    symbolData: { ...symbolData(calmHistory), rsi_14: 30 },
    tenorData: tenor,
    strikeData: strike
});
const highRsiResult = scoreAndGrade({
    symbol: 'HIGHRSI',
    symbolData: { ...symbolData(calmHistory), rsi_14: 70 },
    tenorData: tenor,
    strikeData: strike
});
assert.equal(lowRsiResult.composite_score, highRsiResult.composite_score);
assert.ok((highRsiResult.ranking_score ?? 0) > (lowRsiResult.ranking_score ?? 0));
assert.equal(evaluateComputedSpeculative('ROKU', {
    ...symbolData(choppyHistory),
    pct_from_52w_high: -5
}).isSpeculative, true);
assert.equal(evaluateComputedSpeculative('NVDA', {
    ...symbolData(choppyHistory),
    pct_from_52w_high: -5
}).isSpeculative, false);

const rokuLikeResult = scoreAndGrade({
    symbol: 'ROKU',
    symbolData: {
        ...symbolData(choppyHistory),
        pct_from_52w_high: -5
    },
    tenorData: {
        ...tenor,
        strikes: [{ ...strike, iv: 0.6, mid_price: 4 }]
    },
    strikeData: { ...strike, iv: 0.6, mid_price: 4 }
});
assert.equal(rokuLikeResult.overall_grade, 'CAUTION');
assert.ok(rokuLikeResult.gate_decisions?.some((item) => item.type === 'GRADE_CAP_HIGH_BETA'));

const nvdaLikeResult = scoreAndGrade({
    symbol: 'NVDA',
    symbolData: {
        ...symbolData(choppyHistory),
        pct_from_52w_high: -5
    },
    tenorData: {
        ...tenor,
        strikes: [{ ...strike, iv: 0.6, mid_price: 4 }]
    },
    strikeData: { ...strike, iv: 0.6, mid_price: 4 }
});
assert.equal(nvdaLikeResult.gate_decisions?.some((item) => item.type === 'GRADE_CAP_HIGH_BETA'), false);
assert.equal(scoreBufferSuitability(8), 0);
assert.ok(scoreBufferSuitability(20) > scoreBufferSuitability(12));
assert.equal(scoreBufferSuitability(25), 1);

const shallowResult = scoreAndGrade({
    symbol: 'SHALLOW',
    symbolData: symbolData(calmHistory),
    tenorData: { ...tenor, strikes: [{ ...strike, strike: 90 }] },
    strikeData: { ...strike, strike: 90 }
});
const deepResult = scoreAndGrade({
    symbol: 'DEEP',
    symbolData: symbolData(calmHistory),
    tenorData: { ...tenor, strikes: [{ ...strike, strike: 80 }] },
    strikeData: { ...strike, strike: 80 }
});

assert.ok((deepResult.buffer_score ?? 0) > (shallowResult.buffer_score ?? 0));
assert.ok(deepResult.reasoning_text.includes('Buffer context'));

console.log('scoring-engine VRP tests passed');
