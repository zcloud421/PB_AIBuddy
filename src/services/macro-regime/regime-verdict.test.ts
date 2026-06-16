import assert from 'assert';

import type { DailyPriceBar } from '../../data/massive-fetcher';
import type { FredPoint } from '../../data/fred-series-fetcher';
import {
    computeRealRateBrake,
    computeRegimeVerdict
} from './regime-verdict';
import type {
    AiCloudStressReport,
    CreditFundingStressReport,
    IndicatorReading,
    MacroRegimeIndicators,
    MacroRegimeSnapshot,
    RegimeSeverity,
    SideMonitorStatus,
    SideSubSignal
} from './types';

function reading(status: RegimeSeverity, value = 0): IndicatorReading {
    return {
        name: 'mock',
        value,
        status,
        delta_4w: null,
        notes: [],
        is_skipped: false
    };
}

function sub(score: 0 | 1 | 2 | 3): SideSubSignal {
    const status: SideMonitorStatus = score === 3 ? 'crisis' : score === 2 ? 'stress' : score === 1 ? 'watch' : 'normal';
    return { name: 'mock', value: null, score, status, notes: [] };
}

function credit(state: CreditFundingStressReport['credit_regime_state'] = 'NOISE'): CreditFundingStressReport {
    const score = state === 'BREAK' ? 3 : state === 'BREAK_FORMING' ? 2 : 0;
    const signal = sub(score);
    return {
        overall_score: score,
        overall_status: score === 3 ? 'crisis' : score === 2 ? 'stress' : 'normal',
        kbe_signal: sub(0),
        hy_acceleration_signal: signal,
        funding_proxy_signal: sub(0),
        ccc_leads_hy_signal: signal,
        credit_equity_divergence_signal: signal,
        credit_regime_state: state
    };
}

function aiCloud(): AiCloudStressReport {
    return {
        score: 0,
        status: 'Normal',
        crwv: { ticker: 'CRWV', signal: 0, details: [] },
        nbis: { ticker: 'NBIS', signal: 0, details: [] }
    };
}

function indicators(overrides: Partial<MacroRegimeIndicators> = {}): MacroRegimeIndicators {
    return {
        HY_OAS: reading('Healthy', 280),
        YIELD_CURVE: reading('Healthy', 50),
        VIX: reading('Healthy', 14),
        DGS10_ABS_LEVEL: reading('Neutral', 4.2),
        DGS10_4W_SHOCK: reading('Healthy', 10),
        CONCENTRATION: reading('Neutral', 25),
        AI_BREADTH: reading('Healthy', 85),
        SOX_200DMA_DEVIATION: reading('Neutral', 25),
        BROAD_BREADTH: reading('Healthy', 75),
        ...overrides
    };
}

function snapshot(input: {
    creditState?: CreditFundingStressReport['credit_regime_state'];
    vix?: IndicatorReading;
    fundamentalState?: MacroRegimeSnapshot['fundamental_modifier']['state'];
    fundamentalEscalation?: 0 | 1 | 2;
} = {}): MacroRegimeSnapshot {
    return {
        as_of: '2026-06-16',
        overall: 'Neutral',
        base_overall: 'Neutral',
        indicators: indicators(input.vix ? { VIX: input.vix } : {}),
        ai_cloud_stress: aiCloud(),
        credit_funding_stress: credit(input.creditState ?? 'NOISE'),
        fundamental_modifier: {
            state: input.fundamentalState ?? 'intact',
            escalation_level: input.fundamentalEscalation ?? 0,
            review_quarter: 'Q2',
            next_review_date: '2026-08-01',
            evidence_summary: []
        },
        late_cycle_context: {
            pillars: {
                sentiment_manual: {
                    state: 'normal',
                    summary: '',
                    evidence: [],
                    last_reviewed_at: '2026-06-16',
                    days_since_review: 0,
                    stale_warning: false
                }
            }
        },
        regime_persistence: {
            consecutive_days: 1,
            severity_started_at: '2026-06-16'
        },
        composite_persistence: {
            ai_cloud: { consecutive_days: 1, severity_started_at: '2026-06-16' },
            credit_funding: { consecutive_days: 1, severity_started_at: '2026-06-16' },
            fundamental: { consecutive_days: 1, severity_started_at: '2026-06-16' }
        },
        leading_flags: []
    };
}

function barsFromCloses(closes: number[]): DailyPriceBar[] {
    const start = new Date('2026-01-01T00:00:00Z');
    return closes.map((close, index) => ({
        date: (() => {
            const date = new Date(start);
            date.setUTCDate(start.getUTCDate() + index);
            return date.toISOString().slice(0, 10);
        })(),
        open: close,
        high: close,
        low: close,
        close,
        volume: 1_000_000
    }));
}

function fred(values: number[]): FredPoint[] {
    const start = new Date('2026-01-01T00:00:00Z');
    return values.map((value, index) => {
        const date = new Date(start);
        date.setUTCDate(start.getUTCDate() + index);
        return { date: date.toISOString().slice(0, 10), value };
    });
}

function quietRates() {
    return {
        status: 'quiet' as const,
        latest_real_rate_pct: 1.8,
        delta_8w_bp: 5,
        equity_drawdown_pct: 0,
        equity_below_ma50: false,
        equity_below_ma200: false,
        notes: []
    };
}

function run(): void {
    const risingBars = barsFromCloses(Array.from({ length: 80 }, (_, index) => 100 + index));
    const stressedBars = barsFromCloses([
        ...Array.from({ length: 70 }, (_, index) => 100 + index),
        150, 148, 145, 142, 140, 138, 136, 134, 132, 130
    ]);

    assert.strictEqual(
        computeRegimeVerdict(snapshot(), quietRates(), risingBars).state,
        'STABLE',
        'quiet mechanisms and calm price should be STABLE'
    );

    assert.strictEqual(
        computeRegimeVerdict(snapshot(), quietRates(), stressedBars).state,
        'NOISE',
        'price stress without mechanism confirmation should be NOISE'
    );

    const forming = computeRegimeVerdict(
        snapshot({ creditState: 'BREAK_FORMING', vix: reading('Warning', 27) }),
        quietRates(),
        risingBars
    );
    assert.strictEqual(forming.state, 'BREAK_FORMING');
    assert.strictEqual(forming.mechanism, 'credit');

    const confirmed = computeRegimeVerdict(
        snapshot({ fundamentalState: 'cracking', fundamentalEscalation: 1 }),
        quietRates(),
        risingBars
    );
    assert.strictEqual(confirmed.state, 'CONFIRMED_BREAK');
    assert.strictEqual(confirmed.mechanism, 'fundamental');

    const leadTime = computeRegimeVerdict(
        snapshot({ creditState: 'BREAK', vix: reading('Warning', 28) }),
        quietRates(),
        risingBars
    );
    assert.strictEqual(
        leadTime.state,
        'CONFIRMED_BREAK',
        'price still near highs but credit BREAK + VIX cross must not be buried as STABLE'
    );
    assert.strictEqual(leadTime.mechanism, 'credit');

    const noVixCross = computeRegimeVerdict(
        snapshot({ creditState: 'BREAK', vix: reading('Healthy', 14) }),
        quietRates(),
        risingBars
    );
    assert.strictEqual(noVixCross.state, 'STABLE', 'credit break without VIX cross remains conservative');

    const marginalRatesWatch = {
        status: 'forming' as const,
        latest_real_rate_pct: 1.8,
        delta_8w_bp: 27,
        equity_drawdown_pct: 0.3,
        equity_below_ma50: false,
        equity_below_ma200: false,
        notes: []
    };
    const marginalRatesVerdict = computeRegimeVerdict(snapshot(), marginalRatesWatch, risingBars);
    assert.strictEqual(
        marginalRatesVerdict.state,
        'STABLE',
        'marginal real-rate repricing with QQQ near highs should stay STABLE'
    );
    assert.strictEqual(marginalRatesVerdict.brakes.rates, 'forming');
    assert.strictEqual(marginalRatesVerdict.confidence, 'low');

    const fastRatesWatch = {
        ...marginalRatesWatch,
        delta_8w_bp: 42
    };
    const fastRatesVerdict = computeRegimeVerdict(snapshot(), fastRatesWatch, risingBars);
    assert.strictEqual(
        fastRatesVerdict.state,
        'BREAK_FORMING',
        'fast real-rate repricing >=40bp should drive a rates forming verdict'
    );
    assert.strictEqual(fastRatesVerdict.mechanism, 'rates');

    const pressureRatesWatch = {
        ...marginalRatesWatch,
        equity_drawdown_pct: 3.2,
        equity_below_ma50: false
    };
    const pressureRatesVerdict = computeRegimeVerdict(snapshot(), pressureRatesWatch, risingBars);
    assert.strictEqual(
        pressureRatesVerdict.state,
        'BREAK_FORMING',
        'real-rate repricing >=25bp plus QQQ duration pressure should drive rates forming verdict'
    );

    const realRateBrake = computeRealRateBrake(
        fred([...Array(30).fill(1.2), ...Array(40).fill(1.8)]),
        stressedBars,
        'NOISE'
    );
    assert.strictEqual(realRateBrake.status, 'confirmed');
    const ratesVerdict = computeRegimeVerdict(snapshot(), realRateBrake, stressedBars);
    assert.strictEqual(ratesVerdict.state, 'CONFIRMED_BREAK');
    assert.strictEqual(ratesVerdict.mechanism, 'rates');

    console.log('regime-verdict tests passed');
}

if (require.main === module) {
    run();
}
