import assert from 'assert';
import { annotateSoxEscalationEligibility, applyEscalationsDetailed, computeBaseSeverity } from './aggregate';
import type {
    AiCloudStressReport,
    CreditFundingStressReport,
    IndicatorReading,
    MacroRegimeIndicators,
    RegimeSeverity,
    SideMonitorStatus,
    SideSubSignal
} from './types';

function reading(status: RegimeSeverity, value = 0, days = 1): IndicatorReading {
    return {
        name: 'mock',
        value,
        status,
        delta_4w: null,
        notes: [],
        is_skipped: false,
        persistence: {
            consecutive_days: days,
            severity_started_at: '2026-05-21'
        }
    };
}

function indicators(overrides: Partial<MacroRegimeIndicators> = {}): MacroRegimeIndicators {
    return {
        HY_OAS: reading('Healthy', 286),
        YIELD_CURVE: reading('Healthy', 50),
        VIX: reading('Neutral', 18.1),
        DGS10_ABS_LEVEL: reading('Warning', 4.67),
        DGS10_4W_SHOCK: reading('Neutral', 37),
        CONCENTRATION: reading('Warning', 39.2),
        AI_BREADTH: reading('Healthy', 86.3),
        SOX_200DMA_DEVIATION: reading('Critical', 55.6, 1),
        BROAD_BREADTH: reading('Neutral', 56.4),
        ...overrides
    };
}

function aiCloud(score: 0 | 1 | 2 | 3 = 0): AiCloudStressReport {
    return {
        score,
        status: score === 3 ? 'Crisis' : score === 2 ? 'Stress' : score === 1 ? 'Watch' : 'Normal',
        crwv: { ticker: 'CRWV', signal: 0, details: [] },
        nbis: { ticker: 'NBIS', signal: 0, details: [] }
    };
}

function credit(score: 0 | 1 | 2 | 3 = 0): CreditFundingStressReport {
    const status: SideMonitorStatus = score === 3 ? 'crisis' : score === 2 ? 'stress' : score === 1 ? 'watch' : 'normal';
    const sub: SideSubSignal = { name: 'mock', value: null, score, status, notes: [] };
    return {
        overall_score: score,
        overall_status: status,
        kbe_signal: sub,
        hy_acceleration_signal: sub,
        funding_proxy_signal: sub
    };
}

function aggregate(input: MacroRegimeIndicators, creditReport = credit()) {
    annotateSoxEscalationEligibility(input);
    const base = computeBaseSeverity(input);
    const result = applyEscalationsDetailed(base, aiCloud(), creditReport, input);
    return { base, ...result };
}

function run() {
    const absOnly = aggregate(indicators({
        SOX_200DMA_DEVIATION: reading('Critical', 55.6, 5),
        DGS10_ABS_LEVEL: reading('Warning', 4.67),
        DGS10_4W_SHOCK: reading('Healthy', 20),
        VIX: reading('Neutral', 18.1),
        AI_BREADTH: reading('Healthy', 86.3)
    }));
    assert.strictEqual(absOnly.overall, 'Warning', 'SOX Critical + DGS10 absolute level backdrop must not elevate to Critical');

    const shockResonance = aggregate(indicators({
        SOX_200DMA_DEVIATION: reading('Critical', 55.6, 5),
        DGS10_4W_SHOCK: reading('Warning', 55),
        VIX: reading('Warning', 26),
        HY_OAS: reading('Neutral', 360)
    }));
    assert.strictEqual(shockResonance.overall, 'Critical', 'SOX Critical + fresh shock/vol resonance after persistence can elevate to Critical');

    const dayOne = indicators({
        SOX_200DMA_DEVIATION: reading('Critical', 55.6, 1),
        VIX: reading('Warning', 26)
    });
    const dayOneResult = aggregate(dayOne);
    assert.strictEqual(dayOneResult.overall, 'Warning', 'SOX Critical day 1 must not elevate overall');
    assert.strictEqual(dayOne.SOX_200DMA_DEVIATION.pending_upgrade?.kind, 'escalation_eligibility');
    assert.strictEqual(dayOne.SOX_200DMA_DEVIATION.pending_upgrade?.confirmation_days_elapsed, 1);

    const guarded = aggregate(indicators({
        SOX_200DMA_DEVIATION: reading('Critical', 55.6, 5),
        DGS10_4W_SHOCK: reading('Warning', 55),
        VIX: reading('Neutral', 22),
        HY_OAS: reading('Healthy', 286)
    }), credit(0));
    assert.strictEqual(guarded.overall, 'Warning', 'market-pricing guardrail should cap soft-derived Critical to Warning');
    assert.strictEqual(guarded.guardrail?.applied, true);

    const vixHard = aggregate(indicators({
        VIX: reading('Critical', 36),
        SOX_200DMA_DEVIATION: reading('Warning', 45),
        HY_OAS: reading('Healthy', 286)
    }));
    assert.strictEqual(vixHard.base, 'Critical');
    assert.strictEqual(vixHard.overall, 'Critical', 'hard VIX Critical must pierce guardrail');
    assert.strictEqual(vixHard.guardrail?.applied, undefined);

    const hyHard = aggregate(indicators({
        HY_OAS: reading('Critical', 650),
        VIX: reading('Neutral', 18),
        SOX_200DMA_DEVIATION: reading('Warning', 45)
    }));
    assert.strictEqual(hyHard.base, 'Critical');
    assert.strictEqual(hyHard.overall, 'Critical', 'hard HY OAS Critical must pierce guardrail');

    console.log('aggregate tests passed');
}

if (require.main === module) {
    run();
}
