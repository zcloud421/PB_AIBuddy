/**
 * Aggregation logic — combines indicators + side monitors into a single
 * overall severity.
 *
 * Step 1: base severity from indicators alone
 *   - Any Critical (portfolio-critical-eligible + action-eligible) → Critical
 *   - Soft-capped / non-actionable Critical indicators are downgraded to Warning
 *   - Base severity is the max of the effective indicator severities
 *   - Else → Healthy
 *
 * Step 2: AI Cloud Stress = 3 (Crisis) escalates one step
 * Step 3: Credit/Funding overall = Crisis escalates one step
 */

import type {
    AiCloudStressReport,
    CreditFundingStressReport,
    IndicatorReading,
    MacroRegimeIndicators,
    RegimeSeverity
} from './types';

const LADDER: RegimeSeverity[] = ['Healthy', 'Neutral', 'Warning', 'Critical'];

function escalate(severity: RegimeSeverity, by: number): RegimeSeverity {
    const idx = LADDER.indexOf(severity);
    return LADDER[Math.min(idx + Math.max(0, by), LADDER.length - 1)];
}

function isPortfolioCriticalEligible(name: string, indicator: IndicatorReading): boolean {
    // Concentration is a structural late-cycle/tail-risk marker; it should not
    // solo-trigger portfolio Critical.
    if (name === 'CONCENTRATION') return false;
    // SOX 200DMA deviation is an AI stretch / entry-risk context signal. It can
    // display row-level Critical, but only becomes portfolio-critical when
    // confirmed by breadth, vol, or rates stress in computeBaseSeverity().
    if (name === 'SOX_200DMA_DEVIATION') return false;
    if (indicator.is_skipped) return false;
    return true;
}

function isActionEligible(name: string, indicator: IndicatorReading): boolean {
    // AI_BREADTH Critical needs weighted < 30% OR 10-day persistence to be
    // action-eligible. We approximate "action-eligible" as: weighted < 30%
    // (the value field is already weighted%). The 10-day persistence path
    // would require a multi-day series which we don't yet store; treat the
    // value gate as sufficient for now.
    if (name === 'AI_BREADTH' && indicator.value !== null && indicator.value >= 30) {
        return false;
    }
    return true;
}

function effectiveIndicatorSeverity(
    name: keyof MacroRegimeIndicators,
    indicator: IndicatorReading,
    soxCriticalConfirmed = false
): RegimeSeverity {
    if (indicator.status !== 'Critical') return indicator.status;
    if (name === 'SOX_200DMA_DEVIATION' && soxCriticalConfirmed) return 'Critical';
    if (!isPortfolioCriticalEligible(name, indicator) || !isActionEligible(name, indicator)) {
        return 'Warning';
    }
    return 'Critical';
}

function isWarningOrWorse(indicator: IndicatorReading | undefined): boolean {
    return indicator !== undefined &&
        !indicator.is_skipped &&
        (indicator.status === 'Warning' || indicator.status === 'Critical');
}

function hasSoxCriticalResonance(indicators: MacroRegimeIndicators): boolean {
    const sox = indicators.SOX_200DMA_DEVIATION;
    if (!sox || sox.is_skipped || sox.status !== 'Critical') return false;
    return (
        isWarningOrWorse(indicators.AI_BREADTH) ||
        isWarningOrWorse(indicators.VIX) ||
        isWarningOrWorse(indicators.DGS10_ABS_LEVEL)
    );
}

export function computeBaseSeverity(indicators: MacroRegimeIndicators): RegimeSeverity {
    const entries = Object.entries(indicators) as Array<[keyof MacroRegimeIndicators, IndicatorReading]>;
    const live = entries.filter(([, r]) => !r.is_skipped);
    const soxCriticalConfirmed = hasSoxCriticalResonance(indicators);
    const effectiveStatuses = live.map(([name, r]) => effectiveIndicatorSeverity(name, r, soxCriticalConfirmed));

    const hasActionableCritical = live.some(
        ([name, r]) =>
            r.status === 'Critical' &&
            ((name === 'SOX_200DMA_DEVIATION' && soxCriticalConfirmed) || isPortfolioCriticalEligible(name, r)) &&
            isActionEligible(name, r)
    );

    if (hasActionableCritical) return 'Critical';

    if (effectiveStatuses.some((s) => s === 'Warning' || s === 'Critical')) return 'Warning';
    if (effectiveStatuses.some((s) => s === 'Neutral')) return 'Neutral';

    return 'Healthy';
}

export function applyEscalations(
    base: RegimeSeverity,
    aiCloud: AiCloudStressReport,
    credit: CreditFundingStressReport
): RegimeSeverity {
    let result = base;
    if (aiCloud.score === 3) result = escalate(result, 1);
    if (credit.overall_status === 'crisis') result = escalate(result, 1);
    return result;
}
