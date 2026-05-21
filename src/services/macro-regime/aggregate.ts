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
    // SOX 200DMA deviation is an AI valuation-stretch context signal. It can
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
    indicator: IndicatorReading
): RegimeSeverity {
    if (indicator.status !== 'Critical') return indicator.status;
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
        isWarningOrWorse(indicators.DGS10_4W_SHOCK)
    );
}

function soxCriticalDays(indicators: MacroRegimeIndicators): number {
    return indicators.SOX_200DMA_DEVIATION.persistence?.consecutive_days ?? 0;
}

export function isSoxEligibleForEscalation(indicators: MacroRegimeIndicators): boolean {
    const sox = indicators.SOX_200DMA_DEVIATION;
    if (!sox || sox.is_skipped || sox.status !== 'Critical') return false;
    return soxCriticalDays(indicators) >= 5;
}

export function annotateSoxEscalationEligibility(indicators: MacroRegimeIndicators): void {
    const sox = indicators.SOX_200DMA_DEVIATION;
    if (!sox || sox.is_skipped || sox.status !== 'Critical') return;

    const days = soxCriticalDays(indicators);
    if (days >= 5) {
        if (sox.pending_upgrade?.kind === 'escalation_eligibility') {
            sox.pending_upgrade = undefined;
        }
        return;
    }

    sox.pending_upgrade = {
        kind: 'escalation_eligibility',
        target_severity: 'Critical',
        confirmation_days_elapsed: Math.max(1, days),
        confirmation_days_required: 5,
        notes: 'SOX 已达 Critical 水平,需持续 5 个交易日才参与整体 escalation'
    };
}

export function computeBaseSeverity(indicators: MacroRegimeIndicators): RegimeSeverity {
    const entries = Object.entries(indicators) as Array<[keyof MacroRegimeIndicators, IndicatorReading]>;
    const live = entries.filter(([, r]) => !r.is_skipped);
    const effectiveStatuses = live.map(([name, r]) => effectiveIndicatorSeverity(name, r));

    const hasActionableCritical = live.some(
        ([name, r]) =>
            r.status === 'Critical' &&
            isPortfolioCriticalEligible(name, r) &&
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
    credit: CreditFundingStressReport,
    indicators?: MacroRegimeIndicators
): RegimeSeverity {
    return applyEscalationsDetailed(base, aiCloud, credit, indicators).overall;
}

export interface EscalationResult {
    overall: RegimeSeverity;
    reasons: string[];
    guardrail?: {
        applied: boolean;
        note: string;
        capped_from: 'Critical';
        capped_to: 'Warning';
    };
}

export function applyEscalationsDetailed(
    base: RegimeSeverity,
    aiCloud: AiCloudStressReport,
    credit: CreditFundingStressReport,
    indicators?: MacroRegimeIndicators
): EscalationResult {
    let result = base;
    const reasons: string[] = [];

    if (indicators && hasSoxCriticalResonance(indicators) && isSoxEligibleForEscalation(indicators)) {
        result = 'Critical';
        const resonance = [
            isWarningOrWorse(indicators.AI_BREADTH) ? 'AI_BREADTH' : null,
            isWarningOrWorse(indicators.VIX) ? 'VIX' : null,
            isWarningOrWorse(indicators.DGS10_4W_SHOCK) ? 'DGS10_4W_SHOCK' : null
        ].filter((value): value is string => Boolean(value));
        reasons.push(`SOX Critical + ${resonance.join('/')} 共振`);
    }

    if (aiCloud.score === 3) {
        const next = escalate(result, 1);
        if (next !== result) reasons.push('AI Cloud Crisis escalation');
        result = next;
    }

    if (credit.overall_status === 'crisis') {
        const next = escalate(result, 1);
        if (next !== result) reasons.push('Credit/Funding Crisis escalation');
        result = next;
    }

    if (indicators) {
        const guardrail = applySoftDerivedCriticalGuardrail(base, result, reasons, indicators, credit);
        if (guardrail.guardrailApplied) {
            return {
                overall: guardrail.overall,
                reasons,
                guardrail: {
                    applied: true,
                    note: guardrail.guardrailNote,
                    capped_from: 'Critical',
                    capped_to: 'Warning'
                }
            };
        }
    }

    return { overall: result, reasons };
}

function applySoftDerivedCriticalGuardrail(
    baseOverall: RegimeSeverity,
    finalOverall: RegimeSeverity,
    escalationReasons: string[],
    indicators: MacroRegimeIndicators,
    credit: CreditFundingStressReport
): { overall: RegimeSeverity; guardrailApplied: false } | { overall: RegimeSeverity; guardrailApplied: true; guardrailNote: string } {
    if (finalOverall !== 'Critical') return { overall: finalOverall, guardrailApplied: false };
    if (baseOverall === 'Critical') return { overall: finalOverall, guardrailApplied: false };
    if (escalationReasons.length === 0) return { overall: finalOverall, guardrailApplied: false };

    const softDerivedSources = ['SOX', 'CONCENTRATION', 'BTC', 'valuation-stretch'];
    const allSoftDerived = escalationReasons.every((reason) =>
        softDerivedSources.some((source) => reason.includes(source))
    );
    if (!allSoftDerived) return { overall: finalOverall, guardrailApplied: false };

    const hyOas = indicators.HY_OAS;
    const vix = indicators.VIX;
    const hyCalm = hyOas.value !== null && hyOas.value < 350;
    const hyAccelNormal = credit.hy_acceleration_signal.score === 0;
    const vixCalm = vix.value !== null && vix.value < 25;

    if (hyCalm && hyAccelNormal && vixCalm) {
        return {
            overall: 'Warning',
            guardrailApplied: true,
            guardrailNote: '市场未定价 risk-off — 估值伸展风险维持 Warning,等待波动率 / 信用 / 宽度确认'
        };
    }

    return { overall: finalOverall, guardrailApplied: false };
}
