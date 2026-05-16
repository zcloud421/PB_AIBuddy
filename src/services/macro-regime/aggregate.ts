/**
 * Aggregation logic — combines 7 indicators + side monitors + fundamental
 * modifier into a single overall severity.
 *
 * Step 1: base severity from indicators alone
 *   - Any Critical (portfolio-critical-eligible + action-eligible) → Critical
 *   - ≥ 2 Warning → Warning
 *   - At least 1 Warning/Critical (but not enough for above) → Neutral
 *   - Else → Healthy
 *
 * Step 2: AI Cloud Stress = 3 (Crisis) escalates one step
 * Step 3: Credit/Funding overall = Crisis escalates one step
 * Step 4: Fundamental modifier applies escalation_level (0/1/2)
 *
 * BTC_DRAWDOWN is a liquidity proxy — it never solo-triggers portfolio Critical
 * (its `portfolio_critical_eligible` is false). Other Critical sources do.
 */

import type {
    AiCloudStressReport,
    CreditFundingStressReport,
    FundamentalModifier,
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
    // BTC drawdown is liquidity proxy — does not solo-trigger Critical.
    if (name === 'BTC_DRAWDOWN') return false;
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

export function computeBaseSeverity(indicators: MacroRegimeIndicators): RegimeSeverity {
    const entries = Object.entries(indicators) as Array<[keyof MacroRegimeIndicators, IndicatorReading]>;
    const live = entries.filter(([, r]) => !r.is_skipped);
    const statuses = live.map(([, r]) => r.status);

    const hasActionableCritical = live.some(
        ([name, r]) =>
            r.status === 'Critical' &&
            isPortfolioCriticalEligible(name, r) &&
            isActionEligible(name, r)
    );

    if (hasActionableCritical) return 'Critical';

    const warningCount = statuses.filter((s) => s === 'Warning').length;
    if (warningCount >= 2) return 'Warning';

    if (statuses.some((s) => s === 'Warning' || s === 'Critical')) return 'Neutral';

    return 'Healthy';
}

export function applyEscalations(
    base: RegimeSeverity,
    aiCloud: AiCloudStressReport,
    credit: CreditFundingStressReport,
    modifier: FundamentalModifier
): RegimeSeverity {
    let result = base;
    if (aiCloud.score === 3) result = escalate(result, 1);
    if (credit.overall_status === 'crisis') result = escalate(result, 1);
    result = escalate(result, modifier.escalation_level);
    return result;
}

/**
 * Compose a short human-readable headline summarising the regime.
 * Format: "{emoji} {Severity}: {top issue}"
 */
export function composeHeadline(
    overall: RegimeSeverity,
    indicators: MacroRegimeIndicators,
    aiCloud: AiCloudStressReport,
    credit: CreditFundingStressReport,
    modifier: FundamentalModifier
): string {
    const emoji =
        overall === 'Critical' ? '🔴' :
            overall === 'Warning' ? '🟠' :
                overall === 'Neutral' ? '🟡' : '🟢';

    const reasons: string[] = [];

    const indicatorEntries = Object.entries(indicators) as Array<[string, IndicatorReading]>;
    for (const [key, reading] of indicatorEntries) {
        if (reading.is_skipped) continue;
        if (reading.status === 'Critical' || reading.status === 'Warning') {
            reasons.push(`${reading.name} ${reading.status.toLowerCase()}`);
        }
    }
    if (aiCloud.score >= 2) {
        reasons.push(`AI cloud ${aiCloud.status.toLowerCase()}`);
    }
    if (credit.overall_score >= 2) {
        reasons.push(`credit/funding ${credit.overall_status}`);
    }
    if (modifier.escalation_level >= 1) {
        reasons.push(`AI capex ${modifier.state}`);
    }

    if (reasons.length === 0) {
        // Healthy / Neutral with nothing escalated — highlight strongest signal.
        const positives: string[] = [];
        for (const [, reading] of indicatorEntries) {
            if (reading.is_skipped) continue;
            if (reading.status === 'Healthy') positives.push(reading.name);
        }
        if (positives.length > 0) {
            return `${emoji} Macro ${overall}: ${positives.slice(0, 2).join(', ')} healthy`;
        }
        return `${emoji} Macro ${overall}`;
    }

    return `${emoji} Macro ${overall}: ${reasons.slice(0, 2).join(', ')}`;
}
