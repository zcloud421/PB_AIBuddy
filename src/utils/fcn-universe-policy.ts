import type { Flag, ScoringResult } from '../scoring-engine';

export const MAX_CHINA_ADR_SHOWCASE = 1;
export const CHINA_ADR_WEAK_TREND_PENALTY = 0.12;

const WEAK_TREND_FLAGS = new Set<Flag['type']>([
    'BEARISH_STRUCTURE',
    'BROKEN_TREND',
    'LOWER_HIGH_RISK',
    'WEAK_RECOVERY_PROFILE'
]);

/**
 * China ADR status is not itself a negative score. The tactical penalty only
 * applies when an ADR-risk name also carries an observable weak-trend flag.
 */
export function chinaAdrTacticalPenalty(
    candidate: Pick<ScoringResult, 'flags'>,
    underlying: { adr_risk?: boolean | null } | null
): number {
    if (!underlying?.adr_risk) return 0;
    return candidate.flags.some((flag) => WEAK_TREND_FLAGS.has(flag.type))
        ? CHINA_ADR_WEAK_TREND_PENALTY
        : 0;
}

export function canAddChinaAdrExposure(
    underlying: { adr_risk?: boolean | null } | null,
    existingChinaAdrCount: number
): boolean {
    return !underlying?.adr_risk || existingChinaAdrCount < MAX_CHINA_ADR_SHOWCASE;
}
