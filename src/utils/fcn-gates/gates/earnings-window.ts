import type { GateDecision } from '../types';
import { decision, type GateInput } from './shared';

export function macroAwareEarningsWindowDays(overall: string): number {
    return overall === 'Warning' || overall === 'Critical' ? 14 : 7;
}

export function evaluateEarningsWindowGate(input: GateInput): GateDecision | null {
    const days = input.symbolData.days_to_earnings ?? null;
    if (days === null || days < 0) return null;

    if (days <= 3) {
        return decision({
            type: 'EARNINGS_IMMINENT',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: `财报 ${days} 天内,新增 FCN 事件风险过高`,
            details: { days_to_earnings: days, threshold: 3 }
        });
    }

    const windowDays = macroAwareEarningsWindowDays(input.macro.overall);
    if (days <= windowDays) {
        return decision({
            type: 'EARNINGS_WINDOW',
            failType: 'TIMING_FAIL',
            severity: 'WARN',
            message: `财报 ${days} 天内,建议等待事件落地后再评估`,
            details: { days_to_earnings: days, threshold: windowDays, macro_overall: input.macro.overall }
        });
    }

    return null;
}
