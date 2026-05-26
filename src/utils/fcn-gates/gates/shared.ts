import type { GateDecision } from '../types';
import type { MacroGateContext } from '../macro-context';
import type { OverallGrade, StrikeData, SymbolData } from '../../../scoring-engine';

export interface GateInput {
    symbol: string;
    symbolData: SymbolData;
    strikeData: StrikeData;
    tenorDays: number;
    change5dPct?: number | null;
    todayVolume?: number | null;
    averageVolume60d?: number | null;
    previousClose?: number | null;
    earningsMiss?: boolean;
    guideCut?: boolean;
    historicalMaxDrawdownPct?: number | null;
    macro: MacroGateContext;
}

export interface GateEvaluationResult {
    decisions: GateDecision[];
    grade: OverallGrade;
    wait_reason?: 'WAIT_EARNINGS_RISK' | 'WAIT_POST_EARNINGS_SHOCK' | 'WAIT_SETUP_RESET' | null;
}

export function bufferPct(currentPrice: number, strike: number): number {
    if (!Number.isFinite(currentPrice) || currentPrice <= 0) return 0;
    return ((currentPrice - strike) / currentPrice) * 100;
}

export function decision(input: Omit<GateDecision, 'passed'> & { passed?: boolean }): GateDecision {
    return {
        passed: input.passed ?? false,
        ...input
    };
}
