import type { OverallGrade, SymbolData, StrikeData } from '../../scoring-engine';
import { getEngineMode } from './engine-mode';
import { DEFAULT_MACRO_CONTEXT, type MacroGateContext } from './macro-context';
import { runAllGates } from './gate-orchestrator';
import type { GateDecisionSet } from './types';

export function evaluateShadowGates(_input: {
    symbol: string;
    symbolData: SymbolData;
    strikeData?: StrikeData | null;
    weightedGrade: OverallGrade;
    tenorDays?: number | null;
    macroContext?: MacroGateContext | null;
    earningsMiss?: boolean;
    guideCut?: boolean;
    historicalMaxDrawdownPct?: number | null;
}): GateDecisionSet {
    if (!_input.strikeData || !_input.tenorDays) {
        return {
            decisions: [],
            final_grade: _input.weightedGrade,
            engine_mode: getEngineMode()
        };
    }

    const gates = runAllGates({
        symbol: _input.symbol,
        symbolData: _input.symbolData,
        strikeData: _input.strikeData,
        tenorDays: _input.tenorDays,
        change5dPct: latestReturnPct(_input.symbolData.price_history, 5),
        previousClose: previousClose(_input.symbolData.price_history),
        earningsMiss: _input.earningsMiss,
        guideCut: _input.guideCut,
        historicalMaxDrawdownPct: _input.historicalMaxDrawdownPct,
        macro: _input.macroContext ?? DEFAULT_MACRO_CONTEXT
    });

    // Gates are a downgrade-only safety overlay: they may pull the weighted
    // grade DOWN (GO->CAUTION->AVOID) but must never push it up. A stock the
    // weighted engine rated AVOID for reasons outside the gate set (low score,
    // weak fundamentals, poor risk-reward) has no gate firing, so the raw gate
    // grade defaults to GO — taking the more conservative of the two prevents
    // that permissive reset.
    return {
        decisions: gates.decisions,
        final_grade: moreConservativeGrade(_input.weightedGrade, gates.grade),
        engine_mode: getEngineMode()
    };
}

function gradeRank(grade: OverallGrade): number {
    if (grade === 'GO') return 2;
    if (grade === 'CAUTION') return 1;
    return 0;
}

function moreConservativeGrade(a: OverallGrade, b: OverallGrade): OverallGrade {
    return gradeRank(a) <= gradeRank(b) ? a : b;
}

function latestReturnPct(history: Array<{ close: number }>, barsBack: number): number | null {
    if (history.length <= barsBack) return null;
    const last = history[history.length - 1].close;
    const prior = history[history.length - 1 - barsBack].close;
    if (!Number.isFinite(last) || !Number.isFinite(prior) || prior <= 0) return null;
    return (last / prior - 1) * 100;
}

function previousClose(history: Array<{ close: number }>): number | null {
    if (history.length < 2) return null;
    return history[history.length - 2].close;
}
