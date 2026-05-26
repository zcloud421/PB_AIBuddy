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

    return {
        decisions: gates.decisions,
        final_grade: gates.grade,
        engine_mode: getEngineMode()
    };
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
