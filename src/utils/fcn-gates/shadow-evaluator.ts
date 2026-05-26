import type { OverallGrade, SymbolData, StrikeData } from '../../scoring-engine';
import { getEngineMode } from './engine-mode';
import type { GateDecisionSet } from './types';

export function evaluateShadowGates(_input: {
    symbol: string;
    symbolData: SymbolData;
    strikeData?: StrikeData | null;
    weightedGrade: OverallGrade;
}): GateDecisionSet {
    return {
        decisions: [],
        final_grade: _input.weightedGrade,
        engine_mode: getEngineMode()
    };
}
