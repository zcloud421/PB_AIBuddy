import type { GateDecision } from '../types';
import { bufferPct, decision, type GateInput } from './shared';

export function evaluateBufferFloorGate(input: GateInput): GateDecision | null {
    const buffer = bufferPct(input.symbolData.current_price, input.strikeData.strike);
    if (buffer < 10) {
        return decision({
            type: 'BUFFER_FLOOR',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: `Buffer ${buffer.toFixed(1)}% < 10% 绝对底线`,
            details: { buffer_pct: Number(buffer.toFixed(2)), threshold: 10 }
        });
    }
    return null;
}
