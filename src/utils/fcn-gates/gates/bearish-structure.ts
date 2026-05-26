import type { GateDecision } from '../types';
import { decision, type GateInput } from './shared';

export function evaluateBearishStructureGate(input: GateInput): GateDecision | null {
    const { current_price: price, ma50, ma200 } = input.symbolData;
    if (price < ma200 && ma50 < ma200) {
        return decision({
            type: 'BEARISH_STRUCTURE',
            failType: 'SUITABILITY_FAIL',
            severity: 'WARN',
            message: '价格低于 MA200 且 MA50 低于 MA200,趋势结构偏弱',
            details: { price, ma50, ma200 }
        });
    }
    return null;
}
