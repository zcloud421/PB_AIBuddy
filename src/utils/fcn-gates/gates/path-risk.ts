import type { GateDecision } from '../types';
import { bufferPct, decision, type GateInput } from './shared';

export function evaluatePathRiskGate(input: GateInput): GateDecision | null {
    const historicalMaxDrawdownPct = input.historicalMaxDrawdownPct ?? null;
    if (historicalMaxDrawdownPct === null || !Number.isFinite(historicalMaxDrawdownPct)) {
        return null;
    }
    const buffer = bufferPct(input.symbolData.current_price, input.strikeData.strike);
    const drawdownAbs = Math.abs(historicalMaxDrawdownPct);
    if (drawdownAbs > buffer) {
        return decision({
            type: 'PATH_RISK',
            failType: 'HARD_FAIL',
            severity: 'BLOCK',
            message: `过去 180 天最大回撤 ${drawdownAbs.toFixed(1)}% 已超过当前 buffer ${buffer.toFixed(1)}%`,
            details: {
                historical_max_drawdown_pct: historicalMaxDrawdownPct,
                buffer_pct: Number(buffer.toFixed(2))
            }
        });
    }
    return null;
}
