import type { GateDecision } from '../types';
import { decision, type GateInput } from './shared';

export function evaluateDistributionFallingKnifeGate(input: GateInput): GateDecision | null {
    const { current_price: price, ma50, ma200 } = input.symbolData;
    const structure = price < ma50 && ma50 < ma200;
    const momentum = (input.change5dPct ?? 0) < -3;
    const volume =
        input.todayVolume !== null &&
        input.todayVolume !== undefined &&
        input.averageVolume60d !== null &&
        input.averageVolume60d !== undefined &&
        input.averageVolume60d > 0 &&
        input.previousClose !== null &&
        input.previousClose !== undefined &&
        input.todayVolume > input.averageVolume60d * 1.3 &&
        price < input.previousClose;
    const signals = [
        structure ? 'structure' : null,
        momentum ? 'momentum' : null,
        volume ? 'volume' : null
    ].filter((value): value is string => Boolean(value));

    if (signals.length < 2) return null;

    return decision({
        type: 'DISTRIBUTION_FALLING_KNIFE',
        failType: signals.length >= 3 ? 'HARD_FAIL' : 'SUITABILITY_FAIL',
        severity: signals.length >= 3 ? 'BLOCK' : 'WARN',
        message: signals.length >= 3
            ? '结构、动量、放量下跌同时触发,疑似 falling knife'
            : '结构与动量/成交至少两项转弱,暂不适合 GO',
        details: {
            signals,
            change_5d_pct: input.change5dPct ?? null,
            today_volume: input.todayVolume ?? null,
            average_volume_60d: input.averageVolume60d ?? null
        }
    });
}
