import type { GateDecision } from '../types';
import { bufferPct, decision, type GateInput } from './shared';

const PATH_RISK_LOOKBACK_BARS = 252;
const PATH_RISK_MIN_HISTORY_BARS = 120;
// PROVISIONAL — overlapping-window breach_freq is bimodal; re-calibrate after Workstream C changes buffers.
const PATH_RISK_BREACH_FREQUENCY_THRESHOLD = 0.70;
const PATH_RISK_NEGATIVE_RETURN_BARS = 20;
const PATH_RISK_NEGATIVE_RETURN_THRESHOLD_PCT = -3;

export interface RollingBreachFrequencyResult {
    breach_frequency: number;
    breaching_windows: number;
    windows_evaluated: number;
    buffer_pct: number;
    tenor_days: number;
    lookback_bars: number;
}

export interface PathRiskTrendConfirmation {
    bearish_structure: boolean;
    below_ma200: boolean;
    negative_trend: boolean;
    return_20d_pct: number | null;
    confirmed: boolean;
}

export function computeRollingBreachFrequency(input: {
    priceHistory: Array<{ close: number }>;
    currentPrice: number;
    strike: number;
    tenorDays: number;
    lookbackBars?: number;
    minHistoryBars?: number;
}): RollingBreachFrequencyResult | null {
    const buffer = bufferPct(input.currentPrice, input.strike);
    const tenorDays = Math.round(input.tenorDays);
    if (!Number.isFinite(buffer) || buffer <= 0 || !Number.isFinite(tenorDays) || tenorDays <= 0) {
        return null;
    }

    const validHistory = input.priceHistory.filter((point) => Number.isFinite(point.close) && point.close > 0);
    const lookbackBars = input.lookbackBars ?? PATH_RISK_LOOKBACK_BARS;
    const minHistoryBars = input.minHistoryBars ?? PATH_RISK_MIN_HISTORY_BARS;
    const lookback = validHistory.slice(-lookbackBars);
    if (lookback.length < minHistoryBars || lookback.length < tenorDays) {
        return null;
    }

    let breachingWindows = 0;
    let windowsEvaluated = 0;
    for (let start = 0; start <= lookback.length - tenorDays; start += 1) {
        const window = lookback.slice(start, start + tenorDays);
        const entry = window[0]?.close;
        if (!entry || !Number.isFinite(entry) || entry <= 0) continue;
        const trough = Math.min(...window.map((point) => point.close));
        const drawdownPct = ((entry - trough) / entry) * 100;
        if (drawdownPct > buffer) {
            breachingWindows += 1;
        }
        windowsEvaluated += 1;
    }

    if (windowsEvaluated === 0) return null;

    return {
        breach_frequency: breachingWindows / windowsEvaluated,
        breaching_windows: breachingWindows,
        windows_evaluated: windowsEvaluated,
        buffer_pct: buffer,
        tenor_days: tenorDays,
        lookback_bars: lookback.length
    };
}

export function computePathRiskTrendConfirmation(input: {
    priceHistory: Array<{ close: number }>;
    currentPrice: number;
    ma50: number;
    ma200: number;
}): PathRiskTrendConfirmation {
    const bearishStructure =
        Number.isFinite(input.currentPrice) &&
        Number.isFinite(input.ma50) &&
        Number.isFinite(input.ma200) &&
        input.currentPrice < input.ma200 &&
        input.ma50 < input.ma200;
    const belowMa200 =
        Number.isFinite(input.currentPrice) &&
        Number.isFinite(input.ma200) &&
        input.currentPrice < input.ma200;
    const return20d = latestReturnPct(input.priceHistory, PATH_RISK_NEGATIVE_RETURN_BARS);
    const negativeTrend =
        (return20d !== null && return20d <= PATH_RISK_NEGATIVE_RETURN_THRESHOLD_PCT) ||
        (
            Number.isFinite(input.currentPrice) &&
            Number.isFinite(input.ma50) &&
            input.currentPrice < input.ma50
        );

    return {
        bearish_structure: bearishStructure,
        below_ma200: belowMa200,
        negative_trend: negativeTrend,
        return_20d_pct: return20d !== null ? Number(return20d.toFixed(2)) : null,
        confirmed: bearishStructure || belowMa200 || negativeTrend
    };
}

export function evaluatePathRiskGate(input: GateInput): GateDecision | null {
    const breach = computeRollingBreachFrequency({
        priceHistory: input.symbolData.price_history ?? [],
        currentPrice: input.symbolData.current_price,
        strike: input.strikeData.strike,
        tenorDays: input.tenorDays
    });
    if (!breach) {
        return null;
    }
    const trendConfirmation = computePathRiskTrendConfirmation({
        priceHistory: input.symbolData.price_history ?? [],
        currentPrice: input.symbolData.current_price,
        ma50: input.symbolData.ma50,
        ma200: input.symbolData.ma200
    });
    if (breach.breach_frequency > PATH_RISK_BREACH_FREQUENCY_THRESHOLD && trendConfirmation.confirmed) {
        return decision({
            type: 'PATH_RISK',
            failType: 'SUITABILITY_FAIL',
            severity: 'WARN',
            message: `历史 ${breach.tenor_days} 日窗口中 ${(breach.breach_frequency * 100).toFixed(1)}% 曾跌穿当前 buffer ${breach.buffer_pct.toFixed(1)}%,且当前趋势确认偏弱,路径风险 cap at CAUTION`,
            details: {
                breach_freq: Number(breach.breach_frequency.toFixed(4)),
                breaching_windows: breach.breaching_windows,
                windows_evaluated: breach.windows_evaluated,
                buffer_pct: Number(breach.buffer_pct.toFixed(2)),
                tenor_days: breach.tenor_days,
                lookback_bars: breach.lookback_bars,
                threshold: PATH_RISK_BREACH_FREQUENCY_THRESHOLD,
                trend_confirmation: trendConfirmation
            }
        });
    }
    return null;
}

function latestReturnPct(history: Array<{ close: number }>, barsBack: number): number | null {
    if (history.length <= barsBack) return null;
    const last = history[history.length - 1].close;
    const prior = history[history.length - 1 - barsBack].close;
    if (!Number.isFinite(last) || !Number.isFinite(prior) || prior <= 0) return null;
    return (last / prior - 1) * 100;
}
