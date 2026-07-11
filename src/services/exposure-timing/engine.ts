import type { DailyPriceBar } from '../../data/massive-fetcher';
import type { RegimeVerdictState } from '../macro-regime/types';

// label 用 RM 的产品语言(客户持有的基金类别),symbol 是观测代理 ETF。
export const EXPOSURE_TIMING_ASSETS = [
    { symbol: 'SPY', label: '美股大盘' },
    { symbol: 'QQQ', label: '科技成长' },
    { symbol: 'SOXX', label: '半导体' },
    { symbol: 'DRAM', label: '存储芯片' },
    { symbol: 'XLV', label: '医疗健康' },
    { symbol: 'XLF', label: '金融' },
    { symbol: 'XLE', label: '能源资源' },
    { symbol: 'MCHI', label: '中国股票' },
    { symbol: 'GLD', label: '黄金' },
    { symbol: 'GDX', label: '黄金矿业' }
] as const;

export type ExposureTimingStatus = 'WAIT' | 'WATCH_SUPPORT' | 'BUILD_WINDOW' | 'EXTENDED';

export interface ExposureTimingEvidence {
    label: string;
    value: string;
}

export interface ExposureTimingSupport {
    kind: 'moving_average' | 'swing_low';
    label: 'MA50' | 'MA200' | '前低';
    level: number;
    distance_pct: number;
    distance_atr: number | null;
    slope_20d_pct: number | null;
    closes_held_3d: number;
    prior_touch_count: number;
}

export interface ExposureTimingAsset {
    symbol: string;
    label: string;
    status: ExposureTimingStatus;
    status_label: string;
    readiness_rank: number;
    summary: string;
    current_price: number | null;
    change_5d_pct: number | null;
    drawdown_52w_pct: number | null;
    rsi_14: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    atr_20: number | null;
    atr_pct: number | null;
    volume_ratio_20d: number | null;
    price_structure: 'HIGHER_LOW' | 'LOWER_LOW' | 'FLAT' | 'INSUFFICIENT';
    support: ExposureTimingSupport | null;
    evidence: ExposureTimingEvidence[];
    next_trigger: string;
    invalidation: string;
    data_as_of: string | null;
}

export interface ExposureTimingSnapshot {
    as_of: string;
    macro_state: RegimeVerdictState | null;
    assets: ExposureTimingAsset[];
}

interface ComputedState {
    current: number;
    previous: number;
    ma20: number;
    ma50: number;
    ma200: number | null;
    ma50Slope20Pct: number | null;
    ma200Slope20Pct: number | null;
    change5dPct: number | null;
    drawdown52wPct: number;
    rsi14: number | null;
    atr20: number;
    atrPct: number;
    volumeRatio20d: number | null;
    priceStructure: 'HIGHER_LOW' | 'LOWER_LOW' | 'FLAT' | 'INSUFFICIENT';
}

interface PivotLow {
    index: number;
    level: number;
}

const STATUS_META: Record<ExposureTimingStatus, { label: string; rank: number }> = {
    WAIT: { label: '下行风险仍高', rank: 0 },
    WATCH_SUPPORT: { label: '支撑确认中', rank: 1 },
    BUILD_WINDOW: { label: '下行压力缓和', rank: 3 },
    // EXTENDED 与 WATCH_SUPPORT 语义相反(涨太远 vs 未确认),不能共用文案。
    EXTENDED: { label: '价格偏离过高', rank: 2 }
};

export function computeExposureTiming(
    symbol: string,
    label: string,
    rawBars: DailyPriceBar[],
    macroState: RegimeVerdictState | null
): ExposureTimingAsset {
    const bars = normalizeBars(rawBars);
    const dataAsOf = bars[bars.length - 1]?.date ?? null;
    const state = computeState(bars);

    if (!state) {
        return buildResult(symbol, label, 'WAIT', {
            summary: '历史数据不足，暂不形成技术位置判断。',
            current_price: bars[bars.length - 1]?.close ?? null,
            change_5d_pct: null,
            drawdown_52w_pct: null,
            rsi_14: null,
            ma20: null,
            ma50: null,
            ma200: null,
            atr_20: null,
            atr_pct: null,
            volume_ratio_20d: null,
            price_structure: 'INSUFFICIENT',
            support: null,
            evidence: [{ label: '有效历史', value: `${bars.length} 日` }],
            next_trigger: '至少积累 55 个有效交易日后再评估。',
            invalidation: '数据不足，当前状态不应用于形成产品判断。',
            data_as_of: dataAsOf
        });
    }

    const support = selectSupport(state, bars);
    const belowFallingMa200 =
        state.ma200 !== null &&
        state.current < state.ma200 &&
        state.ma50 < state.ma200 &&
        (state.ma50Slope20Pct ?? 0) < 0;
    const fiveDayBreakdownThresholdPct = Math.max(3, state.atrPct * 1.5);
    const fiveDayStabilizationThresholdPct = Math.max(1.5, state.atrPct * 0.75);
    const fastBreakdown =
        state.current < state.ma50 &&
        (state.ma50Slope20Pct ?? 0) < -1 &&
        (state.change5dPct ?? 0) < -fiveDayBreakdownThresholdPct;
    const highVolumeSupportBreak =
        state.volumeRatio20d !== null &&
        state.volumeRatio20d >= 1.3 &&
        state.current < state.previous &&
        (((state.ma50Slope20Pct ?? -1) >= 0 && state.current < state.ma50 - (state.atr20 * 0.5)) ||
            hasBrokenSwingSupport(state, bars));
    const fallingKnife = belowFallingMa200 || fastBreakdown || highVolumeSupportBreak;
    const distanceFromMa50Pct = pctChange(state.current, state.ma50);
    const distanceFromMa50Atr = state.atr20 > 0 ? (state.current - state.ma50) / state.atr20 : 0;
    const extended =
        distanceFromMa50Atr > 3 ||
        ((state.rsi14 ?? 0) >= 72 &&
            state.drawdown52wPct < 4 &&
            (state.change5dPct ?? 0) > 3);
    const nearSupport = support !== null && support.distance_atr !== null && Math.abs(support.distance_atr) <= 1;
    const volumeConfirmsBounce = state.volumeRatio20d === null || state.volumeRatio20d >= 0.8;
    const supportTrendValid = support?.kind === 'swing_low' || (support?.slope_20d_pct ?? -1) >= 0;
    const supportConfirmed =
        support !== null &&
        supportTrendValid &&
        support.closes_held_3d >= 2 &&
        (state.change5dPct ?? -99) > -fiveDayStabilizationThresholdPct &&
        state.current >= state.previous &&
        volumeConfirmsBounce &&
        state.priceStructure !== 'LOWER_LOW';
    const trendConfirmed =
        state.current > state.ma20 &&
        state.ma20 > state.ma50 &&
        (state.ma200 === null || state.ma50 > state.ma200) &&
        (state.ma50Slope20Pct ?? 0) >= 0;
    const orderlyTrendWindow =
        trendConfirmed &&
        distanceFromMa50Atr >= -0.5 &&
        distanceFromMa50Atr <= 2.5 &&
        (state.rsi14 ?? 50) <= 68;
    const evidence = buildEvidence(state, support);

    if (macroState === 'CONFIRMED_BREAK') {
        return completeResult(symbol, label, 'WAIT', state, support, evidence, dataAsOf, {
            summary: '宏观系统性调整已确认，当前技术支撑的可靠性下降。',
            next_trigger: '等待宏观判定退出系统性调整，再重新评估价格结构。',
            invalidation: '在系统性调整确认期间，任何单日反弹都不构成状态升级。'
        });
    }

    if (fallingKnife) {
        return completeResult(symbol, label, 'WAIT', state, support, evidence, dataAsOf, {
            summary: belowFallingMa200
                ? '价格位于下行长期结构之下，尚未出现可验证的止跌条件。'
                : highVolumeSupportBreak
                    ? '放量跌破上行中期均线，原支撑已暂时失效。'
                    : '短期跌速与均线方向共振向下，当前仍属下跌过程。',
            next_trigger: state.ma200 !== null
                ? `先观察能否重回 MA50 $${formatPrice(state.ma50)}，并令 50 日均线停止下行。`
                : `先观察能否重回 MA50 $${formatPrice(state.ma50)}，并连续守住。`,
            invalidation: support
                ? `若继续收于 ${support.label} $${formatPrice(support.level)} 下方，弱结构延续。`
                : '若继续创新低，当前任何反弹均视为未确认。'
        });
    }

    if (extended) {
        return completeResult(symbol, label, 'EXTENDED', state, support, evidence, dataAsOf, {
            summary: '趋势保持完整，但价格距中期支撑较远，潜在回撤空间偏大。',
            next_trigger: `等待价格回到 MA50 上方 2.5 ATR 以内，或通过横盘令均线追上。`,
            invalidation: `若回落并跌破 MA50 $${formatPrice(state.ma50)}，下行风险进一步升高。`
        });
    }

    // 软闸门:宏观 BREAK_FORMING 不再吞掉技术判定 —— 技术条件全数确认时仍给
    // BUILD_WINDOW,只在文案上做置信度降档;宏观状态页面顶部已有,不逐行重复。
    const canFormWindow = nearSupport ? supportConfirmed : orderlyTrendWindow;
    if (canFormWindow) {
        const formingCaveat = macroState === 'BREAK_FORMING' ? '宏观风险仍在形成，本判定置信度降档。' : '';
        return completeResult(symbol, label, 'BUILD_WINDOW', state, support, evidence, dataAsOf, {
            summary: (nearSupport
                ? `${support?.label ?? '中期均线'}附近出现连续守稳，短期跌势未继续扩散。`
                : '中期趋势向上且价格未明显偏离均线，下行压力有所缓和。') + formingCaveat,
            next_trigger: nearSupport
                ? `若继续守住 ${support?.label} 并重回近期短线高点，确认度进一步提高。`
                : `若 MA20 与 MA50 继续上行且价格保持在 MA20 上方，条件维持。`,
            invalidation: support
                ? `收盘有效跌破 ${support.label} $${formatPrice(support.level)}，下行风险重新升高。`
                : `跌破 MA50 $${formatPrice(state.ma50)} 且均线转弱，下行风险重新升高。`
        });
    }

    return completeResult(symbol, label, 'WATCH_SUPPORT', state, support, evidence, dataAsOf, {
        summary: nearSupport
            ? state.volumeRatio20d !== null && state.volumeRatio20d < 0.8
                ? `${support?.label ?? '支撑'}附近价格暂稳，但反弹量能偏弱，尚不构成有效确认。`
                : state.priceStructure === 'LOWER_LOW'
                    ? `${support?.label ?? '支撑'}附近仍在形成更低低点，止跌结构尚未完成。`
                    : `${support?.label ?? '支撑'}附近具备观察价值，但尚缺连续守稳与短期反转确认。`
            : support
                ? `价格位于 ${support.label} $${formatPrice(support.level)} ${support.distance_pct >= 0 ? '上方' : '下方'} ${Math.abs(support.distance_pct).toFixed(1)}%，回踩测试尚未发生。`
                : '尚未进入理想支撑区，趋势与位置条件仍需进一步确认。',
        next_trigger: nearSupport
            ? `至少 2/3 个收盘守住 ${support?.label}，且 5 日跌幅收窄至 ${fiveDayStabilizationThresholdPct.toFixed(1)}%以内。`
            : support
                ? `盯住 ${support.label} $${formatPrice(support.level)}：回踩至 1 ATR 以内并连续守稳，条件即改善。`
                : `等待价格靠近上行 MA50/MA200，或重新形成均线多头结构。`,
        invalidation: support
            ? `若跌破 ${support.label} $${formatPrice(support.level)} 且均线转弱，观察失效。`
            : `若价格跌破 MA200 且 MA50 向下，下行风险转高。`
    });
}

export function buildExposureTimingSnapshot(
    asOf: string,
    macroState: RegimeVerdictState | null,
    histories: Record<string, DailyPriceBar[]>
): ExposureTimingSnapshot {
    return {
        as_of: asOf,
        macro_state: macroState,
        assets: EXPOSURE_TIMING_ASSETS.map(({ symbol, label }) =>
            computeExposureTiming(symbol, label, histories[symbol] ?? [], macroState)
        )
    };
}

function computeState(bars: DailyPriceBar[]): ComputedState | null {
    if (bars.length < 55) return null;
    const closes = bars.map((bar) => bar.close);
    const current = closes[closes.length - 1];
    const previous = closes[closes.length - 2] ?? current;
    const ma20 = average(closes.slice(-20));
    const ma50 = average(closes.slice(-50));
    const ma200 = closes.length >= 200 ? average(closes.slice(-200)) : null;
    const high52w = Math.max(...bars.slice(-252).map((bar) => bar.high));
    const volumes = bars.slice(-20).map((bar) => bar.volume).filter((value) => value > 0);
    const averageVolume = volumes.length > 0 ? average(volumes) : null;
    const atr20 = computeAtr(bars, 20);
    const pivots = findPivotLows(bars, 2, 120);

    return {
        current,
        previous,
        ma20,
        ma50,
        ma200,
        ma50Slope20Pct: movingAverageSlopePct(closes, 50, 20),
        ma200Slope20Pct: ma200 === null ? null : movingAverageSlopePct(closes, 200, 20),
        change5dPct: closes.length >= 6 ? pctChange(current, closes[closes.length - 6]) : null,
        drawdown52wPct: Math.max(0, ((high52w - current) / high52w) * 100),
        rsi14: computeRsi(closes, 14),
        atr20,
        atrPct: pctChange(current + atr20, current),
        volumeRatio20d: averageVolume && averageVolume > 0
            ? bars[bars.length - 1].volume / averageVolume
            : null,
        priceStructure: classifyPivotStructure(pivots, atr20)
    };
}

function selectSupport(state: ComputedState, bars: DailyPriceBar[]): ExposureTimingSupport | null {
    const movingAverageCandidates: ExposureTimingSupport[] = [
        { label: 'MA50' as const, level: state.ma50, slope: state.ma50Slope20Pct },
        ...(state.ma200 === null
            ? []
            : [{ label: 'MA200' as const, level: state.ma200, slope: state.ma200Slope20Pct }])
    ]
        // A falling or unmeasurable moving average is a resistance/reference, not support.
        .filter((candidate) => candidate.slope !== null && candidate.slope >= 0)
        .map((candidate) => buildSupport({
            kind: 'moving_average',
            label: candidate.label,
            level: candidate.level,
            slope: candidate.slope,
            priorTouches: 0
        }, state, bars))
        .filter((candidate) => candidate.distance_atr !== null && candidate.distance_atr >= -0.5 && candidate.distance_atr <= 3);

    const swingSupport = buildSwingLowSupport(state, bars);
    const candidates = [...movingAverageCandidates, ...(swingSupport ? [swingSupport] : [])]
        .sort((a, b) => {
            const aRepeated = a.prior_touch_count >= 2 ? 0 : 1;
            const bRepeated = b.prior_touch_count >= 2 ? 0 : 1;
            return aRepeated - bRepeated ||
                Math.abs(a.distance_atr ?? 99) - Math.abs(b.distance_atr ?? 99);
        });
    return candidates[0] ?? null;
}

function buildSwingLowSupport(state: ComputedState, bars: DailyPriceBar[]): ExposureTimingSupport | null {
    if (state.atr20 <= 0) return null;
    const pivots = findPivotLows(bars, 2, 120);
    const candidates = pivots
        .map((pivot) => {
            const cluster = pivots.filter((other) => Math.abs(other.level - pivot.level) <= state.atr20 * 0.5);
            const level = average(cluster.map((item) => item.level));
            return {
                level,
                priorTouches: cluster.length,
                latestIndex: Math.max(...cluster.map((item) => item.index))
            };
        })
        .filter((candidate, index, all) => all.findIndex((other) => Math.abs(other.level - candidate.level) <= state.atr20 * 0.25) === index)
        .map((candidate) => ({
            ...candidate,
            distanceAtr: (state.current - candidate.level) / state.atr20
        }))
        .filter((candidate) => candidate.distanceAtr >= -0.5 && candidate.distanceAtr <= 3)
        .sort((a, b) =>
            b.priorTouches - a.priorTouches ||
            Math.abs(a.distanceAtr) - Math.abs(b.distanceAtr) ||
            b.latestIndex - a.latestIndex
        );
    const strongest = candidates[0];
    if (!strongest) return null;
    return buildSupport({
        kind: 'swing_low',
        label: '前低',
        level: strongest.level,
        slope: null,
        priorTouches: strongest.priorTouches
    }, state, bars);
}

function hasBrokenSwingSupport(state: ComputedState, bars: DailyPriceBar[]): boolean {
    if (state.atr20 <= 0) return false;
    return findPivotLows(bars, 2, 120).some((pivot) => {
        const breakDistanceAtr = (pivot.level - state.current) / state.atr20;
        return breakDistanceAtr >= 0.5 && breakDistanceAtr <= 2;
    });
}

function buildSupport(
    candidate: {
        kind: ExposureTimingSupport['kind'];
        label: ExposureTimingSupport['label'];
        level: number;
        slope: number | null;
        priorTouches: number;
    },
    state: ComputedState,
    bars: DailyPriceBar[]
): ExposureTimingSupport {
    const distancePct = pctChange(state.current, candidate.level);
    const distanceAtr = state.atr20 > 0 ? (state.current - candidate.level) / state.atr20 : null;
    const closesHeld = bars.slice(-3).filter((bar) => bar.close >= candidate.level - (state.atr20 * 0.25)).length;
    return {
        kind: candidate.kind,
        label: candidate.label,
        level: round2(candidate.level),
        distance_pct: round1(distancePct),
        distance_atr: distanceAtr === null ? null : round1(distanceAtr),
        slope_20d_pct: candidate.slope === null ? null : round1(candidate.slope),
        closes_held_3d: closesHeld,
        prior_touch_count: candidate.priorTouches
    };
}

function buildEvidence(state: ComputedState, support: ExposureTimingSupport | null): ExposureTimingEvidence[] {
    const evidence: ExposureTimingEvidence[] = [
        { label: '现价', value: `$${formatPrice(state.current)}` },
        { label: '5日', value: formatSignedPct(state.change5dPct) },
        { label: '距52周高', value: `-${state.drawdown52wPct.toFixed(1)}%` }
    ];
    if (support) {
        evidence.push({
            label: support.label,
            value: `$${formatPrice(support.level)} · ${formatSignedPct(support.distance_pct)}`
        });
        evidence.push({ label: '3日守稳', value: `${support.closes_held_3d}/3` });
        if (support.kind === 'swing_low') {
            evidence.push({ label: '前低测试', value: `${support.prior_touch_count} 次` });
        }
    }
    if (state.rsi14 !== null) evidence.push({ label: 'RSI14', value: state.rsi14.toFixed(0) });
    evidence.push({ label: 'ATR20', value: `${state.atrPct.toFixed(1)}%` });
    if (state.volumeRatio20d !== null) {
        const volumeLabel = state.volumeRatio20d >= 1.3 ? '放量' : state.volumeRatio20d < 0.8 ? '缩量' : '常态';
        evidence.push({ label: '量价确认', value: `${volumeLabel} · ${state.volumeRatio20d.toFixed(2)}x` });
    }
    if (state.priceStructure !== 'INSUFFICIENT') {
        const structureLabel = state.priceStructure === 'HIGHER_LOW'
            ? '低点抬高'
            : state.priceStructure === 'LOWER_LOW'
                ? '低点下移'
                : '低点持平';
        evidence.push({ label: '价格结构', value: structureLabel });
    }
    return evidence;
}

function completeResult(
    symbol: string,
    label: string,
    status: ExposureTimingStatus,
    state: ComputedState,
    support: ExposureTimingSupport | null,
    evidence: ExposureTimingEvidence[],
    dataAsOf: string | null,
    copy: Pick<ExposureTimingAsset, 'summary' | 'next_trigger' | 'invalidation'>
): ExposureTimingAsset {
    return buildResult(symbol, label, status, {
        ...copy,
        current_price: round2(state.current),
        change_5d_pct: state.change5dPct === null ? null : round1(state.change5dPct),
        drawdown_52w_pct: round1(state.drawdown52wPct),
        rsi_14: state.rsi14 === null ? null : round1(state.rsi14),
        ma20: round2(state.ma20),
        ma50: round2(state.ma50),
        ma200: state.ma200 === null ? null : round2(state.ma200),
        atr_20: round2(state.atr20),
        atr_pct: round1(state.atrPct),
        volume_ratio_20d: state.volumeRatio20d === null ? null : round2(state.volumeRatio20d),
        price_structure: state.priceStructure,
        support,
        evidence,
        data_as_of: dataAsOf
    });
}

function buildResult(
    symbol: string,
    label: string,
    status: ExposureTimingStatus,
    rest: Omit<ExposureTimingAsset, 'symbol' | 'label' | 'status' | 'status_label' | 'readiness_rank'>
): ExposureTimingAsset {
    return {
        symbol,
        label,
        status,
        status_label: STATUS_META[status].label,
        readiness_rank: STATUS_META[status].rank,
        ...rest
    };
}

function normalizeBars(bars: DailyPriceBar[]): DailyPriceBar[] {
    return [...bars]
        .filter((bar) => Number.isFinite(bar.close) && bar.close > 0)
        .sort((a, b) => a.date.localeCompare(b.date))
        .slice(-260);
}

function movingAverageSlopePct(closes: number[], window: number, lag: number): number | null {
    if (closes.length < window + lag) return null;
    const current = average(closes.slice(-window));
    const previous = average(closes.slice(-(window + lag), -lag));
    return pctChange(current, previous);
}

function computeRsi(closes: number[], period: number): number | null {
    if (closes.length <= period) return null;
    let gains = 0;
    let losses = 0;
    for (let i = closes.length - period; i < closes.length; i += 1) {
        const change = closes[i] - closes[i - 1];
        if (change >= 0) gains += change;
        else losses += Math.abs(change);
    }
    if (losses === 0) return gains === 0 ? 50 : 100;
    const relativeStrength = (gains / period) / (losses / period);
    return 100 - (100 / (1 + relativeStrength));
}

export function computeAtr(bars: DailyPriceBar[], period = 20): number {
    if (bars.length < 2) return 0;
    const trueRanges: number[] = [];
    for (let i = Math.max(1, bars.length - period); i < bars.length; i += 1) {
        const bar = bars[i];
        const previousClose = bars[i - 1].close;
        trueRanges.push(Math.max(
            bar.high - bar.low,
            Math.abs(bar.high - previousClose),
            Math.abs(bar.low - previousClose)
        ));
    }
    return trueRanges.length > 0 ? average(trueRanges) : 0;
}

function findPivotLows(bars: DailyPriceBar[], radius: number, lookback: number): PivotLow[] {
    const start = Math.max(radius, bars.length - lookback);
    const end = bars.length - radius;
    const pivots: PivotLow[] = [];
    for (let index = start; index < end; index += 1) {
        const level = bars[index].low;
        let isPivot = true;
        for (let offset = 1; offset <= radius; offset += 1) {
            if (level > bars[index - offset].low || level > bars[index + offset].low) {
                isPivot = false;
                break;
            }
        }
        const hasStrictTurn = level < bars[index - 1].low && level < bars[index + 1].low;
        if (isPivot && hasStrictTurn) pivots.push({ index, level });
    }
    return pivots;
}

function classifyPivotStructure(
    pivots: PivotLow[],
    atr: number
): ComputedState['priceStructure'] {
    if (pivots.length < 2 || atr <= 0) return 'INSUFFICIENT';
    const previous = pivots[pivots.length - 2].level;
    const current = pivots[pivots.length - 1].level;
    const differenceAtr = (current - previous) / atr;
    if (differenceAtr >= 0.25) return 'HIGHER_LOW';
    if (differenceAtr <= -0.25) return 'LOWER_LOW';
    return 'FLAT';
}

function average(values: number[]): number {
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function pctChange(current: number, base: number): number {
    return base === 0 ? 0 : ((current - base) / base) * 100;
}

function formatSignedPct(value: number | null): string {
    if (value === null) return '—';
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function formatPrice(value: number): string {
    return value >= 100 ? value.toFixed(1) : value.toFixed(2);
}

function round1(value: number): number {
    return Math.round(value * 10) / 10;
}

function round2(value: number): number {
    return Math.round(value * 100) / 100;
}
