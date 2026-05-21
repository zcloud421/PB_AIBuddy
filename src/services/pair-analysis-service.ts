import { MassiveDataFetcher } from '../data/massive-fetcher';
import { getRecentPriceHistoryBySymbol, type PriceHistoryPointRow } from '../db/queries/ideas';

export interface PairAnalysisResponse {
    symbolA: string;
    symbolB: string;
    data_as_of: string;
    trading_days_overlap: number;
    correlation: {
        d60?: number;
        d120?: number;
        d90: number;
        d180: number;
        d252: number;
        bear_2022: number;
    };
    volatility: {
        symbolA_annualized: number;
        symbolB_annualized: number;
        gap: number;
        ratio: number;
        gap_flag: boolean;
        gap_leg: string | null;
    };
    downside_sync: number;
    correlation_stability: 'STABLE' | 'MODERATE' | 'UNSTABLE';
    suitability: 'HIGH' | 'MEDIUM' | 'LOW';
    suitability_note: string;
    suitability_note_structured: SuitabilityNote;
}

export interface SuitabilityNote {
    reason: string;
    weakness: string;
    next_step: string;
}

export type PairAnalysisResult =
    | { kind: 'ok'; data: PairAnalysisResponse }
    | { kind: 'not_found'; missing: 'symbolA' | 'symbolB' | 'both' }
    | { kind: 'insufficient_data' };

interface AlignedPricePoint {
    date: string;
    closeA: number;
    closeB: number;
}

interface ReturnPoint {
    date: string;
    returnA: number;
    returnB: number;
}

const RECENT_TRADING_DAYS = 252;
const EXTENDED_LOOKBACK_DAYS = 1900;
const BEAR_2022_START = '2022-01-01';
const BEAR_2022_END = '2022-12-31';
const massiveFetcher = new MassiveDataFetcher();

export async function analyzePairSuitability(symbolA: string, symbolB: string): Promise<PairAnalysisResult> {
    const normalizedA = symbolA.trim().toUpperCase();
    const normalizedB = symbolB.trim().toUpperCase();

    const [historyA, historyB] = await Promise.all([
        getPairPriceHistory(normalizedA),
        getPairPriceHistory(normalizedB)
    ]);

    if (historyA.length === 0 && historyB.length === 0) {
        return { kind: 'not_found', missing: 'both' };
    }

    if (historyA.length === 0) {
        return { kind: 'not_found', missing: 'symbolA' };
    }

    if (historyB.length === 0) {
        return { kind: 'not_found', missing: 'symbolB' };
    }

    const alignedSeries = alignPriceSeries(historyA, historyB);

    if (alignedSeries.length < 60) {
        return { kind: 'insufficient_data' };
    }

    const returnSeries = computeLogReturns(alignedSeries);
    const recentReturnSeries = takeRecentTradingWindow(returnSeries, RECENT_TRADING_DAYS);
    const recentAlignedSeries = alignedSeries.slice(-(recentReturnSeries.length + 1));
    const corr90 = roundMetric(calculateCorrelation(takeRecentTradingWindow(returnSeries, 90).map((point) => point.returnA), takeRecentTradingWindow(returnSeries, 90).map((point) => point.returnB)));
    const corr180 = roundMetric(calculateCorrelation(takeRecentTradingWindow(returnSeries, 180).map((point) => point.returnA), takeRecentTradingWindow(returnSeries, 180).map((point) => point.returnB)));
    const corr252 = roundMetric(calculateCorrelation(recentReturnSeries.map((point) => point.returnA), recentReturnSeries.map((point) => point.returnB)));
    const bear2022Series = filterReturnSeriesByDateRange(returnSeries, BEAR_2022_START, BEAR_2022_END);
    const corrBear2022 = roundMetric(calculateCorrelation(
        bear2022Series.map((point) => point.returnA),
        bear2022Series.map((point) => point.returnB)
    ));
    const recent60Returns = takeRecentTradingWindow(recentReturnSeries, 60);
    const volA = roundMetric(calculateAnnualizedVolatility(recent60Returns.map((point) => point.returnA)));
    const volB = roundMetric(calculateAnnualizedVolatility(recent60Returns.map((point) => point.returnB)));
    const volatilityGap = roundMetric(Math.abs(volA - volB));
    const volRatio = volA > 0 && volB > 0 ? roundMetric(Math.max(volA, volB) / Math.min(volA, volB)) : 1;
    const volGapFlag = volRatio > 1.4;
    const volGapLeg = volGapFlag ? (volA > volB ? normalizedA : normalizedB) : null;
    const downsideSync = roundMetric(calculateDownsideSync(returnSeries));
    const correlationStability = determineCorrelationStability([corr90, corr180, corr252]);
    const suitability = determineSuitability(corr90, corr180, corr252, corrBear2022, downsideSync);
    const suitabilityNote = buildSuitabilityNote(suitability, {
        corr90,
        corr180,
        corr252,
        corrBear2022,
        downsideSync,
        volGapFlag,
        volGapLeg,
        volRatio
    });

    return {
        kind: 'ok',
        data: {
            symbolA: normalizedA,
            symbolB: normalizedB,
            data_as_of: recentAlignedSeries[recentAlignedSeries.length - 1].date,
            trading_days_overlap: recentAlignedSeries.length,
            correlation: {
                // Backward-compatible aliases for older mobile builds that still expect 60/120-day keys.
                d60: corr90,
                d120: corr180,
                d90: corr90,
                d180: corr180,
                d252: corr252,
                bear_2022: corrBear2022
            },
            volatility: {
                symbolA_annualized: volA,
                symbolB_annualized: volB,
                gap: volatilityGap,
                ratio: volRatio,
                gap_flag: volGapFlag,
                gap_leg: volGapLeg
            },
            downside_sync: downsideSync,
            correlation_stability: correlationStability,
            suitability,
            suitability_note: formatSuitabilityNote(suitabilityNote),
            suitability_note_structured: suitabilityNote
        }
    };
}

async function getPairPriceHistory(symbol: string): Promise<PriceHistoryPointRow[]> {
    try {
        const bars = await massiveFetcher.fetchPriceHistory(symbol, EXTENDED_LOOKBACK_DAYS);
        const massiveHistory = bars
            .map((bar) => ({
                date: bar.date,
                close: bar.close
            }))
            .filter((point) => Number.isFinite(point.close) && point.close > 0);

        if (massiveHistory.length > 0) {
            return massiveHistory;
        }
    } catch (error) {
        console.warn(`[pair-analysis] Massive history fetch failed for ${symbol}`, error);
    }

    return getRecentPriceHistoryBySymbol(symbol, RECENT_TRADING_DAYS);
}

function filterReturnSeriesByDateRange(series: ReturnPoint[], startDate: string, endDate: string): ReturnPoint[] {
    return series.filter((point) => point.date >= startDate && point.date <= endDate);
}

function alignPriceSeries(historyA: PriceHistoryPointRow[], historyB: PriceHistoryPointRow[]): AlignedPricePoint[] {
    const symbolBByDate = new Map(historyB.map((point) => [point.date, point.close]));

    return historyA.flatMap((pointA) => {
        const closeB = symbolBByDate.get(pointA.date);
        if (closeB == null) {
            return [];
        }

        return [{
            date: pointA.date,
            closeA: pointA.close,
            closeB
        }];
    });
}

function computeLogReturns(alignedSeries: AlignedPricePoint[]): ReturnPoint[] {
    const returns: ReturnPoint[] = [];

    for (let index = 1; index < alignedSeries.length; index += 1) {
        const previous = alignedSeries[index - 1];
        const current = alignedSeries[index];

        returns.push({
            date: current.date,
            returnA: Math.log(current.closeA / previous.closeA),
            returnB: Math.log(current.closeB / previous.closeB)
        });
    }

    return returns;
}

function takeRecentTradingWindow(series: ReturnPoint[], tradingDays: number): ReturnPoint[] {
    const returnCount = Math.max(tradingDays - 1, 1);
    return series.slice(-Math.min(returnCount, series.length));
}

function calculateCorrelation(seriesA: number[], seriesB: number[]): number {
    if (seriesA.length !== seriesB.length || seriesA.length < 2) {
        return 0;
    }

    const meanA = calculateMean(seriesA);
    const meanB = calculateMean(seriesB);

    let numerator = 0;
    let varianceA = 0;
    let varianceB = 0;

    for (let index = 0; index < seriesA.length; index += 1) {
        const diffA = seriesA[index] - meanA;
        const diffB = seriesB[index] - meanB;
        numerator += diffA * diffB;
        varianceA += diffA * diffA;
        varianceB += diffB * diffB;
    }

    const denominator = Math.sqrt(varianceA * varianceB);
    return denominator === 0 ? 0 : numerator / denominator;
}

function calculateAnnualizedVolatility(returns: number[]): number {
    if (returns.length < 2) {
        return 0;
    }

    return calculateSampleStdDev(returns) * Math.sqrt(252);
}

function calculateDownsideSync(returnSeries: ReturnPoint[]): number {
    const triggerDays = returnSeries.filter((point) => Math.min(point.returnA, point.returnB) < -0.01);

    if (triggerDays.length === 0) {
        return 1;
    }

    const syncedDays = triggerDays.filter((point) => {
        if (point.returnA <= point.returnB) {
            return point.returnB < 0;
        }

        return point.returnA < 0;
    }).length;

    return syncedDays / triggerDays.length;
}

export function determineSuitability(
    corr90: number,
    corr180: number,
    corr252: number,
    corrBear2022: number,
    downsideSync: number
): 'HIGH' | 'MEDIUM' | 'LOW' {
    const maxDailyCorr = Math.max(corr90, corr180, corr252);
    if (downsideSync < 0.55) return 'LOW';
    if (corrBear2022 < 0.40) return 'LOW';
    if (maxDailyCorr < 0.30) return 'LOW';

    const dailyStrong = maxDailyCorr >= 0.50 && corr180 >= 0.40;
    if (downsideSync >= 0.70 && corrBear2022 >= 0.60 && dailyStrong) return 'HIGH';

    return 'MEDIUM';
}

function determineCorrelationStability(
    correlations: [number, number, number] | number[]
): 'STABLE' | 'MODERATE' | 'UNSTABLE' {
    const finiteValues = correlations.filter((value) => Number.isFinite(value));

    if (finiteValues.length === 0) {
        return 'UNSTABLE';
    }

    const range = Math.max(...finiteValues) - Math.min(...finiteValues);

    if (range <= 0.15) {
        return 'STABLE';
    }

    if (range <= 0.30) {
        return 'MODERATE';
    }

    return 'UNSTABLE';
}

interface SuitabilityNoteMetrics {
    corr90: number;
    corr180: number;
    corr252: number;
    corrBear2022: number;
    downsideSync: number;
    volGapFlag: boolean;
    volGapLeg: string | null;
    volRatio: number;
}

function pct(value: number): string {
    return `${Math.round(value * 100)}%`;
}

function corr(value: number): string {
    return value.toFixed(2);
}

function maxDailyCorr(metrics: Pick<SuitabilityNoteMetrics, 'corr90' | 'corr180' | 'corr252'>): number {
    return Math.max(metrics.corr90, metrics.corr180, metrics.corr252);
}

function volGapText(metrics: SuitabilityNoteMetrics): string | null {
    if (!metrics.volGapFlag || !metrics.volGapLeg) return null;
    return `${metrics.volGapLeg} 年化波动率高出对手 ${Math.round((metrics.volRatio - 1) * 100)}%,worst-of 风险倾向于该标的`;
}

export function buildSuitabilityNote(
    suitability: 'HIGH' | 'MEDIUM' | 'LOW',
    metrics: SuitabilityNoteMetrics
): SuitabilityNote {
    const syncPct = pct(metrics.downsideSync);
    const bear = corr(metrics.corrBear2022);
    const maxDaily = corr(maxDailyCorr(metrics));
    const volGap = volGapText(metrics);

    if (suitability === 'LOW') {
        if (metrics.downsideSync < 0.55) {
            return {
                reason: `下跌同步率仅 ${syncPct},低于 55% baseline,压力情景下两只标的会出现单边掉队风险`,
                weakness: `主要短板:下跌同步率 ${syncPct}`,
                next_step: '不建议双标结构,改单标方案'
            };
        }
        if (metrics.corrBear2022 < 0.40) {
            return {
                reason: `2022 熊市相关性仅 ${bear},压力情景下脱钩,无法依靠双标的结构对冲`,
                weakness: `主要短板:压力情景相关性 ${bear}`,
                next_step: '压力情景脱钩,改单标或更换其中一只'
            };
        }
        return {
            reason: `日常相关性最高仅 ${maxDaily}(90/180/252 日),两只标的不应配对`,
            weakness: '主要短板:日常相关性过低',
            next_step: '配对不合理,重选标的'
        };
    }

    if (suitability === 'HIGH') {
        const weakness = volGap ?? '无明显短板';
        const nextStep = volGap && metrics.volGapLeg
            ? `可直接进入询价 / pricing 流程;询价时可同时索取 ${metrics.volGapLeg} 的 vol/skew 数据`
            : '可直接进入询价 / pricing 流程';
        return {
            reason: `下跌同步率 ${syncPct}、2022 熊市相关性 ${bear}、日常相关性 ${corr(metrics.corr180)}(180 日)三项均达标`,
            weakness,
            next_step: nextStep
        };
    }

    const weaknesses: Array<{ label: string; reason: string; next: string }> = [];
    if (metrics.downsideSync < 0.70) {
        weaknesses.push({
            label: `下跌同步率 ${syncPct}`,
            reason: `下跌同步率 ${syncPct} 达 baseline 但未到 70% 强同步区间`,
            next: '可继续询价,结构端讨论执行价下调 5-10% 或缩短至 6 个月'
        });
    }
    if (metrics.corrBear2022 < 0.60) {
        weaknesses.push({
            label: `2022 熊市相关性 ${bear}`,
            reason: `2022 熊市相关性 ${bear} 达 baseline 但未到 0.60 强联动`,
            next: '可询价,pricing 后建议对比同 sector 替代 pair'
        });
    }
    if (!(maxDailyCorr(metrics) >= 0.50 && metrics.corr180 >= 0.40)) {
        weaknesses.push({
            label: `日常相关性偏弱`,
            reason: `日常相关性偏弱(corr180 = ${corr(metrics.corr180)},maxDaily = ${maxDaily})`,
            next: '可继续询价,但短期交易节奏可能分化,建议关注 pricing 时的隐含相关性'
        });
    }

    const primary = weaknesses[0] ?? {
        label: '指标未完全进入强确认区间',
        reason: '核心指标达 baseline,但未全部进入 HIGH 区间',
        next: '可继续询价,并在 pricing 后复核风险补偿'
    };
    const weaknessLabels = weaknesses.slice(0, 2).map((item) => item.label);
    let weakness = `主要短板:${weaknessLabels.length > 0 ? weaknessLabels.join('、') : primary.label}`;
    let nextStep = primary.next;

    if (volGap && metrics.volGapLeg) {
        weakness = `${weakness};${volGap}`;
        nextStep = `${nextStep},并优先评估 ${metrics.volGapLeg} 单标方案`;
    }

    return {
        reason: primary.reason,
        weakness,
        next_step: nextStep
    };
}

function formatSuitabilityNote(note: SuitabilityNote): string {
    return [note.reason, note.weakness, note.next_step].join('\n\n');
}

function calculateMean(values: number[]): number {
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function calculateSampleStdDev(values: number[]): number {
    if (values.length < 2) {
        return 0;
    }

    const mean = calculateMean(values);
    const variance = values.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / (values.length - 1);
    return Math.sqrt(variance);
}

function roundMetric(value: number): number {
    if (!Number.isFinite(value)) {
        return 0;
    }

    return Math.round(value * 10000) / 10000;
}
