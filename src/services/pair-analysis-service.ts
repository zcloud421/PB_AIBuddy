import { MassiveDataFetcher } from '../data/massive-fetcher';
import { getRecentPriceHistoryBySymbol, type PriceHistoryPointRow } from '../db/queries/ideas';

export interface PairAnalysisResponse {
    symbolA: string;
    symbolB: string;
    data_as_of: string;
    trading_days_overlap: number;
    correlation: {
        d90: number;
        d180: number;
        d252: number;
        bear_2022: number;
    };
    volatility: {
        symbolA_annualized: number;
        symbolB_annualized: number;
        gap: number;
    };
    downside_sync: number;
    correlation_stability: 'STABLE' | 'MODERATE' | 'UNSTABLE';
    suitability: 'HIGH' | 'MEDIUM' | 'LOW';
    suitability_note: string;
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
    const downsideSync = roundMetric(calculateDownsideSync(returnSeries));
    const correlationStability = determineCorrelationStability([corr90, corr180, corr252]);
    const suitability = determineSuitability(corr90, corr180, downsideSync);

    return {
        kind: 'ok',
        data: {
            symbolA: normalizedA,
            symbolB: normalizedB,
            data_as_of: recentAlignedSeries[recentAlignedSeries.length - 1].date,
            trading_days_overlap: recentAlignedSeries.length,
            correlation: {
                d90: corr90,
                d180: corr180,
                d252: corr252,
                bear_2022: corrBear2022
            },
            volatility: {
                symbolA_annualized: volA,
                symbolB_annualized: volB,
                gap: volatilityGap
            },
            downside_sync: downsideSync,
            correlation_stability: correlationStability,
            suitability,
            suitability_note: getSuitabilityNote(suitability)
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

function determineSuitability(
    corr60: number,
    corr120: number,
    downsideSync: number
): 'HIGH' | 'MEDIUM' | 'LOW' {
    if (corr60 >= 0.60 && corr120 >= 0.60 && downsideSync >= 0.60) {
        return 'HIGH';
    }

    if (corr60 < 0.40 || corr120 < 0.35 || downsideSync < 0.40) {
        return 'LOW';
    }

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

function getSuitabilityNote(suitability: 'HIGH' | 'MEDIUM' | 'LOW'): string {
    if (suitability === 'HIGH') {
        return '相关性与下跌同步率均达标，可考虑提供双标的报价参考';
    }

    if (suitability === 'LOW') {
        return '两标的走势分化显著，双标的挂钩结构不建议直接使用，可考虑单标的方案';
    }

    return '相关性中等，建议优先讨论执行价下调或缩短期限以控制风险';
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
