import type { ChainData, DataFetcherInterface, StrikeData, SymbolData } from '../scoring-engine';
import { MassiveClient } from './massive-client';
import { getUpcomingEarningsBySymbol } from '../db/queries/ideas';

interface MassiveOptionChainResponse {
    next_url?: string;
    results?: Array<Record<string, unknown>>;
}

interface MassiveReferenceContractsResponse {
    next_url?: string;
    results?: Array<Record<string, unknown>>;
}

interface MassiveAggregatesResponse {
    results?: Array<Record<string, unknown>>;
}

interface MassivePreviousCloseResponse {
    results?: Array<Record<string, unknown>>;
}

interface MassiveTickerReferenceResponse {
    results?: {
        name?: string;
    };
}

export class MassiveDataFetcher implements DataFetcherInterface {
    private readonly client: MassiveClient;

    constructor(client = new MassiveClient()) {
        this.client = client;
    }

    async fetchChainData(symbol: string, currentPrice: number): Promise<ChainData> {
        const minExpiry = isoDateOffsetDays(85);
        const maxExpiry = isoDateOffsetDays(180);
        const maxStrike = Math.floor(currentPrice * 0.90);
        const eligibleContracts = await this.fetchEligibleContracts(symbol, minExpiry, maxExpiry, maxStrike);
        const rows = await this.fetchOptionSnapshotFirstPage(symbol, minExpiry, maxStrike);
        const eligibleTickers = new Set(eligibleContracts);

        return rows
            .map((row) => mapOptionRow(row))
            .filter((row): row is StrikeDataWithRaw => row !== null)
            .filter((row) => {
                const ticker = getNestedString(row.raw, ['details', 'ticker']);
                return ticker !== null && eligibleTickers.has(ticker);
            })
            .filter((row) => {
                const dte = daysUntilExpiry(row.expiry_date);
                return dte >= 85 && dte <= 180;
            })
            .filter((row) => row.open_interest >= 10)
            .filter((row) => row.delta >= -0.40 && row.delta <= -0.15)
            .map(({ raw: _raw, ...strike }) => strike);
    }

    async fetchSymbolData(symbol: string): Promise<SymbolData> {
        const fromDate = isoDateOffsetDays(-365);
        const toDate = todayIsoDate();

        const historyResponse = await this.client.get<MassiveAggregatesResponse>(
            `/v2/aggs/ticker/${symbol}/range/1/day/${fromDate}/${toDate}`,
            {
                adjusted: true,
                sort: 'asc',
                limit: 365
            }
        );

        const history = (historyResponse.results ?? [])
            .map((row) => mapPriceRow(row))
            .filter((row): row is { date: string; close: number; high: number } => row !== null)
            .sort((a, b) => a.date.localeCompare(b.date));

        if (history.length < 250) {
            console.warn(`[fetcher] only ${history.length} days of history for ${symbol}, proceeding anyway`);
        }

        const historyWindow = history.slice(-250);
        const closes = historyWindow.map((row) => row.close);
        const highs = historyWindow.map((row) => row.high);
        const macd = computeMacd(closes);
        const rsi14 = computeRsi(closes, 14);

        const previousCloseResponse = await this.client.get<MassivePreviousCloseResponse>(
            `/v2/aggs/ticker/${symbol}/prev`,
            { adjusted: true }
        );
        const upcomingEarnings = await getUpcomingEarningsBySymbol(symbol);

        const currentPrice = extractPreviousClose(previousCloseResponse.results) ?? closes[closes.length - 1];
        const high52w = Math.max(...highs);

        return {
            price_history: historyWindow,
            high_52w: high52w,
            low_52w: Math.min(...closes),
            current_price: currentPrice,
            ma20: averageTail(closes, 20),
            ma50: averageTail(closes, 50),
            ma200: averageTail(closes, 200),
            days_since_52w_high: daysSince52wHigh(historyWindow),
            pct_from_52w_high: percentFromHigh(currentPrice, high52w),
            iv_rank: null,
            macd_line: macd.macdLine,
            macd_signal: macd.signalLine,
            macd_histogram: macd.histogram,
            rsi_14: rsi14,
            // Earnings are maintained manually via house_overrides or internal workflows.
            earnings_date: upcomingEarnings?.report_date ?? null,
            days_to_earnings: upcomingEarnings?.days_until ?? null
        };
    }

    private async fetchEligibleContracts(
        symbol: string,
        minExpiry: string,
        maxExpiry: string,
        maxStrike: number
    ): Promise<string[]> {
        const tickers: string[] = [];
        let nextPath: string | null = '/v3/reference/options/contracts';
        let nextParams: Record<string, string | number | boolean | undefined> | undefined = {
            underlying_ticker: symbol,
            contract_type: 'put',
            'expiration_date.gte': minExpiry,
            'expiration_date.lte': maxExpiry,
            'strike_price.lte': maxStrike,
            limit: 250,
            sort: 'expiration_date',
            order: 'asc'
        };

        while (nextPath) {
            const response: MassiveReferenceContractsResponse =
                await this.client.get<MassiveReferenceContractsResponse>(nextPath, nextParams);
            const pageRows = response.results ?? [];

            for (const row of pageRows) {
                const ticker = getString(row, 'ticker');
                if (ticker) {
                    tickers.push(ticker);
                }
            }

            if (!response.next_url) {
                nextPath = null;
                continue;
            }

            const nextUrl: URL = new URL(response.next_url);
            nextPath = `${nextUrl.pathname}${nextUrl.search}`;
            nextParams = undefined;
        }

        return tickers;
    }

    private async fetchOptionSnapshotFirstPage(
        symbol: string,
        minExpiry: string,
        maxStrike: number
    ): Promise<Array<Record<string, unknown>>> {
        const path = `/v3/snapshot/options/${symbol}`;
        const params: Record<string, string | number | boolean | undefined> = {
            contract_type: 'put',
            limit: 250,
            'expiration_date.gte': minExpiry,
            'strike_price.lte': maxStrike,
            sort: 'expiration_date',
            order: 'asc'
        };

        const response: MassiveOptionChainResponse = await this.client.get<MassiveOptionChainResponse>(path, params);

        return response.results ?? [];
    }
}

export async function fetchTickerCompanyName(symbol: string): Promise<string | null> {
    const client = new MassiveClient();
    const response = await client.get<MassiveTickerReferenceResponse>(`/v3/reference/tickers/${symbol}`);
    const name = response.results?.name;
    return typeof name === 'string' && name.trim().length > 0 ? name.trim() : null;
}

type StrikeDataWithRaw = StrikeData & { raw: Record<string, unknown> };

function mapOptionRow(row: Record<string, unknown>): StrikeDataWithRaw | null {
    const optionType = getNestedString(row, ['details', 'contract_type']);
    if (optionType !== 'put') {
        return null;
    }

    const greeks = getObject(row, 'greeks');
    if (!greeks) {
        return null;
    }

    const strike = getNestedNumber(row, ['details', 'strike_price']);
    const expiryDate = getNestedString(row, ['details', 'expiration_date']);
    const iv = getNumber(row, 'implied_volatility');
    const delta = getNestedNumber(row, ['greeks', 'delta']);
    const gamma = getNestedNumber(row, ['greeks', 'gamma']);
    const vega = getNestedNumber(row, ['greeks', 'vega']);
    const theta = getNestedNumber(row, ['greeks', 'theta']);
    const volume = getNestedNumber(row, ['day', 'volume']) ?? 0;
    const openInterest = getNumber(row, 'open_interest');
    const midpoint = getNestedNumber(row, ['last_quote', 'midpoint']);
    const ask = getNestedNumber(row, ['last_quote', 'ask']);
    const bid = getNestedNumber(row, ['last_quote', 'bid']);
    const dayClose = getNestedNumber(row, ['day', 'close']);

    if (
        strike === null ||
        expiryDate === null ||
        iv === null ||
        delta === null ||
        gamma === null ||
        vega === null ||
        theta === null ||
        openInterest === null
    ) {
        return null;
    }

    const computedMidpoint = computeMidpoint(bid, ask);
    const midPrice = computedMidpoint ?? midpoint ?? dayClose;
    const midPriceSource: 'last_quote' | 'day.close' | 'none' =
        computedMidpoint !== null || midpoint !== null ? 'last_quote' : dayClose !== null ? 'day.close' : 'none';

    return {
        strike,
        expiry_date: expiryDate,
        iv,
        delta,
        gamma,
        vega,
        theta,
        volume,
        open_interest: openInterest,
        mid_price: midPrice,
        mid_price_source: midPriceSource,
        raw: row
    };
}

function mapPriceRow(row: Record<string, unknown>): { date: string; close: number; high: number } | null {
    const timestamp = getNumber(row, 't');
    const close = getNumber(row, 'c');
    const high = getNumber(row, 'h');

    if (timestamp === null || close === null || high === null) {
        return null;
    }

    return {
        date: toIsoDate(timestamp),
        close,
        high
    };
}

function extractPreviousClose(results: Array<Record<string, unknown>> | undefined): number | null {
    if (!results || results.length === 0) {
        return null;
    }

    return getNumber(results[0], 'c');
}

function computeMidpoint(bid: number | null, ask: number | null): number | null {
    if (bid === null && ask === null) {
        return null;
    }

    return ((bid ?? 0) + (ask ?? 0)) / 2;
}

function averageTail(values: number[], lookback: number): number {
    const slice = values.slice(-lookback);
    if (slice.length === 0) {
        return 0;
    }

    return slice.reduce((sum, value) => sum + value, 0) / slice.length;
}

function computeMacd(values: number[]): { macdLine: number | null; signalLine: number | null; histogram: number | null } {
    if (values.length < 35) {
        return {
            macdLine: null,
            signalLine: null,
            histogram: null
        };
    }

    const ema12 = computeEmaSeries(values, 12);
    const ema26 = computeEmaSeries(values, 26);
    const macdSeries: number[] = [];

    for (let index = 0; index < values.length; index += 1) {
        const fast = ema12[index];
        const slow = ema26[index];
        if (fast !== null && slow !== null) {
            macdSeries.push(fast - slow);
        }
    }

    if (macdSeries.length < 9) {
        return {
            macdLine: null,
            signalLine: null,
            histogram: null
        };
    }

    const signalSeries = computeEmaSeries(macdSeries, 9).filter((value): value is number => value !== null);
    const macdLine = macdSeries[macdSeries.length - 1] ?? null;
    const signalLine = signalSeries[signalSeries.length - 1] ?? null;

    return {
        macdLine,
        signalLine,
        histogram: macdLine !== null && signalLine !== null ? macdLine - signalLine : null
    };
}

function computeRsi(values: number[], period: number): number | null {
    if (values.length <= period) {
        return null;
    }

    let gains = 0;
    let losses = 0;

    for (let index = 1; index <= period; index += 1) {
        const delta = values[index] - values[index - 1];
        if (delta >= 0) {
            gains += delta;
        } else {
            losses += Math.abs(delta);
        }
    }

    let averageGain = gains / period;
    let averageLoss = losses / period;

    for (let index = period + 1; index < values.length; index += 1) {
        const delta = values[index] - values[index - 1];
        const gain = delta > 0 ? delta : 0;
        const loss = delta < 0 ? Math.abs(delta) : 0;
        averageGain = ((averageGain * (period - 1)) + gain) / period;
        averageLoss = ((averageLoss * (period - 1)) + loss) / period;
    }

    if (averageLoss === 0) {
        return 100;
    }

    const rs = averageGain / averageLoss;
    return 100 - (100 / (1 + rs));
}

function computeEmaSeries(values: number[], period: number): Array<number | null> {
    if (values.length < period) {
        return values.map(() => null);
    }

    const multiplier = 2 / (period + 1);
    const result: Array<number | null> = values.map(() => null);
    let ema = values.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
    result[period - 1] = ema;

    for (let index = period; index < values.length; index += 1) {
        ema = ((values[index] - ema) * multiplier) + ema;
        result[index] = ema;
    }

    return result;
}

function daysSince52wHigh(history: Array<{ date: string; close: number; high: number }>): number {
    if (history.length === 0) {
        return 0;
    }

    let highIndex = 0;
    for (let index = 1; index < history.length; index += 1) {
        if (history[index].high >= history[highIndex].high) {
            highIndex = index;
        }
    }

    return history.length - 1 - highIndex;
}

function percentFromHigh(currentPrice: number, high52w: number): number {
    if (high52w === 0) {
        return 0;
    }

    return ((currentPrice - high52w) / high52w) * 100;
}

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function isoDateOffsetDays(offsetDays: number): string {
    const date = new Date();
    date.setDate(date.getDate() + offsetDays);
    return date.toISOString().split('T')[0];
}

function daysUntilExpiry(expiryDate: string): number {
    const today = new Date();
    const expiry = new Date(expiryDate);
    return Math.ceil((expiry.getTime() - today.getTime()) / (24 * 60 * 60 * 1000));
}


function toIsoDate(timestampMs: number): string {
    return new Date(timestampMs).toISOString().slice(0, 10);
}

function getObject(row: Record<string, unknown>, key: string): Record<string, unknown> | null {
    const value = row[key];
    return value && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null;
}

function getString(row: Record<string, unknown>, key: string): string | null {
    const value = row[key];
    return typeof value === 'string' ? value : null;
}

function getNestedString(row: Record<string, unknown>, path: string[]): string | null {
    const value = getNestedValue(row, path);
    return typeof value === 'string' ? value : null;
}

function getNumber(row: Record<string, unknown>, key: string): number | null {
    const value = row[key];
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getNestedNumber(row: Record<string, unknown>, path: string[]): number | null {
    const value = getNestedValue(row, path);
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getNestedValue(row: Record<string, unknown>, path: string[]): unknown {
    let current: unknown = row;

    for (const key of path) {
        if (!current || typeof current !== 'object' || !(key in current)) {
            return null;
        }
        current = (current as Record<string, unknown>)[key];
    }

    return current;
}
