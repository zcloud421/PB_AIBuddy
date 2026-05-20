import { MassiveClient } from './massive-client';

export interface SoxPoint {
    date: string;
    close: number;
}

export interface SoxHistoryResult {
    points: SoxPoint[];
    source: 'Polygon I:SOX' | 'SMH fallback';
}

interface MassiveAggregatesResponse {
    results?: Array<Record<string, unknown>>;
}

const CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const DEFAULT_LOOKBACK_DAYS = 730;

let cache: { value: SoxHistoryResult; expiresAt: number; lookbackDays: number } | null = null;

function isoDateOffsetDays(offsetDays: number): string {
    const date = new Date();
    date.setUTCDate(date.getUTCDate() + offsetDays);
    return date.toISOString().slice(0, 10);
}

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function getNumber(row: Record<string, unknown>, key: string): number | null {
    const value = row[key];
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function mapAggRow(row: Record<string, unknown>): SoxPoint | null {
    const timestamp = getNumber(row, 't');
    const close = getNumber(row, 'c');
    if (timestamp === null || close === null || close <= 0) return null;
    return {
        date: new Date(timestamp).toISOString().slice(0, 10),
        close
    };
}

async function fetchTickerHistory(client: MassiveClient, ticker: string, lookbackDays: number): Promise<SoxPoint[]> {
    const response = await client.get<MassiveAggregatesResponse>(
        `/v2/aggs/ticker/${ticker}/range/1/day/${isoDateOffsetDays(-lookbackDays)}/${todayIsoDate()}`,
        {
            adjusted: true,
            sort: 'asc',
            limit: Math.max(500, Math.min(5000, lookbackDays + 64))
        }
    );

    return (response.results ?? [])
        .map((row) => mapAggRow(row))
        .filter((point): point is SoxPoint => point !== null)
        .sort((left, right) => left.date.localeCompare(right.date));
}

/**
 * Fetch Philadelphia Semiconductor Index daily closes. Falls back to SMH when
 * the account/vendor cannot serve I:SOX aggregates. The fallback is deliberately
 * labelled so downstream notes remain transparent.
 */
export async function fetchSoxIndexHistoryWithSource(
    lookbackDays: number = DEFAULT_LOOKBACK_DAYS
): Promise<SoxHistoryResult> {
    if (cache && cache.expiresAt > Date.now() && cache.lookbackDays >= lookbackDays) {
        return cache.value;
    }

    let client: MassiveClient;
    try {
        client = new MassiveClient();
    } catch (error) {
        console.warn('[sox-index-fetcher] MASSIVE_API_KEY missing:', error instanceof Error ? error.message : error);
        return { points: [], source: 'SMH fallback' };
    }
    try {
        const sox = await fetchTickerHistory(client, 'I:SOX', lookbackDays);
        if (sox.length >= 200) {
            const value: SoxHistoryResult = { points: sox, source: 'Polygon I:SOX' };
            cache = { value, expiresAt: Date.now() + CACHE_TTL_MS, lookbackDays };
            return value;
        }
        console.warn(`[sox-index-fetcher] I:SOX returned insufficient history (${sox.length}); falling back to SMH`);
    } catch (error) {
        console.warn('[sox-index-fetcher] I:SOX fetch failed; falling back to SMH:', error instanceof Error ? error.message : error);
    }

    try {
        const smh = await fetchTickerHistory(client, 'SMH', lookbackDays);
        const value: SoxHistoryResult = { points: smh, source: 'SMH fallback' };
        cache = { value, expiresAt: Date.now() + CACHE_TTL_MS, lookbackDays };
        return value;
    } catch (error) {
        console.warn('[sox-index-fetcher] SMH fallback failed:', error instanceof Error ? error.message : error);
        return { points: [], source: 'SMH fallback' };
    }
}

export async function fetchSoxIndexHistory(lookbackDays: number = DEFAULT_LOOKBACK_DAYS): Promise<SoxPoint[]> {
    return (await fetchSoxIndexHistoryWithSource(lookbackDays)).points;
}
