import { MassiveClient } from '../data/massive-client';
import {
    getActiveRecommendationTrackers,
    getRecommendationTrackerHistoryRows,
    getRecommendationTrackerSummaryRows,
    type RecommendationTrackerRow,
    updateRecommendationTrackerStatus
} from '../db/queries/ideas';
import type { TrackerHistoryEntry, TrackerPosition, TrackerSummaryResponse } from '../types/api';

interface MassivePreviousCloseResponse {
    results?: Array<Record<string, unknown>>;
}

export async function runPriceTracker(): Promise<void> {
    const rows = await getActiveRecommendationTrackers();
    if (rows.length === 0) {
        return;
    }

    const client = new MassiveClient();
    const today = todayIsoDate();

    for (const row of rows) {
        const currentPrice = await fetchPreviousClose(client, row.symbol);
        const pctAboveStrike =
            currentPrice !== null && row.recommended_strike !== null && row.recommended_strike !== 0
                ? Number((((currentPrice - row.recommended_strike) / row.recommended_strike) * 100).toFixed(2))
                : null;

        const isExpired = row.expiry_date !== null && row.expiry_date < today;
        const breachedToday =
            currentPrice !== null &&
            row.recommended_strike !== null &&
            currentPrice <= row.recommended_strike;

        let status: RecommendationTrackerRow['status'] = row.status;
        let breachedDate = row.breached_date;

        if (!isExpired) {
            if (breachedToday) {
                status = 'BREACHED';
                breachedDate = breachedDate ?? today;
            } else {
                status = 'ACTIVE';
            }
        } else {
            status = row.breached_date ? 'EXPIRED_BREACHED' : 'EXPIRED_SAFE';
        }

        await updateRecommendationTrackerStatus({
            id: row.id,
            currentPrice,
            pctAboveStrike,
            status,
            breachedDate
        });
    }
}

export async function getTrackerSummary(): Promise<TrackerSummaryResponse> {
    const rows = await getRecommendationTrackerSummaryRows();
    const totalRecommendations = rows.length;
    const activeRows = rows.filter((row) => row.status === 'ACTIVE');
    const breachedOpenRows = rows.filter((row) => row.status === 'BREACHED');
    const openRows = [...activeRows, ...breachedOpenRows];
    const activePositions = openRows.map(mapTrackerPosition);
    const expiredSafe = rows.filter((row) => row.status === 'EXPIRED_SAFE').length;
    const expiredBreached = rows.filter((row) => row.status === 'EXPIRED_BREACHED').length;
    const safeDenominator = expiredSafe + expiredBreached;
    const avgSafetyBufferRows = openRows.filter((row) => row.pct_above_strike !== null);
    const avgSafetyBuffer =
        avgSafetyBufferRows.length > 0
            ? avgSafetyBufferRows.reduce((sum, row) => sum + Number(row.pct_above_strike ?? 0), 0) / avgSafetyBufferRows.length
            : 0;

    return {
        total_recommendations: totalRecommendations,
        active: activeRows.length,
        breached_open: breachedOpenRows.length,
        expired_safe: expiredSafe,
        expired_breached: expiredBreached,
        safe_rate: safeDenominator === 0 ? '-' : `${((expiredSafe / safeDenominator) * 100).toFixed(1)}%`,
        avg_safety_buffer: `${avgSafetyBuffer.toFixed(1)}%`,
        positions_near_strike: openRows.filter((row) => (row.pct_above_strike ?? Number.POSITIVE_INFINITY) < 10).length,
        active_positions: activePositions
    };
}

export async function getTrackerHistory(): Promise<TrackerHistoryEntry[]> {
    const rows = await getRecommendationTrackerHistoryRows();
    return rows.map((row) => ({
        symbol: row.symbol,
        grade: row.grade,
        recommendation_date: row.recommendation_date,
        entry_price: toNullableNumber(row.entry_price),
        recommended_strike: toNullableNumber(row.recommended_strike),
        moneyness_pct: toNullableNumber(row.moneyness_pct),
        expiry_date: row.expiry_date,
        current_price: toNullableNumber(row.current_price),
        pct_above_strike: toNullableNumber(row.pct_above_strike),
        status: row.status as 'EXPIRED_SAFE' | 'EXPIRED_BREACHED',
        last_checked: row.last_checked,
        breached_date: row.breached_date
    }));
}

async function fetchPreviousClose(client: MassiveClient, symbol: string): Promise<number | null> {
    const response = await client.get<MassivePreviousCloseResponse>(`/v2/aggs/ticker/${symbol}/prev`, {
        adjusted: true
    });

    const close = response.results?.[0]?.c;
    return typeof close === 'number' && Number.isFinite(close) ? close : null;
}

function mapTrackerPosition(row: RecommendationTrackerRow): TrackerPosition {
    return {
        symbol: row.symbol,
        grade: row.grade,
        recommendation_date: row.recommendation_date,
        entry_price: toNullableNumber(row.entry_price),
        recommended_strike: toNullableNumber(row.recommended_strike),
        moneyness_pct: toNullableNumber(row.moneyness_pct),
        expiry_date: row.expiry_date,
        current_price: toNullableNumber(row.current_price),
        pct_above_strike: toNullableNumber(row.pct_above_strike),
        status: row.status,
        days_remaining:
            row.expiry_date !== null ? daysBetween(todayIsoDate(), row.expiry_date) : null
    };
}

function daysBetween(fromDate: string, toDate: string): number | null {
    const from = Date.parse(fromDate);
    const to = Date.parse(toDate);
    if (Number.isNaN(from) || Number.isNaN(to)) {
        return null;
    }

    return Math.max(0, Math.ceil((to - from) / (24 * 60 * 60 * 1000)));
}

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function toNullableNumber(value: number | null): number | null {
    return value === null || value === undefined ? null : Number(value);
}
