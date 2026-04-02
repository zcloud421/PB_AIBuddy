import { MassiveClient } from '../data/massive-client';
import {
    getActiveRecommendationTrackers,
    getRecommendationTrackerHistoryRows,
    getRecommendationTrackerSummaryRows,
    getShowcaseTrackerSummaryRows,
    type RecommendationTrackerRow,
    updateRecommendationTrackerStatus
} from '../db/queries/ideas';
import type {
    TrackerHistoryEntry,
    TrackerPosition,
    TrackerReviewBucket,
    TrackerReviewResponse,
    TrackerReviewWindow,
    TrackerSummaryBucket,
    TrackerSummaryResponse
} from '../types/api';

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
    const [rows, showcaseRows] = await Promise.all([
        getRecommendationTrackerSummaryRows(),
        getShowcaseTrackerSummaryRows()
    ]);
    const overall = summarizeTrackerRows(rows);
    const activeRows = rows.filter((row) => row.status === 'ACTIVE');
    const breachedOpenRows = rows.filter((row) => row.status === 'BREACHED');
    const openRows = [...activeRows, ...breachedOpenRows];
    const activePositions = openRows.map(mapTrackerPosition);
    const avgSafetyBufferRows = openRows.filter((row) => row.pct_above_strike !== null);
    const avgSafetyBuffer =
        avgSafetyBufferRows.length > 0
            ? avgSafetyBufferRows.reduce((sum, row) => sum + Number(row.pct_above_strike ?? 0), 0) / avgSafetyBufferRows.length
            : 0;
    const showcaseSummary = {
        hero: summarizeTrackerRows(showcaseRows.filter((row) => row.placement === 'HERO')),
        recommended: summarizeTrackerRows(showcaseRows.filter((row) => row.placement === 'RECOMMENDED'))
    };

    return {
        total_recommendations: overall.total_recommendations,
        active: overall.active,
        breached_open: overall.breached_open,
        expired_safe: overall.expired_safe,
        expired_breached: overall.expired_breached,
        safe_rate: overall.safe_rate,
        breach_rate: overall.breach_rate,
        avg_safety_buffer: `${avgSafetyBuffer.toFixed(1)}%`,
        positions_near_strike: openRows.filter((row) => (row.pct_above_strike ?? Number.POSITIVE_INFINITY) < 10).length,
        active_positions: activePositions,
        showcase_summary: showcaseSummary
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

export async function getTrackerReview(): Promise<TrackerReviewResponse> {
    const rows = await getRecommendationTrackerSummaryRows();
    const today = todayIsoDate();
    const rolling30dRows = rows.filter((row) => daysSince(row.recommendation_date, today) <= 30);
    const rolling90dRows = rows.filter((row) => daysSince(row.recommendation_date, today) <= 90);
    const matured3mRows = rows.filter((row) => isThreeMonthTenor(row) && isMatured(row));

    return {
        generated_at: new Date().toISOString(),
        overall: summarizeReviewRows(rows),
        rolling_30d: buildReviewWindow('最近30天推荐', rolling30dRows),
        rolling_90d: buildReviewWindow('最近90天推荐', rolling90dRows),
        matured_3m: buildReviewWindow('已到期3个月期限', matured3mRows)
    };
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

function summarizeTrackerRows(rows: RecommendationTrackerRow[]): TrackerSummaryBucket {
    const active = rows.filter((row) => row.status === 'ACTIVE').length;
    const breachedOpen = rows.filter((row) => row.status === 'BREACHED').length;
    const expiredSafe = rows.filter((row) => row.status === 'EXPIRED_SAFE').length;
    const expiredBreached = rows.filter((row) => row.status === 'EXPIRED_BREACHED').length;
    const expiredTotal = expiredSafe + expiredBreached;

    return {
        total_recommendations: rows.length,
        active,
        breached_open: breachedOpen,
        expired_safe: expiredSafe,
        expired_breached: expiredBreached,
        safe_rate: expiredTotal === 0 ? '-' : `${((expiredSafe / expiredTotal) * 100).toFixed(1)}%`,
        breach_rate: expiredTotal === 0 ? '-' : `${((expiredBreached / expiredTotal) * 100).toFixed(1)}%`
    };
}

function summarizeReviewRows(rows: RecommendationTrackerRow[]): TrackerReviewBucket {
    const maturedRows = rows.filter((row) => row.status === 'EXPIRED_SAFE' || row.status === 'EXPIRED_BREACHED');
    const maturitySafeCount = maturedRows.filter((row) => row.status === 'EXPIRED_SAFE').length;
    const maturityBreachedCount = maturedRows.filter((row) => row.status === 'EXPIRED_BREACHED').length;
    const everBreachedCount = rows.filter((row) => row.breached_date !== null).length;
    const activeRows = rows.filter((row) => row.status === 'ACTIVE' || row.status === 'BREACHED');
    const activeBelowStrikeCount = activeRows.filter((row) => row.status === 'BREACHED').length;

    return {
        total_recommendations: rows.length,
        matured_recommendations: maturedRows.length,
        active_recommendations: rows.length - maturedRows.length,
        ever_breached_count: everBreachedCount,
        maturity_safe_count: maturitySafeCount,
        maturity_breached_count: maturityBreachedCount,
        active_below_strike_count: activeBelowStrikeCount,
        path_breach_rate: rows.length === 0 ? '-' : `${((everBreachedCount / rows.length) * 100).toFixed(1)}%`,
        maturity_safe_rate:
            maturedRows.length === 0 ? '-' : `${((maturitySafeCount / maturedRows.length) * 100).toFixed(1)}%`,
        active_below_strike_rate:
            activeRows.length === 0 ? '-' : `${((activeBelowStrikeCount / activeRows.length) * 100).toFixed(1)}%`
    };
}

function buildReviewWindow(label: string, rows: RecommendationTrackerRow[]): TrackerReviewWindow {
    return {
        label,
        summary: summarizeReviewRows(rows)
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

function daysSince(fromDate: string, toDate: string): number {
    const from = Date.parse(fromDate);
    const to = Date.parse(toDate);
    if (Number.isNaN(from) || Number.isNaN(to)) {
        return Number.POSITIVE_INFINITY;
    }

    return Math.max(0, Math.floor((to - from) / (24 * 60 * 60 * 1000)));
}

function isMatured(row: RecommendationTrackerRow): boolean {
    return row.status === 'EXPIRED_SAFE' || row.status === 'EXPIRED_BREACHED';
}

function isThreeMonthTenor(row: RecommendationTrackerRow): boolean {
    const tenor = row.recommended_tenor_days;
    return tenor !== null && tenor >= 75 && tenor <= 100;
}

function toNullableNumber(value: number | null): number | null {
    return value === null || value === undefined ? null : Number(value);
}
