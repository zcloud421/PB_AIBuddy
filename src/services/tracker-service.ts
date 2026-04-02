import { MassiveClient } from '../data/massive-client';
import {
    getActiveRecommendationTrackers,
    getDailyRecommendationHistoryRowsInRange,
    getRecommendationTrackerHistoryRows,
    getRecommendationTrackerSummaryRows,
    getShowcaseTrackerSummaryRows,
    type DailyRecommendationHistoryRow,
    type RecommendationTrackerRow,
    updateRecommendationTrackerStatus
} from '../db/queries/ideas';
import type {
    TrackerHistoryEntry,
    TrackerMonthlyConcentrationItem,
    TrackerMonthlyMetric,
    TrackerPosition,
    TrackerReviewResponse,
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
    const today = todayInHongKongIsoDate();
    const { start: reportStart, end: reportEnd, label: reportMonth } = previousCalendarMonth(today);
    const { start: baselineStart, end: baselineEnd, label: baselineMonth } = previousCalendarMonth(reportStart);
    const [rows, reportDailyHistory] = await Promise.all([
        getRecommendationTrackerSummaryRows(),
        getDailyRecommendationHistoryRowsInRange(reportStart, reportEnd)
    ]);
    const reportRows = rows.filter((row) => row.recommendation_date >= reportStart && row.recommendation_date <= reportEnd);
    const baselineRows = rows.filter((row) => row.recommendation_date >= baselineStart && row.recommendation_date <= baselineEnd);
    const reportSummary = summarizeMonthlyRows(reportRows);
    const baselineSummary = summarizeMonthlyRows(baselineRows);

    return {
        generated_at: new Date().toISOString(),
        report_month: reportMonth,
        baseline_month: baselineMonth,
        summary: {
            new_recommendations: metricCountOnly(reportRows.length),
            path_breach: metricFromSummary(reportSummary.total, reportSummary.pathBreached),
            still_below_strike: metricFromSummary(reportSummary.total, reportSummary.activeBelowStrike),
            matured_3m_safe: metricFromSummary(reportSummary.matured3mTotal, reportSummary.matured3mSafe)
        },
        comparison: {
            path_breach_rate_change_pct: rateDelta(reportSummary.total, reportSummary.pathBreached, baselineSummary.total, baselineSummary.pathBreached),
            still_below_strike_rate_change_pct: rateDelta(
                reportSummary.total,
                reportSummary.activeBelowStrike,
                baselineSummary.total,
                baselineSummary.activeBelowStrike
            ),
            matured_3m_safe_rate_change_pct: rateDelta(
                reportSummary.matured3mTotal,
                reportSummary.matured3mSafe,
                baselineSummary.matured3mTotal,
                baselineSummary.matured3mSafe
            )
        },
        concentration: buildMonthlyConcentration(reportDailyHistory),
        rolling_30d: buildMonthlySection('最近30天推荐', rows.filter((row) => daysSince(row.recommendation_date, today) <= 30)),
        rolling_90d: buildMonthlySection('最近90天推荐', rows.filter((row) => daysSince(row.recommendation_date, today) <= 90)),
        matured_3m: buildMonthlySection('已到期3个月期限', rows.filter((row) => isThreeMonthTenor(row) && isMatured(row)))
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

interface MonthlyReviewCounters {
    total: number;
    pathBreached: number;
    activeBelowStrike: number;
    matured3mTotal: number;
    matured3mSafe: number;
}

function summarizeMonthlyRows(rows: RecommendationTrackerRow[]): MonthlyReviewCounters {
    const matured3mRows = rows.filter((row) => isThreeMonthTenor(row) && isMatured(row));

    return {
        total: rows.length,
        pathBreached: rows.filter((row) => row.breached_date !== null).length,
        activeBelowStrike: rows.filter((row) => row.status === 'BREACHED').length,
        matured3mTotal: matured3mRows.length,
        matured3mSafe: matured3mRows.filter((row) => row.status === 'EXPIRED_SAFE').length
    };
}

function metricFromSummary(total: number, count: number): TrackerMonthlyMetric {
    return {
        count,
        rate: total === 0 ? '-' : `${((count / total) * 100).toFixed(1)}%`
    };
}

function metricCountOnly(count: number): TrackerMonthlyMetric {
    return {
        count,
        rate: '-'
    };
}

function buildMonthlySection(
    label: string,
    rows: RecommendationTrackerRow[]
): TrackerReviewResponse['rolling_30d'] {
    const summary = summarizeMonthlyRows(rows);

    return {
        label,
        summary: {
            total_recommendations: rows.length,
            path_breach_rate: metricFromSummary(summary.total, summary.pathBreached).rate,
            active_below_strike_rate: metricFromSummary(summary.total, summary.activeBelowStrike).rate,
            maturity_safe_rate: metricFromSummary(summary.matured3mTotal, summary.matured3mSafe).rate
        }
    };
}

function rateDelta(currentTotal: number, currentCount: number, baselineTotal: number, baselineCount: number): string {
    if (currentTotal === 0 || baselineTotal === 0) {
        return '-';
    }

    const currentRate = (currentCount / currentTotal) * 100;
    const baselineRate = (baselineCount / baselineTotal) * 100;
    const delta = currentRate - baselineRate;
    const sign = delta > 0 ? '+' : '';
    return `${sign}${delta.toFixed(1)}pct`;
}

function buildMonthlyConcentration(rows: DailyRecommendationHistoryRow[]): TrackerReviewResponse['concentration'] {
    const symbolCounts = new Map<string, number>();
    const heroCounts = new Map<string, number>();

    for (const row of rows) {
        symbolCounts.set(row.symbol, (symbolCounts.get(row.symbol) ?? 0) + 1);
        if (row.placement === 'HERO') {
            heroCounts.set(row.symbol, (heroCounts.get(row.symbol) ?? 0) + 1);
        }
    }

    return {
        unique_symbols: symbolCounts.size,
        top_symbols: topCounts(symbolCounts, 3),
        top_hero_symbol: topCounts(heroCounts, 1)[0] ?? null
    };
}

function topCounts(counts: Map<string, number>, limit: number): TrackerMonthlyConcentrationItem[] {
    return [...counts.entries()]
        .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
        .slice(0, limit)
        .map(([symbol, appearances]) => ({ symbol, appearances }));
}

function todayInHongKongIsoDate(): string {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Hong_Kong',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(new Date());
}

function daysSince(fromDate: string, toDate: string): number {
    const from = Date.parse(fromDate);
    const to = Date.parse(toDate);
    if (Number.isNaN(from) || Number.isNaN(to)) {
        return Number.POSITIVE_INFINITY;
    }

    return Math.max(0, Math.floor((to - from) / (24 * 60 * 60 * 1000)));
}

function previousCalendarMonth(anchorDate: string): { start: string; end: string; label: string } {
    const [yearText, monthText] = anchorDate.split('-');
    const year = Number(yearText);
    const month = Number(monthText);
    const firstOfCurrentMonthUtc = new Date(Date.UTC(year, month - 1, 1));
    const endDate = new Date(firstOfCurrentMonthUtc.getTime() - 24 * 60 * 60 * 1000);
    const startDate = new Date(Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), 1));

    return {
        start: formatDateUtc(startDate),
        end: formatDateUtc(endDate),
        label: `${endDate.getUTCFullYear()}-${String(endDate.getUTCMonth() + 1).padStart(2, '0')}`
    };
}

function formatDateUtc(value: Date): string {
    return value.toISOString().slice(0, 10);
}
