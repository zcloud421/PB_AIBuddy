import { pool } from '../db/client';

export interface SymbolFinancials {
    symbol: string;
    latest_quarter: string;
    revenue_yoy_pct: number | null;
    gross_margin_pct: number | null;
    gross_margin_yoy_pp: number | null;
    operating_margin_pct: number | null;
    top_segment?: {
        name: string;
        yoy_pct: number;
    };
}

export interface IncomeStatementRow {
    date?: string | null;
    calendarYear?: string | number | null;
    period?: string | null;
    revenue?: string | number | null;
    grossProfit?: string | number | null;
    operatingIncome?: string | number | null;
}

const FMP_BASE_URL = 'https://financialmodelingprep.com/stable';
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const FAILED_CACHE_TTL_MS = 60 * 60 * 1000;
const REQUEST_TIMEOUT_MS = 8000;

const memoryCache = new Map<string, { data: SymbolFinancials | null; expires: number }>();
const warned = new Set<string>();

export async function ensureSymbolFinancialsCacheTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS symbol_financials_cache (
            symbol TEXT PRIMARY KEY,
            payload JSONB,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_symbol_financials_cache_fetched_at
        ON symbol_financials_cache (fetched_at DESC)
    `);
}

export async function fetchSymbolFinancials(symbol: string): Promise<SymbolFinancials | null> {
    const normalized = symbol.trim().toUpperCase();
    if (!normalized) return null;

    const mem = memoryCache.get(normalized);
    if (mem && mem.expires > Date.now()) return mem.data;

    const cached = await readCachedFinancials(normalized);
    if (cached && isCacheFresh(cached.fetched_at)) {
        memoryCache.set(normalized, { data: cached.payload, expires: Date.now() + CACHE_TTL_MS });
        return cached.payload;
    }

    const apiKey = process.env.FMP_API_KEY?.trim();
    if (!apiKey) {
        warnOnce(normalized, 'FMP_API_KEY not set');
        memoryCache.set(normalized, { data: null, expires: Date.now() + FAILED_CACHE_TTL_MS });
        return null;
    }

    try {
        const [incomeRows, segmentRows, metricRows] = await Promise.all([
            fetchFmpJson<IncomeStatementRow[]>('/income-statement', { symbol: normalized, period: 'quarter', limit: '8' }, apiKey),
            fetchFmpJson<unknown[]>('/revenue-product-segmentation', { symbol: normalized, period: 'quarter', limit: '8' }, apiKey),
            fetchFmpJson<unknown[]>('/key-metrics', { symbol: normalized, period: 'quarter', limit: '8' }, apiKey)
        ]);

        const financials = buildFinancialsFromFmp(normalized, incomeRows ?? [], segmentRows ?? [], metricRows ?? []);
        await writeCachedFinancials(normalized, financials);
        memoryCache.set(normalized, { data: financials, expires: Date.now() + CACHE_TTL_MS });
        return financials;
    } catch (error) {
        warnOnce(normalized, error instanceof Error ? error.message : String(error));
        memoryCache.set(normalized, { data: null, expires: Date.now() + FAILED_CACHE_TTL_MS });
        return null;
    }
}

export function buildFinancialsFromFmp(
    symbol: string,
    incomeRows: IncomeStatementRow[],
    segmentationRows: unknown[],
    keyMetricRows: unknown[] = []
): SymbolFinancials | null {
    const latest = normalizeIncomeRow(incomeRows[0]);
    if (!latest) return null;

    const prior = incomeRows.map(normalizeIncomeRow).find((row) => row && sameQuarterLastYear(latest, row));
    const revenueYoyPct = prior ? pctChange(latest.revenue, prior.revenue) : null;
    const grossMarginPct = ratioPct(latest.grossProfit, latest.revenue);
    const priorGrossMarginPct = prior ? ratioPct(prior.grossProfit, prior.revenue) : null;
    const grossMarginYoyPp =
        grossMarginPct !== null && priorGrossMarginPct !== null ? round1(grossMarginPct - priorGrossMarginPct) : null;
    const operatingMarginPct = ratioPct(latest.operatingIncome, latest.revenue) ?? extractMetricMargin(keyMetricRows[0], 'operating');
    const topSegment = extractTopSegmentGrowth(segmentationRows);

    return {
        symbol: symbol.toUpperCase(),
        latest_quarter: formatQuarter(latest),
        revenue_yoy_pct: revenueYoyPct,
        gross_margin_pct: grossMarginPct,
        gross_margin_yoy_pp: grossMarginYoyPp,
        operating_margin_pct: operatingMarginPct,
        ...(topSegment ? { top_segment: topSegment } : {})
    };
}

export function extractTopSegmentGrowth(rows: unknown[]): SymbolFinancials['top_segment'] | undefined {
    const normalized = rows
        .map(normalizeSegmentRow)
        .filter((row): row is SegmentRow => row !== null)
        .sort((a, b) => b.date.localeCompare(a.date));
    const latest = normalized[0];
    if (!latest) return undefined;
    const prior = normalized.find((row) => sameSegmentQuarterLastYear(latest, row));
    if (!prior) return undefined;

    let best: { name: string; latest: number; prior: number } | null = null;
    for (const [name, latestValue] of Object.entries(latest.segments)) {
        const priorValue = prior.segments[name];
        if (!Number.isFinite(latestValue) || !Number.isFinite(priorValue) || Math.abs(priorValue) < 1) continue;
        if (!best || latestValue > best.latest) best = { name, latest: latestValue, prior: priorValue };
    }
    if (!best) return undefined;
    const yoy = pctChange(best.latest, best.prior);
    return yoy === null ? undefined : { name: cleanSegmentName(best.name), yoy_pct: yoy };
}

export function isCacheFresh(fetchedAt: string | Date, now = new Date(), ttlMs = CACHE_TTL_MS): boolean {
    const ts = typeof fetchedAt === 'string' ? new Date(fetchedAt).getTime() : fetchedAt.getTime();
    return Number.isFinite(ts) && now.getTime() - ts < ttlMs;
}

async function fetchFmpJson<T>(
    path: string,
    params: Record<string, string>,
    apiKey: string
): Promise<T | null> {
    const url = new URL(`${FMP_BASE_URL}${path}`);
    for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
    url.searchParams.set('apikey', apiKey);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
        const response = await fetch(url.toString(), { signal: controller.signal });
        if (!response.ok) {
            throw new Error(`${path} returned ${response.status}`);
        }
        const json = (await response.json()) as unknown;
        if (Array.isArray(json)) return json as T;
        return null;
    } finally {
        clearTimeout(timer);
    }
}

async function readCachedFinancials(symbol: string): Promise<{ payload: SymbolFinancials | null; fetched_at: string } | null> {
    try {
        const result = await pool.query<{ payload: SymbolFinancials | null; fetched_at: string }>(
            `SELECT payload, fetched_at FROM symbol_financials_cache WHERE symbol = $1`,
            [symbol]
        );
        return result.rows[0] ?? null;
    } catch {
        return null;
    }
}

async function writeCachedFinancials(symbol: string, payload: SymbolFinancials | null): Promise<void> {
    try {
        await pool.query(
            `
            INSERT INTO symbol_financials_cache (symbol, payload, fetched_at)
            VALUES ($1, $2::jsonb, NOW())
            ON CONFLICT (symbol) DO UPDATE
            SET payload = EXCLUDED.payload,
                fetched_at = EXCLUDED.fetched_at
            `,
            [symbol, payload ? JSON.stringify(payload) : null]
        );
    } catch (error) {
        warnOnce(symbol, `cache write failed: ${error instanceof Error ? error.message : String(error)}`);
    }
}

interface NormalizedIncomeRow {
    date: string;
    year: number;
    quarter: number;
    revenue: number;
    grossProfit: number | null;
    operatingIncome: number | null;
}

function normalizeIncomeRow(row: IncomeStatementRow | null | undefined): NormalizedIncomeRow | null {
    if (!row) return null;
    const revenue = toNumber(row.revenue);
    if (revenue === null) return null;
    const period = normalizeQuarter(row.period);
    const year = toNumber(row.calendarYear) ?? parseYear(row.date);
    const quarter = period ?? quarterFromDate(row.date);
    if (!year || !quarter) return null;
    return {
        date: row.date ?? `${year}-Q${quarter}`,
        year,
        quarter,
        revenue,
        grossProfit: toNumber(row.grossProfit),
        operatingIncome: toNumber(row.operatingIncome)
    };
}

function sameQuarterLastYear(latest: NormalizedIncomeRow, candidate: NormalizedIncomeRow | null): candidate is NormalizedIncomeRow {
    return Boolean(candidate && candidate.quarter === latest.quarter && candidate.year === latest.year - 1);
}

function formatQuarter(row: NormalizedIncomeRow): string {
    return `Q${row.quarter} ${row.year}`;
}

interface SegmentRow {
    date: string;
    year: number;
    quarter: number;
    segments: Record<string, number>;
}

function normalizeSegmentRow(row: unknown): SegmentRow | null {
    if (!row || typeof row !== 'object') return null;
    const record = row as Record<string, unknown>;
    const date = typeof record.date === 'string' ? record.date : '';
    const year = toNumber(record.calendarYear) ?? parseYear(date);
    const quarter = normalizeQuarter(typeof record.period === 'string' ? record.period : null) ?? quarterFromDate(date);
    if (!year || !quarter) return null;

    const source =
        typeof record.revenueProductSegmentation === 'object' && record.revenueProductSegmentation !== null
            ? (record.revenueProductSegmentation as Record<string, unknown>)
            : record;
    const segments: Record<string, number> = {};
    for (const [key, value] of Object.entries(source)) {
        if (['date', 'symbol', 'period', 'calendarYear', 'reportedCurrency', 'cik', 'fillingDate', 'acceptedDate'].includes(key)) {
            continue;
        }
        const n = toNumber(value);
        if (n !== null) segments[key] = n;
    }
    return Object.keys(segments).length > 0 ? { date: date || `${year}-Q${quarter}`, year, quarter, segments } : null;
}

function sameSegmentQuarterLastYear(latest: SegmentRow, candidate: SegmentRow): boolean {
    return candidate.quarter === latest.quarter && candidate.year === latest.year - 1;
}

function extractMetricMargin(row: unknown, kind: 'operating'): number | null {
    if (!row || typeof row !== 'object') return null;
    const record = row as Record<string, unknown>;
    const keys = kind === 'operating' ? ['operatingProfitMargin', 'operatingMargin', 'operatingIncomeRatio'] : [];
    for (const key of keys) {
        const value = toNumber(record[key]);
        if (value === null) continue;
        return Math.abs(value) <= 1 ? round1(value * 100) : round1(value);
    }
    return null;
}

function pctChange(current: number, previous: number): number | null {
    if (!Number.isFinite(current) || !Number.isFinite(previous) || Math.abs(previous) < 1) return null;
    return round1(((current - previous) / Math.abs(previous)) * 100);
}

function ratioPct(numerator: number | null, denominator: number): number | null {
    if (numerator === null || !Number.isFinite(denominator) || Math.abs(denominator) < 1) return null;
    return round1((numerator / denominator) * 100);
}

function normalizeQuarter(period: string | null | undefined): number | null {
    if (!period) return null;
    const match = period.match(/Q?([1-4])$/i);
    return match ? Number(match[1]) : null;
}

function quarterFromDate(date: string | null | undefined): number | null {
    if (!date) return null;
    const month = Number(date.slice(5, 7));
    if (!Number.isFinite(month) || month < 1 || month > 12) return null;
    return Math.floor((month - 1) / 3) + 1;
}

function parseYear(date: string | null | undefined): number | null {
    if (!date) return null;
    const year = Number(date.slice(0, 4));
    return Number.isFinite(year) ? year : null;
}

function toNumber(value: unknown): number | null {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function round1(value: number): number {
    return Math.round(value * 10) / 10;
}

function cleanSegmentName(name: string): string {
    return name
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 42);
}

function warnOnce(symbol: string, reason: string): void {
    const key = `${symbol}:${reason}`;
    if (warned.has(key)) return;
    warned.add(key);
    console.warn(`[financials-fetcher] ${symbol}: ${reason}`);
}
