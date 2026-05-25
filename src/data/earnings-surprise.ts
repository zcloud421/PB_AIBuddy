export interface EarningsSurprise {
    symbol: string;
    period: string;
    report_date: string | null;
    eps_actual: number;
    eps_estimate: number;
    eps_surprise_pct: number;
    revenue_actual?: number;
    revenue_estimate?: number;
    revenue_surprise_pct?: number;
}

const FINNHUB_BASE_URL = 'https://finnhub.io/api/v1';
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const REQUEST_TIMEOUT_MS = 8000;

const cache = new Map<string, { data: EarningsSurprise | null; expires: number }>();
const warnedSymbols = new Set<string>();

interface FinnhubEarningsRow {
    actual?: number | string | null;
    estimate?: number | string | null;
    period?: string | null;
    quarter?: number | string | null;
    surprise?: number | string | null;
    surprisePercent?: number | string | null;
    symbol?: string | null;
    year?: number | string | null;
    date?: string | null;
    reportDate?: string | null;
    revenueActual?: number | string | null;
    revenueEstimate?: number | string | null;
    revenueSurprisePercent?: number | string | null;
}

export async function getLatestEarningsSurprise(symbol: string): Promise<EarningsSurprise | null> {
    const normalized = symbol.toUpperCase();
    const cached = cache.get(normalized);
    if (cached && cached.expires > Date.now()) return cached.data;

    const apiKey = process.env.FINNHUB_API_KEY;
    if (!apiKey) {
        warnOnce(normalized, 'FINNHUB_API_KEY not set');
        cache.set(normalized, { data: null, expires: Date.now() + CACHE_TTL_MS });
        return null;
    }

    const url = new URL(`${FINNHUB_BASE_URL}/stock/earnings`);
    url.searchParams.set('symbol', normalized);
    url.searchParams.set('token', apiKey);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
        const response = await fetch(url.toString(), { signal: controller.signal });
        if (!response.ok) {
            warnOnce(normalized, `Finnhub returned ${response.status}`);
            cache.set(normalized, { data: null, expires: Date.now() + CACHE_TTL_MS });
            return null;
        }

        const rows = (await response.json()) as FinnhubEarningsRow[];
        const latest = Array.isArray(rows) ? rows.map(normalizeRow).find((row): row is EarningsSurprise => row !== null) : null;
        cache.set(normalized, { data: latest ?? null, expires: Date.now() + CACHE_TTL_MS });
        return latest ?? null;
    } catch (error) {
        warnOnce(normalized, error instanceof Error ? error.message : String(error));
        cache.set(normalized, { data: null, expires: Date.now() + 60_000 });
        return null;
    } finally {
        clearTimeout(timer);
    }
}

function normalizeRow(row: FinnhubEarningsRow): EarningsSurprise | null {
    const epsActual = toNumber(row.actual);
    const epsEstimate = toNumber(row.estimate);
    const epsSurprisePct = toNumber(row.surprisePercent);
    if (epsActual === null || epsEstimate === null || epsSurprisePct === null) return null;

    const period = normalizePeriod(row.period, row.quarter, row.year);
    if (!period) return null;

    const revenueActual = toNumber(row.revenueActual);
    const revenueEstimate = toNumber(row.revenueEstimate);
    const revenueSurprisePct = toNumber(row.revenueSurprisePercent);

    return {
        symbol: String(row.symbol ?? '').toUpperCase(),
        period,
        report_date: row.reportDate ?? row.date ?? null,
        eps_actual: epsActual,
        eps_estimate: epsEstimate,
        eps_surprise_pct: epsSurprisePct,
        ...(revenueActual !== null ? { revenue_actual: revenueActual } : {}),
        ...(revenueEstimate !== null ? { revenue_estimate: revenueEstimate } : {}),
        ...(revenueSurprisePct !== null ? { revenue_surprise_pct: revenueSurprisePct } : {})
    };
}

function normalizePeriod(period: string | null | undefined, quarter: number | string | null | undefined, year: number | string | null | undefined): string | null {
    const q = toNumber(quarter);
    const y = toNumber(year);
    if (q !== null && y !== null && q >= 1 && q <= 4) return `Q${Math.round(q)} ${Math.round(y)}`;
    if (period && period.trim()) return period.trim().replace(/^(\d{4})-([1-4])$/, 'Q$2 $1');
    return null;
}

function toNumber(value: number | string | null | undefined): number | null {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function warnOnce(symbol: string, reason: string): void {
    if (warnedSymbols.has(symbol)) return;
    warnedSymbols.add(symbol);
    console.warn(`[earnings-surprise] ${symbol}: ${reason}`);
}
