import axios from 'axios';

const FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations';
const CACHE_TTL_MS = 12 * 60 * 60 * 1000;

export interface BreakevenInflationTrend {
    latest_date: string;
    breakeven_5y_pct: number | null;
    breakeven_10y_pct: number | null;
    change_5d_5y_pct: number | null;
    change_5d_10y_pct: number | null;
    direction_5y: 'rising' | 'falling' | 'flat';
    direction_10y: 'rising' | 'falling' | 'flat';
}

interface FredObservation {
    date: string;
    value: string;
}

let memoryCache: { fetchedAt: number; trend: BreakevenInflationTrend } | null = null;

async function fetchSeries(seriesId: string, apiKey: string): Promise<FredObservation[]> {
    const response = await axios.get<{ observations?: FredObservation[] }>(FRED_BASE_URL, {
        params: {
            series_id: seriesId,
            api_key: apiKey,
            file_type: 'json',
            sort_order: 'desc',
            limit: 30
        },
        timeout: 8000
    });

    return (response.data.observations ?? []).filter((observation) => observation.value !== '.');
}

function parseLatestAndDelta(observations: FredObservation[]): {
    latest: { date: string; value: number } | null;
    change5d: number | null;
    direction: 'rising' | 'falling' | 'flat';
} {
    if (observations.length === 0) {
        return { latest: null, change5d: null, direction: 'flat' };
    }

    const numeric = observations
        .map((observation) => ({ date: observation.date, value: Number(observation.value) }))
        .filter((point) => Number.isFinite(point.value));

    if (numeric.length === 0) {
        return { latest: null, change5d: null, direction: 'flat' };
    }

    const latest = numeric[0];
    const fiveDaysBack = numeric[5] ?? numeric[numeric.length - 1];
    const rawChange5d = fiveDaysBack ? latest.value - fiveDaysBack.value : null;
    const change5d = rawChange5d !== null ? Math.round(rawChange5d * 100) / 100 : null;
    const direction: 'rising' | 'falling' | 'flat' =
        change5d === null ? 'flat' : change5d > 0.05 ? 'rising' : change5d < -0.05 ? 'falling' : 'flat';

    return { latest, change5d, direction };
}

export async function getBreakevenInflationTrend(): Promise<BreakevenInflationTrend | null> {
    if (memoryCache && Date.now() - memoryCache.fetchedAt < CACHE_TTL_MS) {
        return memoryCache.trend;
    }

    const apiKey = process.env.FRED_API_KEY?.trim();
    if (!apiKey) {
        console.warn('[fred-fetcher] skipped: FRED_API_KEY missing');
        return null;
    }

    try {
        const [obs5y, obs10y] = await Promise.all([
            fetchSeries('T5YIE', apiKey),
            fetchSeries('T10YIE', apiKey)
        ]);

        const result5y = parseLatestAndDelta(obs5y);
        const result10y = parseLatestAndDelta(obs10y);

        if (!result5y.latest && !result10y.latest) {
            return null;
        }

        const trend: BreakevenInflationTrend = {
            latest_date: result10y.latest?.date ?? result5y.latest?.date ?? '',
            breakeven_5y_pct: result5y.latest?.value ?? null,
            breakeven_10y_pct: result10y.latest?.value ?? null,
            change_5d_5y_pct: result5y.change5d,
            change_5d_10y_pct: result10y.change5d,
            direction_5y: result5y.direction,
            direction_10y: result10y.direction
        };

        memoryCache = { fetchedAt: Date.now(), trend };
        return trend;
    } catch (error) {
        console.warn('[fred-fetcher] failed:', error instanceof Error ? error.message : error);
        return null;
    }
}
