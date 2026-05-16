import axios from 'axios';

/**
 * Generic FRED series fetcher for Macro Regime indicators.
 *
 * Separate from fred-fetcher.ts (which only handles break-even inflation for
 * FCN narrative). This one pulls arbitrary FRED series with configurable lookback
 * and returns chronologically-sorted observations for indicator computation.
 *
 * Caching: per-series in-memory, 12h TTL. FRED publishes daily so refresh more
 * often than that is wasted.
 */

const FRED_BASE_URL = 'https://api.stlouisfed.org/fred/series/observations';
const CACHE_TTL_MS = 12 * 60 * 60 * 1000;

export interface FredPoint {
    date: string;   // yyyy-mm-dd
    value: number;  // parsed; '.' sentinel values dropped upstream
}

interface FredRawObservation {
    date: string;
    value: string;
}

interface SeriesCacheEntry {
    fetchedAt: number;
    points: FredPoint[];
}

const cache = new Map<string, SeriesCacheEntry>();

/**
 * Fetch a single FRED series. Returns chronological (ascending) date series
 * with '.' missing values filtered out and value parsed to number.
 *
 * On API key missing or HTTP failure: logs a warning and returns null so the
 * caller can fall back gracefully (indicator marked is_skipped = true).
 *
 * @param seriesId FRED series ID e.g. "BAMLH0A0HYM2"
 * @param limit number of recent observations to fetch (default 90 to cover
 *              4-week lookback + safety buffer for weekends/gaps)
 */
export async function fetchFredSeries(
    seriesId: string,
    limit = 90
): Promise<FredPoint[] | null> {
    const cacheKey = `${seriesId}:${limit}`;
    const cached = cache.get(cacheKey);
    if (cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
        return cached.points;
    }

    const apiKey = process.env.FRED_API_KEY?.trim();
    if (!apiKey) {
        console.warn(`[fred-series] skipped: FRED_API_KEY missing (series=${seriesId})`);
        return null;
    }

    try {
        const response = await axios.get<{ observations?: FredRawObservation[] }>(FRED_BASE_URL, {
            params: {
                series_id: seriesId,
                api_key: apiKey,
                file_type: 'json',
                sort_order: 'desc',
                limit
            },
            timeout: 10000
        });

        const observations = (response.data.observations ?? [])
            .filter((observation) => observation.value !== '.')
            .map((observation) => ({
                date: observation.date,
                value: Number(observation.value)
            }))
            .filter((point) => Number.isFinite(point.value))
            // FRED returned desc; reverse to ascending for indicator math
            .reverse();

        cache.set(cacheKey, { fetchedAt: Date.now(), points: observations });
        return observations;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[fred-series] fetch failed (series=${seriesId}): ${message}`);
        return null;
    }
}

/**
 * Latest non-missing point in a series.
 */
export function latestPoint(points: FredPoint[]): FredPoint | null {
    if (points.length === 0) return null;
    return points[points.length - 1];
}

/**
 * Find the point closest to (latest - daysBack trading days). Uses calendar
 * day arithmetic — sufficient for trend deltas where exact trading-day count
 * is not critical.
 */
export function pointDaysBack(points: FredPoint[], calendarDaysBack: number): FredPoint | null {
    if (points.length === 0) return null;
    const latest = points[points.length - 1];
    const target = new Date(latest.date);
    target.setDate(target.getDate() - calendarDaysBack);
    const targetIso = target.toISOString().slice(0, 10);

    // Walk back to the latest point whose date <= target
    for (let i = points.length - 1; i >= 0; i -= 1) {
        if (points[i].date <= targetIso) {
            return points[i];
        }
    }
    return points[0];
}
