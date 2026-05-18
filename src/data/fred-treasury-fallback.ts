/**
 * FRED treasury curve fallback for when Massive (Polygon) treasury-yields
 * endpoint is unavailable.
 *
 * Why this exists:
 *   - Primary source: Massive `/fed/v1/treasury-yields` (canonical, matches
 *     US Treasury Daily Yield Curve).
 *   - Fallback: FRED `DGS10` + `DGS2` series, also sourced from US Treasury
 *     daily curve — verified 2026-05-18 to match Massive byte-for-byte for
 *     5/12-5/14 readings.
 *   - Both have the same inherent ~1-day publication lag (Treasury publishes
 *     end-of-day data the next business day around 6pm ET).
 *
 * Returns the same `UsTreasuryPoint[]` shape as `massive-treasury-yields.ts`
 * so callers can treat them interchangeably.
 */
import { fetchFredSeries } from './fred-series-fetcher';
import type { UsTreasuryPoint } from './massive-treasury-yields';

export async function fetchFredTreasuryCurve(daysBack: number = 60): Promise<UsTreasuryPoint[]> {
    const [s10, s2] = await Promise.all([
        fetchFredSeries('DGS10', daysBack),
        fetchFredSeries('DGS2', daysBack)
    ]);

    if (!s10 || s10.length === 0) return [];

    // Index DGS2 by date for fast lookup; FRED occasionally has gaps where
    // one series posted but the other didn't (rare, holiday-adjacent).
    const s2Map = new Map((s2 ?? []).map((point) => [point.date, point.value]));

    return s10
        .map((point) => ({
            date: point.date,
            y10: point.value,
            y2: s2Map.get(point.date) ?? null
        }))
        .filter((point) => point.y10 !== null && point.date)
        .sort((left, right) => left.date.localeCompare(right.date));
}
