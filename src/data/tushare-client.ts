import axios from 'axios';

const TUSHARE_URL = 'https://api.tushare.pro';
const CACHE_TTL_MS = 6 * 60 * 60 * 1000;

export interface UsTycrPoint {
    date: string;  // YYYYMMDD
    y2: number | null;
    y10: number | null;
}

interface TushareResponse {
    code: number;
    msg: string;
    data?: {
        fields: string[];
        items: Array<Array<string | number | null>>;
    };
}

const cache = new Map<string, { value: UsTycrPoint[]; expiresAt: number }>();

function formatTushareDate(date: Date): string {
    return `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;
}

function readNumber(value: string | number | null | undefined): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
}

/**
 * Fetch US Treasury yield curve from Tushare Pro (us_tycr).
 * Returns ascending by date (oldest first) for downstream convenience.
 * 6-hour cache; fail-soft returns [].
 */
export async function fetchUsTreasuryCurve(daysBack: number = 60): Promise<UsTycrPoint[]> {
    const cacheKey = `us_tycr:${daysBack}`;
    const cached = cache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.value;

    const token = process.env.TUSHARE_TOKEN?.trim();
    if (!token) {
        console.warn('[tushare-client] TUSHARE_TOKEN missing, skipping');
        return [];
    }

    const today = new Date();
    const past = new Date(today.getTime() - daysBack * 24 * 60 * 60 * 1000);

    try {
        const response = await axios.post<TushareResponse>(
            TUSHARE_URL,
            {
                api_name: 'us_tycr',
                token,
                params: { start_date: formatTushareDate(past), end_date: formatTushareDate(today) },
                fields: 'date,y2,y10'
            },
            { timeout: 15000 }
        );

        if (response.data.code !== 0 || !response.data.data) {
            console.warn('[tushare-client] us_tycr error:', response.data.msg);
            return [];
        }

        const { fields, items } = response.data.data;
        const dateIdx = fields.indexOf('date');
        const y2Idx = fields.indexOf('y2');
        const y10Idx = fields.indexOf('y10');
        if (dateIdx < 0 || y2Idx < 0 || y10Idx < 0) {
            console.warn('[tushare-client] us_tycr missing expected fields');
            return [];
        }

        const points = items
            .map((row) => ({
                date: String(row[dateIdx] ?? ''),
                y2: readNumber(row[y2Idx]),
                y10: readNumber(row[y10Idx])
            }))
            .filter((point) => point.date && point.y10 !== null)
            .sort((left, right) => left.date.localeCompare(right.date));

        cache.set(cacheKey, { value: points, expiresAt: Date.now() + CACHE_TTL_MS });
        return points;
    } catch (error) {
        console.warn('[tushare-client] fetch failed:', error instanceof Error ? error.message : String(error));
        return [];
    }
}

export function latestY10(points: UsTycrPoint[]): number | null {
    for (let i = points.length - 1; i >= 0; i -= 1) {
        if (points[i].y10 !== null) return points[i].y10;
    }
    return null;
}

export function y10DaysBack(points: UsTycrPoint[], calendarDaysBack: number): number | null {
    if (points.length === 0) return null;
    const latest = points[points.length - 1];
    const lastDate = new Date(
        Number(latest.date.slice(0, 4)),
        Number(latest.date.slice(4, 6)) - 1,
        Number(latest.date.slice(6, 8))
    );
    const target = new Date(lastDate.getTime() - calendarDaysBack * 24 * 60 * 60 * 1000);
    const targetStr = formatTushareDate(target);

    let closest: UsTycrPoint | null = null;
    for (const point of points) {
        if (point.date <= targetStr && point.y10 !== null) closest = point;
    }
    return closest ? closest.y10 : null;
}

export function latestY2(points: UsTycrPoint[]): number | null {
    for (let i = points.length - 1; i >= 0; i -= 1) {
        if (points[i].y2 !== null) return points[i].y2;
    }
    return null;
}
