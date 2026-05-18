import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

const BASE_URL = 'https://api.polygon.io/fed/v1/treasury-yields';
const CACHE_TTL_MS = 6 * 60 * 60 * 1000;

export interface UsTreasuryPoint {
    date: string; // YYYY-MM-DD
    y2: number | null;
    y10: number | null;
}

interface MassiveTreasuryResponse {
    results?: Array<{
        date: string;
        yield_2_year?: number | null;
        yield_10_year?: number | null;
    }>;
    status?: string;
    next_url?: string;
}

const cache = new Map<string, { value: UsTreasuryPoint[]; expiresAt: number }>();

function formatIsoDate(date: Date): string {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function readNumber(value: number | null | undefined): number | null {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

/**
 * Fetch US Treasury yield curve from Massive/Polygon treasury-yields endpoint.
 * Returns ascending by date (oldest first) for downstream convenience.
 * 6-hour cache; fail-soft returns [].
 */
export async function fetchUsTreasuryCurve(daysBack: number = 60): Promise<UsTreasuryPoint[]> {
    const cacheKey = `massive_treasury:${daysBack}`;
    const cached = cache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return cached.value;

    const apiKey = process.env.MASSIVE_API_KEY?.trim();
    if (!apiKey) {
        console.warn('[massive-treasury] MASSIVE_API_KEY missing, skipping');
        return [];
    }

    const today = new Date();
    const past = new Date(today.getTime() - daysBack * 24 * 60 * 60 * 1000);

    try {
        const response = await axios.get<MassiveTreasuryResponse>(BASE_URL, {
            params: {
                'date.gte': formatIsoDate(past),
                'date.lte': formatIsoDate(today),
                order: 'asc',
                sort: 'date',
                limit: 5000,
                apiKey
            },
            timeout: 15000
        });

        if (!response.data.results) {
            console.warn('[massive-treasury] no results in response');
            return [];
        }

        const points = response.data.results
            .map((row) => ({
                date: row.date,
                y2: readNumber(row.yield_2_year),
                y10: readNumber(row.yield_10_year)
            }))
            .filter((point) => point.date && point.y10 !== null)
            .sort((left, right) => left.date.localeCompare(right.date));

        cache.set(cacheKey, { value: points, expiresAt: Date.now() + CACHE_TTL_MS });
        return points;
    } catch (error) {
        console.warn('[massive-treasury] fetch failed:', error instanceof Error ? error.message : String(error));
        return [];
    }
}

export function latestY10(points: UsTreasuryPoint[]): number | null {
    for (let i = points.length - 1; i >= 0; i -= 1) {
        if (points[i].y10 !== null) return points[i].y10;
    }
    return null;
}

export function latestY2(points: UsTreasuryPoint[]): number | null {
    for (let i = points.length - 1; i >= 0; i -= 1) {
        if (points[i].y2 !== null) return points[i].y2;
    }
    return null;
}

export function y10DaysBack(points: UsTreasuryPoint[], calendarDaysBack: number): number | null {
    if (points.length === 0) return null;
    const latest = points[points.length - 1];
    const lastDate = new Date(latest.date);
    const target = new Date(lastDate.getTime() - calendarDaysBack * 24 * 60 * 60 * 1000);
    const targetStr = formatIsoDate(target);

    let closest: UsTreasuryPoint | null = null;
    for (const point of points) {
        if (point.date <= targetStr && point.y10 !== null) closest = point;
    }
    return closest ? closest.y10 : null;
}
