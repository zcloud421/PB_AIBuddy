import axios from 'axios';

export interface FearGreedReading {
    score: number;
    rating: string;
    timestamp: string;
    previous_close: number;
    previous_1_week: number;
    previous_1_month: number;
}

const URL = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata';
const CACHE_TTL_MS = 6 * 60 * 60 * 1000;

let cache: { value: FearGreedReading | null; expiresAt: number } | null = null;

function toFiniteNumber(value: unknown, fallback: number): number {
    const numeric = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
}

export async function fetchFearGreedIndex(): Promise<FearGreedReading | null> {
    if (cache && cache.expiresAt > Date.now()) return cache.value;

    try {
        const response = await axios.get(URL, {
            headers: {
                'User-Agent':
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                Accept: 'application/json'
            },
            timeout: 10000
        });
        const fg = response.data?.fear_and_greed;
        if (!fg || typeof fg.score !== 'number') {
            console.warn('[cnn-fear-greed-fetcher] unexpected payload shape');
            cache = { value: null, expiresAt: Date.now() + CACHE_TTL_MS };
            return null;
        }

        const score = fg.score;
        const reading: FearGreedReading = {
            score,
            rating: String(fg.rating ?? '').toLowerCase(),
            timestamp: String(fg.timestamp ?? ''),
            previous_close: toFiniteNumber(fg.previous_close, score),
            previous_1_week: toFiniteNumber(fg.previous_1_week, score),
            previous_1_month: toFiniteNumber(fg.previous_1_month, score)
        };
        cache = { value: reading, expiresAt: Date.now() + CACHE_TTL_MS };
        return reading;
    } catch (error) {
        console.warn('[cnn-fear-greed-fetcher] fetch failed:', error);
        cache = { value: null, expiresAt: Date.now() + CACHE_TTL_MS };
        return null;
    }
}
