import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';

dotenv.config();

const DEFAULT_BASE_URL = 'https://api.massive.com';
const MAX_RETRIES = 3;
const BASE_DELAY_MS = 30000;
const MIN_REQUEST_INTERVAL_MS = 2000;
const REQUEST_TIMEOUT_MS = 30000;
const TICKER_OVERVIEW_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export interface MassiveTickerOverview {
    name: string | null;
    sic_description: string | null;
    type: string | null;
    market_cap: number | null;
    primary_exchange: string | null;
}

interface MassiveTickerOverviewResponse {
    results?: {
        name?: string;
        sic_description?: string;
        type?: string;
        market_cap?: number;
        primary_exchange?: string;
    };
}

const tickerOverviewCache = new Map<string, { data: MassiveTickerOverview | null; expires: number }>();
const tickerOverviewWarned = new Set<string>();

export class MassiveApiError extends Error {
    public readonly statusCode: number;

    constructor(statusCode: number, message: string) {
        super(message);
        this.statusCode = statusCode;
    }
}

export class MassiveClient {
    private static nextAvailableRequestAt = 0;
    private readonly apiKey: string;
    private readonly baseUrl: string;

    constructor() {
        const apiKey = process.env.MASSIVE_API_KEY;
        if (!apiKey) {
            throw new Error('MASSIVE_API_KEY is not set');
        }

        this.apiKey = apiKey;
        this.baseUrl = process.env.MASSIVE_API_BASE_URL ?? DEFAULT_BASE_URL;
    }

    async get<T>(path: string, params: Record<string, string | number | boolean | undefined> = {}): Promise<T> {
        return this.requestWithRetry<T>(path, params, 0);
    }

    private async requestWithRetry<T>(
        path: string,
        params: Record<string, string | number | boolean | undefined>,
        attempt: number
    ): Promise<T> {
        const requestId = uuidv4();
        const url = new URL(path, this.baseUrl);

        for (const [key, value] of Object.entries(params)) {
            if (value !== undefined) {
                url.searchParams.set(key, String(value));
            }
        }
        url.searchParams.set('apiKey', this.apiKey);

        await MassiveClient.waitForRequestSlot();

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
        let response: Response;
        try {
            response = await fetch(url, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Request-Id': requestId
                },
                signal: controller.signal
            });
        } finally {
            clearTimeout(timer);
        }

        if (response.status === 401) {
            throw new MassiveApiError(401, 'Massive API authentication failed');
        }

        if (response.status === 429) {
            if (attempt >= MAX_RETRIES) {
                throw new MassiveApiError(429, 'Massive API rate limit exceeded after retries');
            }

            const retryAfterHeader = response.headers.get('retry-after');
            const retryAfterMs = parseRetryAfterMs(retryAfterHeader);
            const retryDelay = retryAfterMs > 0 ? retryAfterMs : BASE_DELAY_MS;
            await delay(retryDelay);
            return this.requestWithRetry<T>(path, params, attempt + 1);
        }

        if (response.status === 503 || response.status === 504) {
            if (attempt >= MAX_RETRIES) {
                throw new MassiveApiError(response.status, `Massive API request failed with status ${response.status}`);
            }

            await delay(BASE_DELAY_MS);
            return this.requestWithRetry<T>(path, params, attempt + 1);
        }

        if (!response.ok) {
            throw new MassiveApiError(response.status, `Massive API request failed with status ${response.status}`);
        }

        return response.json() as Promise<T>;
    }

    private static async waitForRequestSlot(): Promise<void> {
        const now = Date.now();
        const waitMs = Math.max(0, MassiveClient.nextAvailableRequestAt - now);
        if (waitMs > 0) {
            await delay(waitMs);
        }

        MassiveClient.nextAvailableRequestAt = Date.now() + MIN_REQUEST_INTERVAL_MS;
    }
}

export async function getMassiveTickerOverview(symbol: string): Promise<MassiveTickerOverview | null> {
    const normalized = symbol.toUpperCase();
    const cached = tickerOverviewCache.get(normalized);
    if (cached && cached.expires > Date.now()) return cached.data;

    if (!process.env.MASSIVE_API_KEY) {
        tickerOverviewCache.set(normalized, { data: null, expires: Date.now() + TICKER_OVERVIEW_TTL_MS });
        return null;
    }

    try {
        const client = new MassiveClient();
        const response = await client.get<MassiveTickerOverviewResponse>(`/v3/reference/tickers/${normalized}`);
        const result = response.results;
        const overview: MassiveTickerOverview | null = result
            ? {
                  name: normalizeString(result.name),
                  sic_description: normalizeString(result.sic_description),
                  type: normalizeString(result.type),
                  market_cap: typeof result.market_cap === 'number' && Number.isFinite(result.market_cap) ? result.market_cap : null,
                  primary_exchange: normalizeString(result.primary_exchange)
              }
            : null;
        tickerOverviewCache.set(normalized, { data: overview, expires: Date.now() + TICKER_OVERVIEW_TTL_MS });
        return overview;
    } catch (error) {
        if (!tickerOverviewWarned.has(normalized)) {
            tickerOverviewWarned.add(normalized);
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[massive-overview] ${normalized}: ${message}`);
        }
        tickerOverviewCache.set(normalized, { data: null, expires: Date.now() + TICKER_OVERVIEW_TTL_MS });
        return null;
    }
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function parseRetryAfterMs(headerValue: string | null): number {
    if (!headerValue) {
        return 0;
    }

    const seconds = Number(headerValue);
    if (Number.isFinite(seconds) && seconds > 0) {
        return seconds * 1000;
    }

    const retryAt = Date.parse(headerValue);
    if (Number.isNaN(retryAt)) {
        return 0;
    }

    return Math.max(0, retryAt - Date.now());
}

function normalizeString(value: unknown): string | null {
    return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}
