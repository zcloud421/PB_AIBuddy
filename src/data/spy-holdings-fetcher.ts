import axios from 'axios';
import * as XLSX from 'xlsx';

export interface SpyHolding {
    ticker: string;
    weight_pct: number;
    name?: string | null;
}

const SPY_HOLDINGS_XLSX_URL =
    'https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx';
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const FETCH_HEADERS = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    Accept: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*'
};

let memoryCache: { fetchedAt: number; holdings: SpyHolding[] } | null = null;

function normalizeHeader(value: unknown): string {
    return String(value ?? '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '');
}

function parseWeightPct(value: unknown): number | null {
    if (typeof value === 'number') {
        if (!Number.isFinite(value) || value <= 0) return null;
        return value;
    }
    const text = String(value ?? '').replace('%', '').trim();
    const numeric = Number(text);
    if (!Number.isFinite(numeric) || numeric <= 0) return null;
    return numeric;
}

function parseWorkbook(buffer: Buffer): SpyHolding[] {
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    for (const sheetName of workbook.SheetNames) {
        const sheet = workbook.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json<unknown[]>(sheet, { header: 1, raw: true });
        const headerIndex = rows.findIndex((row) => {
            const normalized = row.map(normalizeHeader);
            return normalized.includes('ticker') && normalized.some((value) => value.includes('weight'));
        });
        if (headerIndex < 0) continue;

        const header = rows[headerIndex].map(normalizeHeader);
        const tickerIndex = header.findIndex((value) => value === 'ticker');
        const nameIndex = header.findIndex((value) => value === 'name' || value === 'companyname');
        const weightIndex = header.findIndex((value) => value.includes('weight'));
        if (tickerIndex < 0 || weightIndex < 0) continue;

        const holdings = rows
            .slice(headerIndex + 1)
            .map((row): SpyHolding | null => {
                const ticker = String(row[tickerIndex] ?? '').trim().toUpperCase();
                const weight = parseWeightPct(row[weightIndex]);
                if (!ticker || weight === null) return null;
                return {
                    ticker,
                    name: nameIndex >= 0 ? String(row[nameIndex] ?? '').trim() || null : null,
                    weight_pct: Math.round(weight * 1000) / 1000
                };
            })
            .filter((holding): holding is SpyHolding => Boolean(holding));

        if (holdings.length > 50) {
            return holdings;
        }
    }

    throw new Error('SPY holdings workbook did not contain ticker/weight columns');
}

export async function fetchSpyHoldings(): Promise<SpyHolding[] | null> {
    if (memoryCache && Date.now() - memoryCache.fetchedAt < CACHE_TTL_MS) {
        return memoryCache.holdings;
    }

    try {
        const response = await axios.get<ArrayBuffer>(SPY_HOLDINGS_XLSX_URL, {
            headers: FETCH_HEADERS,
            responseType: 'arraybuffer',
            timeout: 10000
        });
        const holdings = parseWorkbook(Buffer.from(response.data));
        memoryCache = { fetchedAt: Date.now(), holdings };
        return holdings;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[spy-holdings] failed: ${message}`);
        return null;
    }
}
