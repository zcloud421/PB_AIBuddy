import axios from 'axios';

interface FmpEarningsCalendarEntry {
    symbol?: string;
    date?: string;
}

interface FinnhubEarningsCalendarEntry {
    symbol: string;
    date: string;
}

interface FinnhubEarningsCalendarResponse {
    earningsCalendar?: FinnhubEarningsCalendarEntry[];
}

export interface EarningsCalendarEntry {
    symbol: string;
    report_date: string;
    days_until: number;
    source: 'fmp' | 'finnhub';
}

export async function fetchEarningsCalendar(symbols: string[]): Promise<EarningsCalendarEntry[]> {
    if (symbols.length === 0) {
        return [];
    }

    const fmpApiKey = process.env.FMP_API_KEY;
    const finnhubApiKey = process.env.FINNHUB_API_KEY;
    const symbolSet = new Set(symbols.map((symbol) => symbol.toUpperCase()));

    if (!fmpApiKey && !finnhubApiKey) {
        return [];
    }

    const from = new Date(Date.now() - (3 * 24 * 60 * 60 * 1000))
        .toISOString()
        .split('T')[0];
    const future = new Date(Date.now() + (14 * 24 * 60 * 60 * 1000))
        .toISOString()
        .split('T')[0];

    const startedAt = Date.now();

    if (fmpApiKey) {
        try {
            const response = await axios.get<FmpEarningsCalendarEntry[]>(
                'https://financialmodelingprep.com/stable/earnings-calendar',
                {
                    params: {
                        from,
                        to: future,
                        apikey: fmpApiKey
                    },
                    timeout: 30000
                }
            );

            const earningsData = Array.isArray(response.data) ? response.data : [];
            const filteredRows = earningsData
                .map((entry) => normalizeEarningsRow(entry.symbol, entry.date, symbolSet, 'fmp'))
                .filter((entry): entry is EarningsCalendarEntry => Boolean(entry));

            console.log(
                `[earnings] FMP calendar fetched ${earningsData.length} rows, matched ${filteredRows.length} symbols in ${Date.now() - startedAt}ms`
            );

            return filteredRows;
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(
                `[earnings] FMP calendar fetch failed after ${Date.now() - startedAt}ms (${message})`
            );
        }
    }

    if (!finnhubApiKey) {
        return [];
    }

    try {
        const response = await axios.get<FinnhubEarningsCalendarResponse>(
            'https://finnhub.io/api/v1/calendar/earnings',
            {
                params: {
                    from,
                    to: future,
                    token: finnhubApiKey
                },
                timeout: 30000
            }
        );

        const earningsData = response.data.earningsCalendar ?? [];
        const filteredRows = earningsData
            .map((entry: FinnhubEarningsCalendarEntry) => normalizeEarningsRow(entry.symbol, entry.date, symbolSet, 'finnhub'))
            .filter((entry): entry is EarningsCalendarEntry => Boolean(entry));

        console.log(
            `[earnings] Finnhub calendar fetched ${earningsData.length} rows, matched ${filteredRows.length} symbols in ${Date.now() - startedAt}ms`
        );

        return filteredRows;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(
            `[earnings] Finnhub calendar fetch failed after ${Date.now() - startedAt}ms (${message})`
        );
        throw error;
    }
}

function normalizeEarningsRow(
    symbol: string | undefined,
    reportDate: string | undefined,
    symbolSet: Set<string>,
    source: 'fmp' | 'finnhub'
): EarningsCalendarEntry | null {
    const normalizedSymbol = symbol?.toUpperCase().trim();
    if (!normalizedSymbol || !symbolSet.has(normalizedSymbol) || !reportDate) {
        return null;
    }

    const timestamp = new Date(reportDate).getTime();
    if (!Number.isFinite(timestamp)) {
        return null;
    }

    return {
        symbol: normalizedSymbol,
        report_date: reportDate,
        days_until: Math.ceil((timestamp - Date.now()) / (1000 * 60 * 60 * 24)),
        source
    };
}
