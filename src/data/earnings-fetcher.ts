import axios from 'axios';

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
}

export async function fetchEarningsCalendar(symbols: string[]): Promise<EarningsCalendarEntry[]> {
    const finnhubApiKey = process.env.FINNHUB_API_KEY;

    if (!finnhubApiKey || symbols.length === 0) {
        return [];
    }

    const today = new Date().toISOString().split('T')[0];
    const future = new Date(Date.now() + (14 * 24 * 60 * 60 * 1000))
        .toISOString()
        .split('T')[0];

    const startedAt = Date.now();

    try {
        const response = await axios.get<FinnhubEarningsCalendarResponse>(
            'https://finnhub.io/api/v1/calendar/earnings',
            {
                params: {
                    from: today,
                    to: future,
                    token: finnhubApiKey
                },
                timeout: 30000
            }
        );

        const earningsData = response.data.earningsCalendar ?? [];
        const filteredRows = earningsData
            .filter((entry: FinnhubEarningsCalendarEntry) => symbols.includes(entry.symbol))
            .map((entry: FinnhubEarningsCalendarEntry) => ({
                symbol: entry.symbol,
                report_date: entry.date,
                days_until: Math.ceil(
                    (new Date(entry.date).getTime() - Date.now()) / (1000 * 60 * 60 * 24)
                )
            }));

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
