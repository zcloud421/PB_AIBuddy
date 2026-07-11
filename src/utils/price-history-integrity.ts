export const TICKER_IDENTITY_BREAK_RATIO = 4;

/**
 * Adjusted daily data should already normalize splits. A 4x overnight identity
 * jump is therefore treated as ticker reuse and history before the latest jump
 * is discarded (for example, an ETF ticker later reassigned to a new company).
 */
export function truncateLikelyTickerReuse<T extends { date: string; close: number | string }>(history: T[]): T[] {
    let latestBreakIndex = 0;

    for (let index = 1; index < history.length; index += 1) {
        const previous = Number(history[index - 1].close);
        const current = Number(history[index].close);
        if (!Number.isFinite(previous) || !Number.isFinite(current) || previous <= 0 || current <= 0) continue;

        const ratio = current / previous;
        if (ratio >= TICKER_IDENTITY_BREAK_RATIO || ratio <= 1 / TICKER_IDENTITY_BREAK_RATIO) {
            latestBreakIndex = index;
        }
    }

    return latestBreakIndex > 0 ? history.slice(latestBreakIndex) : history;
}
