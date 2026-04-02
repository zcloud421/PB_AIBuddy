import dotenv from 'dotenv';

import { pool } from '../db/client';
import { getRecentPriceHistoryBySymbol } from '../db/queries/ideas';

dotenv.config();

type HistoryPoint = {
    date: string;
    close: number;
};

type Extremum = {
    type: 'peak' | 'trough';
    index: number;
    date: string;
    price: number;
};

type ThresholdEvent = {
    peak_date: string;
    peak_price: number;
    trough_date: string;
    trough_price: number;
    max_drawdown_pct: number;
    recovery_days: number | null;
    recovered: boolean;
};

async function main(): Promise<void> {
    const symbol = (getArgValue('--symbol') ?? process.argv[2] ?? '').toUpperCase();
    const threshold = Number(getArgValue('--threshold') ?? '25');
    const limit = Number(getArgValue('--history-limit') ?? '1500');

    if (!symbol) {
        throw new Error('Usage: ts-node src/scripts/debug-drawdown-threshold.ts --symbol=PDD --threshold=25');
    }

    const rawHistory = await getRecentPriceHistoryBySymbol(symbol, Number.isFinite(limit) ? limit : 1500);
    const history = rawHistory
        .map((point) => ({
            date: point.date,
            close: toFiniteNumber(point.close)
        }))
        .filter((point): point is HistoryPoint => point.close !== null);

    if (history.length < 2) {
        throw new Error(`Not enough price history for ${symbol}`);
    }

    const events = buildThresholdDrawdownEvents(history, threshold);
    const recoveredDays = events
        .map((event) => event.recovery_days)
        .filter((value): value is number => value !== null)
        .sort((left, right) => left - right);
    const medianRecoveryDays =
        recoveredDays.length === 0 ? null : calculateMedian(recoveredDays);

    console.log(`[debug-drawdown] symbol=${symbol} threshold=-${threshold.toFixed(1)}% history_points=${history.length}`);
    console.log(`[debug-drawdown] events=${events.length} median_recovery_days=${medianRecoveryDays ?? 'n/a'}`);
    console.log('');

    events.forEach((event, index) => {
        console.log(
            [
                `${index + 1}.`,
                `${event.peak_date} ${formatPrice(event.peak_price)}`,
                '->',
                `${event.trough_date} ${formatPrice(event.trough_price)}`,
                `drawdown=${event.max_drawdown_pct.toFixed(1)}%`,
                `recovered=${event.recovered ? 'yes' : 'no'}`,
                `recovery_days=${event.recovery_days ?? 'n/a'}`
            ].join(' ')
        );
    });

    await pool.end();
}

function getArgValue(flag: string): string | null {
    const prefixed = `${flag}=`;
    const entry = process.argv.find((arg) => arg.startsWith(prefixed));
    return entry ? entry.slice(prefixed.length) : null;
}

function buildThresholdDrawdownEvents(history: HistoryPoint[], thresholdPct: number): ThresholdEvent[] {
    const extrema = buildLocalExtrema(history, 15);
    const events: ThresholdEvent[] = [];

    for (let index = 0; index < extrema.length - 1; index += 1) {
        const current = extrema[index];
        const next = extrema[index + 1];
        if (current.type !== 'peak' || next.type !== 'trough') {
            continue;
        }

        const drawdownPct = ((next.price / current.price) - 1) * 100;
        if (Math.abs(drawdownPct) < thresholdPct) {
            continue;
        }

        const recoveryDays = findRecoveryDays(history, current.index, next.index, current.price);
        events.push({
            peak_date: current.date,
            peak_price: current.price,
            trough_date: next.date,
            trough_price: next.price,
            max_drawdown_pct: Number(drawdownPct.toFixed(1)),
            recovery_days: recoveryDays,
            recovered: recoveryDays !== null
        });
    }

    return dedupeNearbyThresholdEvents(events);
}

function buildLocalExtrema(history: HistoryPoint[], windowSize: number): Extremum[] {
    const extrema: Extremum[] = [];

    for (let index = 0; index < history.length; index += 1) {
        const start = Math.max(0, index - windowSize);
        const end = Math.min(history.length - 1, index + windowSize);
        const slice = history.slice(start, end + 1).map((point) => point.close);
        const current = history[index].close;
        const localMax = Math.max(...slice);
        const localMin = Math.min(...slice);

        if (current === localMax) {
            extrema.push({ type: 'peak', index, date: history[index].date, price: current });
            continue;
        }

        if (current === localMin) {
            extrema.push({ type: 'trough', index, date: history[index].date, price: current });
        }
    }

    const compressed: Extremum[] = [];
    for (const point of extrema) {
        const previous = compressed[compressed.length - 1];
        if (!previous) {
            compressed.push(point);
            continue;
        }

        if (previous.type !== point.type) {
            compressed.push(point);
            continue;
        }

        const shouldReplace = point.type === 'peak' ? point.price >= previous.price : point.price <= previous.price;
        if (shouldReplace) {
            compressed[compressed.length - 1] = point;
        }
    }

    const first = history[0];
    const last = history[history.length - 1];
    if (!compressed.length || compressed[0].index !== 0) {
        compressed.unshift({ type: 'peak', index: 0, date: first.date, price: first.close });
    }
    if (compressed[compressed.length - 1]?.index !== history.length - 1) {
        const previous = compressed[compressed.length - 1];
        compressed.push({
            type: previous?.type === 'peak' ? 'trough' : 'peak',
            index: history.length - 1,
            date: last.date,
            price: last.close
        });
    }

    return compressed;
}

function dedupeNearbyThresholdEvents(events: ThresholdEvent[]): ThresholdEvent[] {
    if (events.length <= 1) {
        return events;
    }

    const deduped: ThresholdEvent[] = [events[0]];
    for (let index = 1; index < events.length; index += 1) {
        const current = events[index];
        const previous = deduped[deduped.length - 1];
        const gapDays = daysBetweenIso(previous.trough_date, current.peak_date);
        if (gapDays !== null && gapDays <= 20) {
            if (current.max_drawdown_pct < previous.max_drawdown_pct) {
                deduped[deduped.length - 1] = current;
            }
            continue;
        }

        deduped.push(current);
    }

    return deduped;
}

function findRecoveryDays(
    history: HistoryPoint[],
    peakIndex: number,
    troughIndex: number,
    peakPrice: number
): number | null {
    for (let index = troughIndex + 1; index < history.length; index += 1) {
        if (history[index].close >= peakPrice) {
            return index - peakIndex;
        }
    }

    return null;
}

function calculateMedian(values: number[]): number {
    const middle = Math.floor(values.length / 2);
    if (values.length % 2 === 1) {
        return values[middle];
    }

    return Math.round((values[middle - 1] + values[middle]) / 2);
}

function daysBetweenIso(fromDate: string, toDate: string): number | null {
    const from = Date.parse(fromDate);
    const to = Date.parse(toDate);
    if (Number.isNaN(from) || Number.isNaN(to)) {
        return null;
    }

    return Math.round((to - from) / (24 * 60 * 60 * 1000));
}

function formatPrice(value: number | string | null | undefined): string {
    const numeric = toFiniteNumber(value);
    return numeric === null ? '—' : `$${numeric.toFixed(2)}`;
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const numeric = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

void main().catch(async (error) => {
    console.error(error instanceof Error ? error.stack ?? error.message : String(error));
    await pool.end().catch(() => undefined);
    process.exitCode = 1;
});
