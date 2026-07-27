import assert from 'node:assert/strict';

import { evaluateOptionQuoteQuality, MassiveDataFetcher } from './massive-fetcher';
import type { StrikeData } from '../scoring-engine';

const calls: Array<{ path: string; params?: Record<string, unknown> }> = [];
const fakeClient = {
    async get(path: string, params?: Record<string, unknown>) {
        calls.push({ path, params });
        if (calls.length === 1) {
            return {
                results: [{ id: 1 }],
                next_url: 'https://api.massive.com/v3/snapshot/options/NVDA?cursor=abc'
            };
        }
        return { results: [{ id: 2 }], next_url: undefined };
    }
};

async function run() {
    const fetcher = new MassiveDataFetcher(fakeClient as never);
    const rows = await (fetcher as unknown as {
        fetchOptionSnapshotPaginated(symbol: string, minExpiry: string, maxStrike: number): Promise<Array<Record<string, unknown>>>;
    }).fetchOptionSnapshotPaginated('NVDA', '2026-08-01', 160);

    assert.deepStrictEqual(rows, [{ id: 1 }, { id: 2 }]);
    assert.strictEqual(calls.length, 2);
    assert.ok(calls[0].params);
    assert.strictEqual(calls[1].path, '/v3/snapshot/options/NVDA?cursor=abc');

    const quote: StrikeData = {
        strike: 85,
        iv: 0.5,
        delta: -0.25,
        volume: 50,
        open_interest: 100,
        mid_price: 3.1,
        mid_price_source: 'last_quote',
        bid_price: 3,
        ask_price: 3.2,
        quote_spread_pct: 6.45,
        expiry_date: '2026-10-16'
    };
    assert.equal(evaluateOptionQuoteQuality(quote).passed, true);
    assert.equal(evaluateOptionQuoteQuality({ ...quote, bid_price: 0 }).reason, 'non_positive_bid');
    assert.equal(
        evaluateOptionQuoteQuality({ ...quote, bid_price: 1, ask_price: 2, quote_spread_pct: 66.7 }).reason,
        'spread_too_wide'
    );
    assert.equal(
        evaluateOptionQuoteQuality({
            ...quote,
            mid_price_source: 'day.close',
            bid_price: null,
            ask_price: null,
            volume: 100,
            day_range_pct: 12
        }).passed,
        true
    );
    assert.equal(
        evaluateOptionQuoteQuality({
            ...quote,
            mid_price_source: 'day.close',
            bid_price: null,
            ask_price: null,
            volume: 2,
            day_range_pct: 12
        }).reason,
        'insufficient_day_liquidity'
    );
    console.log('massive-fetcher pagination tests passed');
}

void run();
