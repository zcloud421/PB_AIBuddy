import assert from 'node:assert/strict';
import { getLatestEarningsSurprise } from './earnings-surprise';

const originalKey = process.env.FINNHUB_API_KEY;
const originalFetch = global.fetch;

async function run(): Promise<void> {
    process.env.FINNHUB_API_KEY = 'test-key';

    global.fetch = (async () => ({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => [
            {
                symbol: 'BEAT',
                period: '2026-3',
                actual: 1.12,
                estimate: 1,
                surprisePercent: 12,
                date: '2026-05-20',
                revenueActual: 100,
                revenueEstimate: 98,
                revenueSurprisePercent: 2.04
            }
        ]
    })) as unknown as typeof fetch;
    const beat = await getLatestEarningsSurprise('BEAT');
    assert.equal(beat?.period, 'Q3 2026');
    assert.equal(beat?.eps_surprise_pct, 12);
    assert.equal(beat?.revenue_surprise_pct, 2.04);

    global.fetch = (async () => ({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => [
            {
                symbol: 'NOREV',
                quarter: 4,
                year: 2026,
                actual: '1.03',
                estimate: '1.00',
                surprisePercent: '3'
            }
        ]
    })) as unknown as typeof fetch;
    const noRevenue = await getLatestEarningsSurprise('NOREV');
    assert.equal(noRevenue?.period, 'Q4 2026');
    assert.equal(noRevenue?.eps_actual, 1.03);
    assert.equal(noRevenue?.revenue_surprise_pct, undefined);

    global.fetch = (async () => ({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => [{ symbol: 'BAD', period: '2026-1', actual: 'n/a', estimate: 1, surprisePercent: 4 }]
    })) as unknown as typeof fetch;
    assert.equal(await getLatestEarningsSurprise('BAD'), null);

    global.fetch = (async () => ({
        ok: false,
        status: 429,
        headers: new Headers(),
        json: async () => ({})
    })) as unknown as typeof fetch;
    assert.equal(await getLatestEarningsSurprise('ERR'), null);
}

run()
    .then(() => {
        if (originalKey === undefined) delete process.env.FINNHUB_API_KEY;
        else process.env.FINNHUB_API_KEY = originalKey;
        global.fetch = originalFetch;
        console.log('earnings-surprise tests passed');
    })
    .catch((error) => {
        if (originalKey === undefined) delete process.env.FINNHUB_API_KEY;
        else process.env.FINNHUB_API_KEY = originalKey;
        global.fetch = originalFetch;
        throw error;
    });
