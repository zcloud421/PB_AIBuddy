import assert from 'node:assert/strict';

import { MassiveDataFetcher } from './massive-fetcher';

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
    console.log('massive-fetcher pagination tests passed');
}

void run();
