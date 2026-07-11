import assert from 'node:assert/strict';
import { truncateLikelyTickerReuse } from './price-history-integrity';

const reused = [
    { date: '2026-06-10', close: 22.1 },
    { date: '2026-06-11', close: 21.98 },
    { date: '2026-06-12', close: 160.95 },
    { date: '2026-06-15', close: 192.5 }
];
assert.deepEqual(truncateLikelyTickerReuse(reused), reused.slice(2));

const ordinaryVolatility = [
    { date: '2026-01-01', close: 100 },
    { date: '2026-01-02', close: 55 },
    { date: '2026-01-03', close: 80 }
];
assert.deepEqual(truncateLikelyTickerReuse(ordinaryVolatility), ordinaryVolatility);

const reverseBreak = [
    { date: '2026-01-01', close: '100' },
    { date: '2026-01-02', close: '20' },
    { date: '2026-01-03', close: '21' }
];
assert.deepEqual(truncateLikelyTickerReuse(reverseBreak), reverseBreak.slice(1));

console.log('price-history-integrity tests passed');
