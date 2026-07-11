import assert from 'node:assert/strict';
import { DEPRECATED_SYMBOLS, UNDERLYINGS } from './seed-underlyings';

const symbols = new Set<string>();
for (const row of UNDERLYINGS) {
    assert.ok(row.symbol, 'symbol required');
    assert.equal(row.symbol, row.symbol.toUpperCase(), `${row.symbol} must be uppercase`);
    assert.ok(row.exchange, `${row.symbol} exchange required`);
    assert.ok(row.sector, `${row.symbol} sector required`);
    assert.ok(row.currency, `${row.symbol} currency required`);
    assert.ok(row.themes.length > 0, `${row.symbol} themes required`);
    assert.ok(row.classification === 'blue_chip' || row.classification === 'theme' || row.classification === 'both');
    assert.equal(symbols.has(row.symbol), false, `${row.symbol} duplicate`);
    symbols.add(row.symbol);
}

const t1 = UNDERLYINGS.filter((row) => row.tier === 1);
const t2 = UNDERLYINGS.filter((row) => row.tier === 2);
const active = UNDERLYINGS.filter((row) => row.status === 'active');
const underReview = UNDERLYINGS.filter((row) => row.status === 'under_review');

assert.equal(UNDERLYINGS.length, 56, 'PB FCN governed universe should have 56 tickers');
assert.equal(active.length, 53, 'PB FCN daily recommendation pool should have 53 active tickers');
assert.deepEqual(underReview.map((row) => row.symbol).sort(), ['BIDU', 'JD', 'PDD']);
assert.equal(t1.length, 36, 'T1 should have 36 tickers');
assert.equal(t2.length, 20, 'T2 should have 20 tickers');

for (const symbol of ['USO', 'NEM', 'BILI', 'LI', 'XPEV']) {
    assert.ok(DEPRECATED_SYMBOLS.includes(symbol), `${symbol} should be explicitly deprecated`);
    assert.equal(symbols.has(symbol), false, `${symbol} should not be active`);
}

for (const symbol of ['BABA', 'PDD', 'JD', 'BIDU']) {
    assert.equal(UNDERLYINGS.find((row) => row.symbol === symbol)?.adr_risk, true, `${symbol} adr_risk`);
}

for (const symbol of ['PDD', 'JD', 'BIDU']) {
    const row = UNDERLYINGS.find((entry) => entry.symbol === symbol);
    assert.equal(row?.active, false, `${symbol} must not enter the scheduled recommendation pool`);
    assert.equal(row?.status, 'under_review', `${symbol} must remain searchable under IC review`);
    assert.ok(row?.status_reason, `${symbol} review reason required`);
    assert.ok(row?.holdable_concern, `${symbol} holdability concern required`);
    assert.equal(row?.reviewed_at, '2026-07-11T00:00:00Z', `${symbol} review timestamp must be stable`);
}

for (const symbol of ['RTX', 'ISRG', 'GDX']) {
    const row = UNDERLYINGS.find((entry) => entry.symbol === symbol);
    assert.equal(row?.status, 'active', `${symbol} should enter the scheduled recommendation pool`);
    assert.equal(row?.active, true, `${symbol} active/status parity`);
}

for (const symbol of ['INTC', 'NKE', 'LULU', 'NVO']) {
    assert.equal(UNDERLYINGS.find((row) => row.symbol === symbol)?.turnaround_watch, true, `${symbol} turnaround_watch`);
}

console.log('seed-underlyings tests passed');
