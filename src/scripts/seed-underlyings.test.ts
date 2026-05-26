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

assert.equal(UNDERLYINGS.length, 53, 'PB FCN active universe should have 53 tickers');
assert.equal(t1.length, 34, 'T1 should have 34 tickers');
assert.equal(t2.length, 19, 'T2 should have 19 tickers');

for (const symbol of ['USO', 'NEM', 'BILI', 'LI', 'XPEV']) {
    assert.ok(DEPRECATED_SYMBOLS.includes(symbol), `${symbol} should be explicitly deprecated`);
    assert.equal(symbols.has(symbol), false, `${symbol} should not be active`);
}

for (const symbol of ['BABA', 'PDD', 'JD', 'BIDU']) {
    assert.equal(UNDERLYINGS.find((row) => row.symbol === symbol)?.adr_risk, true, `${symbol} adr_risk`);
}

for (const symbol of ['INTC', 'NKE', 'LULU', 'NVO']) {
    assert.equal(UNDERLYINGS.find((row) => row.symbol === symbol)?.turnaround_watch, true, `${symbol} turnaround_watch`);
}

console.log('seed-underlyings tests passed');
