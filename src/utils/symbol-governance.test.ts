import assert from 'node:assert/strict';
import { getRestrictedSymbol, getRestrictedSymbols, resetRestrictedSymbolsCacheForTests } from './symbol-governance';

resetRestrictedSymbolsCacheForTests();

const restricted = getRestrictedSymbols();
assert.ok(restricted.size >= 10);
assert.equal(getRestrictedSymbol('tqqq')?.symbol, 'TQQQ');
assert.equal(getRestrictedSymbol('USO')?.symbol, 'USO');
assert.equal(getRestrictedSymbol('JPM'), null);

console.log('symbol-governance tests passed');
