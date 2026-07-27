import assert from 'node:assert/strict';
import { calculateShowcaseConcentrationPenalty } from './showcase-diversification';

assert.deepEqual(
    calculateShowcaseConcentrationPenalty({ subsector: 0, cycle_family: 0, theme: 0, sector: 0 }),
    { total: 0, subsector: 0, cycle_family: 0, theme: 0, sector: 0 }
);
assert.equal(
    calculateShowcaseConcentrationPenalty({ subsector: 1, cycle_family: 1, theme: 1, sector: 1 }).total,
    0.31
);
assert.equal(
    calculateShowcaseConcentrationPenalty({ subsector: 0, cycle_family: 0, theme: 0, sector: 2 }).sector,
    0.12
);

console.log('showcase diversification tests passed');
