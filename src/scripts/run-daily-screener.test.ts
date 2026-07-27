import assert from 'node:assert/strict';
import { assessDailyRunCoverage } from './run-daily-screener';

assert.deepEqual(assessDailyRunCoverage(53, 53), {
    passed: true,
    processed: 53,
    expected: 53,
    coverage_pct: 100
});
assert.equal(assessDailyRunCoverage(51, 53).passed, true);
assert.equal(assessDailyRunCoverage(50, 53).passed, false);
assert.equal(assessDailyRunCoverage(46, 53).coverage_pct, 86.8);
assert.equal(assessDailyRunCoverage(0, 0).passed, false);

console.log('daily screener coverage tests passed');
