import assert from 'node:assert/strict';
import { computeGradeDistributionMetrics, formatGradeDistributionReport } from './check-grade-distribution';

const history = [
    { run_id: 'h1', run_date: '2026-05-18', grade_counts: { GO: 10, CAUTION: 8, AVOID: 12 }, hard_fail_count: 4 },
    { run_id: 'h2', run_date: '2026-05-19', grade_counts: { GO: 10, CAUTION: 7, AVOID: 13 }, hard_fail_count: 4 },
    { run_id: 'h3', run_date: '2026-05-20', grade_counts: { GO: 10, CAUTION: 9, AVOID: 11 }, hard_fail_count: 4 }
];

const goDrop = computeGradeDistributionMetrics(
    { run_id: 'latest', run_date: '2026-05-21', grade_counts: { GO: 5, CAUTION: 10, AVOID: 15 }, hard_fail_count: 4 },
    history
);
assert.ok(goDrop.issues.some((issue) => issue.includes('GO count changed')));

const highShare = computeGradeDistributionMetrics(
    { run_id: 'latest', run_date: '2026-05-21', grade_counts: { GO: 13, CAUTION: 9, AVOID: 8 }, hard_fail_count: 4 },
    history
);
// GO share 13/30 = 43.3% > 35% (raised from 25% on 2026-06-07); GO 13 vs avg 10 = +30% < 40% so only the share issue fires.
assert.ok(highShare.issues.some((issue) => issue.includes('GO share')));

const hardFailSpike = computeGradeDistributionMetrics(
    { run_id: 'latest', run_date: '2026-05-21', grade_counts: { GO: 10, CAUTION: 8, AVOID: 12 }, hard_fail_count: 7 },
    history
);
assert.ok(hardFailSpike.issues.some((issue) => issue.includes('Hard fail count changed')));

const unknown = computeGradeDistributionMetrics(
    { run_id: 'latest', run_date: '2026-05-21', grade_counts: { GO: 10, WATCH: 1 }, hard_fail_count: 4 },
    history
);
assert.ok(unknown.issues.some((issue) => issue.includes('Unknown grade')));
assert.ok(formatGradeDistributionReport(goDrop).includes('FCN Grade Distribution'));

console.log('check-grade-distribution tests passed');
