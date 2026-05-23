import assert from 'node:assert/strict';
import { validateEventAnchors } from './narrative-event-validator';

const newsWithNasdaqAnchor = [
    { title: 'Lumentum to Join Nasdaq-100 Index Beginning May 18', published_at: '2026-05-09T00:00:00.000Z' }
];
const result1 = validateEventAnchors('近期纳入 Nasdaq-100', newsWithNasdaqAnchor);
assert.equal(result1.passed, true, '纳入 with anchor should pass');

const newsNoAcquisition = [
    { title: 'Q4 Revenue Up 12%', published_at: '2026-05-20T00:00:00.000Z' }
];
const result2 = validateEventAnchors('收购 X 公司', newsNoAcquisition);
assert.equal(result2.passed, false, '收购 without anchor should fail');

const newsEarnings = [
    { title: 'Lumentum Q3 Revenue Jumps', published_at: '2026-05-05T00:00:00.000Z' }
];
const result3 = validateEventAnchors('营收同比增长 X%', newsEarnings);
assert.equal(result3.passed, true, '营收 with earnings anchor should pass');

const result4 = validateEventAnchors('近期纳入 Nasdaq-100', []);
assert.equal(result4.passed, false, 'empty news should fail any claim');

console.log('narrative-event-validator tests passed');
