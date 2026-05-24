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

const policyNoAnchor = validateEventAnchors(
    '先进芯片禁令升级仍持续,影响对华订单预期',
    [{ title: 'Earnings beat expectations', published_at: '2026-05-22T00:00:00.000Z' }]
);
assert.equal(policyNoAnchor.passed, false, 'policy claim without anchor should fail');
assert.equal(policyNoAnchor.unanchored.some((item) => item.snippet === '禁令'), true);

const policyWithAnchor = validateEventAnchors(
    '先进芯片禁令升级仍持续',
    [{ title: 'US Tightens Chip Export Ban to China', published_at: '2026-05-20T00:00:00.000Z' }]
);
assert.equal(policyWithAnchor.passed, true, 'policy claim with ban/tighten anchor should pass');

const genericRisk = validateEventAnchors('高隐含波动率环境下敲入风险上升', []);
assert.equal(genericRisk.passed, true, 'generic structural risk should not need an event anchor');

const incomeExpectation = validateEventAnchors('高 IV 环境下,可能影响中国区收入预期', []);
assert.equal(incomeExpectation.passed, true, '收入 should not be treated as an earnings claim');

const genericRestriction = validateEventAnchors('高 IV 限制风险定价空间', []);
assert.equal(genericRestriction.passed, true, '限制 should not be treated as a policy claim');

const retainedPolicyKeyword = validateEventAnchors(
    '先进芯片禁令升级仍持续',
    [{ title: 'Earnings beat', published_at: '2026-05-22T00:00:00.000Z' }]
);
assert.equal(retainedPolicyKeyword.passed, false, '禁令 should still require a news anchor');
assert.equal(retainedPolicyKeyword.unanchored.some((item) => item.snippet === '禁令'), true);

console.log('narrative-event-validator tests passed');
