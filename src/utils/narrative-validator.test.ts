import assert from 'node:assert/strict';
import { validateNarrativeNumbers } from './narrative-validator';
import type { NarrativeInput } from './narrative-generator';

const baseInput: NarrativeInput = {
    symbol: 'LITE',
    theme: 'AI infrastructure',
    grade: 'GO',
    recommended_strike: 680,
    estimated_coupon_range: '25%-34%',
    current_price: 944.99,
    pct_from_52w_high: -12.96,
    ma20: null,
    ma50: null,
    ma200: null,
    iv_level: '高',
    flags: [],
    tenor_days: 90,
    news_headlines: [],
    news_items: []
};

const result1 = validateNarrativeNumbers('营收同比增 56%', baseInput);
assert.equal(result1.passed, false);
assert.equal(result1.unauthorized.length, 1);
assert.equal(result1.unauthorized[0].value, 56);

const result2 = validateNarrativeNumbers('约 71% 进场价', baseInput);
assert.equal(result2.passed, true);

const result3 = validateNarrativeNumbers(
    '当前价 $944.99, 执行价 $680(72% 进场价), 距 52 周高点 -12.96%, 年化票息 25%-34%',
    baseInput
);
assert.equal(result3.passed, true);
assert.equal(result3.authorizedCount, 6);

console.log('narrative-validator tests passed');
