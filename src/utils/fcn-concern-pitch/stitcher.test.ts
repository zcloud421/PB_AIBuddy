import assert from 'node:assert/strict';
import type { NarrativeInput } from '../narrative-generator';
import { parseConcernOutput, type ConcernLLMOutput } from './llm-stitcher';
import type { ConcernPitchInputs } from './template';
import { buildCautionPitch, buildCautionTemplate } from './template';
import { validateConcernPitch } from './validator';

const input: NarrativeInput = {
    symbol: 'TEST',
    company_name: 'Test Corp',
    theme: 'AI',
    grade: 'CAUTION',
    composite_score: 0.46,
    recommended_strike: 85,
    estimated_coupon_range: '12-16%',
    current_price: 100,
    change_5d_pct: -2,
    change_ytd_pct: -1,
    pct_from_52w_high: -10,
    ma20: 98,
    ma50: 95,
    ma200: 90,
    iv_level: 'high',
    flags: [],
    tenor_days: 90,
    news_headlines: [],
    news_items: [],
    days_to_earnings: 6
};

const p: ConcernPitchInputs = {
    symbol: 'TEST',
    company_short_desc: 'Test Corp 是数据中心基础设施供应商',
    display_description: 'Test Corp 是基础设施供应商',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    caution_tags: ['earnings_window_imminent'],
    avoid_tags: [],
    input
};

const llm: ConcernLLMOutput = {
    concern_sentence: '财报窗口临近，短期事件风险仍未落地，股价节奏也偏弱，建议先保留观察名单，等待更清晰的价格和事件确认，当前卖 put 条件不够友好。',
    used_tags: ['earnings_window_imminent'],
    primary_concern_signal: '财报窗口临近',
    numeric_claims: []
};

const finalText = buildCautionPitch(llm.concern_sentence, p);
const validation = validateConcernPitch('CAUTION', finalText, p, llm);

assert.equal(validation.passed, true);
assert.ok(finalText.includes(llm.concern_sentence));
assert.ok(finalText.includes('再评估'));

assert.equal(parseConcernOutput('not json'), null);
assert.equal(parseConcernOutput(''), null);

const wrapped = parseConcernOutput(`prefix
{
  "concern_sentence": "财报窗口临近，短期事件风险仍未落地，当前卖 put 条件需要观察。",
  "used_tags": ["earnings_window_imminent"],
  "primary_concern_signal": "财报窗口临近",
  "numeric_claims": [],
  "extra": "ignored"
}
suffix`);
assert.ok(wrapped);
assert.equal(wrapped?.used_tags[0], 'earnings_window_imminent');

const missingTags = parseConcernOutput('{"concern_sentence":"财报窗口临近，短期事件风险仍未落地，当前卖 put 条件需要观察。","primary_concern_signal":"财报窗口临近","numeric_claims":[]}');
assert.ok(missingTags);
assert.equal(validateConcernPitch('CAUTION', buildCautionPitch(missingTags.concern_sentence, p), p, missingTags).passed, false);

const unauthorizedNumber: ConcernLLMOutput = {
    ...llm,
    concern_sentence: '财报窗口临近，短期事件风险仍未落地，另有 999% 下修风险，当前卖 put 条件不够友好。',
    numeric_claims: [{ value: 999, unit: '%', context: '下修风险' }]
};
assert.ok(validateConcernPitch('CAUTION', buildCautionPitch(unauthorizedNumber.concern_sentence, p), p, unauthorizedNumber).reasons.some((reason) => reason.includes('未授权数字')));

const tooLong: ConcernLLMOutput = {
    ...llm,
    concern_sentence: '财报窗口临近，短期事件风险仍未落地，股价节奏也偏弱，建议先保留观察名单，等待更清晰的价格和事件确认，当前卖 put 条件不够友好，同时 IV 与票息补偿仍需继续观察，后续还需要结合财报后的趋势修复和波动率条件再判断。'
};
assert.ok(validateConcernPitch('CAUTION', buildCautionPitch(tooLong.concern_sentence, p), p, tooLong).reasons.some((reason) => reason.includes('concern_sentence 字数')));

const fallbackText = buildCautionTemplate(p);
assert.equal(validateConcernPitch('CAUTION', fallbackText, p).passed, true);

console.log('fcn-concern-pitch stitcher tests passed');
