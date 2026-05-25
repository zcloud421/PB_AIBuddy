import assert from 'node:assert/strict';
import { parsePitchOutput, type PitchInputs, type PitchLLMOutput } from './llm-stitcher';
import { buildHybridPitch } from './template';
import { validatePitch } from './validator';

const pitchInputs: PitchInputs = {
    symbol: 'AVGO',
    company_short_desc: 'Broadcom 是网络芯片与定制 ASIC 供应商',
    display_description: 'Broadcom 是网络芯片供应商',
    current_price: 190,
    recommended_strike: 160,
    discount_pct: 16,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    lit_tags: {
        holding: ['backlog'],
        timing: ['momentum_intact']
    },
    change_5d_pct: 3.2,
    pct_from_52w_high: -4.5
};

const llmOutput: PitchLLMOutput = {
    why_sentence: '定制 ASIC 订单可见度较高，股价近 5 日 +3.2%，趋势仍保持稳健，承接节奏更清晰。',
    used_tags: ['backlog', 'momentum_intact'],
    timing_signal: '近 5 日 +3.2%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 3.2, unit: '%', context: '近 5 日' }]
};

const finalPitch = buildHybridPitch(llmOutput.why_sentence, pitchInputs, '在这个背景下，');
const validation = validatePitch(llmOutput, pitchInputs.lit_tags, pitchInputs, finalPitch);

assert.equal(validation.passed, true);
assert.ok(finalPitch.includes('在这个背景下，让您以 $160'));
assert.ok(finalPitch.includes('若股价未跌破 $160'));
assert.ok(finalPitch.length > llmOutput.why_sentence.length);

assert.equal(parsePitchOutput('not json'), null);
assert.equal(parsePitchOutput(''), null);

const wrapped = parsePitchOutput(`prefix
{
  "why_sentence": "定制 ASIC 订单能见度清晰，股价近 5 日 +3.2%，趋势保持稳健。",
  "used_tags": ["backlog", "momentum_intact"],
  "timing_signal": "近 5 日 +3.2%",
  "referenced_news_index": -1,
  "numeric_claims": [{"value": 3.2, "unit": "%"}],
  "extra": true
}
suffix`);
assert.ok(wrapped);
assert.equal(wrapped?.used_tags.length, 2);

const missingTags = parsePitchOutput('{"why_sentence":"定制 ASIC 订单能见度清晰，股价近 5 日 +3.2%，趋势保持稳健。","timing_signal":"近 5 日 +3.2%","numeric_claims":[]}');
assert.ok(missingTags);
assert.equal(validatePitch(missingTags, pitchInputs.lit_tags, pitchInputs).passed, false);

const unauthorizedNumber: PitchLLMOutput = {
    ...llmOutput,
    why_sentence: '定制 ASIC 订单可见度较高，股价近 5 日 +3.2%，另有 999% 增长预期。',
    numeric_claims: [{ value: 999, unit: '%', context: '增长预期' }]
};
assert.ok(validatePitch(unauthorizedNumber, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('未授权数字')));

const outOfRangeNews: PitchLLMOutput = {
    ...llmOutput,
    referenced_news_index: 7
};
assert.ok(validatePitch(outOfRangeNews, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('referenced_news_index')));

const tooLong: PitchLLMOutput = {
    ...llmOutput,
    why_sentence: '定制 ASIC 订单可见度较高，股价近 5 日 +3.2%，趋势保持稳健，客户承接节奏更清晰，同时市场对数据中心网络芯片的关注度仍在提升，管理层执行力也持续获得认可，渠道反馈和供应链节奏也体现出较强延续性。'
};
assert.ok(validatePitch(tooLong, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('why_sentence 字数')));

console.log('fcn-go-pitch stitcher tests passed');
