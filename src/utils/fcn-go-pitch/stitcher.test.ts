import assert from 'node:assert/strict';
import { buildPitchPrompt, parsePitchOutput, type PitchInputs, type PitchLLMOutput } from './llm-stitcher';
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
    comm_reference:
        'Broadcom 是网络芯片与定制 ASIC 供应商,近 5 日 +3.2%,显示市场仍在确认其 AI 网络订单能见度。定制 ASIC 与网络芯片需求提供未来 3-6 个月的持有主线,基本面兑现节奏相对清晰。订单积压继续强化收入可见性,使其仍具备 AI 基础设施核心资产属性。',
    used_tags: ['backlog', 'momentum_intact'],
    timing_signal: '近 5 日 +3.2%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 3.2, unit: '%', context: '近 5 日' }]
};

const finalPitch = buildHybridPitch(llmOutput.comm_reference, pitchInputs, '在这个背景下，');
const validation = validatePitch(llmOutput, pitchInputs.lit_tags, pitchInputs, finalPitch);
const prompt = buildPitchPrompt(pitchInputs);

assert.equal(validation.passed, true);
assert.ok(finalPitch.startsWith('Broadcom 是网络芯片与定制 ASIC 供应商'));
assert.ok(finalPitch.includes('未来 3-6 个月'));
assert.equal(finalPitch.includes('让您以 $160'), false);
assert.equal(finalPitch.includes('若股价未跌破'), false);
assert.ok(prompt.includes('必须以「客户展示公司定位」为权威事实边界'));
assert.ok(prompt.includes('不得自行补充未提供的全球排名、市场份额'));

assert.equal(parsePitchOutput('not json'), null);
assert.equal(parsePitchOutput(''), null);

const wrapped = parsePitchOutput(`prefix
{
  "comm_reference": "Broadcom 是网络芯片与定制 ASIC 供应商,近 5 日 +3.2%,显示市场仍在确认其 AI 网络订单能见度。定制 ASIC 与网络芯片需求提供未来 3-6 个月的持有主线,基本面兑现节奏相对清晰。订单积压继续强化收入可见性,使其仍具备 AI 基础设施核心资产属性。",
  "used_tags": ["backlog", "momentum_intact"],
  "timing_signal": "近 5 日 +3.2%",
  "referenced_news_index": -1,
  "numeric_claims": [{"value": 3.2, "unit": "%"}],
  "extra": true
}
suffix`);
assert.ok(wrapped);
assert.equal(wrapped?.used_tags.length, 2);

const legacy = parsePitchOutput('{"why_sentence":"旧字段仍可解析为过渡兼容文本。","used_tags":["backlog"],"timing_signal":"订单","numeric_claims":[]}');
assert.equal(legacy?.comm_reference, '旧字段仍可解析为过渡兼容文本。');

const missingTags = parsePitchOutput('{"comm_reference":"Broadcom 是网络芯片与定制 ASIC 供应商,近 5 日 +3.2%,显示市场仍在确认其 AI 网络订单能见度。定制 ASIC 与网络芯片需求提供未来 3-6 个月的持有主线,基本面兑现节奏相对清晰。订单积压继续强化收入可见性,使其仍具备 AI 基础设施核心资产属性。","timing_signal":"近 5 日 +3.2%","numeric_claims":[]}');
assert.ok(missingTags);
assert.equal(validatePitch(missingTags, pitchInputs.lit_tags, pitchInputs).passed, false);

const unauthorizedNumber: PitchLLMOutput = {
    ...llmOutput,
    comm_reference:
        'Broadcom 是网络芯片与定制 ASIC 供应商,近 5 日 +3.2%,显示市场仍在确认其 AI 网络订单能见度。定制 ASIC 与网络芯片需求提供未来 3-6 个月的持有主线,但另有 999% 增长预期。订单积压继续强化收入可见性,使其仍具备 AI 基础设施核心资产属性。',
    numeric_claims: [{ value: 999, unit: '%', context: '增长预期' }]
};
assert.ok(validatePitch(unauthorizedNumber, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('未授权数字')));

const termsLeak: PitchLLMOutput = {
    ...llmOutput,
    comm_reference:
        'Broadcom 是网络芯片与定制 ASIC 供应商,近 5 日 +3.2%,显示市场仍在确认其 AI 网络订单能见度。定制 ASIC 与网络芯片需求提供未来 3-6 个月的持有主线。若跌破执行价仍有安全垫,票息条款强化承接价值。'
};
assert.ok(validatePitch(termsLeak, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('含禁词')));

const tooLong: PitchLLMOutput = {
    ...llmOutput,
    comm_reference: `${llmOutput.comm_reference}${'补充说明。'.repeat(30)}`
};
assert.ok(validatePitch(tooLong, pitchInputs.lit_tags, pitchInputs).reasons.some((reason) => reason.includes('comm_reference 字数')));

// LITE-like: a verified holding thesis plus price data is enough even when no
// timing tag is lit.
const holdingOnlyInputs: PitchInputs = {
    ...pitchInputs,
    symbol: 'LITE',
    company_short_desc: 'Lumentum 是数据中心光通信器件供应商',
    display_description: 'Lumentum 是数据中心光通信器件供应商',
    lit_tags: { holding: ['infrastructure_capacity_cycle', 'backlog'], timing: [] },
    change_5d_pct: -4.9,
    pct_from_52w_high: -26.1
};
const holdingOnlyOutput: PitchLLMOutput = {
    comm_reference:
        'Lumentum 是数据中心光通信器件供应商。近 5 日股价回调 4.9%，数据中心扩容周期仍为订单积压提供支撑，当前价格波动形成可观察的承接窗口。业务周期支撑叠加短期波动提升票息水平，使 Lumentum 成为当前较具吸引力的 FCN 挂钩标的。',
    used_tags: ['infrastructure_capacity_cycle', 'backlog'],
    timing_signal: '近 5 日回调 4.9%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 4.9, unit: '%', context: '近 5 日回调' }]
};
assert.equal(validatePitch(holdingOnlyOutput, holdingOnlyInputs.lit_tags, holdingOnlyInputs).passed, true);

// CRWD-like: a verified pullback signal can carry the pitch when financials
// and holding tags are unavailable.
const timingOnlyInputs: PitchInputs = {
    ...pitchInputs,
    symbol: 'CRWD',
    company_short_desc: 'CrowdStrike 是云原生终端安全龙头',
    display_description: 'CrowdStrike 是云原生终端安全龙头',
    lit_tags: { holding: [], timing: ['quality_pullback'] },
    change_5d_pct: -3.5,
    pct_from_52w_high: -10.7
};
const timingOnlyOutput: PitchLLMOutput = {
    comm_reference:
        'CrowdStrike 是云原生终端安全龙头。股价距 52 周高点回调 10.7%，优质公司近期价格调整形成更合理的观察窗口，当前估值压力有所释放。基本面定位清晰叠加短期波动提升票息水平，使 CrowdStrike 成为当前较具吸引力的 FCN 挂钩标的。',
    used_tags: ['quality_pullback'],
    timing_signal: '距 52 周高点回调 10.7%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 10.7, unit: '%', context: '距 52 周高点回调' }]
};
assert.equal(validatePitch(timingOnlyOutput, timingOnlyInputs.lit_tags, timingOnlyInputs).passed, true);

console.log('fcn-go-pitch stitcher tests passed');
