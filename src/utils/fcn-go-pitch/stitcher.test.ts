import assert from 'node:assert/strict';
import type { PitchInputs, PitchLLMOutput } from './llm-stitcher';
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

console.log('fcn-go-pitch stitcher tests passed');
