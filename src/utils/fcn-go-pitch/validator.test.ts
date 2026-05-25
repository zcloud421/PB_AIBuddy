import assert from 'node:assert/strict';
import type { PitchInputs, PitchLLMOutput } from './llm-stitcher';
import { buildHybridPitch } from './template';
import { validatePitch } from './validator';

const pitchInputs: PitchInputs = {
    symbol: 'NVDA',
    company_short_desc: 'NVIDIA 是全球 AI 算力 GPU 核心供应商',
    display_description: 'NVIDIA 是AI算力GPU供应商',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    lit_tags: {
        holding: ['super_cycle'],
        timing: ['momentum_intact', 'quality_pullback']
    },
    recent_news_titles: ['OPEC 决议带动相关板块重新定价'],
    change_5d_pct: 2.4,
    pct_from_52w_high: -12.8
};

function output(overrides: Partial<PitchLLMOutput> = {}): PitchLLMOutput {
    return {
        why_sentence: 'AI 算力需求支撑行业上行周期，股价距 52 周高点回调 12.8%，承接水平更有纪律。',
        used_tags: ['super_cycle', 'quality_pullback'],
        timing_signal: '股价距 52 周高点回调 12.8%',
        referenced_news_index: -1,
        numeric_claims: [{ value: 12.8, unit: '%', context: '距 52 周高点' }],
        ...overrides
    };
}

function validate(overrides: Partial<PitchLLMOutput> = {}) {
    const candidate = output(overrides);
    return validatePitch(candidate, pitchInputs.lit_tags, pitchInputs, buildHybridPitch(candidate.why_sentence, pitchInputs));
}

assert.equal(validate().passed, true);
assert.ok(validate({ used_tags: ['super_cycle', 'fake_tag'] }).reasons.some((reason) => reason.includes('未点亮 tag')));
assert.ok(validate({ used_tags: ['quality_pullback'] }).reasons.some((reason) => reason.includes('holding')));
assert.equal(validate({ timing_signal: '近 5 日 +2.4%' }).passed, true);
assert.equal(validate({ timing_signal: 'OPEC 决议', referenced_news_index: 0 }).passed, true);
assert.equal(validate({ timing_signal: '趋势稳健', used_tags: ['super_cycle', 'momentum_intact'] }).passed, true);
assert.ok(validate({ timing_signal: '近期' }).reasons.some((reason) => reason.includes('空泛')));
assert.ok(validate({ why_sentence: '太短', timing_signal: '回调' }).reasons.some((reason) => reason.includes('35-90')));
assert.ok(validate({ why_sentence: `${output().why_sentence}${'补充说明'.repeat(20)}` }).reasons.some((reason) => reason.includes('35-90')));
assert.ok(validate({ why_sentence: `${output().why_sentence} 敲入。` }).reasons.some((reason) => reason.includes('禁词')));
assert.ok(validate({ why_sentence: `${output().why_sentence} 目标价 800。` }).reasons.some((reason) => reason.includes('未授权数字')));

const tooLongFinal = `${buildHybridPitch(output().why_sentence, pitchInputs)}${'补充说明。'.repeat(40)}`;
assert.ok(validatePitch(output(), pitchInputs.lit_tags, pitchInputs, tooLongFinal).reasons.some((reason) => reason.includes('100-220')));

console.log('fcn-go-pitch validator tests passed');
