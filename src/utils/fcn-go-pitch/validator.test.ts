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
assert.equal(
    validate({ why_sentence: `${output().why_sentence} 2026 年指引仍是主要观察点。` }).passed,
    true
);
assert.ok(
    validate({ why_sentence: `${output().why_sentence} 营收 2026 亿元仍需观察。` }).reasons.some((reason) =>
        reason.includes('未授权数字')
    )
);

const earningsInputs: PitchInputs = {
    ...pitchInputs,
    lit_tags: {
        holding: ['earnings_strong_beat'],
        timing: ['quality_pullback']
    },
    earnings_surprise: {
        period: 'Q3 2026',
        eps_surprise_pct: 8.3
    },
    pct_from_52w_high: -8.5
};

const earningsOutput: PitchLLMOutput = {
    why_sentence: 'Q3 2026 EPS 超预期 8.3%，股价距 52 周高点回调 8.5%，承接水平更有纪律。',
    used_tags: ['earnings_strong_beat', 'quality_pullback'],
    timing_signal: '股价距 52 周高点回调 8.5%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 8.3, unit: '%', context: 'EPS 超预期' }]
};

assert.equal(validatePitch(earningsOutput, earningsInputs.lit_tags, earningsInputs, buildHybridPitch(earningsOutput.why_sentence, earningsInputs)).passed, true);
assert.ok(
    validatePitch(
        { ...earningsOutput, why_sentence: 'Q3 2026 数字 8.3%，股价距 52 周高点回调 8.5%，承接水平更有纪律。' },
        earningsInputs.lit_tags,
        earningsInputs,
        buildHybridPitch(earningsOutput.why_sentence, earningsInputs)
    ).reasons.some((reason) => reason.includes('未授权数字'))
);
assert.ok(
    validatePitch(
        { ...earningsOutput, why_sentence: 'Q3 2026 EPS 超预期 8.5%，承接水平更有纪律。' },
        earningsInputs.lit_tags,
        earningsInputs,
        buildHybridPitch('Q3 2026 EPS 超预期 8.5%，承接水平更有纪律。', earningsInputs)
    ).reasons.some((reason) => reason.includes('earnings surprise'))
);
assert.ok(
    validatePitch(
        { ...output(), why_sentence: `${output().why_sentence} 财报超预期。` },
        pitchInputs.lit_tags,
        pitchInputs,
        buildHybridPitch(`${output().why_sentence} 财报超预期。`, pitchInputs)
    ).reasons.some((reason) => reason.includes('财报 beat tag'))
);
assert.ok(
    validatePitch(
        {
            ...output(),
            why_sentence: '公司被纳入重要指数强化配置逻辑，同时股价距 52 周高点回调 12.8%。',
            used_tags: ['super_cycle', 'quality_pullback'],
            timing_signal: '股价距 52 周高点回调 12.8%'
        },
        pitchInputs.lit_tags,
        pitchInputs,
        buildHybridPitch('公司被纳入重要指数强化配置逻辑，同时股价距 52 周高点回调 12.8%。', pitchInputs)
    ).reasons.some((reason) => reason.includes('tag_conditional_ban_hit: index_inclusion'))
);

const indexInputs: PitchInputs = {
    ...pitchInputs,
    lit_tags: {
        holding: ['super_cycle', 'index_inclusion'],
        timing: ['quality_pullback']
    },
    recent_news_titles: ['Company will join the Nasdaq-100 Index next month']
};
const indexOutput: PitchLLMOutput = {
    why_sentence: '公司被纳入重要指数强化配置逻辑，同时股价距 52 周高点回调 12.8%，承接水平更有纪律。',
    used_tags: ['super_cycle', 'index_inclusion', 'quality_pullback'],
    timing_signal: '股价距 52 周高点回调 12.8%',
    referenced_news_index: 0,
    numeric_claims: [{ value: 12.8, unit: '%', context: '距 52 周高点' }]
};
assert.equal(validatePitch(indexOutput, indexInputs.lit_tags, indexInputs, buildHybridPitch(indexOutput.why_sentence, indexInputs)).passed, true);

const tooLongFinal = `${buildHybridPitch(output().why_sentence, pitchInputs)}${'补充说明。'.repeat(40)}`;
assert.ok(validatePitch(output(), pitchInputs.lit_tags, pitchInputs, tooLongFinal).reasons.some((reason) => reason.includes('100-220')));

console.log('fcn-go-pitch validator tests passed');
