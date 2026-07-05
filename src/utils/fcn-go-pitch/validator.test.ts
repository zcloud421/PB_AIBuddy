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
    recent_news_titles: ['NVIDIA launches a new AI accelerator platform'],
    change_5d_pct: 2.4,
    pct_from_52w_high: -12.8
};

function output(overrides: Partial<PitchLLMOutput> = {}): PitchLLMOutput {
    return {
        comm_reference:
            'NVIDIA 是全球 AI 算力 GPU 核心供应商,股价距 52 周高点回调 12.8%,显示估值进入更可观察的位置。AI 算力周期仍是未来 3-6 个月的主要持有主线,需求兑现节奏支撑核心资产定位。新 AI 加速平台发布强化产品护城河,使其在 AI 基础设施链条中保持战略重要性。',
        used_tags: ['super_cycle', 'quality_pullback'],
        timing_signal: '股价距 52 周高点回调 12.8%',
        referenced_news_index: -1,
        numeric_claims: [{ value: 12.8, unit: '%', context: '距 52 周高点' }],
        ...overrides
    };
}

function validate(overrides: Partial<PitchLLMOutput> = {}) {
    const candidate = output(overrides);
    return validatePitch(candidate, pitchInputs.lit_tags, pitchInputs, buildHybridPitch(candidate.comm_reference, pitchInputs));
}

assert.equal(validate().passed, true);
assert.ok(validate({ used_tags: ['super_cycle', 'fake_tag'] }).reasons.some((reason) => reason.includes('未点亮 tag')));
assert.ok(validate({ used_tags: ['quality_pullback'] }).reasons.some((reason) => reason.includes('holding')));
assert.equal(validate({ timing_signal: '近 5 日 +2.4%' }).passed, true);
assert.equal(validate({ timing_signal: 'NVIDIA launches', referenced_news_index: 0 }).passed, true);
assert.equal(validate({ timing_signal: '趋势稳健', used_tags: ['super_cycle', 'momentum_intact'] }).passed, true);
assert.ok(validate({ timing_signal: '近期' }).reasons.some((reason) => reason.includes('空泛')));
assert.ok(validate({ comm_reference: '太短', timing_signal: '回调' }).reasons.some((reason) => reason.includes('comm_reference 字数')));
assert.ok(validate({ comm_reference: `${output().comm_reference}${'补充说明'.repeat(30)}` }).reasons.some((reason) => reason.includes('comm_reference 字数')));
// 新政策:定性 Why-FCN 语汇(敲入风险低/票息水平/FCN 挂钩标的)放行;条款复述与条款数字仍禁。
assert.equal(validate({ comm_reference: `${output().comm_reference}基本面稳健、敲入风险相对可控,适合作为 FCN 挂钩标的。` }).passed, true);
assert.ok(validate({ comm_reference: `${output().comm_reference} 若跌破执行价。` }).reasons.some((reason) => reason.includes('禁词')));
assert.ok(validate({ comm_reference: `${output().comm_reference} 敲入价 $85。` }).reasons.some((reason) => reason.includes('条款数字')));
assert.ok(validate({ comm_reference: `${output().comm_reference} 年化票息 14%。` }).reasons.some((reason) => reason.includes('条款数字')));
assert.ok(validate({ comm_reference: `${output().comm_reference} 目标价 800。` }).reasons.some((reason) => reason.includes('未授权数字')));
assert.ok(validate({ comm_reference: `${output().comm_reference} 基本面强劲。` }).reasons.some((reason) => reason.includes('空话表达')));
assert.equal(
    validate({ comm_reference: `${output().comm_reference} 2026 年指引仍是主要观察点。` }).passed,
    true
);
assert.ok(
    validate({ comm_reference: `${output().comm_reference} 营收 2026 亿元仍需观察。` }).reasons.some((reason) =>
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
    comm_reference:
        'NVIDIA 是全球 AI 算力 GPU 核心供应商,Q3 2026 EPS 超预期 8.3%,显示盈利兑现仍有支撑。股价距 52 周高点回调 8.5%,未来 3-6 个月持有逻辑更依赖需求延续。产品平台迭代强化 AI 基础设施核心资产定位,使其保持战略重要性。',
    used_tags: ['earnings_strong_beat', 'quality_pullback'],
    timing_signal: '股价距 52 周高点回调 8.5%',
    referenced_news_index: -1,
    numeric_claims: [{ value: 8.3, unit: '%', context: 'EPS 超预期' }]
};

assert.equal(validatePitch(earningsOutput, earningsInputs.lit_tags, earningsInputs, buildHybridPitch(earningsOutput.comm_reference, earningsInputs)).passed, true);
assert.ok(
    validatePitch(
        { ...earningsOutput, comm_reference: 'NVIDIA 是全球 AI 算力 GPU 核心供应商,Q3 2026 数字 8.3%,显示盈利兑现仍有支撑。股价距 52 周高点回调 8.5%,未来 3-6 个月持有逻辑更依赖需求延续。产品平台迭代强化 AI 基础设施核心资产定位,使其保持战略重要性。' },
        earningsInputs.lit_tags,
        earningsInputs,
        buildHybridPitch(earningsOutput.comm_reference, earningsInputs)
    ).reasons.some((reason) => reason.includes('未授权数字'))
);
assert.ok(
    validatePitch(
        { ...output(), comm_reference: `${output().comm_reference} 财报超预期。` },
        pitchInputs.lit_tags,
        pitchInputs,
        buildHybridPitch(`${output().comm_reference} 财报超预期。`, pitchInputs)
    ).reasons.some((reason) => reason.includes('财报 beat tag'))
);

const noEpsInput: PitchInputs = {
    ...earningsInputs,
    earnings_surprise: null
};
assert.ok(
    validatePitch(
        earningsOutput,
        noEpsInput.lit_tags,
        noEpsInput,
        buildHybridPitch(earningsOutput.comm_reference, noEpsInput)
    ).reasons.some((reason) => reason.includes('未提供有效 earnings_surprise'))
);

assert.ok(
    validatePitch(
        {
            ...output(),
            comm_reference:
                'NVIDIA 是全球 AI 算力 GPU 核心供应商,公司被纳入重要指数强化配置逻辑,显示机构可见度提升。AI 算力周期仍是未来 3-6 个月的主要持有主线,需求兑现节奏支撑核心资产定位。股价距 52 周高点回调 12.8%,使其在 AI 基础设施链条中保持战略重要性。',
            used_tags: ['super_cycle', 'quality_pullback'],
            timing_signal: '股价距 52 周高点回调 12.8%'
        },
        pitchInputs.lit_tags,
        pitchInputs,
        buildHybridPitch(output().comm_reference, pitchInputs)
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
    comm_reference:
        'NVIDIA 是全球 AI 算力 GPU 核心供应商,公司被纳入重要指数,显示机构可见度进一步提升。AI 算力周期仍是未来 3-6 个月的主要持有主线,需求兑现节奏支撑核心资产定位。股价距 52 周高点回调 12.8%,使其在 AI 基础设施链条中保持战略重要性。',
    used_tags: ['super_cycle', 'index_inclusion', 'quality_pullback'],
    timing_signal: '股价距 52 周高点回调 12.8%',
    referenced_news_index: 0,
    numeric_claims: [{ value: 12.8, unit: '%', context: '距 52 周高点' }]
};
assert.equal(validatePitch(indexOutput, indexInputs.lit_tags, indexInputs, buildHybridPitch(indexOutput.comm_reference, indexInputs)).passed, true);

const financialInputs: PitchInputs = {
    ...pitchInputs,
    lit_tags: {
        holding: ['backlog'],
        timing: ['momentum_intact']
    },
    financials_latest_quarter: 'Q1 2026',
    revenue_yoy_pct: 7,
    gross_margin_yoy_pp: 3,
    top_segment: {
        name: 'Data Center and AI',
        yoy_pct: 22
    }
};
const financialOutput: PitchLLMOutput = {
    comm_reference:
        'Intel 是全球 PC 与服务器 CPU 供应商,Q1 2026 收入同比 +7.0%、Data Center and AI 收入同比 +22.0%,显示盈利改善正在兑现。订单积压提供未来 3-6 个月收入能见度,经营修复节奏相对清晰。产品与本土半导体政策催化强化其美国 AI 半导体核心资产定位。',
    used_tags: ['backlog', 'momentum_intact'],
    timing_signal: '趋势仍保持在关键均线上方',
    referenced_news_index: -1,
    numeric_claims: [
        { value: 7, unit: '%', context: '收入同比' },
        { value: 22, unit: '%', context: 'Data Center and AI 收入同比' }
    ]
};
assert.equal(
    validatePitch(financialOutput, financialInputs.lit_tags, financialInputs, buildHybridPitch(financialOutput.comm_reference, financialInputs)).passed,
    true
);

const tooLongFinal = `${buildHybridPitch(output().comm_reference, pitchInputs)}${'补充说明。'.repeat(40)}`;
assert.ok(validatePitch(output(), pitchInputs.lit_tags, pitchInputs, tooLongFinal).reasons.some((reason) => reason.includes('80-220')));

// 连字符标识符里的数字(GLP-1)不是财务声明,不得触发「未授权数字」(LLY 曾被公司
// 描述里的 GLP-1 反复误杀,连兜底模板都被降级成空壳)。
const glpOutput: PitchLLMOutput = {
    ...financialOutput,
    comm_reference:
        'Eli Lilly 是全球 GLP-1 与创新药龙头,Q1 2026 收入同比 +7.0%、Data Center and AI 收入同比 +22.0%,显示盈利兑现节奏清晰。订单积压提供未来 3-6 个月收入能见度,经营节奏相对稳健。产品管线催化强化其创新药核心资产定位。'
};
const glpResult = validatePitch(glpOutput, financialInputs.lit_tags, financialInputs, buildHybridPitch(glpOutput.comm_reference, financialInputs));
assert.ok(
    !glpResult.reasons.some((reason) => reason.includes('未授权数字')),
    `GLP-1 误判: ${glpResult.reasons.join(';')}`
);

console.log('fcn-go-pitch validator tests passed');
