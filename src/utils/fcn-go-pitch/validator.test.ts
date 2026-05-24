import assert from 'node:assert/strict';
import type { PitchInputs, PitchLLMOutput } from './llm-stitcher';
import { validatePitch } from './validator';

const pitchInputs: PitchInputs = {
    symbol: 'NVDA',
    company_short_desc: 'NVIDIA 是全球 AI 算力 GPU 核心供应商',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    lit_tags: {
        holding: ['super_cycle'],
        timing: ['momentum_intact']
    }
};

function output(overrides: Partial<PitchLLMOutput> = {}): PitchLLMOutput {
    return {
        paragraph:
            'NVIDIA 是全球 AI 算力 GPU 核心供应商,所在行业处于上行周期、趋势稳健。这只 FCN 让您以 $85、较现价低 15% 的水平承接 NVDA,年化票息 12%-16%、期限 3 个月;若股价未跌破 $85,您收取票息并赎回本金;若跌破,则以 $85 持有该标的。',
        used_holding_tags: ['super_cycle'],
        used_timing_tags: ['momentum_intact'],
        numeric_claims: ['$85', '15%', '12%-16%', '3 个月'],
        ...overrides
    };
}

assert.equal(validatePitch(output(), pitchInputs.lit_tags, pitchInputs).passed, true);
assert.ok(validatePitch(output({ used_holding_tags: ['guide_raise'] }), pitchInputs.lit_tags, pitchInputs).reasons.some((r) => r.includes('未点亮')));
assert.ok(validatePitch(output({ paragraph: '太短 $85 15% 12%-16% 3 个月' }), pitchInputs.lit_tags, pitchInputs).reasons.some((r) => r.includes('字数')));
assert.ok(validatePitch(output({ paragraph: `${output().paragraph} 敲入。` }), pitchInputs.lit_tags, pitchInputs).reasons.some((r) => r.includes('禁词')));
assert.ok(validatePitch(output({ paragraph: `${output().paragraph} 目标价 999。` }), pitchInputs.lit_tags, pitchInputs).reasons.some((r) => r.includes('未授权数字')));

console.log('fcn-go-pitch validator tests passed');
