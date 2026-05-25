import assert from 'node:assert/strict';
import type { NarrativeInput } from '../narrative-generator';
import type { ConcernLLMOutput } from './llm-stitcher';
import type { ConcernPitchInputs } from './template';
import { buildAvoidPitch, buildCautionPitch } from './template';
import { validateConcernPitch } from './validator';

const narrativeInput: NarrativeInput = {
    symbol: 'TEST',
    company_name: 'Test Corp',
    theme: 'AI',
    grade: 'CAUTION',
    composite_score: 0.46,
    recommended_strike: 85,
    estimated_coupon_range: '12-16%',
    current_price: 100,
    change_5d_pct: -4,
    change_ytd_pct: -2,
    pct_from_52w_high: -18,
    ma20: 98,
    ma50: 95,
    ma200: 90,
    iv_level: 'high',
    flags: [],
    tenor_days: 90,
    news_headlines: [],
    news_items: [],
    has_recent_earnings: false,
    days_to_earnings: 5,
    days_since_earnings: 20
};

const p: ConcernPitchInputs = {
    symbol: 'TEST',
    company_short_desc: 'Test Corp 是数据中心基础设施供应商',
    current_price: 100,
    recommended_strike: 85,
    discount_pct: 15,
    coupon_low: 12,
    coupon_high: 16,
    tenor_label: '3 个月',
    caution_tags: ['earnings_window_imminent', 'distribution_pattern'],
    avoid_tags: [],
    input: narrativeInput
};

const llm: ConcernLLMOutput = {
    concern_sentence: '财报窗口临近，短期事件风险仍未落地，股价节奏也偏弱，建议先保留观察名单，等待更清晰的价格和事件确认，当前卖 put 条件不够友好。',
    used_tags: ['earnings_window_imminent'],
    primary_concern_signal: '财报窗口临近',
    numeric_claims: []
};

const cautionText = buildCautionPitch(llm.concern_sentence, p);
assert.equal(validateConcernPitch('CAUTION', cautionText, p, llm).passed, true);

assert.ok(
    validateConcernPitch('CAUTION', `${cautionText}千万别碰。`, p, llm).reasons.some((reason) => reason.includes('禁词'))
);
assert.ok(
    validateConcernPitch('CAUTION', `${cautionText}当前是机会。`, p, llm).reasons.some((reason) => reason.includes('禁词'))
);
assert.ok(
    validateConcernPitch('CAUTION', '财报窗口临近，当前卖 put 条件不够友好。', p, llm).reasons.some((reason) =>
        reason.includes('trigger')
    )
);
assert.ok(
    validateConcernPitch('CAUTION', cautionText, p, { ...llm, used_tags: ['fake_tag'] }).reasons.some((reason) =>
        reason.includes('未点亮 tag')
    )
);
assert.ok(
    validateConcernPitch('CAUTION', cautionText, p, { ...llm, concern_sentence: '太短' }).reasons.some((reason) =>
        reason.includes('25-90')
    )
);
assert.ok(
    validateConcernPitch('CAUTION', `${cautionText}目标价 999。`, p, llm).reasons.some((reason) =>
        reason.includes('未授权数字')
    )
);

const avoidInputs: ConcernPitchInputs = {
    ...p,
    caution_tags: [],
    avoid_tags: ['guide_cut', 'breakdown_below_ma']
};
const avoidText = buildAvoidPitch(avoidInputs);
assert.equal(validateConcernPitch('AVOID', avoidText, avoidInputs).passed, true);
assert.ok(
    validateConcernPitch('AVOID', `${avoidText}若改善即可推进。`, avoidInputs).reasons.some((reason) =>
        reason.includes('过度乐观')
    )
);
assert.ok(
    validateConcernPitch('AVOID', 'Test Corp。主要原因是价格跌破关键均线。', avoidInputs).reasons.some((reason) =>
        reason.includes('收尾')
    )
);

console.log('fcn-concern-pitch validator tests passed');
