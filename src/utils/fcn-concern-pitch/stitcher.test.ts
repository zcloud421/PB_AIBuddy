import assert from 'node:assert/strict';
import type { NarrativeInput } from '../narrative-generator';
import type { ConcernLLMOutput } from './llm-stitcher';
import type { ConcernPitchInputs } from './template';
import { buildCautionPitch } from './template';
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

console.log('fcn-concern-pitch stitcher tests passed');
