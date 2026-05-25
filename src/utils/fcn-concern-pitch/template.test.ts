import assert from 'node:assert/strict';
import type { NarrativeInput } from '../narrative-generator';
import type { ConcernPitchInputs } from './template';
import { buildAvoidEnding, buildAvoidPitch, buildCautionPitch, buildCautionTemplate, buildTriggerSentence, pickBridge } from './template';

const input: NarrativeInput = {
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
    news_items: []
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

assert.match(pickBridge('TEST'), /在这个节奏下，|对应当前结构，|节奏上，/);
assert.match(buildTriggerSentence('CAUTION', ['earnings_window_imminent'], p), /财报落地.*事件风险消化.*再评估承接/);
assert.match(buildTriggerSentence('CAUTION', ['breakdown_below_ma'], p), /重新站回 MA50 \/ MA200.*趋势企稳.*再评估承接/);
assert.match(buildTriggerSentence('CAUTION', ['iv_too_low'], p), /IV 回升.*票息条件更具吸引力/);
assert.match(buildTriggerSentence('CAUTION', ['regulatory_overhang'], p), /相关事件明朗.*风险落地/);

const caution = buildCautionPitch('财报窗口临近，当前卖 put 条件不够友好', p);
assert.ok(caution.includes('财报窗口临近'));
assert.ok(caution.includes('再评估'));

const template = buildCautionTemplate({ ...p, caution_tags: ['iv_too_low'] });
assert.ok(template.includes('当前 IV 偏低'));
assert.ok(template.includes('不够友好'));

const avoidP: ConcernPitchInputs = { ...p, caution_tags: [], avoid_tags: ['guide_cut', 'breakdown_below_ma'] };
const avoid = buildAvoidPitch(avoidP);
assert.ok(avoid.includes('当前不建议推进该 FCN 结构'));
assert.ok(avoid.includes('后再评估'));
assert.ok(buildAvoidEnding(['regulatory_overhang'], p).includes('相关事件明朗'));

console.log('fcn-concern-pitch template tests passed');
