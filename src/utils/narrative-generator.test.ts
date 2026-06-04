import assert from 'node:assert/strict';
import {
    detectNarrativeGradeMismatch,
    enforceNarrativeGradeConsistency,
    generateNarrative,
    type NarrativeInput
} from './narrative-generator';

const previousDeepSeekKey = process.env.DEEPSEEK_API_KEY;
process.env.DEEPSEEK_API_KEY = '';

const baseInput: NarrativeInput = {
    symbol: 'ADBE',
    company_name: 'Adobe Inc.',
    theme: 'Software',
    grade: 'CAUTION',
    composite_score: 0.77,
    recommended_strike: 300,
    estimated_coupon_range: '10%-14%',
    current_price: 360,
    change_1d_pct: null,
    change_5d_pct: 0,
    change_ytd_pct: 2,
    pct_from_52w_high: -8,
    ma20: 355,
    ma50: 350,
    ma200: 330,
    iv_level: '中',
    flags: [],
    tenor_days: 90,
    news_headlines: ['Adobe product update'],
    news_items: [{ title: 'Adobe product update' }],
    has_recent_earnings: false,
    earnings_weight: 0,
    days_to_earnings: 5,
    days_since_earnings: null,
    active_attribution_rules: []
};

async function run(): Promise<void> {
    const caution = await generateNarrative(baseInput);
    assert.equal(caution.source_quality, 'caution_pitch_template');
    assert.ok(caution.why_now.includes('Adobe 是'));

    const sparseGo = await generateNarrative({
        ...baseInput,
        symbol: 'NVDA',
        company_name: 'NVIDIA Corporation',
        grade: 'GO',
        change_5d_pct: null,
        news_headlines: [],
        news_items: [],
        days_to_earnings: null
    });
    assert.match(sparseGo.source_quality ?? '', /^go_pitch_/);
    assert.ok(sparseGo.why_now.includes('NVIDIA 是'));

    const unknown = await generateNarrative({
        ...baseInput,
        grade: 'WATCH'
    });
    assert.equal(unknown.source_quality, 'template_fallback');

    const goMismatch = detectNarrativeGradeMismatch(
        { grade: 'GO' },
        {
            why_now: '当前不建议推进该 FCN 结构，本期先以观察为主。',
            risk_note: '',
            sentiment_score: 0.45,
            key_events: [],
            source_quality: 'caution_pitch_template'
        }
    );
    assert.equal(goMismatch.mismatched, true);
    assert.ok(goMismatch.reasons.some((reason) => reason.startsWith('source_quality_not_go')));
    assert.ok(goMismatch.reasons.some((reason) => reason.startsWith('go_text_contains_concern')));

    const cautionFixed = await enforceNarrativeGradeConsistency(baseInput, {
        why_now: '让您以 $300 承接 ADBE，年化票息 10%-14%；若股价未跌破 $300，您收取票息并赎回本金。',
        risk_note: '',
        sentiment_score: 0.6,
        key_events: [],
        source_quality: 'go_pitch_hybrid_validated'
    });
    assert.equal(cautionFixed.source_quality, 'caution_pitch_template');
    assert.ok(!cautionFixed.why_now.includes('若股价未跌破'));

    const avoidFixed = await enforceNarrativeGradeConsistency(
        {
            ...baseInput,
            grade: 'AVOID',
            days_to_earnings: null,
            news_headlines: [],
            news_items: []
        },
        {
            why_now: '条款上，让您以 $300 承接 ADBE，年化票息 10%-14%，当前是机会。',
            risk_note: '',
            sentiment_score: 0.6,
            key_events: [],
            source_quality: 'go_pitch_hybrid_validated'
        }
    );
    assert.equal(avoidFixed.source_quality, 'avoid_pitch_deterministic');
    assert.ok(avoidFixed.why_now.includes('当前不建议推进'));

    process.env.DEEPSEEK_API_KEY = previousDeepSeekKey;
    console.log('narrative-generator tests passed');
}

run().catch((error) => {
    process.env.DEEPSEEK_API_KEY = previousDeepSeekKey;
    console.error(error);
    process.exit(1);
});
