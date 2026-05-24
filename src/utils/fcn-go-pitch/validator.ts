import type { PitchInputs, PitchLLMOutput } from './llm-stitcher';
import type { HoldingTag, LitTags, TimingTag } from './tag-detector';
import { validateEventAnchors } from '../narrative-event-validator';

export interface PitchValidationResult {
    passed: boolean;
    reasons: string[];
}

const FORBIDDEN_PHRASES = [
    '敲入',
    '接货',
    '安全垫',
    '摊薄',
    'assignment',
    '正是好时机',
    '不过是',
    '您本就看好',
    '非保本',
    '信用风险',
    '流动性风险'
];

export function validatePitch(
    output: PitchLLMOutput,
    litTags: LitTags,
    pitchInputs: PitchInputs
): PitchValidationResult {
    const reasons: string[] = [];
    const paragraph = output.paragraph ?? '';
    const len = paragraph.replace(/\s+/g, '').length;
    if (len < 100 || len > 180) reasons.push(`字数 ${len} 不在 100-180 范围`);

    for (const tag of output.used_holding_tags) {
        if (!litTags.holding.includes(tag as HoldingTag)) reasons.push(`未点亮 holding tag: ${tag}`);
    }
    for (const tag of output.used_timing_tags) {
        if (!litTags.timing.includes(tag as TimingTag)) reasons.push(`未点亮 timing tag: ${tag}`);
    }
    if (output.used_holding_tags.length === 0) reasons.push('未使用任何 holding tag');
    if (output.used_timing_tags.length === 0) reasons.push('未使用任何 timing tag');

    for (const phrase of FORBIDDEN_PHRASES) {
        if (paragraph.includes(phrase)) reasons.push(`含禁词: ${phrase}`);
    }

    if (pitchInputs.recent_news_titles && pitchInputs.recent_news_titles.length > 0) {
        const eventValidation = validateEventAnchors(
            paragraph,
            pitchInputs.recent_news_titles.map((title) => ({
                title,
                published_at: new Date().toISOString()
            }))
        );
        if (!eventValidation.passed) {
            reasons.push(`含未锚定事件: ${eventValidation.unanchored.map((item) => item.snippet).join(', ')}`);
        }
    }

    const allowed = [
        pitchInputs.current_price,
        pitchInputs.recommended_strike,
        pitchInputs.discount_pct,
        pitchInputs.coupon_low,
        pitchInputs.coupon_high,
        Number(pitchInputs.tenor_label.match(/\d+/)?.[0] ?? '3')
    ];
    const textToScan = `${paragraph}\n${output.numeric_claims.join('\n')}`;
    const numbers = (textToScan.match(/\d+(?:\.\d+)?/g) ?? []).map(Number);
    for (const num of numbers) {
        if (!allowed.some((allowedNum) => Math.abs(allowedNum - num) < 0.6)) {
            reasons.push(`未授权数字: ${num}`);
        }
    }

    return { passed: reasons.length === 0, reasons };
}
