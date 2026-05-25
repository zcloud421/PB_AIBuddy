import type { PitchInputs, PitchLLMOutput } from './llm-stitcher';
import type { HoldingTag, LitTags } from './tag-detector';
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

const GENERIC_TIMING_PHRASES = ['近期', '最近', '当前', '市场关注', '情绪改善'];
const PRICE_DATA_SIGNAL_PATTERNS = [/距\s*52\s*周高点/, /回调/, /近\s*5\s*日/, /趋势/, /均线/];

export function validatePitch(
    output: PitchLLMOutput,
    litTags: LitTags,
    pitchInputs: PitchInputs,
    finalParagraph?: string
): PitchValidationResult {
    const reasons: string[] = [];
    const whySentence = output.why_sentence ?? '';
    const whyLength = whySentence.replace(/\s+/g, '').length;
    if (whyLength < 35 || whyLength > 90) reasons.push(`why_sentence 字数 ${whyLength} 不在 35-90 范围`);

    const allLitTags = [...litTags.holding, ...litTags.timing];
    for (const tag of output.used_tags) {
        if (!allLitTags.includes(tag as HoldingTag)) reasons.push(`未点亮 tag: ${tag}`);
    }
    if (!output.used_tags.some((tag) => litTags.holding.includes(tag as HoldingTag))) {
        reasons.push('未使用任何 holding tag');
    }

    const timingSignalResult = validateTimingSignal(output, litTags, pitchInputs);
    reasons.push(...timingSignalResult);
    reasons.push(...validateSpecificity(output, litTags, pitchInputs));

    const textForForbiddenScan = `${whySentence}\n${finalParagraph ?? ''}`;
    for (const phrase of FORBIDDEN_PHRASES) {
        if (textForForbiddenScan.includes(phrase)) reasons.push(`含禁词: ${phrase}`);
    }

    if (finalParagraph !== undefined) {
        const finalLength = finalParagraph.replace(/\s+/g, '').length;
        if (finalLength < 100 || finalLength > 220) reasons.push(`最终段落字数 ${finalLength} 不在 100-220 范围`);
    }

    const numberText = `${whySentence}\n${finalParagraph ?? ''}\n${output.numeric_claims
        .map((claim) => `${claim.value}${claim.unit}`)
        .join('\n')}`;
    for (const num of extractNumbers(numberText)) {
        if (isStructuralWindowNumber(num, numberText)) continue;
        if (!isAllowedNumber(num, pitchInputs)) {
            reasons.push(`未授权数字: ${num.value}`);
        }
    }

    return { passed: reasons.length === 0, reasons };
}

function validateSpecificity(output: PitchLLMOutput, litTags: LitTags, pitchInputs: PitchInputs): string[] {
    const text = output.why_sentence ?? '';
    const hasAllowedNumericFact = extractNumbers(text).some((num) => {
        if (isStructuralWindowNumber(num, text)) return false;
        return isAllowedNumber(num, pitchInputs);
    });
    const hasNewsAnchor = hasNewsTimingSignal(output, pitchInputs);
    const hasTagSpecificPhrase = hasHoldingTagPhrase(text, litTags) || hasTimingTagPhrase(text, litTags);
    return hasAllowedNumericFact || hasNewsAnchor || hasTagSpecificPhrase
        ? []
        : ['why_sentence 缺少安全特异性来源'];
}

function validateTimingSignal(output: PitchLLMOutput, litTags: LitTags, pitchInputs: PitchInputs): string[] {
    const reasons: string[] = [];
    const timingSignal = output.timing_signal?.trim() ?? '';
    if (!timingSignal) {
        return ['timing_signal 为空'];
    }

    const genericOnly = GENERIC_TIMING_PHRASES.includes(timingSignal);
    if (genericOnly) {
        reasons.push(`timing_signal 过于空泛: ${timingSignal}`);
    }

    const hasPriceDataSignal = PRICE_DATA_SIGNAL_PATTERNS.some((pattern) => pattern.test(timingSignal));
    const hasNewsSignal = hasNewsTimingSignal(output, pitchInputs);
    const hasLitTimingPhrase = hasTimingTagPhrase(timingSignal, litTags);
    if (!hasPriceDataSignal && !hasNewsSignal && !hasLitTimingPhrase) {
        reasons.push(`timing_signal 未匹配有效来源: ${timingSignal}`);
    }

    return reasons;
}

function hasNewsTimingSignal(output: PitchLLMOutput, pitchInputs: PitchInputs): boolean {
    if (
        output.referenced_news_index !== undefined &&
        output.referenced_news_index >= 0 &&
        output.referenced_news_index < (pitchInputs.recent_news_titles?.length ?? 0)
    ) {
        return true;
    }

    const eventValidation = validateEventAnchors(
        output.why_sentence,
        (pitchInputs.recent_news_titles ?? []).map((title) => ({
            title,
            published_at: new Date().toISOString()
        }))
    );
    return eventValidation.passed && eventValidation.unanchored.length === 0 && detectNewsClaim(output.why_sentence);
}

function hasTimingTagPhrase(timingSignal: string, litTags: LitTags): boolean {
    if (litTags.timing.includes('quality_pullback')) {
        if (/回调|承接水平|距高|距\s*52\s*周高点/.test(timingSignal)) return true;
    }
    if (litTags.timing.includes('momentum_intact')) {
        if (/趋势|动量|均线|高位稳住/.test(timingSignal)) return true;
    }
    return false;
}

function hasHoldingTagPhrase(text: string, litTags: LitTags): boolean {
    if (litTags.holding.includes('post_earnings_beat') && /财报|超预期|beat|业绩/.test(text)) return true;
    if (litTags.holding.includes('guidance_reaffirmed_or_raised') && /指引|展望|上调|维持/.test(text)) return true;
    if (litTags.holding.includes('index_inclusion') && /指数|纳入|Nasdaq|S&P/.test(text)) return true;
    if (litTags.holding.includes('infrastructure_capacity_cycle') && /数据中心|基础设施|电源|散热|光通信|网络/.test(text)) return true;
    if (litTags.holding.includes('guide_raise') && /指引|财报|超预期/.test(text)) return true;
    if (litTags.holding.includes('super_cycle') && /周期|capex|算力|油气|半导体/.test(text)) return true;
    if (litTags.holding.includes('backlog') && /backlog|订单|积压|能见度/.test(text)) return true;
    return false;
}

function detectNewsClaim(text: string): boolean {
    return /纳入|剔除|收购|并购|分拆|回购|获批|批准|通过|裁决|诉讼|调查|审查|处罚|罚款|禁令|制裁|出口管制|关税|反垄断|上调|下调|发布|推出|签约|合作|罢工|召回|停产|关闭|营收|净利润|EPS|指引|展望|超预期|低于预期|beat|miss/i.test(text);
}

interface ExtractedNumber {
    value: number;
    raw: string;
    index: number;
}

function extractNumbers(text: string): ExtractedNumber[] {
    const numbers: ExtractedNumber[] = [];
    const regex = /\d+(?:\.\d+)?/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
        numbers.push({
            value: Number(match[0]),
            raw: match[0],
            index: match.index
        });
    }
    return numbers;
}

function isStructuralWindowNumber(num: ExtractedNumber, text: string): boolean {
    const context = text.slice(Math.max(0, num.index - 4), num.index + num.raw.length + 8);
    return (
        (num.value === 52 && /52\s*周/.test(context)) ||
        (num.value === 5 && /近\s*5\s*日/.test(context))
    );
}

function isAllowedNumber(num: ExtractedNumber, pitchInputs: PitchInputs): boolean {
    const allowed = [
        pitchInputs.current_price,
        pitchInputs.recommended_strike,
        pitchInputs.discount_pct,
        pitchInputs.coupon_low,
        pitchInputs.coupon_high,
        Number(pitchInputs.tenor_label.match(/\d+/)?.[0] ?? '3')
    ];

    if (typeof pitchInputs.change_5d_pct === 'number') {
        allowed.push(Math.abs(pitchInputs.change_5d_pct));
    }
    if (typeof pitchInputs.pct_from_52w_high === 'number') {
        allowed.push(Math.abs(pitchInputs.pct_from_52w_high));
    }
    if (typeof pitchInputs.days_since_earnings === 'number') {
        allowed.push(pitchInputs.days_since_earnings);
    }

    return allowed.some((allowedNum) => Math.abs(Math.abs(allowedNum) - Math.abs(num.value)) < 0.6);
}
