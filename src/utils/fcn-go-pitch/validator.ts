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
    '执行价',
    '票息',
    '期限',
    '若跌破',
    'sell put',
    'FCN',
    'assignment',
    '正是好时机',
    '不过是',
    '您本就看好',
    '非保本',
    '信用风险',
    '流动性风险'
];

const GENERIC_CLAIMS = ['基本面强劲', '技术面强势', '长期向好', '市场关注度提升', '事实锚', '可持有属性', '依赖基本面兑现'];
const GENERIC_TIMING_PHRASES = ['近期', '最近', '当前', '市场关注', '情绪改善'];
const PRICE_DATA_SIGNAL_PATTERNS = [/距\s*52\s*周高点/, /回调/, /近\s*5\s*日/, /趋势/, /均线/];
const TAG_CONDITIONAL_BANS: Partial<Record<HoldingTag, RegExp[]>> = {
    index_inclusion: [
        /纳入重要指数/,
        /指数纳入/,
        /纳入标普/,
        /纳入纳斯达克/,
        /被纳入.{0,8}指数/,
        /入选.{0,8}指数/,
        /\binclusion in\b.{0,20}\bindex\b/i,
        /\badded to\b.{0,20}\bindex\b/i,
        /\bjoins?\b.{0,20}\bS&P\b/i,
        /\bjoins?\b.{0,20}\bNasdaq\b/i
    ],
    guidance_reaffirmed_or_raised: [
        /上调指引/,
        /维持指引/,
        /重申指引/,
        /上调展望/,
        /上调预期/,
        /\braised guidance\b/i,
        /\breaffirmed outlook\b/i,
        /\bboosted forecast\b/i
    ],
    earnings_strong_beat: [/超预期/, /\bbeat consensus\b/i, /\bbeat estimates\b/i, /业绩超预期/, /财报超预期/],
    earnings_modest_beat: [/超预期/, /\bbeat consensus\b/i, /\bbeat estimates\b/i, /业绩超预期/, /财报超预期/]
};

export function validatePitch(
    output: PitchLLMOutput,
    litTags: LitTags,
    pitchInputs: PitchInputs,
    finalParagraph?: string
): PitchValidationResult {
    const reasons: string[] = [];
    const commReference = output.comm_reference ?? '';
    const commLength = commReference.replace(/\s+/g, '').length;
    if (commLength < 80 || commLength > 180) reasons.push(`comm_reference 字数 ${commLength} 不在 80-180 范围`);

    const allLitTags = [...litTags.holding, ...litTags.timing];
    for (const tag of output.used_tags) {
        if (!allLitTags.includes(tag as HoldingTag)) reasons.push(`未点亮 tag: ${tag}`);
    }
    if (!output.used_tags.some((tag) => litTags.holding.includes(tag as HoldingTag))) {
        reasons.push('未使用任何 holding tag');
    }
    const hasEarningsBeatTag = litTags.holding.includes('earnings_strong_beat') || litTags.holding.includes('earnings_modest_beat');
    const hasGuidanceTag = litTags.holding.includes('guide_raise') || litTags.holding.includes('guidance_reaffirmed_or_raised');
    if (!hasEarningsBeatTag && /超预期|beat/i.test(commReference)) {
        reasons.push('未点亮财报 beat tag,不得使用财报宣传词');
    }
    if (!hasEarningsBeatTag && !hasGuidanceTag && /上调|强劲/i.test(commReference)) {
        reasons.push('未点亮财报/指引 tag,不得使用财报宣传词');
    }
    if (!pitchInputs.earnings_surprise && /EPS\s*超预期|EPS beat|eps beat/i.test(commReference)) {
        reasons.push('未提供有效 earnings_surprise,不得引用 EPS surprise');
    }
    if (/本周|上周/.test(commReference)) {
        reasons.push('不得使用相对时间词:本周/上周');
    }

    const timingSignalResult = validateTimingSignal(output, litTags, pitchInputs);
    reasons.push(...timingSignalResult);
    reasons.push(...validateSpecificity(output, litTags, pitchInputs));
    if (
        output.referenced_news_index !== undefined &&
        output.referenced_news_index >= (pitchInputs.recent_news_titles?.length ?? 0)
    ) {
        reasons.push(`referenced_news_index 越界: ${output.referenced_news_index}`);
    }

    const textForForbiddenScan = `${commReference}\n${finalParagraph ?? ''}`;
    for (const phrase of FORBIDDEN_PHRASES) {
        if (textForForbiddenScan.includes(phrase)) reasons.push(`含禁词: ${phrase}`);
    }
    for (const phrase of GENERIC_CLAIMS) {
        if (commReference.includes(phrase)) reasons.push(`空话表达: ${phrase}`);
    }
    reasons.push(...validateSemanticTagConsistency(textForForbiddenScan, output.used_tags));

    if (finalParagraph !== undefined) {
        const finalLength = finalParagraph.replace(/\s+/g, '').length;
        if (finalLength < 80 || finalLength > 220) reasons.push(`最终段落字数 ${finalLength} 不在 80-220 范围`);
    }

    const numberText = `${commReference}\n${finalParagraph ?? ''}\n${output.numeric_claims
        .map((claim) => `${claim.context ?? ''} ${claim.value}${claim.unit}`)
        .join('\n')}`;
    for (const num of extractNumbers(numberText)) {
        if (isStructuralWindowNumber(num, numberText)) continue;
        const authorization = authorizeNumber(num, numberText, pitchInputs);
        if (!authorization.passed) {
            console.log(
                JSON.stringify({
                    tag: 'numeric_context_mismatch',
                    symbol: pitchInputs.symbol,
                    value: num.value,
                    context: authorization.context,
                    expected_kinds: authorization.expectedKinds,
                    ts: new Date().toISOString()
                })
            );
            reasons.push(`未授权数字: ${num.value}`);
        }
    }

    return { passed: reasons.length === 0, reasons };
}

function validateSemanticTagConsistency(text: string, usedTags: string[]): string[] {
    const used = new Set(usedTags);
    const reasons: string[] = [];

    for (const [tag, patterns] of Object.entries(TAG_CONDITIONAL_BANS) as Array<[HoldingTag, RegExp[]]>) {
        const tagIsPresent = tag === 'earnings_strong_beat' || tag === 'earnings_modest_beat'
            ? used.has('earnings_strong_beat') || used.has('earnings_modest_beat')
            : used.has(tag);
        if (tagIsPresent) continue;

        for (const pattern of patterns) {
            const match = text.match(pattern);
            if (match) reasons.push(`tag_conditional_ban_hit: ${tag}: ${match[0]}`);
        }
    }

    return reasons;
}

export function validateGeneratedPitchText(text: string, pitchInputs: PitchInputs): PitchValidationResult {
    const reasons: string[] = [];

    for (const phrase of FORBIDDEN_PHRASES) {
        if (text.includes(phrase)) reasons.push(`含禁词: ${phrase}`);
    }

    for (const num of extractNumbers(text)) {
        if (isStructuralWindowNumber(num, text)) continue;
        const authorization = authorizeNumber(num, text, pitchInputs);
        if (!authorization.passed) {
            console.log(
                JSON.stringify({
                    tag: 'numeric_context_mismatch',
                    symbol: pitchInputs.symbol,
                    value: num.value,
                    context: authorization.context,
                    expected_kinds: authorization.expectedKinds,
                    ts: new Date().toISOString()
                })
            );
            reasons.push(`未授权数字: ${num.value}`);
        }
    }

    return { passed: reasons.length === 0, reasons };
}

function validateSpecificity(output: PitchLLMOutput, litTags: LitTags, pitchInputs: PitchInputs): string[] {
    const text = output.comm_reference ?? '';
    const hasAllowedNumericFact = extractNumbers(text).some((num) => {
        if (isStructuralWindowNumber(num, text)) return false;
        return authorizeNumber(num, text, pitchInputs).passed;
    });
    const hasNewsAnchor = hasNewsTimingSignal(output, pitchInputs);
    const hasTagSpecificPhrase = hasHoldingTagPhrase(text, litTags) || hasTimingTagPhrase(text, litTags);
    return hasAllowedNumericFact || hasNewsAnchor || hasTagSpecificPhrase
        ? []
        : ['comm_reference 缺少安全特异性来源'];
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
        output.comm_reference,
        (pitchInputs.recent_news_titles ?? []).map((title) => ({
            title,
            published_at: new Date().toISOString()
        }))
    );
    return eventValidation.passed && eventValidation.unanchored.length === 0 && detectNewsClaim(output.comm_reference);
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
    if (litTags.holding.includes('earnings_strong_beat') && /财报|超预期|beat|业绩|EPS/.test(text)) return true;
    if (litTags.holding.includes('earnings_modest_beat') && /财报|超预期|beat|业绩|EPS/.test(text)) return true;
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

interface AllowedFact {
    value: number;
    kind: string;
    contextKeywords: string[];
}

function extractNumbers(text: string): ExtractedNumber[] {
    const numbers: ExtractedNumber[] = [];
    const regex = /\d+(?:\.\d+)?/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
        const prev = match.index > 0 ? text[match.index - 1] : '';
        const next = text[match.index + match[0].length] ?? '';
        if (match[0].length <= 2 && (/[A-Za-z]/.test(prev) || /[A-Za-z]/.test(next))) continue;
        numbers.push({
            value: Number(match[0]),
            raw: match[0],
            index: match.index
        });
    }
    return numbers;
}

function isStructuralWindowNumber(num: ExtractedNumber, text: string): boolean {
    const context = text.slice(Math.max(0, num.index - 10), num.index + num.raw.length + 12);
    const escapedRaw = num.raw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const isCalendarYear =
        /^20\d{2}$/.test(num.raw) &&
        new RegExp(
            `(?:FY|fiscal|Q[1-4]|H[12]|财年|上半年|下半年)\\s*${escapedRaw}|${escapedRaw}\\s*(?:年|财年|指引|上半年|下半年|H[12]|Q[1-4])`,
            'i'
        ).test(context);
    return (
        (num.value === 52 && /52\s*周/.test(context)) ||
        (num.value === 5 && /近\s*5\s*日/.test(context)) ||
        ((num.value === 3 || num.value === 6) && /3\s*[-–~至到]\s*6\s*个月|3\s*到\s*6\s*个月/.test(context)) ||
        isCalendarYear ||
        (num.value >= 1 && num.value <= 4 && new RegExp(`Q\\s*${num.raw}|${num.raw}\\s*季`, 'i').test(context))
    );
}

function authorizeNumber(
    num: ExtractedNumber,
    text: string,
    pitchInputs: PitchInputs
): { passed: boolean; context: string; expectedKinds: string[] } {
    const context = getNumberContext(text, num);
    const candidates = buildAllowedFacts(pitchInputs).filter((fact) => Math.abs(Math.abs(fact.value) - Math.abs(num.value)) < 0.6);
    const passed = candidates.some((fact) => matchesFactContext(fact, context));
    return {
        passed,
        context,
        expectedKinds: candidates.map((fact) => fact.kind)
    };
}

function getNumberContext(text: string, num: ExtractedNumber): string {
    const punctuation = /[，。；;、\n]/;
    let start = num.index;
    while (start > 0 && !punctuation.test(text[start - 1])) start -= 1;
    let end = num.index + num.raw.length;
    while (end < text.length && !punctuation.test(text[end])) end += 1;
    const segment = text.slice(start, end).trim();
    if (segment.length <= 28) return segment;
    return text.slice(Math.max(0, num.index - 10), num.index + num.raw.length + 10);
}

function matchesFactContext(fact: AllowedFact, context: string): boolean {
    switch (fact.kind) {
        case 'distance_52w':
            return context.includes('52') || (context.includes('距') && context.includes('高')) || context.includes('回调');
        case 'eps_surprise':
            return /(EPS|财报|业绩)/i.test(context) && /超预期|beat/i.test(context);
        case 'change_5d':
            return context.includes('5日') || (context.includes('5') && (context.includes('近') || context.includes('日')));
        default:
            return fact.contextKeywords.some((keyword) => context.includes(keyword));
    }
}

function buildAllowedFacts(pitchInputs: PitchInputs): AllowedFact[] {
    const facts: AllowedFact[] = [
        {
            value: pitchInputs.current_price,
            kind: 'price',
            contextKeywords: ['$', '现价', '当前价', '承接', '执行']
        },
        {
            value: pitchInputs.recommended_strike,
            kind: 'price',
            contextKeywords: ['$', '执行', '现价', '承接']
        },
        {
            value: pitchInputs.discount_pct,
            kind: 'discount',
            contextKeywords: ['低', '折让', '较']
        },
        {
            value: pitchInputs.coupon_low,
            kind: 'coupon',
            contextKeywords: ['票息', '年化']
        },
        {
            value: pitchInputs.coupon_high,
            kind: 'coupon',
            contextKeywords: ['票息', '年化']
        },
        {
            value: Number(pitchInputs.tenor_label.match(/\d+/)?.[0] ?? '3'),
            kind: 'tenor',
            contextKeywords: ['个月', '期限']
        }
    ];

    if (typeof pitchInputs.pct_from_52w_high === 'number') {
        facts.push({
            value: Math.abs(pitchInputs.pct_from_52w_high),
            kind: 'distance_52w',
            contextKeywords: ['52', '周', '高', '距', '回调']
        });
    }
    if (typeof pitchInputs.change_5d_pct === 'number') {
        facts.push({
            value: Math.abs(pitchInputs.change_5d_pct),
            kind: 'change_5d',
            contextKeywords: ['近', '5', '日', '5日']
        });
    }
    if (pitchInputs.earnings_surprise) {
        facts.push({
            value: Math.abs(pitchInputs.earnings_surprise.eps_surprise_pct),
            kind: 'eps_surprise',
            contextKeywords: ['EPS', '超预期', '财报', '业绩', 'beat']
        });
    }
    if (typeof pitchInputs.days_since_earnings === 'number') {
        facts.push({
            value: pitchInputs.days_since_earnings,
            kind: 'days_post_earnings',
            contextKeywords: ['财报后', '天']
        });
    }
    if (typeof pitchInputs.revenue_yoy_pct === 'number') {
        facts.push({
            value: Math.abs(pitchInputs.revenue_yoy_pct),
            kind: 'revenue_yoy',
            contextKeywords: ['收入', '营收', '同比', 'revenue']
        });
    }
    if (pitchInputs.top_segment) {
        facts.push({
            value: Math.abs(pitchInputs.top_segment.yoy_pct),
            kind: 'segment_yoy',
            contextKeywords: [pitchInputs.top_segment.name, '分部', '业务', '收入', '同比']
        });
    }
    if (typeof pitchInputs.gross_margin_pct === 'number') {
        facts.push({
            value: Math.abs(pitchInputs.gross_margin_pct),
            kind: 'gross_margin',
            contextKeywords: ['毛利率', 'margin']
        });
    }
    if (typeof pitchInputs.gross_margin_yoy_pp === 'number') {
        facts.push({
            value: Math.abs(pitchInputs.gross_margin_yoy_pp),
            kind: 'gross_margin_yoy',
            contextKeywords: ['毛利率', '同比', 'pct', 'pp']
        });
    }

    return facts;
}
