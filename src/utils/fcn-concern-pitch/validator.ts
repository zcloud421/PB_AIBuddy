import type { ConcernLLMOutput } from './llm-stitcher';
import type { ConcernTag } from './tag-detector';
import type { ConcernPitchInputs } from './template';

export interface ConcernValidationResult {
    passed: boolean;
    reasons: string[];
}

const FORBIDDEN = ['崩盘', '暴跌', '腰斩', '灾难', '危险', '千万别', '绝对', '必然', '肯定', '逃离', '踩踏', '血洗', '将会跌破', '大概率跌破', '必然下跌'];
const ANTI_GO = ['可推进', '可询价', '可承接', '适合承接', '进入询价', '票息具吸引力', '结构有吸引力', '当前是机会', '可以考虑卖 put', '承接水平更有纪律'];
const TRIGGER_TERMS = ['若', '如果', '待', '等', '一旦', '当', '后再评估', '确认后', '落地后', '重新站回', '企稳'];
const AVOID_ENDING = ['建议观望', '暂缓推进', '不建议推进', '先不纳入', '当前不适合', '暂不考虑', '后再评估'];

export function validateConcernPitch(
    mode: 'CAUTION' | 'AVOID',
    text: string,
    p: ConcernPitchInputs,
    llmOutput?: ConcernLLMOutput
): ConcernValidationResult {
    const reasons: string[] = [];
    const len = text.replace(/\s+/g, '').length;
    if (mode === 'CAUTION' && (len < 70 || len > 220)) reasons.push(`CAUTION 字数 ${len} 不在 70-220 范围`);
    if (mode === 'AVOID' && (len < 70 || len > 160)) reasons.push(`AVOID 字数 ${len} 不在 70-160 范围`);

    for (const phrase of [...FORBIDDEN, ...ANTI_GO]) {
        if (text.includes(phrase)) reasons.push(`含禁词: ${phrase}`);
    }

    if (mode === 'CAUTION') {
        if (!hasTriggerTerm(text)) reasons.push('CAUTION 缺少 trigger 语义');
    } else {
        if (!AVOID_ENDING.some((term) => text.includes(term))) reasons.push('AVOID 缺少观望/暂缓收尾');
        if (/若.*(?:改善|企稳).*推进/.test(text)) reasons.push('AVOID 含过度乐观 trigger');
    }

    if (llmOutput) {
        const allTags = new Set<ConcernTag>([...p.caution_tags, ...p.avoid_tags]);
        for (const tag of llmOutput.used_tags) {
            if (!allTags.has(tag as ConcernTag)) reasons.push(`未点亮 tag: ${tag}`);
        }
        const sentenceLen = llmOutput.concern_sentence.replace(/\s+/g, '').length;
        if (sentenceLen < 25 || sentenceLen > 90) reasons.push(`concern_sentence 字数 ${sentenceLen} 不在 25-90 范围`);
        reasons.push(...validateConcernSpecificity(llmOutput.concern_sentence, p));
    }

    const numericText = `${text}\n${llmOutput?.numeric_claims.map((claim) => `${claim.value}${claim.unit}`).join('\n') ?? ''}`;
    for (const number of extractNumbers(numericText)) {
        if (isStructuralNumber(number, numericText)) continue;
        if (!isAllowedNumber(number.value, p)) reasons.push(`未授权数字: ${number.value}`);
    }

    return { passed: reasons.length === 0, reasons };
}

function validateConcernSpecificity(text: string, p: ConcernPitchInputs): string[] {
    const hasAllowedNumber = extractNumbers(text).some((num) => {
        if (isStructuralNumber(num, text)) return false;
        return isAllowedNumber(num.value, p);
    });
    const hasTagPhrase = hasConcernTagPhrase(text, [...p.caution_tags, ...p.avoid_tags]);
    const hasEventWindow = /财报|事件|监管|诉讼|指引|均线|MA50|MA200|IV|评分|反弹|回调|年内|近\s*5\s*日/.test(text);
    return hasAllowedNumber || hasTagPhrase || hasEventWindow
        ? []
        : ['concern_sentence 缺少安全特异性来源'];
}

function hasConcernTagPhrase(text: string, tags: ConcernTag[]): boolean {
    if (tags.includes('earnings_window_imminent') && /财报|窗口/.test(text)) return true;
    if (tags.includes('iv_too_low') && /IV|票息|补偿/.test(text)) return true;
    if (tags.includes('composite_score_borderline') && /评分|边界|优势/.test(text)) return true;
    if (tags.includes('guide_cut') && /指引|下调/.test(text)) return true;
    if (tags.includes('earnings_miss_recent') && /财报|不及预期|重新定价/.test(text)) return true;
    if (tags.includes('breakdown_below_ma') && /均线|MA50|MA200|趋势/.test(text)) return true;
    if (tags.includes('regulatory_overhang') && /监管|法律|事件|诉讼/.test(text)) return true;
    if (tags.includes('post_earnings_gap_down') && /财报后|股价下行|事件风险/.test(text)) return true;
    if (tags.includes('distribution_pattern') && /MA20|动量|价格/.test(text)) return true;
    if (tags.includes('failed_rebound') && /反弹|延续性|确认度/.test(text)) return true;
    if (tags.includes('single_name_news_overhang') && /新闻|不确定性|事件/.test(text)) return true;
    if (tags.includes('high_vol_event_risk') && /IV|事件窗口|财报/.test(text)) return true;
    if (tags.includes('liquidity_or_gap_risk') && /流动性|跳空/.test(text)) return true;
    if (tags.includes('relative_underperformance_5d_20d') && /短期|年内|表现/.test(text)) return true;
    return false;
}

function hasTriggerTerm(text: string): boolean {
    return TRIGGER_TERMS.some((term) => {
        if (term === '当') return /当(?:价格|股价|趋势|IV|财报|事件|相关|风险|信号)/.test(text);
        return text.includes(term);
    });
}

interface ExtractedNumber {
    value: number;
    raw: string;
    index: number;
}

export function extractNumbers(text: string): ExtractedNumber[] {
    const numbers: ExtractedNumber[] = [];
    const regex = /\d+(?:\.\d+)?/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
        numbers.push({ value: Number(match[0]), raw: match[0], index: match.index });
    }
    return numbers;
}

function isStructuralNumber(num: ExtractedNumber, text: string): boolean {
    const context = text.slice(Math.max(0, num.index - 4), num.index + num.raw.length + 8);
    return (num.value === 50 && /MA50/.test(context)) || (num.value === 200 && /MA200/.test(context)) || (num.value === 5 && /近\s*5\s*日/.test(context));
}

function isAllowedNumber(value: number, p: ConcernPitchInputs): boolean {
    const allowed = [
        p.current_price,
        p.recommended_strike,
        p.discount_pct,
        p.coupon_low,
        p.coupon_high,
        Number(p.tenor_label.match(/\d+/)?.[0] ?? '3')
    ];
    if (typeof p.input.change_5d_pct === 'number') allowed.push(Math.abs(p.input.change_5d_pct));
    if (typeof p.input.change_ytd_pct === 'number') allowed.push(Math.abs(p.input.change_ytd_pct));
    if (typeof p.input.pct_from_52w_high === 'number') allowed.push(Math.abs(p.input.pct_from_52w_high));
    if (typeof p.input.days_to_earnings === 'number') allowed.push(p.input.days_to_earnings);
    if (typeof p.input.days_since_earnings === 'number') allowed.push(p.input.days_since_earnings);
    return allowed.some((allowedValue) => Math.abs(Math.abs(allowedValue) - Math.abs(value)) < 0.6);
}
