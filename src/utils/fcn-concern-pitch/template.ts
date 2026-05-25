import type { NarrativeInput } from '../narrative-generator';
import type { ConcernTag } from './tag-detector';

export interface ConcernPitchInputs {
    symbol: string;
    company_short_desc: string;
    current_price: number;
    recommended_strike: number;
    discount_pct: number;
    coupon_low: number;
    coupon_high: number;
    tenor_label: string;
    caution_tags: ConcernTag[];
    avoid_tags: ConcernTag[];
    input: NarrativeInput;
}

const BRIDGES = ['在这个节奏下，', '对应当前结构，', '节奏上，'] as const;

export function pickBridge(symbol: string): string {
    const hash = Array.from(symbol).reduce((sum, char) => sum + char.charCodeAt(0), 0);
    return BRIDGES[hash % BRIDGES.length];
}

export function buildCautionPitch(concernSentence: string, p: ConcernPitchInputs): string {
    return `${normalizeSentence(concernSentence)}${pickBridge(p.symbol)}${buildTriggerSentence('CAUTION', dominantTags(p), p)}`;
}

export function buildCautionTemplate(p: ConcernPitchInputs): string {
    const sentence = `${p.company_short_desc}，${tagConcernPhrase(dominantTags(p))}，当前卖 put 条件不够友好，本期先以观察为主，避免在信号确认前急于卖 put。`;
    return buildCautionPitch(sentence, p);
}

export function buildAvoidPitch(p: ConcernPitchInputs): string {
    const tags = dominantTags(p);
    return `${p.company_short_desc}。当前不建议推进该 FCN 结构，本期先以保护承接纪律为主。主要原因是 ${tagConcernPhrase(tags)}；待 ${neutralizeCondition(tags)} 后再评估。`;
}

export function buildTriggerSentence(_mode: 'CAUTION' | 'AVOID', tags: ConcernTag[], _p: ConcernPitchInputs): string {
    if (tags.includes('earnings_window_imminent') || tags.includes('high_vol_event_risk')) {
        return '本期建议先以观察为主，待财报落地、事件风险消化、波动率回归常态后再评估承接节奏。';
    }
    if (tags.includes('breakdown_below_ma') || tags.includes('distribution_pattern') || tags.includes('failed_rebound')) {
        return '本期建议先以观察为主，待价格重新站回 MA50 / MA200、趋势企稳、动量条件改善后再评估承接。';
    }
    if (tags.includes('iv_too_low')) {
        return '本期建议先以观察为主，待 IV 回升、票息条件更具吸引力、风险收益比更平衡时再评估。';
    }
    if (tags.includes('regulatory_overhang') || tags.includes('single_name_news_overhang')) {
        return '本期建议先以观察为主，待相关事件明朗、风险落地、市场重新定价后再评估承接节奏。';
    }
    return '本期建议先以观察为主，待相关信号企稳、结构条件改善、承接节奏更清晰后再评估。';
}

export function buildAvoidEnding(tags: ConcernTag[], p: ConcernPitchInputs): string {
    return `待 ${neutralizeCondition(tags)} 后再评估 ${p.symbol} 的 FCN 结构。`;
}

function dominantTags(p: ConcernPitchInputs): ConcernTag[] {
    return [...p.avoid_tags, ...p.caution_tags];
}

function tagConcernPhrase(tags: ConcernTag[]): string {
    if (tags.includes('guide_cut')) return '财报指引下调，短期基本面信号转弱';
    if (tags.includes('earnings_miss_recent')) return '近期财报不及预期，市场仍在重新定价';
    if (tags.includes('post_earnings_gap_down')) return '财报后股价下行，事件风险尚未完全消化';
    if (tags.includes('breakdown_below_ma')) return '价格跌破关键均线，趋势结构转弱';
    if (tags.includes('regulatory_overhang')) return '监管或法律事件仍未明朗';
    if (tags.includes('earnings_window_imminent')) return '财报窗口临近，短期事件风险偏高';
    if (tags.includes('iv_too_low')) return '当前 IV 偏低，票息补偿不够充分';
    if (tags.includes('composite_score_borderline')) return '综合评分处于边界，结构优势不够清晰';
    if (tags.includes('distribution_pattern')) return '短期价格低于 MA20 且动量转弱';
    if (tags.includes('failed_rebound')) return '反弹延续性不足，趋势确认度偏低';
    if (tags.includes('single_name_news_overhang')) return '个股负面新闻密集，短期不确定性偏高';
    if (tags.includes('relative_underperformance_5d_20d')) return '短期和年内表现均偏弱，承接节奏需放慢';
    return '结构条件尚未充分改善';
}

function neutralizeCondition(tags: ConcernTag[]): string {
    if (tags.includes('guide_cut') || tags.includes('earnings_miss_recent') || tags.includes('post_earnings_gap_down')) return '财报影响消化、后续指引重新稳定';
    if (tags.includes('breakdown_below_ma') || tags.includes('distribution_pattern') || tags.includes('failed_rebound')) return '价格重新站回关键均线、趋势企稳';
    if (tags.includes('regulatory_overhang') || tags.includes('single_name_news_overhang')) return '相关事件明朗、新闻压力缓和';
    if (tags.includes('iv_too_low')) return 'IV 回升、票息补偿改善';
    if (tags.includes('earnings_window_imminent') || tags.includes('high_vol_event_risk')) return '财报落地、事件风险消化';
    return '风险信号缓和、结构条件改善';
}

function normalizeSentence(text: string): string {
    const trimmed = text.trim();
    if (!trimmed) return '';
    return /[。！？.!?]$/.test(trimmed) ? trimmed : `${trimmed}。`;
}
