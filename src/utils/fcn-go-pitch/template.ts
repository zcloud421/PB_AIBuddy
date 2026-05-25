import type { PitchInputs } from './llm-stitcher';
import { isSafeTemplateHeadline } from './headline-filter';

const BRIDGES = ['在这个背景下，', '对应到结构上，', '条款上，'] as const;

export function buildDealStructureSentence(p: PitchInputs): string {
    return `让您以 $${p.recommended_strike}（较现价低 ${p.discount_pct}%）承接 ${p.symbol}，年化票息 ${p.coupon_low}%-${p.coupon_high}%，期限 ${p.tenor_label}；若股价未跌破 $${p.recommended_strike}，您收取票息并赎回本金，若跌破则以 $${p.recommended_strike} 持有该标的。`;
}

export function buildHybridPitch(whySentence: string, p: PitchInputs, bridge = pickBridge(p.symbol)): string {
    return `${normalizeSentence(p.display_description)}${normalizeSentence(whySentence)}${bridge}${buildDealStructureSentence(p)}`;
}

export function buildDeterministicPitch(p: PitchInputs): string {
    return buildHybridPitch(buildTemplateWhySentence(p, true), p);
}

export function buildMinimalPitch(p: PitchInputs): string {
    return buildHybridPitch(buildTemplateWhySentence(p, false), p);
}

export function pickBridge(symbol: string): string {
    const hash = Array.from(symbol).reduce((sum, char) => sum + char.charCodeAt(0), 0);
    return BRIDGES[hash % BRIDGES.length];
}

function buildTemplateWhySentence(p: PitchInputs, includeTags: boolean): string {
    const parts: string[] = [];
    if (includeTags) {
        parts.push(...buildTagSupplements(p).slice(0, 1));
    }
    parts.push(...buildSpecificSignals(p).slice(0, 2));
    return parts.length > 0
        ? `${parts.join('，')}。`
        : `承接节奏以条款纪律为主。`;
}

function buildSpecificSignals(p: PitchInputs): string[] {
    const signals: string[] = [];

    if (p.earnings_surprise) {
        signals.push(`${p.earnings_surprise.period} EPS 超预期 ${p.earnings_surprise.eps_surprise_pct.toFixed(1)}%`);
    }

    if (typeof p.change_5d_pct === 'number' && Math.abs(p.change_5d_pct) >= 1) {
        const sign = p.change_5d_pct >= 0 ? '+' : '';
        signals.push(`近 5 日 ${sign}${p.change_5d_pct.toFixed(1)}%`);
    }

    if (typeof p.pct_from_52w_high === 'number') {
        const distance = Math.abs(p.pct_from_52w_high);
        if (distance < 5) {
            signals.push(`接近 52 周高点（距高 ${distance.toFixed(1)}%）`);
        } else if (distance <= 25) {
            signals.push(`距 52 周高点 ${distance.toFixed(1)}%`);
        }
    }

    if (typeof p.days_since_earnings === 'number' && p.days_since_earnings <= 14) {
        signals.push(`财报已于 ${p.days_since_earnings} 天前发布`);
    }

    const headline = p.recent_news_titles?.find((title) => isSafeTemplateHeadline(title));
    if (headline && headline.length > 10) {
        signals.push(`近期消息：${truncateHeadline(headline, 50)}`);
    }

    return signals;
}

function buildTagSupplements(p: PitchInputs): string[] {
    const tags: string[] = [];
    if (p.lit_tags.holding.includes('index_inclusion')) tags.push('近期指数纳入提升关注度');
    if (p.lit_tags.holding.includes('guidance_reaffirmed_or_raised')) tags.push('近期指引维持或上调');
    if (p.lit_tags.holding.includes('earnings_strong_beat')) tags.push('最近一期 EPS 明显超预期');
    if (p.lit_tags.holding.includes('earnings_modest_beat')) tags.push('最近一期 EPS 小幅超预期');
    if (p.lit_tags.holding.includes('infrastructure_capacity_cycle')) tags.push('数据中心基础设施扩容周期延续');
    if (p.lit_tags.holding.includes('guide_raise')) tags.push('财报指引强劲');
    if (p.lit_tags.holding.includes('super_cycle')) tags.push('所在行业处于上行周期');
    if (p.lit_tags.holding.includes('backlog')) tags.push('订单积压提供能见度');
    if (p.lit_tags.timing.includes('momentum_intact')) tags.push('趋势仍保持在关键均线上方');
    return tags;
}

function normalizeSentence(text: string): string {
    const trimmed = text.trim();
    if (!trimmed) return '';
    return /[。！？.!?]$/.test(trimmed) ? trimmed : `${trimmed}。`;
}

function truncateHeadline(headline: string, maxChars: number): string {
    if (headline.length <= maxChars) return headline;

    const sliced = headline.slice(0, maxChars);
    const wordBoundary = sliced.replace(/\s+\S*$/, '');
    return `${(wordBoundary.length >= 20 ? wordBoundary : sliced).trim()}...`;
}
