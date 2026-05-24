import type { PitchInputs } from './llm-stitcher';

export function buildDeterministicPitch(p: PitchInputs): string {
    const whyNowParts = buildSpecificSignals(p);
    const tagSupp = buildTagSupplements(p);
    const combined: string[] = [];
    if (tagSupp.length > 0) combined.push(tagSupp[0]);
    combined.push(...whyNowParts.slice(0, 2));

    const whyNowText = combined.length > 0 ? `${combined.join('，')}。` : '';

    return `${p.company_short_desc}，${whyNowText}这只 FCN 让您以 $${p.recommended_strike}，较现价低 ${p.discount_pct}% 的水平承接 ${p.symbol}，年化票息 ${p.coupon_low}%-${p.coupon_high}%，期限 ${p.tenor_label}；若股价未跌破 $${p.recommended_strike}，您收取票息并赎回本金；若跌破，则以 $${p.recommended_strike} 持有该标的。`;
}

export function buildMinimalPitch(p: PitchInputs): string {
    const signals = buildSpecificSignals(p).slice(0, 2);
    const signalText = signals.length > 0 ? `${signals.join('，')}。` : '';

    return `${p.company_short_desc}。${signalText}这只 FCN 让您以 $${p.recommended_strike}，较现价低 ${p.discount_pct}% 的水平承接 ${p.symbol}，年化票息 ${p.coupon_low}%-${p.coupon_high}%，期限 ${p.tenor_label}；若股价未跌破 $${p.recommended_strike}，您收取票息并赎回本金；若跌破，则以 $${p.recommended_strike} 持有该标的。`;
}

function buildSpecificSignals(p: PitchInputs): string[] {
    const signals: string[] = [];

    if (typeof p.change_5d_pct === 'number' && Math.abs(p.change_5d_pct) >= 1) {
        const sign = p.change_5d_pct >= 0 ? '+' : '';
        signals.push(`近 5 日 ${sign}${p.change_5d_pct.toFixed(1)}%`);
    }

    if (typeof p.pct_from_52w_high === 'number') {
        const distance = Math.abs(p.pct_from_52w_high);
        if (distance < 5) {
            signals.push(`接近 52 周高点(距高 ${distance.toFixed(1)}%)`);
        } else if (distance <= 25) {
            signals.push(`距 52 周高点 ${distance.toFixed(1)}%`);
        }
    }

    if (typeof p.days_since_earnings === 'number' && p.days_since_earnings <= 14) {
        signals.push(`财报已于 ${p.days_since_earnings} 天前发布`);
    }

    const headline = p.recent_news_titles?.[0];
    if (headline && headline.length > 10) {
        signals.push(`近期消息：${truncateHeadline(headline, 50)}`);
    }

    return signals;
}

function buildTagSupplements(p: PitchInputs): string[] {
    const tags: string[] = [];
    if (p.lit_tags.holding.includes('guide_raise')) tags.push('财报指引强劲');
    if (p.lit_tags.holding.includes('super_cycle')) tags.push('所在行业上行周期');
    if (p.lit_tags.holding.includes('backlog')) tags.push('订单可见度较高');
    if (p.lit_tags.timing.includes('momentum_intact')) tags.push('技术形态稳健');
    return tags;
}

function truncateHeadline(headline: string, maxChars: number): string {
    if (headline.length <= maxChars) return headline;

    const sliced = headline.slice(0, maxChars);
    const wordBoundary = sliced.replace(/\s+\S*$/, '');
    return `${(wordBoundary.length >= 20 ? wordBoundary : sliced).trim()}...`;
}
