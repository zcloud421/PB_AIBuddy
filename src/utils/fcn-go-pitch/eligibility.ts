import type { PitchInputs } from './llm-stitcher';

export function hasSafePitchSpecificity(p: PitchInputs): boolean {
    const hasFinancials = typeof p.revenue_yoy_pct === 'number' || p.top_segment != null;
    const hasTagSignal = p.lit_tags.holding.length > 0 || p.lit_tags.timing.length > 0;
    const hasPriceSignal =
        typeof p.change_5d_pct === 'number' ||
        typeof p.pct_from_52w_high === 'number' ||
        typeof p.days_since_earnings === 'number';
    const hasSubstantiveNews = (p.recent_news_titles?.length ?? 0) > 0;
    return hasFinancials || hasTagSignal || hasPriceSignal || hasSubstantiveNews;
}
