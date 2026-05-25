export type HoldingTag =
    | 'guide_raise'
    | 'super_cycle'
    | 'backlog'
    | 'earnings_strong_beat'
    | 'earnings_modest_beat'
    | 'guidance_reaffirmed_or_raised'
    | 'index_inclusion'
    | 'infrastructure_capacity_cycle';
export type TimingTag = 'quality_pullback' | 'momentum_intact';

export interface LitTags {
    holding: HoldingTag[];
    timing: TimingTag[];
}

const BACKLOG_TICKERS = new Set(['NVDA', 'TSM', 'AVGO', 'VRT', 'ANET', 'CIEN', 'LITE', 'CRWV', 'NBIS']);
const INFRASTRUCTURE_CAPACITY_TICKERS = new Set(['VRT', 'ETN', 'PWR', 'ANET', 'CIEN', 'LITE']);

export function detectLitTags(input: {
    symbol: string;
    current_price: number | null;
    ma50: number | null;
    ma200: number | null;
    change_5d_pct: number | null | undefined;
    pct_from_52w_high: number | null;
    days_since_earnings: number | null | undefined;
    earnings_beat: boolean | null;
    composite_score?: number;
    sector: string | null;
    industry: string | null;
    is_high_iv: boolean;
    news_headlines?: string[];
    earnings_surprise?: {
        eps_actual: number;
        eps_estimate: number;
        eps_surprise_pct: number;
        revenue_surprise_pct?: number;
    } | null;
}): LitTags {
    const holding: HoldingTag[] = [];
    const timing: TimingTag[] = [];
    const headlines = (input.news_headlines ?? []).join(' ');

    if (input.days_since_earnings !== null && input.days_since_earnings !== undefined && input.days_since_earnings <= 14 && input.earnings_beat === true) {
        holding.push('guide_raise');
    }

    const earningsBeatTag = classifyEarningsBeatTag(input.earnings_surprise ?? null);
    if (earningsBeatTag) holding.push(earningsBeatTag);

    if (matchesAnyHeadline(input.news_headlines, isGuidanceReaffirmedOrRaisedHeadline)) {
        holding.push('guidance_reaffirmed_or_raised');
    }

    if (matchesAnyHeadline(input.news_headlines, isIndexInclusionHeadline)) {
        holding.push('index_inclusion');
    }

    if (INFRASTRUCTURE_CAPACITY_TICKERS.has(input.symbol.toUpperCase())) {
        holding.push('infrastructure_capacity_cycle');
    } else if (isSuperCycle(input.sector, input.industry)) {
        holding.push('super_cycle');
    }

    if (BACKLOG_TICKERS.has(input.symbol.toUpperCase())) {
        holding.push('backlog');
    }

    if (
        input.pct_from_52w_high !== null &&
        input.pct_from_52w_high <= -10 &&
        input.pct_from_52w_high >= -25 &&
        input.composite_score !== undefined &&
        input.composite_score >= 0.8
    ) {
        timing.push('quality_pullback');
    }

    if (
        input.current_price !== null &&
        input.ma50 !== null &&
        input.ma200 !== null &&
        input.current_price > input.ma50 &&
        input.ma50 > input.ma200 &&
        input.change_5d_pct !== null &&
        input.change_5d_pct !== undefined &&
        input.change_5d_pct > 0
    ) {
        timing.push('momentum_intact');
    } else if (
        input.current_price !== null &&
        input.ma200 !== null &&
        input.current_price > input.ma200 &&
        input.pct_from_52w_high !== null &&
        input.pct_from_52w_high > -10
    ) {
        timing.push('momentum_intact');
    }

    return { holding, timing };
}

function matchesAnyHeadline(headlines: string[] | undefined, predicate: (headline: string) => boolean): boolean {
    return (headlines ?? []).some((headline) => predicate(headline));
}

function isGuidanceReaffirmedOrRaisedHeadline(headline: string): boolean {
    const normalized = headline.replace(/\s+/g, ' ').trim();
    return (
        /\b(raises?|raised|reaffirms?|reaffirmed|boosts?|boosted)\s+(?:full[\s-]?year\s+)?(?:guidance|outlook|forecast|target)s?\b/i.test(normalized) ||
        /(上调|维持|重申|提升)\s*(?:全年\s*)?(指引|预期|展望|目标)/i.test(normalized)
    );
}

function isIndexInclusionHeadline(headline: string): boolean {
    const normalized = headline.replace(/\s+/g, ' ').trim();
    if (/\b(?:underweight|removed from|dropped from|exit|exits|deleted from)\b|出场|剔除/i.test(normalized)) {
        return false;
    }
    return (
        /\b(?:joined|will join|joins|added to|to be added to|inclusion in|set to enter)\s+(?:the\s+)?(?:S&P\s*500|Nasdaq[\s-]?100|Russell\s*1000|Russell\s*2000|Dow\s*Jones)\b/i.test(normalized) ||
        /(纳入|加入|入选)\s*(标普\s*500|纳斯达克\s*100|道琼斯|罗素\s*1000|罗素\s*2000)/i.test(normalized)
    );
}

function classifyEarningsBeatTag(
    surprise: {
        eps_actual: number;
        eps_estimate: number;
        eps_surprise_pct: number;
        revenue_surprise_pct?: number;
    } | null
): 'earnings_strong_beat' | 'earnings_modest_beat' | null {
    if (!surprise || surprise.eps_actual <= surprise.eps_estimate || surprise.eps_surprise_pct < 2) return null;

    let tag: 'earnings_strong_beat' | 'earnings_modest_beat' | null =
        surprise.eps_surprise_pct >= 5 ? 'earnings_strong_beat' : 'earnings_modest_beat';

    if (typeof surprise.revenue_surprise_pct === 'number' && surprise.revenue_surprise_pct < -3) {
        tag = tag === 'earnings_strong_beat' ? 'earnings_modest_beat' : null;
    }

    return tag;
}

export function hasMinimumTagsForPitch(lit: LitTags): boolean {
    return lit.holding.length >= 1 && lit.timing.length >= 1;
}

function isSuperCycle(sector: string | null, industry: string | null): boolean {
    const s = (sector ?? '').toLowerCase();
    const i = (industry ?? '').toLowerCase();
    if (s.includes('energy') && (i.includes('oil') || i.includes('gas'))) return true;
    if (!s.includes('technology')) return false;
    return (
        i.includes('semiconductor') ||
        i.includes('communication equipment') ||
        i.includes('computer hardware') ||
        i.includes('information technology services') ||
        i.includes('software')
    );
}
