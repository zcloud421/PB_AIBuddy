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

    if (/guidance|guide|outlook|reaffirm|raise|上调|维持指引/i.test(headlines)) {
        holding.push('guidance_reaffirmed_or_raised');
    }

    if (/join|added to|included in|Nasdaq-100|S&P 500|纳入指数/i.test(headlines)) {
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
