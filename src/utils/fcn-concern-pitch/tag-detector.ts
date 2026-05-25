import type { NarrativeInput } from '../narrative-generator';

export type ConcernTag =
    | 'earnings_window_imminent'
    | 'iv_too_low'
    | 'composite_score_borderline'
    | 'guide_cut'
    | 'earnings_miss_recent'
    | 'breakdown_below_ma'
    | 'regulatory_overhang'
    | 'post_earnings_gap_down'
    | 'distribution_pattern'
    | 'failed_rebound'
    | 'single_name_news_overhang'
    | 'high_vol_event_risk'
    | 'liquidity_or_gap_risk'
    | 'relative_underperformance_5d_20d';

export interface ConcernTagsResult {
    caution_tags: ConcernTag[];
    avoid_tags: ConcernTag[];
    eligible_mode: 'CAUTION' | 'AVOID' | null;
}

const STRONG_AVOID_TAGS = new Set<ConcernTag>([
    'guide_cut',
    'earnings_miss_recent',
    'breakdown_below_ma',
    'regulatory_overhang',
    'post_earnings_gap_down'
]);

const HARD_BLOCK_FLAGS = new Set(['BROKEN_TREND', 'POST_EARNINGS_SHOCK', 'BEARISH_STRUCTURE', 'NO_APPROVED_TENOR', 'NO_APPROVED_STRIKE']);
const LIQUIDITY_FLAGS = new Set(['LOW_LIQUIDITY', 'MATERIAL_NEWS_SHOCK', 'MATERIAL_NEWS_OVERHANG']);
const NEGATIVE_NEWS_REGEX = /miss|lawsuit|probe|investigation|sanction|antitrust|cut|downgrade|recall|监管|诉讼|制裁|反垄断|下调|不及预期/i;

export function detectConcernTags(input: NarrativeInput): ConcernTagsResult {
    const caution = new Set<ConcernTag>();
    const avoid = new Set<ConcernTag>();
    const headlines = (input.news_headlines ?? []).join(' ');

    if (isWithin(input.days_to_earnings, 0, 7)) caution.add('earnings_window_imminent');
    if (/^low$/i.test(input.iv_level) || input.iv_level === '低') caution.add('iv_too_low');
    if (typeof input.composite_score === 'number' && input.composite_score >= 0.42 && input.composite_score <= 0.48) {
        caution.add('composite_score_borderline');
    }
    if (/下调|guide.*(cut|down)|lowers guidance/i.test(headlines)) avoid.add('guide_cut');
    if (input.has_recent_earnings && /miss|low(?:er)? than (?:expected|estimates)|不及预期|低于预期/i.test(headlines)) {
        avoid.add('earnings_miss_recent');
    }
    if (
        input.current_price !== null &&
        input.ma50 !== null &&
        input.ma200 !== null &&
        input.current_price < input.ma50 &&
        input.current_price < input.ma200
    ) {
        avoid.add('breakdown_below_ma');
    }
    if (/investigation|lawsuit|probe|sanction|antitrust|监管|诉讼|制裁|反垄断/i.test(headlines)) avoid.add('regulatory_overhang');
    if (isWithin(input.days_since_earnings, 0, 5) && typeof input.change_5d_pct === 'number' && input.change_5d_pct < -5) {
        avoid.add('post_earnings_gap_down');
    }
    if (input.current_price !== null && input.ma20 !== null && input.current_price < input.ma20 && (input.change_5d_pct ?? 0) < 0) {
        caution.add('distribution_pattern');
    }
    if (
        input.pct_from_52w_high !== null &&
        input.pct_from_52w_high < -15 &&
        (input.change_5d_pct ?? 0) < 0 &&
        (input.days_since_earnings ?? 999) > 30
    ) {
        caution.add('failed_rebound');
    }
    const newsItems = input.news_items ?? [];
    if (newsItems.length >= 3 && newsItems.every((item) => NEGATIVE_NEWS_REGEX.test(item.title))) {
        caution.add('single_name_news_overhang');
    }
    if ((/^high$/i.test(input.iv_level) || input.iv_level === '高') && isWithin(input.days_to_earnings, 0, 14)) {
        caution.add('high_vol_event_risk');
    }
    if (input.flags?.some((flag) => LIQUIDITY_FLAGS.has(flag.type))) {
        caution.add('liquidity_or_gap_risk');
    } else if (!input.flags) {
        console.log('[concern_pitch] tag_data_missing liquidity_or_gap_risk');
    }
    if ((input.change_5d_pct ?? 0) < -3 && (input.change_ytd_pct ?? 0) < 0) {
        caution.add('relative_underperformance_5d_20d');
    }

    const hardBlock = input.flags?.some((flag) => HARD_BLOCK_FLAGS.has(flag.type)) ?? false;
    const avoidTags = [...avoid];
    const cautionTags = [...caution];
    const strongAvoidCount = avoidTags.filter((tag) => STRONG_AVOID_TAGS.has(tag)).length;
    const eligibleMode = hardBlock || strongAvoidCount >= 2
        ? 'AVOID'
        : cautionTags.length > 0 || avoidTags.length > 0
          ? 'CAUTION'
          : null;

    return {
        caution_tags: cautionTags,
        avoid_tags: avoidTags,
        eligible_mode: eligibleMode
    };
}

function isWithin(value: number | null | undefined, min: number, max: number): boolean {
    return value !== null && value !== undefined && value >= min && value <= max;
}
