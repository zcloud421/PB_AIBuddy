import type { Flag, NewsItem } from '../types/api';
import type { NarrativeInput } from './narrative-generator';

export interface NarrativeInputBuilderArgs {
    symbol: string;
    companyName?: string | null;
    theme: string;
    grade: string;
    compositeScore?: number | null;
    recommendedStrike: number;
    estimatedCouponRange: string;
    currentPrice: number | null;
    change1dPct?: number | null;
    change5dPct?: number | null;
    changeYtdPct?: number | null;
    pctFrom52wHigh: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    impliedVolatility: number | null;
    flags: Flag[];
    tenorDays: number;
    newsItems: NewsItem[];
    hasRecentEarnings: boolean;
    earningsWeight: number;
    daysToEarnings: number | null;
    daysSinceEarnings: number | null;
    extendedMovePct?: number | null;
    refreshReason?: string;
    activeAttributionRules?: Array<{
        id: string;
        reason_zh: string;
        driver_type: string;
        family: string;
    }>;
    chinaGoldReserveTrend?: NarrativeInput['china_gold_reserve_trend'];
    gldFlowTrend?: NarrativeInput['gld_flow_trend'];
    breakevenInflationTrend?: NarrativeInput['breakeven_inflation_trend'];
}

export function buildNarrativeInput(args: NarrativeInputBuilderArgs): NarrativeInput {
    return {
        symbol: args.symbol,
        company_name: args.companyName,
        theme: args.theme,
        grade: args.grade,
        composite_score: args.compositeScore ?? undefined,
        recommended_strike: args.recommendedStrike,
        estimated_coupon_range: args.estimatedCouponRange,
        current_price: args.currentPrice,
        change_1d_pct: args.change1dPct ?? null,
        change_5d_pct: args.change5dPct ?? null,
        change_ytd_pct: args.changeYtdPct ?? null,
        pct_from_52w_high: args.pctFrom52wHigh,
        ma20: args.ma20,
        ma50: args.ma50,
        ma200: args.ma200,
        iv_level:
            args.impliedVolatility !== null
                ? args.impliedVolatility >= 0.6
                    ? '高'
                    : args.impliedVolatility >= 0.3
                      ? '中'
                      : '低'
                : '中',
        flags: args.flags,
        tenor_days: args.tenorDays,
        news_headlines: args.newsItems.map((item) => item.title),
        news_items: args.newsItems,
        has_recent_earnings: args.hasRecentEarnings,
        earnings_weight: args.earningsWeight,
        days_to_earnings: args.daysToEarnings,
        days_since_earnings: args.daysSinceEarnings,
        active_attribution_rules: args.activeAttributionRules?.slice(0, 3) ?? [],
        refresh_reason: args.refreshReason ?? 'first_gen',
        china_gold_reserve_trend: args.chinaGoldReserveTrend ?? null,
        gld_flow_trend: args.gldFlowTrend ?? null,
        breakeven_inflation_trend: args.breakevenInflationTrend ?? null
    };
}
