import type { NarrativeInput } from './narrative-generator';
import { parseCouponRange } from './narrative-validator';

export interface TemplateDataTier {
    hasPrice: boolean;
    hasStrike: boolean;
    hasCoupon: boolean;
    hasNews: boolean;
    hasEarnings: boolean;
}

export function classifyTemplateTier(input: NarrativeInput): TemplateDataTier {
    const couponValues = parseCouponRange(input.estimated_coupon_range);
    return {
        hasPrice: typeof input.current_price === 'number' && input.current_price > 0,
        hasStrike: typeof input.recommended_strike === 'number' && input.recommended_strike > 0,
        hasCoupon: couponValues.length >= 2,
        hasNews: (input.news_items?.length ?? 0) > 0,
        hasEarnings:
            input.days_since_earnings !== null &&
            input.days_since_earnings !== undefined &&
            input.days_since_earnings <= 14
    };
}

export function buildTemplateNarrative(input: NarrativeInput): {
    why_now: string;
    risk_note: string;
} {
    const tier = classifyTemplateTier(input);
    const price = input.current_price ? `$${input.current_price.toFixed(2)}` : '现价';
    const strike = input.recommended_strike ? `$${input.recommended_strike.toFixed(0)}` : '执行价待定';
    const moneyness = input.current_price && input.recommended_strike
        ? `${Math.round((input.recommended_strike / input.current_price) * 100)}%`
        : '';
    const protection = input.current_price && input.recommended_strike
        ? `${Math.round(100 - (input.recommended_strike / input.current_price) * 100)}%`
        : '';
    const couponValues = parseCouponRange(input.estimated_coupon_range);
    const couponRange = couponValues.length >= 2
        ? `${couponValues[0]}%-${couponValues[1]}%`
        : input.estimated_coupon_range || '高位';
    const fromHigh = typeof input.pct_from_52w_high === 'number'
        ? `距 52 周高点 ${Math.abs(input.pct_from_52w_high).toFixed(1)}%`
        : '';

    let whyNow: string;
    if (tier.hasPrice && tier.hasStrike && tier.hasCoupon && tier.hasNews) {
        const latestHeadline = input.news_items?.[0]?.title ?? '';
        const catalystLine = latestHeadline ? `近期新闻提及:${truncate(latestHeadline, 40)}。` : '';
        whyNow = `${catalystLine}当前价 ${price},FCN 执行价 ${strike}(${moneyness} 进场价)${fromHigh ? `,${fromHigh}` : ''}。高 IV 环境年化票息 ${couponRange}${protection ? `,下行保护约 ${protection}` : ''}。`;
    } else if (tier.hasPrice && tier.hasStrike && tier.hasCoupon && tier.hasEarnings) {
        whyNow = `近期处于财报窗口期。当前价 ${price},FCN 执行价 ${strike}(${moneyness} 进场价)${fromHigh ? `,${fromHigh}` : ''}。高 IV 环境年化票息 ${couponRange}${protection ? `,下行保护约 ${protection}` : ''}。`;
    } else if (tier.hasPrice && tier.hasStrike && tier.hasCoupon) {
        whyNow = `当前价 ${price},FCN 执行价 ${strike}(${moneyness} 进场价)${fromHigh ? `,${fromHigh}` : ''}。高 IV 环境年化票息 ${couponRange}${protection ? `,下行保护约 ${protection}` : ''}。`;
    } else {
        whyNow = `当前价 ${price},FCN 执行价 ${strike}${moneyness ? `(${moneyness} 进场价)` : ''}。缺少近期催化,仅作结构性参考。`;
    }

    return {
        why_now: whyNow,
        risk_note: '若标的跌破执行价,客户须按执行价买入股票;高隐含波动率环境下敲入风险上升。'
    };
}

function truncate(value: string, maxLength: number): string {
    return value.length <= maxLength ? value : `${value.slice(0, maxLength - 1)}…`;
}
