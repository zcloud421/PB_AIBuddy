import type { NarrativeInput } from './narrative-generator';
import { parseCouponRange } from './narrative-validator';

export function buildTemplateNarrative(input: NarrativeInput): {
    why_now: string;
    risk_note: string;
} {
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

    const whyNow = [
        `当前价 ${price},FCN 执行价 ${strike}${moneyness ? `(${moneyness} 进场价)` : ''}。`,
        fromHigh ? `${fromHigh},` : '',
        `高 IV 环境年化票息 ${couponRange}${protection ? `,下行保护约 ${protection}` : ''}。`
    ].filter(Boolean).join('');

    return {
        why_now: whyNow,
        risk_note: '若标的跌破执行价,客户须按执行价买入股票;高隐含波动率环境下敲入风险上升。'
    };
}
