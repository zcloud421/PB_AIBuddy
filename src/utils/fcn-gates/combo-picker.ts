import type { StrikeData } from '../../scoring-engine';

export const STANDARD_TARGET_COUPON_PCT = Number(process.env.FCN_TARGET_COUPON_PCT ?? '15');

export interface TargetCouponSelection {
    strike: StrikeData;
    achieved_coupon_pct: number;
    max_achievable_coupon_pct: number;
    target_coupon_pct: number;
    target_unreachable: boolean;
    buffer_pct: number;
}

export function calculateBufferPct(strike: number, currentPrice: number): number {
    if (!Number.isFinite(currentPrice) || currentPrice <= 0) return 0;
    return ((currentPrice - strike) / currentPrice) * 100;
}

export function calculateAnnualizedCouponPct(strike: StrikeData, tenorDays: number): number | null {
    if (strike.mid_price === null || tenorDays <= 0) return null;
    return (strike.mid_price / strike.strike) * (365 / tenorDays) * 100;
}

export function selectStrikeAtTargetCoupon(input: {
    strikes: StrikeData[];
    tenorDays: number;
    currentPrice: number;
    targetCouponPct?: number;
    minBufferPct?: number;
}): TargetCouponSelection | null {
    const targetCouponPct = input.targetCouponPct ?? STANDARD_TARGET_COUPON_PCT;
    const minBufferPct = input.minBufferPct ?? 10;
    const candidates = input.strikes
        .map((strike) => ({
            strike,
            coupon: calculateAnnualizedCouponPct(strike, input.tenorDays),
            buffer: calculateBufferPct(strike.strike, input.currentPrice)
        }))
        .filter((candidate) =>
            candidate.coupon !== null &&
            candidate.coupon > 0 &&
            candidate.buffer >= minBufferPct &&
            candidate.strike.open_interest >= 10
        );

    if (candidates.length === 0) return null;
    const maxAchievable = Math.max(...candidates.map((candidate) => candidate.coupon ?? 0));
    const targetMet = candidates.filter((candidate) => (candidate.coupon ?? 0) >= targetCouponPct);
    const best = (targetMet.length > 0 ? targetMet : candidates).sort((a, b) => {
        if (targetMet.length > 0) {
            const bufferDistance = b.buffer - a.buffer;
            if (bufferDistance !== 0) return bufferDistance;
            const couponDistance =
                Math.abs((a.coupon ?? 0) - targetCouponPct) - Math.abs((b.coupon ?? 0) - targetCouponPct);
            if (couponDistance !== 0) return couponDistance;
            return b.strike.open_interest - a.strike.open_interest;
        }

        const couponDistance = (b.coupon ?? 0) - (a.coupon ?? 0);
        if (couponDistance !== 0) return couponDistance;
        const bufferDistance = b.buffer - a.buffer;
        if (bufferDistance !== 0) return bufferDistance;
        return b.strike.open_interest - a.strike.open_interest;
    })[0];

    return {
        strike: best.strike,
        achieved_coupon_pct: Number((best.coupon ?? 0).toFixed(2)),
        max_achievable_coupon_pct: Number(maxAchievable.toFixed(2)),
        target_coupon_pct: targetCouponPct,
        target_unreachable: (best.coupon ?? 0) < targetCouponPct * 0.7,
        buffer_pct: Number(best.buffer.toFixed(2))
    };
}
