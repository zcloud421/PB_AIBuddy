import type { NarrativeInput } from './narrative-generator';
import { parseCouponRange } from './narrative-validator';

export type NarrativeMode = 'llm_validated' | 'template_only' | 'block';

export function classifyNarrativeMode(input: NarrativeInput): {
    mode: NarrativeMode;
    reasons: string[];
} {
    const reasons: string[] = [];
    const couponRange = parseCouponRange(input.estimated_coupon_range);

    const missingCore =
        input.current_price == null ||
        input.current_price <= 0 ||
        input.recommended_strike == null ||
        input.recommended_strike <= 0 ||
        couponRange.length < 2;

    if (missingCore) {
        reasons.push('core_fields_missing');
        return { mode: 'block', reasons };
    }

    const hasCatalyst =
        input.news_headlines.length > 0 ||
        input.days_since_earnings !== null && input.days_since_earnings !== undefined ||
        typeof input.change_5d_pct === 'number';

    if (!hasCatalyst) {
        reasons.push('no_catalyst_data');
        return { mode: 'template_only', reasons };
    }

    return { mode: 'llm_validated', reasons };
}
