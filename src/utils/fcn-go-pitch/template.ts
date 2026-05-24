import type { PitchInputs } from './llm-stitcher';

const HOLDING_PHRASES: Record<string, string> = {
    guide_raise: '近期财报指引强劲',
    super_cycle: '所在行业处于上行周期',
    backlog: '订单可见度较高'
};

const TIMING_PHRASES: Record<string, string> = {
    quality_pullback: '近期股价回调,提供较低承接水平',
    momentum_intact: '技术形态健康、趋势稳健'
};

export function buildDeterministicPitch(p: PitchInputs): string {
    const reasons = [
        ...p.lit_tags.holding.map((tag) => HOLDING_PHRASES[tag]).filter(Boolean),
        ...p.lit_tags.timing.map((tag) => TIMING_PHRASES[tag]).filter(Boolean)
    ].slice(0, 2);
    const reasonsText = reasons.length > 0 ? `,${reasons.join('、')}` : '';

    return `${p.company_short_desc}${reasonsText}。这只 FCN 让您以 $${p.recommended_strike}、较现价低 ${p.discount_pct}% 的水平承接 ${p.symbol},年化票息 ${p.coupon_low}%-${p.coupon_high}%、期限 ${p.tenor_label};若股价未跌破 $${p.recommended_strike},您收取票息并赎回本金;若跌破,则以 $${p.recommended_strike} 持有该标的。`;
}

export function buildMinimalPitch(p: PitchInputs): string {
    return `${p.company_short_desc}。这只 FCN 让您以 $${p.recommended_strike}、较现价低 ${p.discount_pct}% 的水平承接 ${p.symbol},年化票息 ${p.coupon_low}%-${p.coupon_high}%、期限 ${p.tenor_label};若股价未跌破 $${p.recommended_strike},您收取票息并赎回本金;若跌破,则以 $${p.recommended_strike} 持有该标的。`;
}
