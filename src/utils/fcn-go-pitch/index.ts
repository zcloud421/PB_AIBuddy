import { getCompanyDescription } from '../../data/fmp-company-description';
import type { NarrativeInput, NarrativeOutput, NarrativeSourceQuality } from '../narrative-generator';
import { inferEarningsBeat, isHighIVString, parseCouponRange, parseTenorMonths } from './input-adapter';
import { callDeepSeekForPitch, buildPitchPrompt, type PitchInputs } from './llm-stitcher';
import { detectLitTags, hasMinimumTagsForPitch } from './tag-detector';
import { buildDeterministicPitch, buildHybridPitch, buildMinimalPitch, pickBridge } from './template';
import { validatePitch } from './validator';

const CLICKBAIT_PATTERNS = [
    /^Why .+\??$/i,
    /should (investors|you) /i,
    /(best|better) .+ to buy/i,
    /^.+\s+vs\.?\s+.+\?$/i,
    /^prediction:/i,
    /secret weapon/i,
    /skyrocket/i,
    /everyone is talking/i
];

export async function generateGoPitch(input: NarrativeInput): Promise<NarrativeOutput | null> {
    if (input.current_price === null || input.current_price <= 0 || input.recommended_strike <= 0) {
        return null;
    }

    const coupon = parseCouponRange(input.estimated_coupon_range);
    if (!coupon) return null;

    const tenorLabel = parseTenorMonths(input.tenor_days);
    const discount = Math.round(100 - (input.recommended_strike / input.current_price) * 100);
    const desc = await getCompanyDescription(input.symbol);
    const companyDesc = desc?.short_description ?? input.company_name ?? input.symbol;
    const recentNewsTitles = (input.news_items ?? [])
        .map((item) => item.title)
        .filter((title) => !isClickbait(title))
        .slice(0, 3);

    const litTags = detectLitTags({
        symbol: input.symbol,
        current_price: input.current_price,
        ma50: input.ma50,
        ma200: input.ma200,
        change_5d_pct: input.change_5d_pct,
        pct_from_52w_high: input.pct_from_52w_high,
        days_since_earnings: input.days_since_earnings,
        earnings_beat: inferEarningsBeat(input),
        composite_score: input.composite_score,
        sector: desc?.sector ?? null,
        industry: desc?.industry ?? null,
        is_high_iv: isHighIVString(input.iv_level)
    });

    const pitchInputs: PitchInputs = {
        symbol: input.symbol,
        company_short_desc: companyDesc,
        current_price: input.current_price,
        recommended_strike: input.recommended_strike,
        discount_pct: discount,
        coupon_low: coupon.low,
        coupon_high: coupon.high,
        tenor_label: tenorLabel,
        lit_tags: litTags,
        recent_news_titles: recentNewsTitles,
        change_5d_pct: input.change_5d_pct,
        pct_from_52w_high: input.pct_from_52w_high,
        days_since_earnings: input.days_since_earnings
    };

    if (coupon.low <= 0 || coupon.high <= 0 || discount <= 0 || discount > 60) {
        return wrapResult(buildMinimalPitch(pitchInputs), 'go_pitch_minimal');
    }

    if (!hasMinimumTagsForPitch(litTags)) {
        return wrapResult(buildMinimalPitch(pitchInputs), 'go_pitch_minimal');
    }

    const useLLM = process.env.ENABLE_GO_LLM_PITCH !== 'false';
    if (useLLM) {
        try {
            const llmOutput = await callDeepSeekForPitch(buildPitchPrompt(pitchInputs));
            if (llmOutput) {
                const bridge = pickBridge(input.symbol);
                const finalPitch = buildHybridPitch(llmOutput.why_sentence, pitchInputs, bridge);
                const validation = validatePitch(llmOutput, litTags, pitchInputs, finalPitch);
                if (validation.passed) {
                    return wrapResult(finalPitch, 'go_pitch_hybrid_validated');
                }

                console.log(
                    JSON.stringify({
                        tag: 'go_pitch_validation_failed',
                        symbol: input.symbol,
                        reasons: validation.reasons,
                        ts: new Date().toISOString()
                    })
                );
                console.log(
                    JSON.stringify({
                        tag: 'go_pitch_validation_debug',
                        symbol: input.symbol,
                        reasons: validation.reasons,
                        llm_text_length: llmOutput.why_sentence.length,
                        llm_text_preview: llmOutput.why_sentence.slice(0, 80),
                        used_tags: llmOutput.used_tags,
                        timing_signal: llmOutput.timing_signal,
                        lit_holding: litTags.holding,
                        lit_timing: litTags.timing,
                        ts: new Date().toISOString()
                    })
                );
            }
        } catch (error) {
            console.warn('[go_pitch] llm error', error);
        }
    }

    return wrapResult(buildDeterministicPitch(pitchInputs), 'go_pitch_template');
}

function isClickbait(title: string): boolean {
    return CLICKBAIT_PATTERNS.some((pattern) => pattern.test(title));
}

function wrapResult(whyNow: string, sourceQuality: NarrativeSourceQuality): NarrativeOutput {
    return {
        why_now: whyNow,
        risk_note: '',
        sentiment_score: 0.6,
        key_events: [],
        source_quality: sourceQuality
    };
}
