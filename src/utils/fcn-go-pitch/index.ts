import { getCompanyDescription, getDisplayDescription } from '../../data/company-description';
import { getLatestEarningsSurprise } from '../../data/earnings-surprise';
import type { NarrativeInput, NarrativeOutput, NarrativeSourceQuality } from '../narrative-generator';
import { checkRepetitionStyle, logStyleRepetitionWarning } from '../fcn-shared/style-repetition';
import { PITCH_ENGINE_VERSION } from '../fcn-shared/pitch-engine-version';
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
    const pitchInputs = await buildPitchInputsFromNarrativeInput(input);
    if (!pitchInputs) return null;

    if (
        pitchInputs.coupon_low <= 0 ||
        pitchInputs.coupon_high <= 0 ||
        pitchInputs.discount_pct <= 0 ||
        pitchInputs.discount_pct > 60
    ) {
        const text = buildMinimalPitch(pitchInputs);
        logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
        return wrapResult(text, 'go_pitch_minimal');
    }

    if (!hasMinimumTagsForPitch(pitchInputs.lit_tags)) {
        const text = buildMinimalPitch(pitchInputs);
        logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
        return wrapResult(text, 'go_pitch_minimal');
    }

    const useLLM = process.env.ENABLE_GO_LLM_PITCH !== 'false';
    if (useLLM) {
        try {
            const llmOutput = await callDeepSeekForPitch(buildPitchPrompt(pitchInputs));
            if (llmOutput) {
                const bridge = pickBridge(input.symbol);
                const finalPitch = buildHybridPitch(llmOutput.why_sentence, pitchInputs, bridge);
                const validation = validatePitch(llmOutput, pitchInputs.lit_tags, pitchInputs, finalPitch);
                if (validation.passed) {
                    logStyleRepetitionWarning(input.symbol, finalPitch, checkRepetitionStyle(finalPitch));
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
                        lit_holding: pitchInputs.lit_tags.holding,
                        lit_timing: pitchInputs.lit_tags.timing,
                        ts: new Date().toISOString()
                    })
                );
            }
        } catch (error) {
            console.warn('[go_pitch] llm error', error);
        }
    }

    const text = buildDeterministicPitch(pitchInputs);
    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
    return wrapResult(text, 'go_pitch_template');
}

export async function buildGoPitchFailClosed(input: NarrativeInput): Promise<NarrativeOutput | null> {
    const pitchInputs = await buildPitchInputsFromNarrativeInput(input);
    if (!pitchInputs) return null;
    const text = buildMinimalPitch(pitchInputs);
    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
    return wrapResult(text, 'go_pitch_minimal');
}

async function buildPitchInputsFromNarrativeInput(input: NarrativeInput): Promise<PitchInputs | null> {
    if (input.current_price === null || input.current_price <= 0 || input.recommended_strike <= 0) {
        return null;
    }

    const coupon = parseCouponRange(input.estimated_coupon_range);
    if (!coupon) return null;

    const discount = Math.round(100 - (input.recommended_strike / input.current_price) * 100);
    const desc = await getCompanyDescription(input.symbol);
    const displayDescription = await getDisplayDescription(input.symbol, input.company_name);
    const earningsSurprise = await getLatestEarningsSurprise(input.symbol);
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
        is_high_iv: isHighIVString(input.iv_level),
        news_headlines: input.news_headlines ?? [],
        earnings_surprise: earningsSurprise
    });
    const hasEarningsBeatTag =
        litTags.holding.includes('earnings_strong_beat') || litTags.holding.includes('earnings_modest_beat');

    return {
        symbol: input.symbol,
        company_short_desc: desc?.short_description ?? input.company_name ?? input.symbol,
        display_description: displayDescription,
        current_price: input.current_price,
        recommended_strike: input.recommended_strike,
        discount_pct: discount,
        coupon_low: coupon.low,
        coupon_high: coupon.high,
        tenor_label: parseTenorMonths(input.tenor_days),
        lit_tags: litTags,
        recent_news_titles: (input.news_items ?? [])
            .map((item) => item.title)
            .filter((title) => !isClickbait(title))
            .slice(0, 3),
        change_5d_pct: input.change_5d_pct,
        pct_from_52w_high: input.pct_from_52w_high,
        days_since_earnings: input.days_since_earnings,
        earnings_surprise: hasEarningsBeatTag && earningsSurprise
            ? {
                  eps_surprise_pct: earningsSurprise.eps_surprise_pct,
                  period: earningsSurprise.period
              }
            : null
    };
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
        source_quality: sourceQuality,
        engine_version: PITCH_ENGINE_VERSION
    };
}
