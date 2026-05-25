import { getCompanyDescription, getDisplayDescription } from '../../data/company-description';
import type { NarrativeInput, NarrativeOutput, NarrativeSourceQuality } from '../narrative-generator';
import { checkRepetitionStyle, logStyleRepetitionWarning } from '../fcn-shared/style-repetition';
import { PITCH_ENGINE_VERSION } from '../fcn-shared/pitch-engine-version';
import { parseCouponRange, parseTenorMonths } from '../fcn-go-pitch/input-adapter';
import { detectConcernTags } from './tag-detector';
import { buildConcernPrompt, callDeepSeekForConcern } from './llm-stitcher';
import { buildAvoidPitch, buildCautionPitch, buildCautionTemplate, type ConcernPitchInputs } from './template';
import { validateConcernPitch } from './validator';

export async function generateConcernPitch(input: NarrativeInput): Promise<NarrativeOutput | null> {
    const built = await buildConcernPitchInputsFromNarrativeInput(input);
    if (!built || !built.mode) return null;
    const { p, mode } = built;

    if (mode === 'AVOID') {
        const text = buildAvoidPitch(p);
        const validation = validateConcernPitch('AVOID', text, p);
        if (!validation.passed) logValidationFailure(input.symbol, 'AVOID', validation.reasons, text);
        logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
        return wrapResult(text, 'avoid_pitch_deterministic');
    }

    const useLLM = process.env.ENABLE_CONCERN_LLM_PITCH !== 'false';
    if (useLLM) {
        try {
            const llm = await callDeepSeekForConcern(buildConcernPrompt(p));
            if (llm) {
                const text = buildCautionPitch(llm.concern_sentence, p);
                const validation = validateConcernPitch('CAUTION', text, p, llm);
                if (validation.passed) {
                    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
                    return wrapResult(text, 'caution_pitch_hybrid_validated');
                }
                logValidationFailure(input.symbol, 'CAUTION', validation.reasons, text);
            }
        } catch (error) {
            console.warn('[concern_pitch] llm error', error);
        }
    }

    const text = buildCautionTemplate(p);
    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
    return wrapResult(text, 'caution_pitch_template');
}

export async function buildConcernPitchFailClosed(input: NarrativeInput): Promise<NarrativeOutput | null> {
    const built = await buildConcernPitchInputsFromNarrativeInput(input, true);
    if (!built) return null;
    const { p, mode } = built;
    const text = mode === 'AVOID' ? buildAvoidPitch(p) : buildCautionTemplate(p);
    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
    return wrapResult(text, mode === 'AVOID' ? 'avoid_pitch_deterministic' : 'caution_pitch_template');
}

async function buildConcernPitchInputsFromNarrativeInput(
    input: NarrativeInput,
    allowEmptyTags = false
): Promise<{ p: ConcernPitchInputs; mode: 'CAUTION' | 'AVOID' | null } | null> {
    if (input.current_price === null || input.current_price <= 0 || input.recommended_strike <= 0) return null;
    const coupon = parseCouponRange(input.estimated_coupon_range);
    if (!coupon || coupon.low <= 0 || coupon.high <= 0) return null;
    const discount = Math.round(100 - (input.recommended_strike / input.current_price) * 100);
    if (discount <= 0 || discount > 60) return null;

    const tags = detectConcernTags(input);
    let mode = tags.eligible_mode ?? (allowEmptyTags
        ? input.grade === 'AVOID'
            ? 'AVOID'
            : 'CAUTION'
        : null);
    if (!mode) return null;
    if (input.grade === 'CAUTION' && mode === 'AVOID') mode = 'CAUTION';
    if (input.grade === 'AVOID' && mode === 'CAUTION') mode = 'AVOID';

    const desc = await getCompanyDescription(input.symbol);
    const displayDescription = await getDisplayDescription(input.symbol, input.company_name);
    return {
        mode,
        p: {
            symbol: input.symbol,
            company_short_desc: desc?.short_description ?? input.company_name ?? input.symbol,
            display_description: displayDescription,
            current_price: input.current_price,
            recommended_strike: input.recommended_strike,
            discount_pct: discount,
            coupon_low: coupon.low,
            coupon_high: coupon.high,
            tenor_label: parseTenorMonths(input.tenor_days),
            caution_tags: tags.caution_tags,
            avoid_tags: tags.avoid_tags,
            input
        }
    };
}

function logValidationFailure(symbol: string, mode: string, reasons: string[], text: string): void {
    console.log(JSON.stringify({
        tag: 'concern_pitch_validation_failed',
        symbol,
        mode,
        reasons,
        text_preview: text.slice(0, 120),
        ts: new Date().toISOString()
    }));
}

function wrapResult(whyNow: string, sourceQuality: NarrativeSourceQuality): NarrativeOutput {
    return {
        why_now: whyNow,
        risk_note: '',
        sentiment_score: 0.45,
        key_events: [],
        source_quality: sourceQuality,
        engine_version: PITCH_ENGINE_VERSION
    };
}
