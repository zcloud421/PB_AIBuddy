import { getCompanyDescription, getDisplayDescription } from '../../data/company-description';
import { getLatestEarningsSurprise } from '../../data/earnings-surprise';
import type { NarrativeInput, NarrativeOutput, NarrativeSourceQuality } from '../narrative-generator';
import { checkRepetitionStyle, logStyleRepetitionWarning } from '../fcn-shared/style-repetition';
import { PITCH_ENGINE_VERSION } from '../fcn-shared/pitch-engine-version';
import {
    inferEarningsBeat,
    isHighIVString,
    loadFinancialsForPitch,
    parseCouponRange,
    parseTenorMonths,
    sanitizeEarningsSurpriseForPitch
} from './input-adapter';
import { callDeepSeekForPitch, buildPitchPrompt, type PitchInputs } from './llm-stitcher';
import { detectLitTags, hasMinimumTagsForPitch } from './tag-detector';
import { buildDeterministicPitch, buildHybridPitch, buildMinimalPitch, pickBridge } from './template';
import { isClickbait } from './headline-filter';
import { validateGeneratedPitchText, validatePitch } from './validator';

export async function generateGoPitch(input: NarrativeInput): Promise<NarrativeOutput | null> {
    const pitchInputs = await buildPitchInputsFromNarrativeInput(input);
    if (!pitchInputs) return null;

    if (
        pitchInputs.coupon_low <= 0 ||
        pitchInputs.coupon_high <= 0 ||
        pitchInputs.discount_pct <= 0 ||
        pitchInputs.discount_pct > 60
    ) {
        const text = finalizeTemplateText(input.symbol, buildMinimalPitch(pitchInputs), pitchInputs, 'go_pitch_minimal');
        logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
        return wrapResult(text, 'go_pitch_minimal');
    }

    // GO 名单是要直接发给 UHNW 客户的:只要有 ≥1 个 holding tag,且 timing tag 或真实
    // 财务数据(收入/分部)二者有其一,就走 LLM 写完整 3 段 thesis。timing tag 短期熄灭
    // (如单周回调)不应把 GO pitch 降级成一句话。holding tag 全无才降级(validator
    // 要求 used_tags 至少 1 个 holding tag,没有则 LLM 必失败)。
    const hasSubstantiveFinancials =
        typeof pitchInputs.revenue_yoy_pct === 'number' || pitchInputs.top_segment != null;
    // 有真实收入/分部数据时,即使 holding tag 全灭也放行 LLM(validator 对 holding tag
    // 的要求同步做了条件化);tags 和财务数据都没有才落一句话兜底。
    const canAttemptLLM = hasMinimumTagsForPitch(pitchInputs.lit_tags) || hasSubstantiveFinancials;
    if (!canAttemptLLM) {
        console.log(JSON.stringify({
            tag: 'go_pitch_minimal_no_tags',
            symbol: input.symbol,
            lit_holding: pitchInputs.lit_tags.holding,
            lit_timing: pitchInputs.lit_tags.timing,
            has_financials: hasSubstantiveFinancials,
            ts: new Date().toISOString()
        }));
        const text = finalizeTemplateText(input.symbol, buildMinimalPitch(pitchInputs), pitchInputs, 'go_pitch_minimal');
        logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
        return wrapResult(text, 'go_pitch_minimal');
    }

    const useLLM = process.env.ENABLE_GO_LLM_PITCH !== 'false';
    if (useLLM) {
        try {
            const basePrompt = buildPitchPrompt(pitchInputs);
            let retryHint: string | null = null;
            for (let attempt = 0; attempt < 2; attempt += 1) {
                const prompt = retryHint
                    ? `${basePrompt}\n\n上一次输出被校验拒绝,原因:${retryHint}。请修正后重写(尤其:数字必须逐字来自可用数字事实,不得自行推算)。`
                    : basePrompt;
                const llmOutput = await callDeepSeekForPitch(prompt);
                if (!llmOutput) break;

                const bridge = pickBridge(input.symbol);
                const finalPitch = buildHybridPitch(llmOutput.comm_reference, pitchInputs, bridge);
                const validation = validatePitch(llmOutput, pitchInputs.lit_tags, pitchInputs, finalPitch);
                if (validation.passed) {
                    logStyleRepetitionWarning(input.symbol, finalPitch, checkRepetitionStyle(finalPitch));
                    return wrapResult(finalPitch, 'go_pitch_hybrid_validated');
                }

                console.log(
                    JSON.stringify({
                        tag: 'go_pitch_validation_failed',
                        symbol: input.symbol,
                        attempt,
                        reasons: validation.reasons,
                        ts: new Date().toISOString()
                    })
                );
                console.log(
                    JSON.stringify({
                        tag: 'go_pitch_validation_debug',
                        symbol: input.symbol,
                        attempt,
                        reasons: validation.reasons,
                        llm_text_length: llmOutput.comm_reference.length,
                        llm_text_preview: llmOutput.comm_reference.slice(0, 80),
                        used_tags: llmOutput.used_tags,
                        timing_signal: llmOutput.timing_signal,
                        lit_holding: pitchInputs.lit_tags.holding,
                        lit_timing: pitchInputs.lit_tags.timing,
                        ts: new Date().toISOString()
                    })
                );
                retryHint = validation.reasons.join('; ');
            }
        } catch (error) {
            console.warn('[go_pitch] llm error', error);
        }
    }

    const text = finalizeTemplateText(input.symbol, buildDeterministicPitch(pitchInputs), pitchInputs, 'go_pitch_template');
    logStyleRepetitionWarning(input.symbol, text, checkRepetitionStyle(text));
    return wrapResult(text, 'go_pitch_template');
}

export async function buildGoPitchFailClosed(input: NarrativeInput): Promise<NarrativeOutput | null> {
    const pitchInputs = await buildPitchInputsFromNarrativeInput(input);
    if (!pitchInputs) return null;
    const text = finalizeTemplateText(input.symbol, buildMinimalPitch(pitchInputs), pitchInputs, 'go_pitch_minimal');
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
    const rawEarningsSurprise = await getLatestEarningsSurprise(input.symbol);
    const earningsSurprise = sanitizeEarningsSurpriseForPitch(rawEarningsSurprise);
    const financials = await loadFinancialsForPitch(input.symbol);
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
        recent_news_titles: selectSubstantiveNewsTitles((input.news_items ?? []).map((item) => item.title)),
        change_5d_pct: input.change_5d_pct,
        pct_from_52w_high: input.pct_from_52w_high,
        days_since_earnings: input.days_since_earnings,
        earnings_surprise: hasEarningsBeatTag && earningsSurprise
            ? {
                  eps_surprise_pct: earningsSurprise.eps_surprise_pct,
                  period: earningsSurprise.period
              }
            : null,
        financials_latest_quarter: financials?.latest_quarter,
        revenue_yoy_pct: financials?.revenue_yoy_pct ?? null,
        gross_margin_pct: financials?.gross_margin_pct ?? null,
        gross_margin_yoy_pp: financials?.gross_margin_yoy_pp ?? null,
        top_segment: financials?.top_segment ?? undefined,
        high_iv: isHighIVString(input.iv_level)
    };
}

export function selectSubstantiveNewsTitles(titles: string[], limit = 3): string[] {
    return titles
        .filter((title) => title && !isClickbait(title))
        .map((title, index) => ({ title, index, score: scoreNewsTitle(title) }))
        .filter((item) => item.score > 0)
        .sort((a, b) => b.score - a.score || a.index - b.index)
        .slice(0, limit)
        .map((item) => item.title);
}

function scoreNewsTitle(title: string): number {
    const t = title.toLowerCase();
    let score = 0;
    if (/\b(invests?|investment|stake|funding|partnership|deal|contract|order|backlog|acquisition|merger|buyout|approval|approved|launch|unveils?|introduces?|guidance|outlook|forecast|policy|tariff|export control|chip act)\b/i.test(title)) {
        score += 4;
    }
    if (/投资|入股|收购|并购|合作|订单|积压|获批|批准|发布|推出|指引|展望|政策|关税|出口管制|补贴/.test(title)) {
        score += 4;
    }
    if (/\b(revenue|earnings|margin|segment|datacenter|data center|ai|cloud|foundry|fab|asic|gpu|optics|power|nuclear)\b/i.test(title)) {
        score += 2;
    }
    if (/营收|收入|利润率|数据中心|云|算力|晶圆|光通信|电力|核电|半导体/.test(title)) {
        score += 2;
    }
    if (/\b(why|should|buy|sell|hold|prediction|secret|skyrocket)\b/i.test(t)) score -= 5;
    return score;
}

function finalizeTemplateText(
    symbol: string,
    text: string,
    pitchInputs: PitchInputs,
    sourceQuality: NarrativeSourceQuality
): string {
    const validation = validateGeneratedPitchText(text, pitchInputs);
    if (validation.passed) return text;

    console.warn(
        JSON.stringify({
            tag: 'go_pitch_template_validation_failed',
            symbol,
            source_quality: sourceQuality,
            reasons: validation.reasons,
            text_preview: text.slice(0, 120),
            ts: new Date().toISOString()
        })
    );

    const safeInputs: PitchInputs = {
        ...pitchInputs,
        recent_news_titles: [],
        change_5d_pct: null,
        pct_from_52w_high: null,
        days_since_earnings: null,
        earnings_surprise: null
    };
    return buildMinimalPitch(safeInputs);
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
