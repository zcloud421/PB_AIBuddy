/**
 * Snapshot orchestrator — fans out 7 indicator + 2 side-monitor computations
 * in parallel, applies aggregation + escalations, returns a complete
 * MacroRegimeSnapshot.
 *
 * All computations are independently fail-soft (return is_skipped or normal
 * status when their data source fails). Snapshot generation itself never
 * throws — worst case the snapshot has many skipped indicators and an
 * accurate "data unavailable" headline.
 */

import { MassiveDataFetcher } from '../../data/massive-fetcher';
import {
    computeAiBreadth,
    computeBroadBreadth,
    computeBtcDrawdown,
    computeDgs10FourWeekShock,
    computeHyOas,
    computeVix,
    computeYieldCurve
} from './indicators';
import {
    computeAiCloudStress,
    computeCreditFundingStress,
    computeHyOasDelta4wBp,
    fetchHyOasSeriesBp
} from './side-monitors';
import { applyEscalations, composeHeadline, computeBaseSeverity } from './aggregate';
import { loadFundamentalModifier } from './fundamental-modifier';
import type { MacroRegimeIndicators, MacroRegimeSnapshot } from './types';

function todayUtcDate(): string {
    return new Date().toISOString().slice(0, 10);
}

export async function buildMacroRegimeSnapshot(): Promise<MacroRegimeSnapshot> {
    const startedAt = Date.now();
    const fetcher = new MassiveDataFetcher();

    // Side monitor pre-requisites (HY OAS series in bp + Δ4w) reused for
    // indicator and Credit/Funding stress acceleration sub-signal.
    const [hyOasSeries, hyOasDelta4w] = await Promise.all([
        fetchHyOasSeriesBp(),
        computeHyOasDelta4wBp()
    ]);

    const [
        hyOas,
        yieldCurve,
        vix,
        dgs10Shock,
        aiBreadth,
        broadBreadth,
        btcDrawdown,
        aiCloudStress,
        creditFundingStress
    ] = await Promise.all([
        computeHyOas(),
        computeYieldCurve(),
        computeVix(fetcher),
        computeDgs10FourWeekShock(),
        computeAiBreadth(fetcher),
        computeBroadBreadth(fetcher),
        computeBtcDrawdown(),
        computeAiCloudStress(fetcher, hyOasDelta4w),
        computeCreditFundingStress(fetcher, hyOasSeries)
    ]);

    const indicators: MacroRegimeIndicators = {
        HY_OAS: hyOas,
        YIELD_CURVE: yieldCurve,
        VIX: vix,
        DGS10_4W_SHOCK: dgs10Shock,
        AI_BREADTH: aiBreadth,
        BROAD_BREADTH: broadBreadth,
        BTC_DRAWDOWN: btcDrawdown
    };

    const fundamentalModifier = loadFundamentalModifier();
    const baseOverall = computeBaseSeverity(indicators);
    const overall = applyEscalations(
        baseOverall,
        aiCloudStress,
        creditFundingStress,
        fundamentalModifier
    );

    const snapshot: MacroRegimeSnapshot = {
        as_of: todayUtcDate(),
        overall,
        base_overall: baseOverall,
        headline: composeHeadline(overall, indicators, aiCloudStress, creditFundingStress, fundamentalModifier),
        indicators,
        ai_cloud_stress: aiCloudStress,
        credit_funding_stress: creditFundingStress,
        fundamental_modifier: fundamentalModifier
    };

    console.log(
        `[macro-regime] snapshot built in ${Date.now() - startedAt}ms ` +
            `(overall=${overall}, base=${baseOverall})`
    );
    return snapshot;
}
