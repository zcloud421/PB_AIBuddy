/**
 * Snapshot orchestrator — fans out indicator + side-monitor computations
 * in parallel, applies aggregation + escalations, returns a complete
 * MacroRegimeSnapshot.
 *
 * All computations are independently fail-soft (return is_skipped or normal
 * status when their data source fails). Snapshot generation itself never
 * throws — worst case the snapshot has many skipped indicators and an
 * accurate skipped readings.
 */

import { MassiveDataFetcher } from '../../data/massive-fetcher';
import type { DailyPriceBar } from '../../data/massive-fetcher';
import { fetchFredSeries } from '../../data/fred-series-fetcher';
import type { FredPoint } from '../../data/fred-series-fetcher';
import { fetchSoxIndexHistoryWithSource } from '../../data/sox-index-fetcher';
import { fetchSpyHoldings } from '../../data/spy-holdings-fetcher';
import {
    computeAiBreadth,
    computeBroadBreadth,
    computeConcentration,
    computeDgs10AbsLevel,
    computeDgs10FourWeekShock,
    computeHyOas,
    computeSox200DmaDeviation,
    computeVix,
    computeYieldCurve
} from './indicators';
import {
    computeAiCloudStress,
    computeCreditFundingStress,
    computeHyOasDelta4wBp,
    fetchCccOasSeriesBp,
    fetchHyOasSeriesBp
} from './side-monitors';
import { annotateSoxEscalationEligibility, applyEscalationsDetailed, computeBaseSeverity } from './aggregate';
import { loadFundamentalModifier } from './fundamental-modifier';
import { buildLateCycleContext } from './late-cycle-context';
import { persistenceFor, syncAllPersistence, syncIndicatorPersistence } from './persistence';
import { computeRealRateBrake, computeRegimeVerdict } from './regime-verdict';
import type { VerdictExtras } from './regime-verdict';
import { attachSubBandMetadata, persistSubBandHistory } from './sub-band-metadata';
import type {
    AiCloudStressStatus,
    FundamentalState,
    MacroRegimeIndicators,
    MacroRegimeSnapshot,
    RegimeSeverity,
    SideMonitorStatus
} from './types';

function todayUtcDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function sideStatusToSeverity(status: SideMonitorStatus): RegimeSeverity {
    if (status === 'normal') return 'Healthy';
    if (status === 'watch') return 'Neutral';
    if (status === 'stress') return 'Warning';
    return 'Critical';
}

function aiCloudStatusToSeverity(status: AiCloudStressStatus): RegimeSeverity {
    if (status === 'Normal') return 'Healthy';
    if (status === 'Watch') return 'Neutral';
    if (status === 'Stress') return 'Warning';
    return 'Critical';
}

function fundamentalStateToSeverity(state: FundamentalState): RegimeSeverity {
    if (state === 'intact') return 'Healthy';
    if (state === 'weakening') return 'Warning';
    return 'Critical';
}

export async function buildMacroRegimeSnapshot(): Promise<MacroRegimeSnapshot> {
    const startedAt = Date.now();
    const fetcher = new MassiveDataFetcher();
    const asOf = todayUtcDate();

    // Side monitor pre-requisites (HY OAS series in bp + Δ4w) reused for
    // indicator and Credit/Funding stress acceleration sub-signal.
    const [hyOasSeries, cccOasSeries, dfii10Series, hyOasDelta4w, spyHoldings, soxHistory, qqqVerdictBars, dgs30Series, hygBars, iefBars] = await Promise.all([
        fetchHyOasSeriesBp(),
        fetchCccOasSeriesBp(),
        fetchFredSeries('DFII10', 120),
        computeHyOasDelta4wBp(),
        fetchSpyHoldings(),
        fetchSoxIndexHistoryWithSource(),
        fetcher.fetchPriceHistory('QQQ', 330).catch(() => []),
        fetchFredSeries('DGS30', 120).catch(() => []),
        fetcher.fetchPriceHistory('HYG', 90).catch(() => []),
        fetcher.fetchPriceHistory('IEF', 90).catch(() => [])
    ]);

    const hyOas = await computeHyOas();

    const [
        yieldCurve,
        vix,
        dgs10AbsLevel,
        dgs10Shock,
        concentration,
        aiBreadth,
        soxDeviation,
        broadBreadth,
        aiCloudStress,
        creditFundingStress
    ] = await Promise.all([
        computeYieldCurve(),
        computeVix(fetcher),
        computeDgs10AbsLevel(),
        computeDgs10FourWeekShock(),
        Promise.resolve(computeConcentration(spyHoldings)),
        computeAiBreadth(fetcher),
        Promise.resolve(computeSox200DmaDeviation(soxHistory.points, soxHistory.source)),
        computeBroadBreadth(fetcher),
        computeAiCloudStress(fetcher, hyOasDelta4w, hyOas.status),
        computeCreditFundingStress(fetcher, hyOasSeries, cccOasSeries)
    ]);

    const indicators: MacroRegimeIndicators = {
        HY_OAS: hyOas,
        YIELD_CURVE: yieldCurve,
        VIX: vix,
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        CONCENTRATION: concentration,
        AI_BREADTH: aiBreadth,
        SOX_200DMA_DEVIATION: soxDeviation,
        BROAD_BREADTH: broadBreadth
    };

    const fundamentalModifier = loadFundamentalModifier();
    const lateCycleContext = await buildLateCycleContext();
    await syncIndicatorPersistence(asOf, indicators);
    annotateSoxEscalationEligibility(indicators);
    const baseOverall = computeBaseSeverity(indicators);
    const escalationResult = applyEscalationsDetailed(
        baseOverall,
        aiCloudStress,
        creditFundingStress,
        indicators
    );
    const overall = escalationResult.overall;
    await attachSubBandMetadata(asOf, {
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        HY_OAS: hyOas,
        VIX: vix,
        SOX_200DMA_DEVIATION: soxDeviation
    }, lateCycleContext);
    await persistSubBandHistory(asOf, {
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        HY_OAS: hyOas,
        VIX: vix,
        SOX_200DMA_DEVIATION: soxDeviation
    }, lateCycleContext);
    const persistenceRecords = await syncAllPersistence(asOf, indicators, {
        overall,
        ai_cloud: aiCloudStatusToSeverity(aiCloudStress.status),
        credit_funding: sideStatusToSeverity(creditFundingStress.overall_status),
        fundamental: fundamentalStateToSeverity(fundamentalModifier.state)
    });
    const realRateBrake = computeRealRateBrake(
        dfii10Series ?? [],
        qqqVerdictBars,
        creditFundingStress.credit_regime_state ?? 'NOISE'
    );

    const snapshot: MacroRegimeSnapshot = {
        as_of: asOf,
        overall,
        base_overall: baseOverall,
        indicators,
        ai_cloud_stress: aiCloudStress,
        credit_funding_stress: creditFundingStress,
        fundamental_modifier: fundamentalModifier,
        late_cycle_context: lateCycleContext,
        regime_persistence: persistenceFor(persistenceRecords, 'OVERALL'),
        composite_persistence: {
            ai_cloud: persistenceFor(persistenceRecords, 'AI_CLOUD'),
            credit_funding: persistenceFor(persistenceRecords, 'CREDIT_FUNDING'),
            fundamental: persistenceFor(persistenceRecords, 'FUNDAMENTAL')
        },
        guardrail: escalationResult.guardrail,
        escalation_summary: {
            base_overall: baseOverall,
            final_overall: overall,
            escalation_reasons: escalationResult.reasons,
            guardrail: escalationResult.guardrail
                ? {
                    applied: escalationResult.guardrail.applied,
                    note: escalationResult.guardrail.note
                }
                : undefined
        },
        leading_flags: buildLeadingFlags(creditFundingStress)
    };
    snapshot.regime_verdict = computeRegimeVerdict(
        snapshot,
        realRateBrake,
        qqqVerdictBars,
        computeVerdictExtras(dgs30Series ?? [], hygBars ?? [], iefBars ?? [])
    );

    console.log(
        `[macro-regime] snapshot built in ${Date.now() - startedAt}ms ` +
            `(overall=${overall}, base=${baseOverall})`
    );
    return snapshot;
}

// Faithful-to-spec extras for the verdict: 30Y nominal (long-end / term premium)
// and HYG/IEF (credit vs duration). Computed here from raw series; formatted in
// regime-verdict.
function computeVerdictExtras(
    dgs30Series: FredPoint[],
    hygBars: DailyPriceBar[],
    iefBars: DailyPriceBar[]
): VerdictExtras {
    const round1 = (v: number) => Math.round(v * 10) / 10;
    const round2 = (v: number) => Math.round(v * 100) / 100;

    // DGS30: latest level + 8w (56 calendar day) change in bp.
    const sorted30 = [...dgs30Series]
        .filter((p) => Number.isFinite(p.value))
        .sort((a, b) => a.date.localeCompare(b.date));
    const latest30 = sorted30[sorted30.length - 1] ?? null;
    let dgs30Delta8wBp: number | null = null;
    if (latest30) {
        const cutoff = new Date(new Date(latest30.date).getTime() - 56 * 86400000)
            .toISOString()
            .slice(0, 10);
        const back = [...sorted30].reverse().find((p) => p.date <= cutoff) ?? null;
        if (back) dgs30Delta8wBp = (latest30.value - back.value) * 100;
    }

    // HYG/IEF ratio + 4w (20 trading-bar) trend. Falling ratio = credit stress.
    const align = (bars: DailyPriceBar[]) =>
        new Map([...bars].filter((b) => Number.isFinite(b.close) && b.close > 0).map((b) => [b.date, b.close]));
    const hyg = align(hygBars);
    const ief = align(iefBars);
    const ratioSeries = [...hyg.keys()]
        .filter((d) => ief.has(d))
        .sort()
        .map((d) => ({ date: d, ratio: (hyg.get(d) as number) / (ief.get(d) as number) }));
    const latestR = ratioSeries[ratioSeries.length - 1] ?? null;
    let hygIefDelta4wPct: number | null = null;
    if (latestR && ratioSeries.length > 20) {
        const back = ratioSeries[ratioSeries.length - 1 - 20];
        if (back && back.ratio > 0) hygIefDelta4wPct = (latestR.ratio / back.ratio - 1) * 100;
    }

    return {
        dgs30_pct: latest30 ? round2(latest30.value) : null,
        dgs30_delta_8w_bp: dgs30Delta8wBp !== null ? round1(dgs30Delta8wBp) : null,
        hyg_ief_ratio: latestR ? round2(latestR.ratio) : null,
        hyg_ief_delta_4w_pct: hygIefDelta4wPct !== null ? round1(hygIefDelta4wPct) : null
    };
}

function buildLeadingFlags(creditFundingStress: MacroRegimeSnapshot['credit_funding_stress']): string[] {
    const flags: string[] = [];
    if (creditFundingStress.credit_regime_state && creditFundingStress.credit_regime_state !== 'NOISE') {
        flags.push(`credit_regime:${creditFundingStress.credit_regime_state}`);
    }
    if ((creditFundingStress.ccc_leads_hy_signal?.score ?? 0) >= 2) {
        flags.push('credit:ccc_leads_hy');
    }
    if ((creditFundingStress.credit_equity_divergence_signal?.score ?? 0) >= 2) {
        flags.push('credit:equity_divergence');
    }
    if (creditFundingStress.hy_acceleration_signal.score >= 2) {
        flags.push('credit:hy_acceleration');
    }
    return flags;
}
