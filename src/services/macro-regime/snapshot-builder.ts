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
import { fetchFredSeries } from '../../data/fred-series-fetcher';
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
    const [hyOasSeries, cccOasSeries, dfii10Series, hyOasDelta4w, spyHoldings, soxHistory, qqqVerdictBars] = await Promise.all([
        fetchHyOasSeriesBp(),
        fetchCccOasSeriesBp(),
        fetchFredSeries('DFII10', 120),
        computeHyOasDelta4wBp(),
        fetchSpyHoldings(),
        fetchSoxIndexHistoryWithSource(),
        fetcher.fetchPriceHistory('QQQ', 330).catch(() => [])
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
    snapshot.regime_verdict = computeRegimeVerdict(snapshot, realRateBrake, qqqVerdictBars);

    console.log(
        `[macro-regime] snapshot built in ${Date.now() - startedAt}ms ` +
            `(overall=${overall}, base=${baseOverall})`
    );
    return snapshot;
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
