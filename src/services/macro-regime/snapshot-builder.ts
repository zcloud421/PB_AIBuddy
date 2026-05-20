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
import { fetchSpyHoldings } from '../../data/spy-holdings-fetcher';
import {
    computeAiBreadth,
    computeBroadBreadth,
    computeConcentration,
    computeDgs10AbsLevel,
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
import { applyEscalations, computeBaseSeverity } from './aggregate';
import { loadFundamentalModifier } from './fundamental-modifier';
import { buildLateCycleContext } from './late-cycle-context';
import { persistenceFor, syncAllPersistence } from './persistence';
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

    // Side monitor pre-requisites (HY OAS series in bp + Δ4w) reused for
    // indicator and Credit/Funding stress acceleration sub-signal.
    const [hyOasSeries, hyOasDelta4w, spyHoldings] = await Promise.all([
        fetchHyOasSeriesBp(),
        computeHyOasDelta4wBp(),
        fetchSpyHoldings()
    ]);

    const hyOas = await computeHyOas();

    const [
        yieldCurve,
        vix,
        dgs10AbsLevel,
        dgs10Shock,
        concentration,
        aiBreadth,
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
        computeBroadBreadth(fetcher),
        computeAiCloudStress(fetcher, hyOasDelta4w, hyOas.status),
        computeCreditFundingStress(fetcher, hyOasSeries)
    ]);

    const indicators: MacroRegimeIndicators = {
        HY_OAS: hyOas,
        YIELD_CURVE: yieldCurve,
        VIX: vix,
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        CONCENTRATION: concentration,
        AI_BREADTH: aiBreadth,
        BROAD_BREADTH: broadBreadth
    };

    const fundamentalModifier = loadFundamentalModifier();
    const lateCycleContext = await buildLateCycleContext();
    const baseOverall = computeBaseSeverity(indicators);
    const overall = applyEscalations(
        baseOverall,
        aiCloudStress,
        creditFundingStress
    );
    const asOf = todayUtcDate();
    await attachSubBandMetadata(asOf, {
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        HY_OAS: hyOas,
        VIX: vix
    }, lateCycleContext);
    await persistSubBandHistory(asOf, {
        DGS10_ABS_LEVEL: dgs10AbsLevel,
        DGS10_4W_SHOCK: dgs10Shock,
        HY_OAS: hyOas,
        VIX: vix
    }, lateCycleContext);
    const persistenceRecords = await syncAllPersistence(asOf, indicators, {
        overall,
        ai_cloud: aiCloudStatusToSeverity(aiCloudStress.status),
        credit_funding: sideStatusToSeverity(creditFundingStress.overall_status),
        fundamental: fundamentalStateToSeverity(fundamentalModifier.state)
    });

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
        }
    };

    console.log(
        `[macro-regime] snapshot built in ${Date.now() - startedAt}ms ` +
            `(overall=${overall}, base=${baseOverall})`
    );
    return snapshot;
}
