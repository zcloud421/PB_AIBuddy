import { getLatestMacroRegimeSnapshot } from '../../db/queries/macro-regime';
import type {
    AiCloudStressStatus,
    FundamentalState,
    MacroRegimeSnapshot,
    RegimeVerdictState,
    RegimeSeverity,
    SideMonitorStatus
} from '../../services/macro-regime/types';

export interface MacroGateContext {
    overall: RegimeSeverity;
    base_overall: RegimeSeverity;
    credit_funding_status: SideMonitorStatus;
    ai_cloud_status: AiCloudStressStatus;
    fundamental_state: FundamentalState;
    fundamental_escalation: 0 | 1 | 2;
    ai_breadth_status: RegimeSeverity;
    sox_status: RegimeSeverity;
    broad_breadth_status: RegimeSeverity;
    vix_status: RegimeSeverity;
    hy_oas_status: RegimeSeverity;
    regime_persistence_days: number;
    guardrail_applied: boolean;
    regime_verdict_state: RegimeVerdictState | null;
    snapshot_stale: boolean;
    as_of: string | null;
}

export const DEFAULT_MACRO_CONTEXT: MacroGateContext = {
    overall: 'Neutral',
    base_overall: 'Neutral',
    credit_funding_status: 'normal',
    ai_cloud_status: 'Normal',
    fundamental_state: 'intact',
    fundamental_escalation: 0,
    ai_breadth_status: 'Neutral',
    sox_status: 'Neutral',
    broad_breadth_status: 'Neutral',
    vix_status: 'Neutral',
    hy_oas_status: 'Neutral',
    regime_persistence_days: 0,
    guardrail_applied: false,
    regime_verdict_state: null,
    snapshot_stale: true,
    as_of: null
};

export async function loadMacroContext(): Promise<MacroGateContext> {
    const snapshot = await getLatestMacroRegimeSnapshot().catch(() => null);
    if (!snapshot || isStale(snapshot)) {
        return { ...DEFAULT_MACRO_CONTEXT, snapshot_stale: true, as_of: snapshot?.as_of ?? null };
    }
    return parseMacroSnapshot(snapshot);
}

export function parseMacroSnapshot(snapshot: MacroRegimeSnapshot): MacroGateContext {
    return {
        overall: snapshot.overall,
        base_overall: snapshot.base_overall,
        credit_funding_status: snapshot.credit_funding_stress.overall_status,
        ai_cloud_status: snapshot.ai_cloud_stress.status,
        fundamental_state: snapshot.fundamental_modifier.state,
        fundamental_escalation: snapshot.fundamental_modifier.escalation_level,
        ai_breadth_status: snapshot.indicators.AI_BREADTH.status,
        sox_status: snapshot.indicators.SOX_200DMA_DEVIATION.status,
        broad_breadth_status: snapshot.indicators.BROAD_BREADTH.status,
        vix_status: snapshot.indicators.VIX.status,
        hy_oas_status: snapshot.indicators.HY_OAS.status,
        regime_persistence_days: snapshot.regime_persistence?.consecutive_days ?? 0,
        guardrail_applied: Boolean(snapshot.guardrail?.applied || snapshot.escalation_summary?.guardrail?.applied),
        regime_verdict_state: snapshot.regime_verdict?.state ?? null,
        snapshot_stale: false,
        as_of: snapshot.as_of
    };
}

function isStale(snapshot: MacroRegimeSnapshot): boolean {
    const parsed = Date.parse(`${snapshot.as_of}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return true;
    return Date.now() - parsed > 36 * 60 * 60 * 1000;
}
