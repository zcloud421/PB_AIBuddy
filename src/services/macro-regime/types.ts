/**
 * Macro Regime Dashboard type definitions.
 *
 * Sourced from a Portfolio Optimization project that went through 9 rounds of
 * audit. This is the data shape returned by the daily snapshot generator and
 * consumed by the Markets tab in the mobile app.
 */

export type RegimeSeverity = 'Healthy' | 'Neutral' | 'Warning' | 'Critical';

export type FundamentalState = 'intact' | 'weakening' | 'cracking';

export type SideMonitorStatus = 'normal' | 'watch' | 'stress' | 'crisis';

export type AiCloudStressStatus = 'Normal' | 'Watch' | 'Stress' | 'Crisis';

export type CreditRegimeState = 'NOISE' | 'BREAK_FORMING' | 'BREAK';

export type PillarState = 'normal' | 'elevated' | 'extreme';

export type RegimeVerdictState = 'STABLE' | 'BREAK_FORMING' | 'NOISE' | 'CONFIRMED_BREAK';

export type RegimeVerdictMechanism = 'credit' | 'rates' | 'fundamental' | null;

export type RegimeVerdictBrakeStatus = 'quiet' | 'forming' | 'confirmed';

export interface RegimeVerdict {
    state: RegimeVerdictState;
    mechanism: RegimeVerdictMechanism;
    one_line: string;
    confidence: 'low' | 'medium' | 'high';
    watch: string;
    brakes: {
        credit: RegimeVerdictBrakeStatus;
        rates: RegimeVerdictBrakeStatus;
        fundamental: RegimeVerdictBrakeStatus;
        crowding: 'quiet' | 'elevated';
    };
}

export interface IndicatorReading {
    name: string;
    value: number | null;
    status: RegimeSeverity;
    delta_4w: number | null;
    notes: string[];
    is_skipped: boolean;
    persistence?: IndicatorPersistence;
    tight_zone?: {
        active: boolean;
        label: string;
        historical_anchor: string;
    };
    pending_upgrade?: {
        kind?: 'row_promotion' | 'escalation_eligibility';
        target_severity: RegimeSeverity;
        confirmation_days_elapsed: number;
        confirmation_days_required: number;
        notes: string;
    };
    band_position_pct?: number;
    next_anchor?: SubBandNextAnchor;
    velocity_5d?: IndicatorVelocity | null;
}

export interface IndicatorPersistence {
    consecutive_days: number;
    severity_started_at: string;
}

export interface SubBandNextAnchor {
    value: number;
    label: string;
    distance?: number;
    distance_label?: string;
}

export interface IndicatorVelocity {
    value: number;
    unit: 'bp' | 'pct' | 'pts' | 'pp';
    label: string;
}

export interface SideSubSignal {
    name: string;
    value: number | null;
    score: 0 | 1 | 2 | 3;
    status: SideMonitorStatus;
    notes: string[];
}

export interface AiCloudStressTickerSignal {
    ticker: string;
    signal: 0 | 1 | 2 | 3;
    details: string[];
}

export interface AiCloudStressReport {
    score: 0 | 1 | 2 | 3;
    status: AiCloudStressStatus;
    crwv: AiCloudStressTickerSignal;
    nbis: AiCloudStressTickerSignal;
}

export interface CreditFundingStressReport {
    overall_score: 0 | 1 | 2 | 3;
    overall_status: SideMonitorStatus;
    kbe_signal: SideSubSignal;
    hy_acceleration_signal: SideSubSignal;
    funding_proxy_signal: SideSubSignal;
    ccc_leads_hy_signal?: SideSubSignal;
    credit_equity_divergence_signal?: SideSubSignal;
    credit_regime_state?: CreditRegimeState;
}

export interface FundamentalModifier {
    state: FundamentalState;
    escalation_level: 0 | 1 | 2;
    review_quarter: string;
    next_review_date: string;
    evidence_summary: string[];
}

export interface LateCyclePillar {
    state: PillarState;
    summary: string;
    evidence: string[];
    last_reviewed_at: string;
    days_since_review: number;
    stale_warning: boolean;
    band_position_pct?: number;
    next_anchor?: SubBandNextAnchor;
    velocity_5d?: IndicatorVelocity | null;
}

export interface LateCycleContext {
    pillars: {
        sentiment_manual: LateCyclePillar;
    };
}

export interface MacroRegimeIndicators {
    HY_OAS: IndicatorReading;
    YIELD_CURVE: IndicatorReading;
    VIX: IndicatorReading;
    DGS10_ABS_LEVEL: IndicatorReading;
    DGS10_4W_SHOCK: IndicatorReading;
    CONCENTRATION: IndicatorReading;
    AI_BREADTH: IndicatorReading;
    SOX_200DMA_DEVIATION: IndicatorReading;
    BROAD_BREADTH: IndicatorReading;
}

export interface MacroRegimeSnapshot {
    as_of: string;                  // ISO date "2026-05-16"
    overall: RegimeSeverity;        // final severity after all escalations
    base_overall: RegimeSeverity;   // before any escalation
    indicators: MacroRegimeIndicators;
    ai_cloud_stress: AiCloudStressReport;
    credit_funding_stress: CreditFundingStressReport;
    fundamental_modifier: FundamentalModifier;
    late_cycle_context: LateCycleContext;
    regime_persistence: IndicatorPersistence;
    composite_persistence: {
        ai_cloud: IndicatorPersistence;
        credit_funding: IndicatorPersistence;
        fundamental: IndicatorPersistence;
    };
    guardrail?: {
        applied: boolean;
        note: string;
        capped_from: 'Critical';
        capped_to: 'Warning';
    };
    escalation_summary?: EscalationSummary;
    leading_flags?: string[];
    regime_verdict?: RegimeVerdict;
}

export interface EscalationSummary {
    base_overall: RegimeSeverity;
    final_overall: RegimeSeverity;
    escalation_reasons: string[];
    guardrail?: {
        applied: boolean;
        note: string;
    };
}
