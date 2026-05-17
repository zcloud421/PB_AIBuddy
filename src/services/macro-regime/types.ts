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

export type PillarState = 'normal' | 'elevated' | 'extreme';

export interface IndicatorReading {
    name: string;
    value: number | null;
    status: RegimeSeverity;
    delta_4w: number | null;
    notes: string[];
    is_skipped: boolean;
    persistence?: IndicatorPersistence;
}

export interface IndicatorPersistence {
    consecutive_days: number;
    severity_started_at: string;
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
}

export interface LateCycleContext {
    elevated_count: number;
    soft_pause_active: boolean;
    headline_suffix: string;
    consecutive_days_active: number;
    fatigue_warning: boolean;
    pillars: {
        valuation: LateCyclePillar;
        concentration_display: LateCyclePillar;
        oas_complacency: LateCyclePillar;
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
    BROAD_BREADTH: IndicatorReading;
    BTC_DRAWDOWN: IndicatorReading;
}

export interface MacroRegimeSnapshot {
    as_of: string;                  // ISO date "2026-05-16"
    overall: RegimeSeverity;        // final severity after all escalations
    base_overall: RegimeSeverity;   // before any escalation
    headline: string;               // one-liner human readable summary
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
}
