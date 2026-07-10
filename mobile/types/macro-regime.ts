/**
 * Mobile-side mirror of backend MacroRegimeSnapshot payload.
 *
 * Source of truth: src/services/macro-regime/types.ts
 * Endpoint: GET /macro-regime/latest
 *
 * Keep field names and shapes in sync — payload is forwarded as-is.
 */

export type RegimeSeverity = 'Healthy' | 'Neutral' | 'Warning' | 'Critical';

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

export type IndicatorKey =
  | 'HY_OAS'
  | 'YIELD_CURVE'
  | 'VIX'
  | 'DGS10_ABS_LEVEL'
  | 'DGS10_4W_SHOCK'
  | 'CONCENTRATION'
  | 'AI_BREADTH'
  | 'SOX_200DMA_DEVIATION'
  | 'BROAD_BREADTH';

export type MacroRegimeIndicators = Record<IndicatorKey, IndicatorReading>;

export interface AiCloudStressTickerSignal {
  ticker: string;
  signal: 0 | 1 | 2 | 3;
  details: string[];
}

export type AiCloudStressStatus = 'normal' | 'Watch' | 'warning' | 'critical';

export interface AiCloudStressReport {
  score: number;
  status: AiCloudStressStatus;
  crwv: AiCloudStressTickerSignal;
  nbis: AiCloudStressTickerSignal;
}

export type CreditFundingSubStatus = 'normal' | 'watch' | 'warning' | 'critical';

export interface CreditFundingSubSignal {
  name: string;
  value: number | null;
  score: 0 | 1 | 2 | 3;
  status: CreditFundingSubStatus;
  notes: string[];
}

export interface CreditFundingStressReport {
  overall_score: number;
  overall_status: CreditFundingSubStatus;
  kbe_signal: CreditFundingSubSignal;
  hy_acceleration_signal: CreditFundingSubSignal;
  funding_proxy_signal: CreditFundingSubSignal;
  ccc_leads_hy_signal?: CreditFundingSubSignal;
  credit_equity_divergence_signal?: CreditFundingSubSignal;
  credit_regime_state?: 'NOISE' | 'BREAK_FORMING' | 'BREAK';
}

export type FundamentalState = 'intact' | 'softening' | 'breaking' | 'weakening' | 'cracking';

export interface CapexGuidancePerCompany {
  ticker: string;
  fy_capex_b: number;
  direction: 'raised' | 'maintained' | 'cut';
}

export interface CapexGuidanceSnapshot {
  fy_label: string;
  total_b: number;
  yoy_change_pct: number;
  raised_count: number;
  maintained_count: number;
  cuts_count: number;
  per_company: CapexGuidancePerCompany[];
}

export interface CapexRealizedPerCompany {
  ticker: string;
  ttm_capex_b: number;
  ttm_capex_yoy_pct: number;
  last_reported_quarter: string;
}

export interface CapexRealizedSnapshot {
  ttm_capex_b: number;
  capex_yoy_pct: number;
  execution_variance_pp: number;
  last_reported_quarter: string;
  per_company: CapexRealizedPerCompany[];
  data_source: string;
}

export interface FundamentalModifier {
  state: FundamentalState | string;
  escalation_level: number;
  review_quarter: string;
  next_review_date: string;
  evidence_summary: string[];
  guidance?: CapexGuidanceSnapshot;
  realized?: CapexRealizedSnapshot;
  state_source?: 'derived' | 'override';
}

export type PillarState = 'normal' | 'elevated' | 'extreme';

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

export type RegimeVerdictState = 'STABLE' | 'BREAK_FORMING' | 'NOISE' | 'CONFIRMED_BREAK';
export type RegimeVerdictMechanism = 'credit' | 'rates' | 'fundamental' | null;
export type RegimeVerdictBrakeStatus = 'quiet' | 'watch' | 'forming' | 'confirmed';
export type RegimeVerdictContextStatus = 'normal' | 'elevated' | 'extreme';

export interface RegimeVerdictEvidence {
  label: string;
  value: string;
}

export interface RegimeVerdictMechanismView {
  status: RegimeVerdictBrakeStatus;
  evidence: RegimeVerdictEvidence[];
  next_trigger: string | null;
}

export interface RegimeVerdictContextView {
  status: RegimeVerdictContextStatus;
  evidence: RegimeVerdictEvidence[];
}

export interface RegimeVerdict {
  state: RegimeVerdictState;
  mechanism: RegimeVerdictMechanism;
  one_line: string;
  confidence: 'low' | 'medium' | 'high';
  nearest_watch: string | null;
  mechanisms: {
    credit: RegimeVerdictMechanismView;
    rates: RegimeVerdictMechanismView;
    fundamental: RegimeVerdictMechanismView;
  };
  context: RegimeVerdictContextView;
  /** @deprecated Use nearest_watch. */
  watch: string;
  /** @deprecated Use mechanisms/context. */
  brakes: {
    credit: RegimeVerdictBrakeStatus;
    rates: RegimeVerdictBrakeStatus;
    fundamental: RegimeVerdictBrakeStatus;
    crowding: 'quiet' | 'elevated';
  };
}

export type ExposureTimingStatus = 'WAIT' | 'WATCH_SUPPORT' | 'BUILD_WINDOW' | 'EXTENDED';

export interface ExposureTimingEvidence {
  label: string;
  value: string;
}

export interface ExposureTimingSupport {
  label: 'MA50' | 'MA200';
  level: number;
  distance_pct: number;
  slope_20d_pct: number | null;
  closes_held_3d: number;
}

export interface ExposureTimingAsset {
  symbol: string;
  label: string;
  status: ExposureTimingStatus;
  status_label: string;
  readiness_rank: number;
  summary: string;
  current_price: number | null;
  change_5d_pct: number | null;
  drawdown_52w_pct: number | null;
  rsi_14: number | null;
  ma20: number | null;
  ma50: number | null;
  ma200: number | null;
  support: ExposureTimingSupport | null;
  evidence: ExposureTimingEvidence[];
  next_trigger: string;
  invalidation: string;
  data_as_of: string | null;
}

export interface ExposureTimingSnapshot {
  as_of: string;
  macro_state: RegimeVerdictState | null;
  assets: ExposureTimingAsset[];
}

export interface MacroRegimeSnapshot {
  as_of: string;
  overall: RegimeSeverity;
  base_overall: RegimeSeverity;
  regime_verdict?: RegimeVerdict;
  exposure_timing?: ExposureTimingSnapshot;
  indicators: MacroRegimeIndicators;
  ai_cloud_stress: AiCloudStressReport;
  credit_funding_stress: CreditFundingStressReport;
  fundamental_modifier: FundamentalModifier;
  late_cycle_context?: LateCycleContext;
  regime_persistence?: IndicatorPersistence;
  composite_persistence?: {
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
  /** 近 14 日 verdict 状态(升序);regime_verdict 上线前的旧快照 state 为 null。 */
  verdict_history?: Array<{ date: string; state: RegimeVerdictState | null }>;
  /** 当前状态已持续天数。 */
  verdict_days_in_state?: number | null;
  /** 较昨日方向:任一机制刹车升档=worse,降档=better,否则 same。 */
  verdict_direction?: 'worse' | 'same' | 'better' | null;
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
