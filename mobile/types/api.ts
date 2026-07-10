export type Grade = 'GO' | 'CAUTION' | 'AVOID' | 'NOT_RECOMMENDABLE';
export type SignalColor = 'green' | 'amber' | 'red' | 'gray';
export type FlagSeverity = 'WARN' | 'BLOCK';
export type WaitReason = 'WAIT_EARNINGS_RISK' | 'WAIT_POST_EARNINGS_SHOCK' | 'WAIT_SETUP_RESET';

export interface Flag {
  type: string;
  severity: FlagSeverity;
  message: string;
}

export interface NewsItem {
  title: string;
  source: string;
  url: string;
  published_at: string;
}

export interface NarrativeOutput {
  why_now: string;
  risk_note: string;
  sentiment_score: number;
  key_events: string[];
  source_quality?:
    | 'llm_validated'
    | 'llm_retry_validated'
    | 'template_fallback'
    | 'llm_failed_validation'
    | 'blocked'
    | 'deterministic'
    | 'go_pitch_llm_validated'
    | 'go_pitch_hybrid_validated'
    | 'go_pitch_template'
    | 'go_pitch_minimal'
    | 'caution_pitch_hybrid_validated'
    | 'caution_pitch_template'
    | 'avoid_pitch_deterministic';
  engine_version?: string;
}

export interface EligibilityStatus {
  passed: boolean;
  reason?:
    | 'outside_universe'
    | 'status_suspended'
    | 'status_under_review'
    | 'status_deprecated'
    | 'restricted'
    | 'house_override_avoid';
  message?: string;
}

export interface HouseOverrideStatus {
  action: 'FORCE_AVOID' | 'FORCE_CAUTION' | 'WHITELIST_ONLY';
  reason: string;
  set_by: string;
  set_at: string;
}

export type UnderlyingClassification = 'blue_chip' | 'theme' | 'both';

export interface UnderlyingUniverseItem {
  symbol: string;
  exchange: string;
  company_name: string | null;
  sector: string | null;
  themes: string[];
  tier: number;
  active: boolean;
  status?: 'active' | 'suspended' | 'under_review' | 'deprecated';
  classification?: UnderlyingClassification | null;
  adr_risk?: boolean | null;
  turnaround_watch?: boolean | null;
  holdable_concern?: string | null;
}

export type FcnEngineMode = 'weighted' | 'gated_shadow' | 'gated_live';
export type GateFailType = 'HARD_FAIL' | 'TIMING_FAIL' | 'SUITABILITY_FAIL';
export type GateSeverity = 'INFO' | 'WARN' | 'BLOCK';
export type GateDecisionType =
  | 'OUTSIDE_UNIVERSE'
  | 'STATUS_BLOCK'
  | 'RESTRICTED'
  | 'BUFFER_FLOOR'
  | 'BEARISH_STRUCTURE'
  | 'KI_BARRIER_RISK'
  | 'PATH_RISK'
  | 'EARNINGS_IMMINENT'
  | 'EARNINGS_WINDOW'
  | 'FUNDAMENTAL_DETERIORATION'
  | 'DISTRIBUTION_FALLING_KNIFE'
  | 'MACRO_REGIME_CRITICAL_CAP'
  | 'MACRO_CREDIT_CRISIS'
  | 'MACRO_AI_CAPEX_STRESS'
  | 'MACRO_AI_CAPEX_CAP'
  | 'MACRO_GUARDRAIL_AI_CAPEX_CAP'
  | 'MACRO_CONTEXT_UNAVAILABLE'
  | 'HARD_AVOID_TRIGGERED'
  | 'HIGH_COUPON_OVERRIDE'
  | 'QUALITY_DIP_RESCUE'
  | 'HIGH_VOL_CAUTION_OVERRIDE'
  | 'GRADE_CAP_COMMODITY_BETA'
  | 'GRADE_CAP_HIGH_BETA'
  | 'GRADE_CAP_NEWS_SHOCK'
  | 'GRADE_CAP_OVEREXTENDED'
  | 'GRADE_CAP_ASSIGNMENT_QUALITY';

export interface GateDecision {
  type: GateDecisionType;
  failType?: GateFailType;
  passed: boolean;
  severity: GateSeverity;
  message: string;
  details?: Record<string, unknown>;
  old_grade?: 'GO' | 'CAUTION' | 'AVOID';
  new_grade?: 'GO' | 'CAUTION' | 'AVOID';
  shadow?: boolean;
}

export interface SignalRowData {
  name: string;
  value: string;
  color: SignalColor;
  priority: number;
}

export interface PriceContext {
  current_price: number | null;
  ma20: number | null;
  ma50: number | null;
  ma200: number | null;
  pct_from_52w_high: number | null;
  implied_volatility: number | null;
  data_date: string | null;
  earnings_date: string | null;
  days_to_earnings: number | null;
  days_since_earnings?: number | null;
  earnings_phase?: 'PRE_EARNINGS' | 'POST_EARNINGS' | 'NONE';
  extended_price?: number | null;
  extended_move_pct?: number | null;
}

export interface PriceHistoryPoint {
  date: string;
  close: number;
}

export interface DrawdownEpisode {
  peak_date: string;
  peak_price: number;
  trough_date: string;
  trough_price: number;
  max_drawdown_pct: number;
  decline_days: number;
  recovery_days: number | null;
  total_duration_days: number | null;
  recovered: boolean;
}

export interface DrawdownAttribution {
  peak_date: string;
  peak_price: number;
  trough_date: string;
  max_drawdown_pct: number;
  primary_rule_id?: string | null;
  display_order?: number | null;
  recovery_days: number | null;
  total_duration_days: number | null;
  recovered: boolean;
  closed_by_partial_recovery: boolean;
  business_archetype?: string | null;
  subsector?: string | null;
  cycle_family?: string | null;
  event_signals?: string[] | null;
  event_signal_details?: Array<{
    tag: string;
    matched_keywords: string[];
    source_count: number;
  }> | null;
  reason_family?: string | null;
  background_regime?: string | null;
  primary_driver_type?: 'macro' | 'policy' | 'sector' | 'company' | 'geopolitical' | 'mixed' | null;
  primary_driver?: string | null;
  secondary_driver?: string | null;
  reason_zh: string | null;
}

export interface TailRiskStats {
  history_start_date: string | null;
  history_end_date: string | null;
  max_drawdown_pct: number | null;
  max_drawdown_peak_date: string | null;
  max_drawdown_trough_date: string | null;
  drawdown_20_count: number;
  drawdown_30_count: number;
  median_recovery_days: number | null;
  median_total_duration_days: number | null;
  worst_episode: DrawdownEpisode | null;
  longest_recovery_episode: DrawdownEpisode | null;
}

export interface TailRiskFooterSummary {
  max_drawdown_text: string;
  longest_recovery_text: string;
  has_unrecovered_drawdown: boolean;
}

export interface StrikeRiskMetricCard {
  label: string;
  value: string;
  tone: 'default' | 'warning';
}

export interface StrikeRiskRecoveryDistributionItem {
  label: string;
  value: string;
  count: number;
  tone: 'fast' | 'mid' | 'slow' | 'unrecovered';
}

export interface StrikeRiskGroupedDrawdownEvent {
  yearLabel: string;
  max_drawdown_pct: number;
  displayReason: string;
  hasUnrecovered: boolean;
  displayOrder: number | null;
}

export interface InteractiveStrikeRiskSummary {
  breachCount: number;
  thresholdPct: number;
  medianRecoveryDays: number | null;
  recoveryDaysSample: number[];
  recoveredCount: number;
  averageRecoveryDays: number | null;
  longestRecoveryDays: number | null;
  breachProbabilityPct: number | null;
  maxOvershootPct: number | null;
  unrecoveredCount: number;
  conclusion: string;
  conclusionStatsLine: string;
  conclusionRiskLine: {
    breachFrequencyLabel: '较低' | '中等' | '较高';
    tailRiskLabel: '较低' | '中等' | '较高';
  } | null;
  conclusionQualifierLine: string | null;
  currentPriceLabel: string;
  strikePriceLabel: string;
  metricCards: StrikeRiskMetricCard[];
  recoveryDistribution: StrikeRiskRecoveryDistributionItem[];
  groupedEvents: StrikeRiskGroupedDrawdownEvent[];
}

export interface IdeaCardData {
  symbol: string;
  exchange: string;
  company_name: string | null;
  sector: string | null;
  themes: string[];
  tier: number;
  grade: 'GO' | 'CAUTION';
  composite_score: number;
  ranking_score?: number | null;
  trend_score?: number | null;
  event_risk_score?: number | null;
  iv_premium_score?: number | null;
  realized_volatility?: number | null;
  volatility_risk_premium?: number | null;
  recommended_strike: number | null;
  recommended_tenor_days: number | null;
  recommended_expiry_date: string | null;
  estimated_coupon_range: string | null;
  coupon_note: string;
  moneyness_pct: number | null;
  reasoning_text: string;
  narrative: NarrativeOutput | null;
  news_items: NewsItem[];
  flags: Flag[];
  gate_decisions?: GateDecision[];
  shadow_grade?: 'GO' | 'CAUTION' | 'AVOID' | null;
  engine_mode?: FcnEngineMode;
  target_coupon_pct?: number | null;
  achieved_coupon_pct?: number | null;
  max_achievable_coupon_pct?: number | null;
  target_unreachable?: boolean | null;
  current_price: number | null;
  pct_from_52w_high: number | null;
  ma50: number | null;
  ma200: number | null;
  implied_volatility: number | null;
  sentiment_score: number | null;
  actionable_caution?: boolean;
  wait_reason?: WaitReason | null;
  assignment_quality_score?: number | null;
  assignment_quality_label?: 'LOW' | 'MEDIUM' | 'HIGH' | null;
}

export interface DailyBestData {
  symbol: string;
  company_name: string | null;
  theme: string;
  theme_narrative: string;
  grade: 'GO';
  ranking_score?: number | null;
  trend_score?: number | null;
  event_risk_score?: number | null;
  iv_premium_score?: number | null;
  realized_volatility?: number | null;
  volatility_risk_premium?: number | null;
  recommended_strike: number;
  recommended_tenor_days: number;
  recommended_expiry_date: string | null;
  estimated_coupon_range: string;
  moneyness_pct: number;
  reasoning_text: string;
  narrative: NarrativeOutput | null;
  news_items: NewsItem[];
  flags: Flag[];
  gate_decisions?: GateDecision[];
  shadow_grade?: 'GO' | 'CAUTION' | 'AVOID' | null;
  engine_mode?: FcnEngineMode;
  target_coupon_pct?: number | null;
  achieved_coupon_pct?: number | null;
  max_achievable_coupon_pct?: number | null;
  target_unreachable?: boolean | null;
  sentiment_score: number | null;
}

export interface AvoidEntry {
  symbol: string;
  narrative: NarrativeOutput | null;
  primary_flag_type: string;
  primary_flag_detail: string;
  gate_decisions?: GateDecision[];
  shadow_grade?: 'GO' | 'CAUTION' | 'AVOID' | null;
  engine_mode?: FcnEngineMode;
  target_coupon_pct?: number | null;
  achieved_coupon_pct?: number | null;
  max_achievable_coupon_pct?: number | null;
  target_unreachable?: boolean | null;
  wait_reason?: WaitReason | null;
}

export interface TodayIdeasResponse {
  run_date: string;
  run_id: string;
  market_context: {
    vix: number;
    vix_change_1d_pct?: number | null;
    notable_macro: string;
  };
  daily_best: DailyBestData | null;
  recommended: IdeaCardData[];
  caution: IdeaCardData[];
  not_recommended: AvoidEntry[];
}

export interface ClientFocusQuestionData {
  question: string;
  answer: string;
  category?: string;
  logic?: string;
  observation?: string;
}

export interface ClientFocusDailyVerdictData {
  risk_appetite: '偏谨慎' | '中性' | '偏积极';
  fcn_impact: string;
  key_change: string;
  primary_event?: string;
  pitch_focus_summary?: string;
}

export interface ThemeBasketItemData {
  id: string;
  label: string;
  labelEn: string;
  symbols?: string[];
  war_perf: number;
  ceasefire_perf: number;
  ytd_perf: number;
  driver: string;
}

export interface ThemePerformanceInterpretationData {
  summary: string;
  laggards: string;
  client: string;
}

export interface ThemeWinnersLosersResultData {
  scenario_label: string;
  updated_at: string;
  winners: ThemeBasketItemData[];
  losers: ThemeBasketItemData[];
  interpretation?: ThemePerformanceInterpretationData | null;
}

export interface ClientFocusUpdateData {
  time: string;
  title: string;
  impact: string;
  date?: string;
  source?: string;
}

export interface ClientFocusTransmissionItemData {
  order: '一阶传导' | '二阶传导';
  title: string;
  pricing: '已定价' | '部分定价' | '未充分定价';
  summary: string;
  latest_evidence?: string | null;
}

export interface WhatChangedGroupData {
  group_label: string;
  group_icon: string;
  items: Array<{
    time: string;
    headline: string;
  }>;
}

export interface ClientFocusListItemData {
  slug: string;
  title: string;
  status?: string;
  updated_at: string;
  summary: string;
  accent: string;
  preview_questions?: Array<Pick<ClientFocusQuestionData, 'question'>>;
  client_questions: Array<Pick<ClientFocusQuestionData, 'question'>>;
}

export interface ClientFocusDetailData {
  slug: string;
  title: string;
  status?: string;
  updated_at: string;
  summary: string;
  accent: string;
  latest_updates: ClientFocusUpdateData[];
  what_changed?: WhatChangedGroupData[];
  client_questions: ClientFocusQuestionData[];
  transmission_chain: ClientFocusTransmissionItemData[];
  related_assets: string[];
  market_snapshot?: ClientFocusMarketSnapshotData | null;
  market_chart?: ClientFocusMarketChartData | null;
  hibor?: ClientFocusHiborData | null;
  sector_rotation?: ClientFocusSectorRotationData | null;
  focus_price_snapshot?: ClientFocusPriceSnapshotData | null;
  focus_price_history?: ClientFocusPriceHistoryPointData[] | null;
  focus_secondary_price_snapshot?: ClientFocusPriceSnapshotData | null;
  focus_secondary_price_history?: ClientFocusPriceHistoryPointData[] | null;
  gold_drivers?: ClientFocusDriverItemData[] | null;
  theme_winners_losers?: ThemeWinnersLosersResultData | null;
  market_client_focus?: {
    items: Array<{ label: string; content: string }>;
  } | null;
  conversation_openers?: Array<{
    scenario: string;
    question: string;
  }> | null;
  daily_verdict?: ClientFocusDailyVerdictData | null;
  disclaimer: string;
}

export interface AssetBucketNarrative {
  bucket: '美股' | '港股' | '黄金' | '美债' | '汇率';
  thesis_check: string;
  today_signal: string;
  portfolio_implication: string;
  trigger?: string;
  client_type?: string;
  pitch_line?: string;
}

export interface DailyMarketNarrative {
  regime_label: string;
  primary_slug: string;
  narrative: string;
  ranked_slugs: string[];
  rank_changes: Record<string, 'up' | 'down' | 'stable'>;
  momentum_days: number;
  daily_pitch_triggers?: DailyPitchTrigger[];
  asset_buckets: AssetBucketNarrative[];
  default_expanded_bucket: '美股' | '港股' | '黄金' | '美债' | '汇率';
  generated_at: string;
}

export interface DailyPitchTrigger {
  id?: number;
  headline?: string;
  context?: string;
  talking_point?: string;
  asset_tags?: string[];
  materiality_trigger?: string;
  risk_flag?: boolean;
  time_sensitivity?: 'immediate' | 'this_week' | 'watch';
  source_summary?: string;
  hook: string;
  why_now: string;
  client_type: string;
  pitch_line: string;
  watchpoints: string[];
  related_assets?: string[];
}

export interface ClientFocusMarketSnapshotItemData {
  code: string;
  name: string;
  latest: number | null;
  change_pct: number | null;
  change_5d_pct?: number | null;
  change_ytd_pct?: number | null;
  streak_days?: number | null;
  streak_direction?: 'up' | 'down' | null;
}

export interface ClientFocusMarketSnapshotData {
  summary: string;
  updated_at: string;
  indices: ClientFocusMarketSnapshotItemData[];
}

export type ClientFocusMarketStateResponseData = ClientFocusMarketSnapshotData;

export interface ClientFocusMarketChartPointData {
  date: string;
  net_buy: number | null;
  hsi_close: number | null;
}

export interface ClientFocusMarketChartStatsData {
  latest_net_buy: number | null;
  sum_10d: number | null;
  sum_20d: number | null;
  sum_60d: number | null;
}

export interface ClientFocusMarketChartData {
  series_name: string;
  unit: string;
  latest_trade_date: string | null;
  points: ClientFocusMarketChartPointData[];
  stats: ClientFocusMarketChartStatsData;
}

export interface ClientFocusHiborData {
  rate_1m: number | null;
  rate_3m: number | null;
  change_1m: number | null;
  change_3m: number | null;
  as_of: string;
}

export interface ClientFocusSectorRotationItemData {
  name: string;
  change_pct: number;
}

export interface ClientFocusSectorRotationData {
  top: ClientFocusSectorRotationItemData[];
  bottom: ClientFocusSectorRotationItemData[];
  as_of: string;
}

export interface ClientFocusPriceSnapshotData {
  code: string;
  name: string;
  latest: number | null;
  change_pct: number | null;
  as_of: string | null;
}

export interface ClientFocusPriceHistoryPointData {
  date: string;
  close: number;
}

export type ClientFocusDriverTagData =
  | '支撑'
  | '有限支撑'
  | '净流入'
  | '中性'
  | '待确认'
  | '数据加载中'
  | '压制'
  | '走弱'
  | '净流出';

export interface ClientFocusDriverItemData {
  label: string;
  status?: string;
  tag?: ClientFocusDriverTagData;
  detail?: string;
  explanation?: string;
  data_source?: 'real-data' | 'news-keyword';
}

export interface ClientFocusPolymarketHistoryPointData {
  t: number;
  p: number;
}

export interface ClientFocusPolymarketOutcomeData {
  display_label: string;
  probability: number;
  history: ClientFocusPolymarketHistoryPointData[];
}

export interface ClientFocusPolymarketMarketData {
  condition_id: string;
  label: string;
  outcomes: ClientFocusPolymarketOutcomeData[];
}

export interface ClientFocusPolymarketResponseData {
  markets: ClientFocusPolymarketMarketData[];
}

export interface SymbolIdeaResponse {
  symbol: string;
  exchange: string;
  company_name: string | null;
  run_date: string;
  cached: boolean;
  grade: Grade;
  in_recommendation_pool: boolean;
  eligibility?: EligibilityStatus;
  composite_score: number;
  ranking_score?: number | null;
  risk_reward_score: number | null;
  trend_score?: number | null;
  event_risk_score?: number | null;
  iv_premium_score?: number | null;
  realized_volatility?: number | null;
  volatility_risk_premium?: number | null;
  verdict_headline: string;
  verdict_sub: string;
  data_as_of_date: string | null;
  recommended_strike: number | null;
  recommended_tenor_days: number | null;
  recommended_expiry_date: string | null;
  estimated_coupon_range: string | null;
  coupon_note: string;
  moneyness_pct: number | null;
  reasoning_text: string;
  narrative: NarrativeOutput | null;
  house_override?: HouseOverrideStatus;
  news_items: NewsItem[];
  flags: Flag[];
  gate_decisions?: GateDecision[];
  shadow_grade?: 'GO' | 'CAUTION' | 'AVOID' | null;
  engine_mode?: FcnEngineMode;
  target_coupon_pct?: number | null;
  achieved_coupon_pct?: number | null;
  max_achievable_coupon_pct?: number | null;
  target_unreachable?: boolean | null;
  actionable_caution?: boolean;
  wait_reason?: WaitReason | null;
  signals: SignalRowData[];
  price_context: PriceContext;
  sentiment_score: number | null;
}

export interface SymbolNarrativeResponse {
  ready: boolean;
  narrative: NarrativeOutput | null;
}

export interface SymbolPriceHistoryResponse {
  symbol: string;
  data_as_of_date: string | null;
  price_history: PriceHistoryPoint[];
  tail_risk: TailRiskStats | null;
  tail_risk_footer_summary?: TailRiskFooterSummary | null;
  drawdown_attributions?: DrawdownAttribution[];
  interactive_strike_risk_summary?: InteractiveStrikeRiskSummary | null;
  display_drawdown_events?: StrikeRiskGroupedDrawdownEvent[];
}

export interface SuitabilityNote {
  reason: string;
  weakness: string;
  next_step: string | null;
}

export interface PairAnalysisResponse {
  symbolA: string;
  symbolB: string;
  data_as_of: string;
  trading_days_overlap: number;
  correlation: {
    d60?: number;
    d120: number;
    d90: number;
    d180: number;
    d252: number;
    bear_2022: number | null;
  };
  volatility: {
    symbolA_annualized: number;
    symbolB_annualized: number;
    gap: number;
    ratio: number;
    gap_flag: boolean;
    gap_leg: string | null;
  };
  downside_sync: number;
  correlation_stability: 'STABLE' | 'MODERATE' | 'UNSTABLE';
  suitability: 'HIGH' | 'MEDIUM' | 'LOW';
  suitability_note: string;
  suitability_note_structured?: SuitabilityNote;
}
