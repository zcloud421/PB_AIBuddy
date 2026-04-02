export type Grade = 'GO' | 'CAUTION' | 'AVOID';
export type SignalColor = 'green' | 'amber' | 'red' | 'gray';
export type FlagSeverity = 'INFO' | 'WARN' | 'BLOCK';
export type FlagType =
    | 'BROKEN_TREND'
    | 'COMMODITY_BETA_CAUTION'
    | 'EARNINGS_PROXIMITY'
    | 'HIGH_BETA_THEME_CAUTION'
    | 'HIGH_VOL_LOW_STRIKE'
    | 'HIGH_COUPON_OVERRIDE'
    | 'HOUSE_OVERRIDE'
    | 'MATERIAL_NEWS_SHOCK'
    | 'MATERIAL_NEWS_OVERHANG'
    | 'BEARISH_STRUCTURE'
    | 'LOWER_HIGH_RISK'
    | 'LOW_COUPON'
    | 'LOW_LIQUIDITY'
    | 'NO_APPROVED_TENOR'
    | 'NO_APPROVED_STRIKE';

export interface Flag {
    type: FlagType;
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
}

export interface IdeaCard {
    symbol: string;
    exchange: string;
    company_name: string | null;
    sector: string | null;
    themes: string[];
    tier: number;
    grade: Extract<Grade, 'GO' | 'CAUTION'>;
    composite_score: number;
    recommended_strike: number;
    recommended_tenor_days: number;
    recommended_expiry_date: string | null;
    estimated_coupon_range: string | null;
    coupon_note: string;
    moneyness_pct: number;
    reasoning_text: string;
    narrative: NarrativeOutput | null;
    news_items: NewsItem[];
    flags: Flag[];
    current_price: number | null;
    pct_from_52w_high: number | null;
    ma50: number | null;
    ma200: number | null;
    implied_volatility: number | null;
    sentiment_score: number | null;
}

export interface AvoidEntry {
    symbol: string;
    primary_flag_type: FlagType;
    primary_flag_detail: string;
}

export interface DailyBestCard {
    symbol: string;
    company_name: string | null;
    theme: string;
    theme_narrative: string;
    grade: 'GO';
    recommended_strike: number;
    recommended_tenor_days: number;
    recommended_expiry_date: string | null;
    estimated_coupon_range: string;
    moneyness_pct: number;
    reasoning_text: string;
    narrative: NarrativeOutput | null;
    news_items: NewsItem[];
    flags: Flag[];
    sentiment_score: number | null;
}

export interface MarketContext {
    vix: number;
    hk_vix?: number;
    notable_macro: string;
}

export interface TodayIdeasResponse {
    run_date: string;
    run_id: string;
    market_context: MarketContext;
    daily_best: DailyBestCard | null;
    recommended: IdeaCard[];
    caution: IdeaCard[];
    not_recommended: AvoidEntry[];
}

export interface ClientFocusQuestion {
    question: string;
    answer: string;
}

export interface ClientFocusUpdate {
    time: string;
    date?: string;
    title: string;
    impact: string;
    source?: string;
}

export interface ClientFocusTransmissionItem {
    order: '一阶传导' | '二阶传导';
    title: string;
    pricing: '已定价' | '部分定价' | '未充分定价';
    summary: string;
    latest_evidence?: string | null;
}

export interface WhatChangedGroup {
    group_label: string;
    group_icon: string;
    items: Array<{
        time: string;
        headline: string;
    }>;
}

export interface ClientFocusListItem {
    slug: string;
    title: string;
    status?: string;
    updated_at: string;
    summary: string;
    accent: string;
    client_questions: Array<Pick<ClientFocusQuestion, 'question'>>;
}

export interface ClientFocusDetailResponse {
    slug: string;
    title: string;
    status?: string;
    updated_at: string;
    summary: string;
    accent: string;
    latest_updates: ClientFocusUpdate[];
    what_changed?: WhatChangedGroup[];
    client_questions: ClientFocusQuestion[];
    transmission_chain: ClientFocusTransmissionItem[];
    related_assets: string[];
    market_snapshot?: ClientFocusMarketSnapshot | null;
    market_chart?: ClientFocusMarketChart | null;
    hibor?: ClientFocusHibor | null;
    sector_rotation?: ClientFocusSectorRotation | null;
    focus_price_snapshot?: ClientFocusPriceSnapshot | null;
    focus_price_history?: ClientFocusPriceHistoryPoint[] | null;
    focus_secondary_price_snapshot?: ClientFocusPriceSnapshot | null;
    focus_secondary_price_history?: ClientFocusPriceHistoryPoint[] | null;
    gold_drivers?: ClientFocusDriverItem[] | null;
    disclaimer: string;
}

export interface ClientFocusMarketSnapshotItem {
    code: string;
    name: string;
    latest: number | null;
    change_pct: number | null;
    change_5d_pct?: number | null;
}

export interface ClientFocusMarketSnapshot {
    summary: string;
    indices: ClientFocusMarketSnapshotItem[];
}

export interface ClientFocusMarketChartPoint {
    date: string;
    net_buy: number | null;
    hsi_close: number | null;
}

export interface ClientFocusMarketChartStats {
    latest_net_buy: number | null;
    sum_10d: number | null;
    sum_20d: number | null;
    sum_60d: number | null;
}

export interface ClientFocusMarketChart {
    series_name: string;
    unit: string;
    latest_trade_date: string | null;
    points: ClientFocusMarketChartPoint[];
    stats: ClientFocusMarketChartStats;
}

export interface ClientFocusHibor {
    rate_1m: number | null;
    rate_3m: number | null;
    change_1m: number | null;
    change_3m: number | null;
    as_of: string;
}

export interface ClientFocusSectorRotationItem {
    name: string;
    change_pct: number;
}

export interface ClientFocusSectorRotation {
    top: ClientFocusSectorRotationItem[];
    bottom: ClientFocusSectorRotationItem[];
    as_of: string;
}

export interface ClientFocusPriceSnapshot {
    code: string;
    name: string;
    latest: number | null;
    change_pct: number | null;
    as_of: string | null;
}

export interface ClientFocusPriceHistoryPoint {
    date: string;
    close: number;
}

export interface ClientFocusDriverItem {
    label: string;
    status: string;
}

export interface ClientFocusPolymarketHistoryPoint {
    t: number;
    p: number;
}

export interface ClientFocusPolymarketOutcome {
    display_label: string;
    probability: number;
    history: ClientFocusPolymarketHistoryPoint[];
}

export interface ClientFocusPolymarketMarket {
    condition_id: string;
    label: string;
    outcomes: ClientFocusPolymarketOutcome[];
}

export interface ClientFocusPolymarketResponse {
    markets: ClientFocusPolymarketMarket[];
}

export interface SignalRow {
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
    recovered: boolean;
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
    worst_episode: DrawdownEpisode | null;
}

export interface SymbolIdeaResponse {
    symbol: string;
    exchange: string;
    company_name: string | null;
    run_date: string;
    cached: boolean;
    grade: Grade;
    composite_score: number;
    risk_reward_score: number | null;
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
    news_items: NewsItem[];
    flags: Flag[];
    signals: SignalRow[];
    price_context: PriceContext;
    price_history: PriceHistoryPoint[];
    sentiment_score: number | null;
}

export interface AsyncScoringAcceptedResponse {
    symbol: string;
    run_date: string;
    cached: boolean;
    status: 'PENDING' | 'RUNNING';
    job_id: string;
    poll_url: string;
    message: string;
}

export interface SymbolPriceHistoryResponse {
    symbol: string;
    data_as_of_date: string | null;
    price_history: PriceHistoryPoint[];
    tail_risk: TailRiskStats | null;
}

export interface PairAnalysisResponse {
    symbolA: string;
    symbolB: string;
    data_as_of: string;
    trading_days_overlap: number;
    correlation: {
        d60: number;
        d120: number;
        d252: number;
        bear_2022: number;
    };
    volatility: {
        symbolA_annualized: number;
        symbolB_annualized: number;
        gap: number;
    };
    downside_sync: number;
    correlation_stability: 'STABLE' | 'MODERATE' | 'UNSTABLE';
    suitability: 'HIGH' | 'MEDIUM' | 'LOW';
    suitability_note: string;
}

export interface AsyncScoringStatusResponse {
    symbol: string;
    job_id: string;
    status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';
    run_date: string;
    cached: boolean;
    progress_pct: number;
    result: SymbolIdeaResponse | null;
}

export interface ErrorResponse {
    error: {
        code: 'NOT_FOUND' | 'SCORING_ENGINE_UNAVAILABLE';
        message: string;
        request_id: string;
    };
}

export type RecommendationTrackerStatus = 'ACTIVE' | 'BREACHED' | 'EXPIRED_SAFE' | 'EXPIRED_BREACHED';

export interface TrackerPosition {
    symbol: string;
    grade: Extract<Grade, 'GO' | 'CAUTION'>;
    recommendation_date: string;
    entry_price: number | null;
    recommended_strike: number | null;
    moneyness_pct: number | null;
    expiry_date: string | null;
    current_price: number | null;
    pct_above_strike: number | null;
    status: RecommendationTrackerStatus;
    days_remaining: number | null;
}

export interface TrackerSummaryBucket {
    total_recommendations: number;
    active: number;
    breached_open: number;
    expired_safe: number;
    expired_breached: number;
    safe_rate: string;
    breach_rate: string;
}

export interface TrackerSummaryResponse {
    total_recommendations: number;
    active: number;
    breached_open: number;
    expired_safe: number;
    expired_breached: number;
    safe_rate: string;
    breach_rate: string;
    avg_safety_buffer: string;
    positions_near_strike: number;
    active_positions: TrackerPosition[];
    showcase_summary: {
        hero: TrackerSummaryBucket;
        recommended: TrackerSummaryBucket;
    };
}

export interface TrackerHistoryEntry {
    symbol: string;
    grade: Extract<Grade, 'GO' | 'CAUTION'>;
    recommendation_date: string;
    entry_price: number | null;
    recommended_strike: number | null;
    moneyness_pct: number | null;
    expiry_date: string | null;
    current_price: number | null;
    pct_above_strike: number | null;
    status: Extract<RecommendationTrackerStatus, 'EXPIRED_SAFE' | 'EXPIRED_BREACHED'>;
    last_checked: string | null;
    breached_date: string | null;
}

export interface TrackerMonthlyMetric {
    count: number;
    rate: string;
}

export interface TrackerMonthlyConcentrationItem {
    symbol: string;
    appearances: number;
}

export interface TrackerReviewSection {
    label: string;
    summary: {
        total_recommendations: number;
        path_breach_rate: string;
        active_below_strike_rate: string;
        maturity_safe_rate: string;
    };
}

export interface TrackerReviewResponse {
    generated_at: string;
    report_month: string;
    baseline_month: string;
    summary: {
        new_recommendations: TrackerMonthlyMetric;
        path_breach: TrackerMonthlyMetric;
        still_below_strike: TrackerMonthlyMetric;
        matured_3m_safe: TrackerMonthlyMetric;
    };
    comparison: {
        path_breach_rate_change_pct: string;
        still_below_strike_rate_change_pct: string;
        matured_3m_safe_rate_change_pct: string;
    };
    concentration: {
        unique_symbols: number;
        top_symbols: TrackerMonthlyConcentrationItem[];
        top_hero_symbol: TrackerMonthlyConcentrationItem | null;
    };
    rolling_30d: TrackerReviewSection;
    rolling_90d: TrackerReviewSection;
    matured_3m: TrackerReviewSection;
}
