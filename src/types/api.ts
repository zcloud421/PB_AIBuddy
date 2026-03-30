export type Grade = 'GO' | 'CAUTION' | 'AVOID';
export type SignalColor = 'green' | 'amber' | 'red' | 'gray';
export type FlagSeverity = 'INFO' | 'WARN' | 'BLOCK';
export type FlagType =
    | 'BROKEN_TREND'
    | 'COMMODITY_BETA_CAUTION'
    | 'EARNINGS_PROXIMITY'
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
    client_questions: ClientFocusQuestion[];
    transmission_chain: ClientFocusTransmissionItem[];
    related_assets: string[];
    disclaimer: string;
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

export interface SymbolIdeaResponse {
    symbol: string;
    exchange: string;
    company_name: string | null;
    run_date: string;
    cached: boolean;
    grade: Grade;
    composite_score: number;
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

export interface TrackerSummaryResponse {
    total_recommendations: number;
    active: number;
    breached_open: number;
    expired_safe: number;
    expired_breached: number;
    safe_rate: string;
    avg_safety_buffer: string;
    positions_near_strike: number;
    active_positions: TrackerPosition[];
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
