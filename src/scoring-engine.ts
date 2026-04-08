import { fetchStockNewsContext, getCompanyName } from './data/news-fetcher';

export type HouseOverrideType = 'FORCE_AVOID' | 'FORCE_CAUTION' | 'WHITELIST_ONLY';

export type OverallGrade = 'GO' | 'CAUTION' | 'AVOID';

export type FlagSeverity = 'INFO' | 'WARN' | 'BLOCK';

export type FlagType =
    | 'BROKEN_TREND'
    | 'COMMODITY_BETA_CAUTION'
    | 'EARNINGS_PROXIMITY'
    | 'HIGH_BETA_THEME_CAUTION'
    | 'HIGH_VOL_LOW_STRIKE'
    | 'HIGH_COUPON_OVERRIDE'
    | 'HOUSE_OVERRIDE'
    | 'MACRO_SENSITIVITY'
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

export interface StrikeData {
    strike: number;
    iv: number;
    delta: number;
    gamma?: number;
    vega?: number;
    theta?: number;
    volume: number;
    open_interest: number;
    mid_price: number | null;
    mid_price_source?: 'last_quote' | 'day.close' | 'none';
    expiry_date: string;
    skew?: number;
}

export interface SymbolData {
    price_history: Array<{
        date: string;
        close: number;
    }>;
    high_52w: number;
    low_52w: number;
    current_price: number;
    ma20: number;
    ma50: number;
    ma200: number;
    days_since_52w_high: number;
    pct_from_52w_high: number;
    iv_rank: number | null;
    macd_line?: number | null;
    macd_signal?: number | null;
    macd_histogram?: number | null;
    rsi_14?: number | null;
    // Earnings are maintained manually via house_overrides or internal workflows.
    earnings_date: string | null;
    days_to_earnings?: number | null;
    house_override?: HouseOverrideType;
}

export type ChainData = StrikeData[];

export interface TenorWindow {
    tenor_days: number;
    preferred_tenor_days: number;
    expiry_date: string;
    strikes: StrikeData[];
}

export interface ScoringResult {
    symbol: string;
    overall_grade: OverallGrade;
    composite_score: number;
    risk_reward_score: number | null;
    iv_rank_score: number;
    trend_score: number;
    skew_score: number;
    event_risk_score: number;
    premium_score: number | null;
    selected_implied_volatility: number | null;
    recommended_strike: number | null;
    // actual DTE from the selected exchange-listed option expiry
    recommended_tenor_days: number | null;
    recommended_expiry_date: string | null;
    estimated_coupon_range: string | null;
    ref_coupon_pct: number | null;
    moneyness_pct: number | null;
    current_price: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    pct_from_52w_high: number | null;
    reasoning_text: string;
    flags: Flag[];
}

export interface DataFetcherInterface {
    fetchSymbolData(symbol: string): Promise<SymbolData>;
    fetchChainData(symbol: string, currentPrice: number): Promise<ChainData>;
}

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const PREFERRED_TENORS = [90, 180];
const PREFERRED_TENOR_TOLERANCE_DAYS = 15;
const MAX_APPROVED_TENORS = 2;
const MAX_APPROVED_STRIKES = 3;
const NORMAL_TARGET_ABS_DELTA = 0.20;
const SHORT_TENOR_DAYS = 90;
const LONG_TENOR_DAYS = 180;
interface StrikeSelectionConfig {
    minAbsDelta: number;
    maxAbsDelta: number;
    targetAbsDelta: number;
    maxMoneynessPct: number;
}

function latestOneDayMovePct(symbolData: SymbolData): number | null {
    const history = symbolData.price_history;
    if (history.length < 2) {
        return null;
    }

    const previousClose = history[history.length - 2]?.close;
    const latestClose = history[history.length - 1]?.close;
    if (!previousClose || !latestClose) {
        return null;
    }

    return ((latestClose - previousClose) / previousClose) * 100;
}

const DEFAULT_STRIKE_SELECTION: StrikeSelectionConfig = {
    minAbsDelta: 0.15,
    maxAbsDelta: 0.40,
    targetAbsDelta: NORMAL_TARGET_ABS_DELTA,
    maxMoneynessPct: 92
};

const COMMODITY_BETA_STRIKE_SELECTION: StrikeSelectionConfig = {
    minAbsDelta: 0.10,
    maxAbsDelta: 0.28,
    targetAbsDelta: 0.15,
    maxMoneynessPct: 85
};

const COMMODITY_BETA_SYMBOLS = new Set(['GDX', 'USO']);
const HIGH_BETA_THEME_SYMBOLS = new Set(['PLTR', 'TSLA', 'MSTR', 'CRCL', 'COIN', 'LI', 'FUTU']);

function clamp(value: number, min: number, max: number): number {
    return Math.min(Math.max(value, min), max);
}

function getTargetCouponPct(historicalVolatility: number, symbol?: string): number {
    const normalizedSymbol = symbol?.toUpperCase();
    const isCommodityBeta = normalizedSymbol ? COMMODITY_BETA_SYMBOLS.has(normalizedSymbol) : false;

    if (historicalVolatility > 0.6) {
        return isCommodityBeta ? 15 : 20;
    }

    if (historicalVolatility >= 0.3) {
        return isCommodityBeta ? 12 : 15;
    }

    return isCommodityBeta ? 8 : 10;
}

export function getStrikeSelectionConfig(symbol: string): StrikeSelectionConfig {
    return COMMODITY_BETA_SYMBOLS.has(symbol.toUpperCase())
        ? COMMODITY_BETA_STRIKE_SELECTION
        : DEFAULT_STRIKE_SELECTION;
}

function daysUntil(dateString?: string | null): number | null {
    if (!dateString) {
        return null;
    }

    const today = new Date();
    const target = new Date(dateString);
    if (Number.isNaN(target.getTime())) {
        return null;
    }

    const diff = target.getTime() - today.getTime();
    return Math.ceil(diff / DAY_IN_MS);
}

function uniqueExpiryDates(chainData: ChainData): string[] {
    return [...new Set(chainData.map((strike) => strike.expiry_date))];
}

function nearestPreferredTenor(expiryDate: string): number | null {
    const dte = daysUntil(expiryDate);
    if (dte === null) {
        return null;
    }

    let bestTenor: number | null = null;
    let bestDistance = Number.POSITIVE_INFINITY;

    for (const tenor of PREFERRED_TENORS) {
        const distance = Math.abs(dte - tenor);
        if (distance < bestDistance) {
            bestDistance = distance;
            bestTenor = tenor;
        }
    }

    if (bestDistance > PREFERRED_TENOR_TOLERANCE_DAYS || bestTenor === null) {
        return null;
    }

    return bestTenor;
}

function buildReasoningText(
    symbol: string,
    grade: OverallGrade,
    tenorDays: number | null,
    strike: number | null,
    couponRange: string | null,
    flags: Flag[]
): string {
    const gradeText =
        grade === 'GO'
            ? 'The name remains structurally usable for FCN discussion.'
            : grade === 'CAUTION'
              ? 'The name is usable only with tighter risk discipline.'
              : 'The name is not suitable for FCN pitching today.';

    const structureText =
        tenorDays !== null && strike !== null && couponRange !== null
            ? `Current best reference is a ${tenorDays}-day tenor around strike ${strike.toFixed(2)}, with estimated coupon range ${couponRange}.`
            : tenorDays !== null && strike !== null
              ? `Current best reference is a ${tenorDays}-day tenor around strike ${strike.toFixed(2)}, but no coupon range estimate is available from market data.`
            : 'No tenor and strike combination passed the current screening constraints.';

    const flagSummary =
        flags.length > 0
            ? `Key watchpoints: ${flags.map((flag) => flag.message).join('; ')}.`
            : `No immediate event or structure flags were triggered for ${symbol}.`;

    return `${gradeText} ${structureText} ${flagSummary}`;
}

function deriveOverallGrade(compositeScore: number): OverallGrade {
    if (compositeScore >= 0.65) {
        return 'GO';
    }

    if (compositeScore >= 0.45) {
        return 'CAUTION';
    }

    return 'AVOID';
}

export function shouldPreferTenorCandidate(input: {
    candidateTenorDays: number | null;
    candidateCouponDistance: number;
    candidateCompositeScore: number;
    candidateRefCouponPct: number | null;
    bestTenorDays: number | null;
    bestCouponDistance: number;
    bestCompositeScore: number;
    bestRefCouponPct: number | null;
    daysToEarnings?: number | null;
}): boolean {
    const {
        candidateTenorDays,
        candidateCouponDistance,
        candidateCompositeScore,
        candidateRefCouponPct,
        bestTenorDays,
        bestCouponDistance,
        bestCompositeScore,
        bestRefCouponPct
    } = input;

    if (bestTenorDays === null || candidateTenorDays === null || candidateTenorDays === bestTenorDays) {
        return false;
    }

    if (candidateTenorDays === LONG_TENOR_DAYS && bestTenorDays === SHORT_TENOR_DAYS) {
        // Require strongly GO composite score (not just passing GO threshold)
        if (candidateCompositeScore < 0.80) {
            return false;
        }

        // Require the 180-day annualized coupon itself to be sufficiently attractive
        if ((candidateRefCouponPct ?? 0) < 20) {
            return false;
        }

        // Require next earnings to be at least 30 days away — avoid near-term binary risk
        // at the start of a 6-month commitment (note: earnings within the full 180-day
        // window is expected and acceptable for quarterly reporters)
        const daysToEarnings = input.daysToEarnings ?? null;
        if (daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings < 30) {
            return false;
        }

        // Require meaningful annualized coupon improvement over the 90-day alternative
        const couponLevelImprovement = (candidateRefCouponPct ?? Number.NEGATIVE_INFINITY) - (bestRefCouponPct ?? Number.NEGATIVE_INFINITY);
        return couponLevelImprovement >= 10;
    }

    return false;
}

export function checkEligibility(symbolData: SymbolData): { eligible: boolean; flags: Flag[] } {
    const flags: Flag[] = [];
    const hasFullYearHistory = symbolData.price_history.length >= 200;

    if (symbolData.house_override === 'FORCE_AVOID') {
        flags.push({
            type: 'HOUSE_OVERRIDE',
            severity: 'BLOCK',
            message: 'House override forces avoid regardless of model output'
        });
    }

    if (symbolData.pct_from_52w_high < -50) {
        if (hasFullYearHistory) {
            flags.push({
                type: 'BROKEN_TREND',
                severity: 'WARN',
                message: 'Price is more than 50% below the 52-week high'
            });
        } else {
            flags.push({
                type: 'LOWER_HIGH_RISK',
                severity: 'WARN',
                message: 'Price is materially below the available trading-range high, but listed history is shorter than 200 days'
            });
        }
    } else if (symbolData.pct_from_52w_high < -30) {
        flags.push({
            type: 'LOWER_HIGH_RISK',
            severity: 'WARN',
            message: 'Price is more than 30% below the 52-week high'
        });
    }

    // Earnings proximity is temporarily disabled here because event handling
    // is maintained manually via house_overrides or internal workflows.

    if (symbolData.current_price < symbolData.ma200) {
        flags.push({
            type: 'BEARISH_STRUCTURE',
            severity: 'WARN',
            message: 'Spot is below the 200-day moving average'
        });
    }

    const eligible = !flags.some((flag) => flag.severity === 'BLOCK');
    return { eligible, flags };
}

export function approveTenors(symbolData: SymbolData, chainData: ChainData): TenorWindow[] {
    const earningsInDays = daysUntil(symbolData.earnings_date);
    const windows: TenorWindow[] = [];

    for (const expiryDate of uniqueExpiryDates(chainData)) {
        const tenorDays = daysUntil(expiryDate);
        const preferredTenor = nearestPreferredTenor(expiryDate);

        if (tenorDays === null || preferredTenor === null) {
            continue;
        }

        if (earningsInDays !== null && earningsInDays >= 0 && earningsInDays <= 3 && earningsInDays <= tenorDays) {
            continue;
        }

        const strikes = chainData.filter((strike) => strike.expiry_date === expiryDate);
        if (strikes.length === 0) {
            continue;
        }

        windows.push({
            tenor_days: tenorDays,
            preferred_tenor_days: preferredTenor,
            expiry_date: expiryDate,
            strikes
        });
    }

    const ranked = windows
        .map((window) => {
            const avgIv =
                window.strikes.reduce((sum, strike) => sum + strike.iv, 0) / window.strikes.length;
            const ivRichness = avgIv / Math.max(window.tenor_days, 1);
            return { window, ivRichness };
        })
        .sort((a, b) => b.ivRichness - a.ivRichness)
        .slice(0, MAX_APPROVED_TENORS)
        .map((entry) => entry.window);

    return ranked;
}

export function approveStrikes(
    symbol: string,
    symbolData: SymbolData,
    tenorData: TenorWindow,
    config: StrikeSelectionConfig = getStrikeSelectionConfig(symbol)
): StrikeData[] {
    const historicalVolatility = calculateHistoricalVolatility(symbolData.price_history);
    const targetCouponPct = getTargetCouponPct(historicalVolatility, symbol);

    return tenorData.strikes
        .filter((strike) => Math.abs(strike.delta) >= config.minAbsDelta && Math.abs(strike.delta) <= config.maxAbsDelta)
        .filter((strike) => (strike.strike / symbolData.current_price) * 100 <= config.maxMoneynessPct)
        .filter((strike) => strike.open_interest >= 10)
        .sort((a, b) => {
            const refCouponPctA = calculateRefCouponPct(a, tenorData.tenor_days);
            const refCouponPctB = calculateRefCouponPct(b, tenorData.tenor_days);
            const couponDistanceA =
                refCouponPctA === null ? Number.POSITIVE_INFINITY : Math.abs(refCouponPctA - targetCouponPct);
            const couponDistanceB =
                refCouponPctB === null ? Number.POSITIVE_INFINITY : Math.abs(refCouponPctB - targetCouponPct);

            if (couponDistanceA !== couponDistanceB) {
                return couponDistanceA - couponDistanceB;
            }

            const deltaDistanceA = Math.abs(Math.abs(a.delta) - config.targetAbsDelta);
            const deltaDistanceB = Math.abs(Math.abs(b.delta) - config.targetAbsDelta);
            if (deltaDistanceA !== deltaDistanceB) {
                return deltaDistanceA - deltaDistanceB;
            }
            return b.open_interest - a.open_interest;
        })
        .slice(0, MAX_APPROVED_STRIKES);
}

export function scoreAndGrade(candidate: {
    symbol: string;
    symbolData: SymbolData;
    tenorData: TenorWindow;
    strikeData: StrikeData;
    hasRecentEarnings?: boolean;
    sentimentProxy?: number | null;
    hasMaterialNegativeNews?: boolean;
}): ScoringResult {
    const { symbol, symbolData, tenorData, strikeData } = candidate;
    const flags: Flag[] = [];

    const ivRankScore = clamp(strikeData.iv, 0, 1);

    let structureScore = 0.5;
    if (
        symbolData.current_price > symbolData.ma20 &&
        symbolData.ma20 > symbolData.ma50 &&
        symbolData.ma50 > symbolData.ma200
    ) {
        structureScore = 0.95;
    } else if (symbolData.current_price > symbolData.ma50 && symbolData.ma50 > symbolData.ma200) {
        structureScore = 0.8;
    } else if (symbolData.current_price > symbolData.ma200) {
        structureScore = 0.65;
    } else {
        structureScore = 0.3;
    }

    const normalizedPctFromHigh = clamp((symbolData.pct_from_52w_high + 60) / 60, 0, 1);
    const macdScore = scoreMacdMomentum(symbolData);
    const rsiScore = scoreRsiStrength(symbolData.rsi_14 ?? null);
    const trendScore = clamp(
        (structureScore * 0.40) + (normalizedPctFromHigh * 0.20) + (macdScore * 0.25) + (rsiScore * 0.15),
        0,
        1
    );

    const skewValue = strikeData.skew ?? 0.25;
    const skewScore = clamp(1 - skewValue, 0, 1);

    let eventRiskScore = 1;
    const daysToEarnings = symbolData.days_to_earnings ?? null;
    if (daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 3) {
        eventRiskScore = 0.1;
        flags.push({
            type: 'EARNINGS_PROXIMITY',
            severity: 'WARN',
            message: `Earnings are due in ${daysToEarnings} day(s), near-term event risk is elevated`
        });
    } else if (daysToEarnings !== null && daysToEarnings >= 4 && daysToEarnings <= 14) {
        eventRiskScore = 0.4;
        flags.push({
            type: 'EARNINGS_PROXIMITY',
            severity: 'WARN',
            message: `Earnings are due in ${daysToEarnings} day(s), event risk should be monitored closely`
        });
    } else {
        const ivRank = symbolData.iv_rank;
        if (ivRank !== null) {
            eventRiskScore = clamp(1 - Math.max(0, ivRank - 0.5) * 0.4, 0.80, 1.0);
        }
    }

    const premiumScore = adjustedPremiumScore(strikeData);

    const baseCompositeScore = clamp(
        (trendScore * 0.40) +
            (eventRiskScore * 0.25) +
            (((ivRankScore * 0.40) + ((premiumScore ?? 0.5) * 0.40) + (skewScore * 0.20)) * 0.35),
        0,
        1
    );

    if (symbolData.pct_from_52w_high < -30) {
        flags.push({
            type: 'LOWER_HIGH_RISK',
            severity: 'WARN',
            message: 'Trend damage remains material versus the 52-week high'
        });
    }

    if (symbolData.current_price < symbolData.ma200) {
        flags.push({
            type: 'BEARISH_STRUCTURE',
            severity: 'WARN',
            message: 'Long-term trend remains below the 200-day average'
        });
    }

    const couponEstimate = calculateEstimatedCouponRange(strikeData, tenorData.tenor_days);
    const refCouponPct = couponEstimate?.lowerBound ?? null;
    const estimatedCouponRange = couponEstimate?.label ?? null;
    const moneynessPct = (strikeData.strike / symbolData.current_price) * 100;

    if (refCouponPct !== null && refCouponPct < 8) {
        flags.push({
            type: 'LOW_COUPON',
            severity: refCouponPct < 6 ? 'BLOCK' : 'WARN',
            message:
                refCouponPct < 6
                    ? `Estimated coupon range lower bound is only ${Math.round(refCouponPct)}%, below hard floor`
                    : `Estimated coupon range lower bound is only ${Math.round(refCouponPct)}%, below caution floor`
        });
    }

    let compositeScore = baseCompositeScore;
    const oneDayMovePct = latestOneDayMovePct(symbolData);
    const hasMaterialNewsShock =
        (candidate.hasMaterialNegativeNews ?? false) &&
        oneDayMovePct !== null &&
        oneDayMovePct <= -15 &&
        (symbolData.current_price < symbolData.ma50 || symbolData.pct_from_52w_high < -25);

    if (hasMaterialNewsShock) {
        flags.push({
            type: 'MATERIAL_NEWS_SHOCK',
            severity: 'WARN',
            message: `Material adverse news coincided with a ${Math.abs(oneDayMovePct).toFixed(1)}% one-day drop`
        });
    } else if (candidate.hasMaterialNegativeNews ?? false) {
        flags.push({
            type: 'MATERIAL_NEWS_OVERHANG',
            severity: 'WARN',
            message: 'Material adverse news overhang remains in place and should cap conviction'
        });
    }

    if ((candidate.hasRecentEarnings ?? false) && (candidate.sentimentProxy ?? null) !== null && (candidate.sentimentProxy ?? 1) < 0.4) {
        const adjustedEventRiskScore = Math.min(eventRiskScore, 0.35);
        const eventRiskDelta = eventRiskScore - adjustedEventRiskScore;
        eventRiskScore = adjustedEventRiskScore;
        compositeScore = clamp(compositeScore - (eventRiskDelta * 0.25), 0, 1);
    }

    const hardAvoidTriggered = shouldAvoidResult({
        symbolData,
        strikeData,
        refCouponPct,
        flags
    });

    let overallGrade: OverallGrade;

    if (hardAvoidTriggered) {
        const moneynessThresholdMet = moneynessPct <= 75;
        const highCouponOverride =
            refCouponPct !== null &&
            refCouponPct >= 20 &&
            moneynessThresholdMet &&
        strikeData.open_interest >= 10 &&
            daysToEarnings !== null &&
            daysToEarnings > 3 &&
            (candidate.sentimentProxy ?? 0) >= 0.35;

        if (highCouponOverride) {
            compositeScore = clamp(compositeScore, 0.45, 0.55);
            flags.push({
                type: 'HIGH_COUPON_OVERRIDE',
                severity: 'INFO',
                message: '高票息执行价已充分反映下行风险，升级为CAUTION'
            });
            overallGrade = 'CAUTION';
        } else {
        // Hard avoid penalty: multiply final composite score by 0.4
        // This ensures AVOID grade always scores below CAUTION threshold (0.45)
        // e.g. original 0.80 → 0.32 after penalty
        // Penalty factor 0.4 is empirical, review after 30 days of data
            compositeScore = clamp(compositeScore * 0.4, 0, 1);
            overallGrade = deriveOverallGrade(compositeScore);
        }
    } else {
        overallGrade = deriveOverallGrade(compositeScore);
    }

    const isCommodityBeta = COMMODITY_BETA_SYMBOLS.has(symbol.toUpperCase());
    const isHighBetaTheme = HIGH_BETA_THEME_SYMBOLS.has(symbol.toUpperCase());
    const hasBearishStructureFlag = flags.some((flag) => flag.type === 'BEARISH_STRUCTURE');
    const hasLowerHighRiskFlag = flags.some((flag) => flag.type === 'LOWER_HIGH_RISK');
    const commodityBetaNeedsCaution =
        isCommodityBeta &&
        !hardAvoidTriggered &&
        (
            moneynessPct > 85 ||
            strikeData.iv >= 0.4 ||
            symbolData.pct_from_52w_high < -20 ||
            symbolData.current_price < symbolData.ma20
        );

    const highBetaThemeNeedsCaution =
        isHighBetaTheme &&
        !hardAvoidTriggered &&
        (
            hasBearishStructureFlag ||
            hasLowerHighRiskFlag ||
            strikeData.iv >= 0.55 ||
            moneynessPct > 80 ||
            symbolData.pct_from_52w_high > -15 ||
            refCouponPct === null ||
            refCouponPct < 16
        );

    if (commodityBetaNeedsCaution && overallGrade === 'GO') {
        compositeScore = Math.min(compositeScore, 0.62);
        overallGrade = 'CAUTION';
        flags.push({
            type: 'COMMODITY_BETA_CAUTION',
            severity: 'WARN',
            message: 'Commodity-linked ETF uses tighter FCN guardrails because macro, curve, and beta sensitivity can amplify drawdowns'
        });
    }

    if (highBetaThemeNeedsCaution && overallGrade === 'GO') {
        compositeScore = Math.min(compositeScore, 0.6);
        overallGrade = 'CAUTION';
        flags.push({
            type: 'HIGH_BETA_THEME_CAUTION',
            severity: 'WARN',
            message: 'High-beta thematic names require tighter FCN guardrails because valuation, sentiment, and narrative shifts can amplify downside risk'
        });
    }

    if ((candidate.hasMaterialNegativeNews ?? false) && overallGrade === 'GO') {
        compositeScore = Math.min(compositeScore, 0.62);
        overallGrade = 'CAUTION';
    }

    const reasoningText = buildReasoningText(
        symbol,
        overallGrade,
        tenorData.tenor_days,
        strikeData.strike,
        estimatedCouponRange,
        flags
    );

    return {
        symbol,
        overall_grade: overallGrade,
        composite_score: Number(compositeScore.toFixed(4)),
        risk_reward_score: null,
        iv_rank_score: Number(ivRankScore.toFixed(4)),
        trend_score: Number(trendScore.toFixed(4)),
        skew_score: Number(skewScore.toFixed(4)),
        event_risk_score: Number(eventRiskScore.toFixed(4)),
        premium_score: premiumScore !== null ? Number(premiumScore.toFixed(4)) : null,
        selected_implied_volatility: Number.isFinite(strikeData.iv) ? Number(strikeData.iv.toFixed(4)) : null,
        recommended_strike: strikeData.strike,
        recommended_tenor_days: tenorData.tenor_days,
        recommended_expiry_date: tenorData.expiry_date,
        estimated_coupon_range: estimatedCouponRange,
        ref_coupon_pct: refCouponPct !== null ? Number(refCouponPct.toFixed(1)) : null,
        moneyness_pct: Number(moneynessPct.toFixed(2)),
        current_price: symbolData.current_price,
        ma20: symbolData.ma20,
        ma50: symbolData.ma50,
        ma200: symbolData.ma200,
        pct_from_52w_high: symbolData.pct_from_52w_high,
        reasoning_text: reasoningText,
        flags
    };
}

function shouldAvoidResult(input: {
    symbolData: SymbolData;
    strikeData: StrikeData;
    refCouponPct: number | null;
    flags: Flag[];
}): boolean {
    const daysToEarnings = input.symbolData.days_to_earnings ?? null;
    if (daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 3) {
        return true;
    }

    if (input.strikeData.open_interest < 10) {
        return true;
    }

    if (input.refCouponPct !== null && input.refCouponPct < 6) {
        return true;
    }

    const hasBearishStructure = input.flags.some((flag) => flag.type === 'BEARISH_STRUCTURE');
    const hasLowCoupon = input.flags.some((flag) => flag.type === 'LOW_COUPON');
    const hasLowLiquidity = input.flags.some((flag) => flag.type === 'LOW_LIQUIDITY');

    if (
        hasBearishStructure &&
        (hasLowCoupon || hasLowLiquidity || input.symbolData.pct_from_52w_high < -40)
    ) {
        return true;
    }

    const hasMaterialNewsShock = input.flags.some((flag) => flag.type === 'MATERIAL_NEWS_SHOCK');
    if (hasMaterialNewsShock) {
        return true;
    }

    return false;
}

function applyHighVolCautionOverride(input: {
    result: ScoringResult;
    symbolData: SymbolData;
    strikeData: StrikeData;
}): ScoringResult {
    const historicalVolatility = calculateHistoricalVolatility(input.symbolData.price_history);
    const hasDrawdownRisk = input.result.flags.some(
        (flag) => flag.type === 'BEARISH_STRUCTURE' || flag.type === 'LOWER_HIGH_RISK'
    );
    const isHardAvoid = shouldAvoidResult({
        symbolData: input.symbolData,
        strikeData: input.strikeData,
        refCouponPct: input.result.ref_coupon_pct,
        flags: input.result.flags
    });

    if (
        historicalVolatility <= 0.8 ||
        !hasDrawdownRisk ||
        (input.result.ref_coupon_pct ?? 0) < 15 ||
        isHardAvoid
    ) {
        return input.result;
    }

    const flags = mergeUniqueFlags(input.result.flags, [
        {
            type: 'HIGH_VOL_LOW_STRIKE',
            severity: 'WARN',
            message: 'High-volatility caution applied because spot is in a deeper drawdown regime'
        }
    ]);

    const adjustedCompositeScore = Math.max(input.result.composite_score * 0.65, 0.45);
    const adjustedGrade = deriveOverallGrade(adjustedCompositeScore);

    return {
        ...input.result,
        overall_grade: adjustedGrade,
        composite_score: Number(adjustedCompositeScore.toFixed(4)),
        risk_reward_score: input.result.risk_reward_score,
        flags,
        reasoning_text: buildReasoningText(
            input.result.symbol,
            adjustedGrade,
            input.result.recommended_tenor_days,
            input.result.recommended_strike,
            input.result.estimated_coupon_range,
            flags
        )
    };
}

export async function runDailyScreener(
    symbols: string[],
    dataFetcher: DataFetcherInterface
): Promise<ScoringResult[]> {
    const results: ScoringResult[] = [];

    for (const symbol of symbols) {
        const symbolData = await dataFetcher.fetchSymbolData(symbol);
        const newsContext = await fetchStockNewsContext(symbol, getCompanyName(symbol));
        const eligibility = checkEligibility(symbolData);

        if (!eligibility.eligible) {
            results.push({
                symbol,
                overall_grade: 'AVOID',
                composite_score: 0,
                risk_reward_score: null,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: 0,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: buildReasoningText(symbol, 'AVOID', null, null, null, eligibility.flags),
                flags: eligibility.flags
            });
            continue;
        }

        const chainData = await dataFetcher.fetchChainData(symbol, symbolData.current_price);
        const approvedTenors = approveTenors(symbolData, chainData);

        if (approvedTenors.length === 0) {
            const daysToEarnings = symbolData.days_to_earnings ?? null;
            const flags = [
                ...eligibility.flags,
                ...(daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 14
                    ? [
                          {
                              type: 'EARNINGS_PROXIMITY' as const,
                              severity: 'WARN' as const,
                              message:
                                  daysToEarnings === 0
                                      ? 'Earnings are due today, near-term event risk blocks FCN setup'
                                      : `Earnings are due in ${daysToEarnings} day(s), near-term event risk blocks FCN setup`
                          }
                      ]
                    : []),
                {
                    type: 'NO_APPROVED_TENOR' as const,
                    severity: 'WARN' as const,
                    message:
                        daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 14
                            ? 'No tenor windows passed because the earnings event falls inside the FCN tenor window'
                            : 'No tenor windows passed earnings and richness checks'
                }
            ];

            results.push({
                symbol,
                overall_grade: 'AVOID',
                composite_score: 0.2,
                risk_reward_score: null,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: 0,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: buildReasoningText(symbol, 'AVOID', null, null, null, flags),
                flags
            });
            continue;
        }

        const targetCouponPct = getTargetCouponPct(calculateHistoricalVolatility(symbolData.price_history), symbol);
        let best90: { result: ScoringResult; couponDistance: number; strikeData: StrikeData; tenorBucketDays: number } | null = null;
        let best180: { result: ScoringResult; couponDistance: number; strikeData: StrikeData; tenorBucketDays: number } | null = null;

        const shouldReplaceSameTenorChoice = (
            candidate: { result: ScoringResult; couponDistance: number },
            current: { result: ScoringResult; couponDistance: number } | null
        ) => {
            if (!current) {
                return true;
            }

            return (
                candidate.couponDistance < current.couponDistance ||
                (candidate.couponDistance === current.couponDistance &&
                    candidate.result.composite_score > current.result.composite_score) ||
                (candidate.couponDistance === current.couponDistance &&
                    candidate.result.composite_score === current.result.composite_score &&
                    (candidate.result.ref_coupon_pct ?? 0) > (current.result.ref_coupon_pct ?? 0))
            );
        };

        for (const tenorData of approvedTenors) {
            const approvedStrikes = approveStrikes(symbol, symbolData, tenorData, getStrikeSelectionConfig(symbol));

            if (approvedStrikes.length === 0) {
                const emptyStrikeFlags = [
                    ...eligibility.flags,
                    {
                        type: 'NO_APPROVED_STRIKE' as const,
                        severity: 'WARN' as const,
                        message: `No strikes met the delta and liquidity filters for ${tenorData.tenor_days}d tenor`
                    }
                ];

                const emptyStrikeResult: ScoringResult = {
                    symbol,
                    overall_grade: 'AVOID',
                    composite_score: 0.25,
                    risk_reward_score: null,
                    iv_rank_score: 0,
                    trend_score: 0,
                    skew_score: 0,
                    event_risk_score: 0,
                    premium_score: null,
                    selected_implied_volatility: null,
                    recommended_strike: null,
                    recommended_tenor_days: tenorData.tenor_days,
                    recommended_expiry_date: tenorData.expiry_date,
                    estimated_coupon_range: null,
                    ref_coupon_pct: null,
                    moneyness_pct: null,
                    current_price: symbolData.current_price,
                    ma20: symbolData.ma20,
                    ma50: symbolData.ma50,
                    ma200: symbolData.ma200,
                    pct_from_52w_high: symbolData.pct_from_52w_high,
                    reasoning_text: buildReasoningText(symbol, 'AVOID', tenorData.tenor_days, null, null, emptyStrikeFlags),
                    flags: emptyStrikeFlags
                };

                const emptyChoice = {
                    result: emptyStrikeResult,
                    couponDistance: Number.POSITIVE_INFINITY,
                    tenorBucketDays: tenorData.preferred_tenor_days,
                    strikeData: {
                        strike: 0,
                        iv: 0,
                        delta: 0,
                        volume: 0,
                        open_interest: 0,
                        mid_price: null,
                        mid_price_source: 'none' as const,
                        expiry_date: tenorData.expiry_date
                    }
                };

                if (tenorData.preferred_tenor_days === SHORT_TENOR_DAYS && shouldReplaceSameTenorChoice(emptyChoice, best90)) {
                    best90 = emptyChoice;
                } else if (tenorData.preferred_tenor_days === LONG_TENOR_DAYS && shouldReplaceSameTenorChoice(emptyChoice, best180)) {
                    best180 = emptyChoice;
                }
                continue;
            }

            for (const strikeData of approvedStrikes) {
                const candidateResult = scoreAndGrade({
                    symbol,
                    symbolData,
                    tenorData,
                    strikeData,
                    hasRecentEarnings: newsContext.hasRecentEarnings,
                    sentimentProxy: newsContext.sentimentProxy,
                    hasMaterialNegativeNews: newsContext.hasMaterialNegativeNews
                });

                candidateResult.flags = mergeUniqueFlags(eligibility.flags, candidateResult.flags);

                const couponDistance =
                    candidateResult.ref_coupon_pct === null
                        ? Number.POSITIVE_INFINITY
                        : Math.abs(candidateResult.ref_coupon_pct - targetCouponPct);
                const candidateChoice = {
                    result: candidateResult,
                    couponDistance,
                    tenorBucketDays: tenorData.preferred_tenor_days,
                    strikeData
                };
                const tenorDays = tenorData.preferred_tenor_days;

                if (tenorDays === SHORT_TENOR_DAYS && shouldReplaceSameTenorChoice(candidateChoice, best90)) {
                    best90 = candidateChoice;
                } else if (tenorDays === LONG_TENOR_DAYS && shouldReplaceSameTenorChoice(candidateChoice, best180)) {
                    best180 = candidateChoice;
                }
            }
        }

        let bestChoice = best90;
        if (
            best90 !== null &&
            best180 !== null &&
            shouldPreferTenorCandidate({
                candidateTenorDays: best180.tenorBucketDays,
                candidateCouponDistance: best180.couponDistance,
                candidateCompositeScore: best180.result.composite_score,
                candidateRefCouponPct: best180.result.ref_coupon_pct,
                bestTenorDays: best90.tenorBucketDays,
                bestCouponDistance: best90.couponDistance,
                bestCompositeScore: best90.result.composite_score,
                bestRefCouponPct: best90.result.ref_coupon_pct,
                daysToEarnings: symbolData.days_to_earnings ?? null
            })
        ) {
            bestChoice = best180;
        }

        if (bestChoice === null) {
            const flags: Flag[] = [
                ...eligibility.flags,
                {
                    type: 'NO_APPROVED_TENOR',
                    severity: 'WARN',
                    message: 'No 90-day tenor baseline passed current screening constraints'
                }
            ];

            results.push({
                symbol,
                overall_grade: 'AVOID',
                composite_score: 0.2,
                risk_reward_score: null,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: 0,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: buildReasoningText(symbol, 'AVOID', null, null, null, flags),
                flags
            });
            continue;
        }

        results.push(
            applyHighVolCautionOverride({
                result: bestChoice.result,
                symbolData,
                strikeData: bestChoice.strikeData
            })
        );
    }

    return results.sort((a, b) => b.composite_score - a.composite_score);
}

function adjustedPremiumScore(strikeData: StrikeData): number | null {
    if (strikeData.mid_price === null) {
        return null;
    }

    return clamp((strikeData.mid_price / strikeData.strike) * 100, 0, 1);
}

export function calculateHistoricalVolatility(
    priceHistory: Array<{
        date: string;
        close: number;
    }>
): number {
    const closes = priceHistory.slice(-30).map((row) => row.close).filter((close) => Number.isFinite(close) && close > 0);
    if (closes.length < 10) {
        return 0;
    }

    const returns: number[] = [];
    for (let index = 1; index < closes.length; index += 1) {
        returns.push(Math.log(closes[index] / closes[index - 1]));
    }

    if (returns.length < 2) {
        return 0;
    }

    const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
    const variance = returns.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / (returns.length - 1);
    return Math.sqrt(variance) * Math.sqrt(252);
}

function scoreMacdMomentum(symbolData: SymbolData): number {
    const macdLine = symbolData.macd_line ?? null;
    const signalLine = symbolData.macd_signal ?? null;
    const histogram = symbolData.macd_histogram ?? null;

    if (macdLine === null || signalLine === null || histogram === null) {
        return 0.5;
    }

    if (macdLine > signalLine && histogram > 0) {
        return 0.9;
    }

    if (macdLine > signalLine || histogram > 0) {
        return 0.7;
    }

    if (Math.abs(histogram) < 0.05) {
        return 0.5;
    }

    return 0.25;
}

function scoreRsiStrength(rsi: number | null): number {
    if (rsi === null || !Number.isFinite(rsi)) {
        return 0.5;
    }

    if (rsi >= 50 && rsi <= 70) {
        return 0.9;
    }

    if (rsi > 70 && rsi <= 80) {
        return 0.7;
    }

    if (rsi >= 40 && rsi < 50) {
        return 0.6;
    }

    if (rsi >= 30 && rsi < 40) {
        return 0.35;
    }

    return 0.25;
}

function calculateRefCouponPct(strikeData: StrikeData, tenorDays: number): number | null {
    if (strikeData.mid_price === null || tenorDays <= 0) {
        return null;
    }

    return (strikeData.mid_price / strikeData.strike) * (365 / tenorDays) * 100;
}

function calculateEstimatedCouponRange(
    strikeData: StrikeData,
    tenorDays: number
): { lowerBound: number; upperBound: number; label: string } | null {
    const referenceCoupon = calculateRefCouponPct(strikeData, tenorDays);

    if (!Number.isFinite(referenceCoupon ?? NaN) || referenceCoupon === null || referenceCoupon <= 0) {
        return null;
    }

    const lowerBound = referenceCoupon * 0.85;
    const upperBound = referenceCoupon * 1.15;
    return {
        lowerBound,
        upperBound,
        label: `${lowerBound.toFixed(0)}%-${upperBound.toFixed(0)}%`
    };
}

function mergeUniqueFlags(...flagGroups: Flag[][]): Flag[] {
    const byType = new Map<FlagType, Flag>();

    for (const flag of flagGroups.flat()) {
        const existing = byType.get(flag.type);
        if (!existing) {
            byType.set(flag.type, flag);
            continue;
        }

        if (severityRank(flag.severity) > severityRank(existing.severity)) {
            byType.set(flag.type, flag);
        }
    }

    return [...byType.values()];
}

function severityRank(severity: FlagSeverity): number {
    return severity === 'BLOCK' ? 3 : severity === 'WARN' ? 2 : 1;
}
