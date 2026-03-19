import { pool } from '../client';
import type { DailyBestCard, Flag, SymbolIdeaResponse, TodayIdeasResponse } from '../../types/api';

export interface LatestCompletedRun {
    run_id: string;
    run_date: string;
}

export interface UnderlyingRow {
    symbol: string;
    exchange: string;
    company_name: string | null;
    sector: string | null;
    themes: string[];
    tier: number;
}

export interface EarningsCalendarRow {
    symbol: string;
    report_date: string;
    days_until: number | null;
}

export interface PriceContextRow {
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

export interface DailyBestHistoryRow {
    symbol: string;
    run_date: string;
    composite_score: number | null;
    theme: string | null;
}

export interface TodayIdeaRow {
    run_id: string;
    run_date: string;
    symbol: string;
    exchange: string;
    company_name: string | null;
    sector: string | null;
    themes: string[];
    tier: number;
    overall_grade: 'GO' | 'CAUTION' | 'AVOID';
    composite_score: number;
    recommended_strike: number | null;
    // standard PB FCN tenors: 90d or 180d
    recommended_tenor_days: number | null;
    ref_coupon_pct: number | null;
    moneyness_pct: number | null;
    why_now: string | null;
    risk_note: string | null;
    sentiment_score: number | null;
    reasoning_text: string;
    current_price: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    pct_from_52w_high: number | null;
    selected_implied_volatility: number | null;
}

export interface RiskFlagRow {
    run_id: string;
    symbol: string;
    flag_type: Flag['type'];
    severity: Flag['severity'];
    detail_text: string;
}

export interface CachedIdeaRow {
    run_id: string;
    run_date: string;
    created_at: string | null;
    symbol: string;
    exchange: string;
    company_name: string | null;
    overall_grade: 'GO' | 'CAUTION' | 'AVOID';
    composite_score: number | null;
    recommended_strike: number | null;
    // standard PB FCN tenors: 90d or 180d
    recommended_tenor_days: number | null;
    ref_coupon_pct: number | null;
    moneyness_pct: number | null;
    why_now: string | null;
    risk_note: string | null;
    sentiment_score: number | null;
    reasoning_text: string;
    current_price: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    pct_from_52w_high: number | null;
    selected_implied_volatility: number | null;
    earnings_date: string | null;
    days_to_earnings: number | null;
}

export interface SaveIdeaCandidateInput {
    runId: string;
    symbol: string;
    overallGrade: 'GO' | 'CAUTION' | 'AVOID';
    ivRankScore: number;
    trendScore: number;
    skewScore: number;
    eventRiskScore: number;
    compositeScore: number;
    recommendedStrike: number | null;
    // standard PB FCN tenors: 90d or 180d
    recommendedTenorDays: number | null;
    refCouponPct: number | null;
    moneynessPct: number | null;
    selectedImpliedVolatility?: number | null;
    currentPrice?: number | null;
    ma20?: number | null;
    ma50?: number | null;
    ma200?: number | null;
    pctFrom52wHigh?: number | null;
    whyNow?: string | null;
    riskNote?: string | null;
    sentimentScore?: number | null;
    reasoningText: string;
}

export interface UpsertRecommendationTrackerInput {
    symbol: string;
    grade: 'GO' | 'CAUTION';
    recommendedStrike: number | null;
    recommendedTenorDays: number | null;
    moneynessPct: number | null;
    entryPrice: number | null;
    recommendationDate: string;
}

export interface RecommendationTrackerRow {
    id: number;
    symbol: string;
    grade: 'GO' | 'CAUTION';
    recommended_strike: number | null;
    recommended_tenor_days: number | null;
    moneyness_pct: number | null;
    entry_price: number | null;
    recommendation_date: string;
    expiry_date: string | null;
    current_price: number | null;
    pct_above_strike: number | null;
    status: 'ACTIVE' | 'BREACHED' | 'EXPIRED_SAFE' | 'EXPIRED_BREACHED';
    last_checked: string | null;
    breached_date: string | null;
}

export async function getLatestCompletedRun(): Promise<LatestCompletedRun | null> {
    const result = await pool.query<LatestCompletedRun>(
        `
        SELECT run_id, run_date::text
        FROM idea_runs
        WHERE status = 'completed'
        ORDER BY run_date DESC, completed_at DESC, started_at DESC
        LIMIT 1
        `
    );

    return result.rows[0] ?? null;
}

export async function getLatestCompletedScheduledRun(): Promise<LatestCompletedRun | null> {
    const result = await pool.query<LatestCompletedRun>(
        `
        SELECT run_id, run_date::text
        FROM idea_runs
        WHERE status = 'completed'
          AND triggered_by = 'scheduled'::trigger_source
        ORDER BY run_date DESC, completed_at DESC, started_at DESC
        LIMIT 1
        `
    );

    return result.rows[0] ?? null;
}

export async function getUnderlyingBySymbol(symbol: string): Promise<UnderlyingRow | null> {
    const result = await pool.query<UnderlyingRow>(
        `
        SELECT symbol, exchange, company_name, sector, themes, tier
        FROM underlyings
        WHERE symbol = $1
        LIMIT 1
        `,
        [symbol]
    );

    return result.rows[0] ?? null;
}

export async function getUpcomingEarningsBySymbol(symbol: string): Promise<EarningsCalendarRow | null> {
    const result = await pool.query<EarningsCalendarRow>(
        `
        SELECT
            symbol,
            report_date::text AS report_date,
            days_until
        FROM earnings_calendar
        WHERE symbol = $1
          AND report_date >= CURRENT_DATE
        ORDER BY report_date ASC
        LIMIT 1
        `,
        [symbol]
    );

    return result.rows[0] ?? null;
}

export async function getIdeasByRunId(runId: string): Promise<TodayIdeaRow[]> {
    const result = await pool.query<TodayIdeaRow>(
        `
        SELECT
            ic.run_id,
            ir.run_date::text AS run_date,
            ic.created_at::date::text AS created_at,
            ic.symbol,
            u.exchange,
            u.company_name,
            u.sector,
            u.themes,
            u.tier,
            ic.overall_grade,
            ic.composite_score,
            ic.recommended_strike,
            ic.recommended_tenor_days,
            ic.ref_coupon_pct,
            ic.moneyness_pct,
            ic.why_now,
            ic.risk_note,
            ic.sentiment_score,
            ic.reasoning_text,
            ic.current_price,
            ic.ma20,
            ic.ma50,
            ic.ma200,
            ic.pct_from_52w_high,
            ic.selected_implied_volatility
        FROM idea_candidates ic
        JOIN idea_runs ir
            ON ir.run_id = ic.run_id
        JOIN underlyings u
            ON u.symbol = ic.symbol
        WHERE ic.run_id = $1
        ORDER BY ic.composite_score DESC, ic.symbol ASC
        `,
        [runId]
    );

    return result.rows;
}

export async function getRecentDailyBest(days: number): Promise<DailyBestHistoryRow[]> {
    const result = await pool.query<DailyBestHistoryRow>(
        `
        SELECT
            symbol,
            run_date::text AS run_date,
            composite_score,
            theme
        FROM daily_best_history
        WHERE run_date >= (CURRENT_DATE - ($1::int - 1))
        ORDER BY run_date DESC, created_at DESC
        `,
        [days]
    );

    return result.rows;
}

export async function getDailyBestByDate(runDate: string): Promise<DailyBestHistoryRow | null> {
    const result = await pool.query<DailyBestHistoryRow>(
        `
        SELECT
            symbol,
            run_date::text AS run_date,
            composite_score,
            theme
        FROM daily_best_history
        WHERE run_date = $1::date
        ORDER BY created_at DESC
        LIMIT 1
        `,
        [runDate]
    );

    return result.rows[0] ?? null;
}

export async function getRiskFlagsByRunId(runId: string): Promise<RiskFlagRow[]> {
    const result = await pool.query<RiskFlagRow>(
        `
        SELECT
            run_id,
            symbol,
            flag_type,
            severity,
            detail_text
        FROM risk_flags
        WHERE run_id = $1
        ORDER BY symbol ASC, created_at ASC
        `,
        [runId]
    );

    return result.rows;
}

export async function getIdeaBySymbolAndDate(symbol: string, date: string): Promise<CachedIdeaRow | null> {
    const result = await pool.query<CachedIdeaRow>(
        `
        SELECT
            ic.run_id,
            ir.run_date::text AS run_date,
            ic.created_at::date::text AS created_at,
            ic.symbol,
            u.exchange,
            u.company_name,
            ic.overall_grade,
            ic.composite_score,
            ic.recommended_strike,
            ic.recommended_tenor_days,
            ic.ref_coupon_pct,
            ic.moneyness_pct,
            ic.why_now,
            ic.risk_note,
            ic.sentiment_score,
            ic.reasoning_text,
            ic.current_price,
            ic.ma20,
            ic.ma50,
            ic.ma200,
            ic.pct_from_52w_high,
            ic.selected_implied_volatility,
            ec.report_date::text AS earnings_date,
            CASE
                WHEN ec.report_date IS NOT NULL THEN (ec.report_date - CURRENT_DATE)
                ELSE NULL
            END AS days_to_earnings
        FROM idea_candidates ic
        JOIN idea_runs ir
            ON ir.run_id = ic.run_id
        JOIN underlyings u
            ON u.symbol = ic.symbol
        LEFT JOIN LATERAL (
            SELECT report_date
            FROM earnings_calendar
            WHERE symbol = ic.symbol
              AND report_date >= CURRENT_DATE
            ORDER BY report_date ASC
            LIMIT 1
        ) ec ON TRUE
        WHERE ic.symbol = $1
          AND ir.run_date <= $2::date
        ORDER BY
            CASE WHEN ir.triggered_by = 'scheduled'::trigger_source THEN 0 ELSE 1 END,
            ir.run_date DESC,
            ir.completed_at DESC,
            ic.created_at DESC
        LIMIT 1
        `,
        [symbol, date]
    );

    return result.rows[0] ?? null;
}

export async function getIdeaBySymbolAndRunId(symbol: string, runId: string): Promise<CachedIdeaRow | null> {
    const result = await pool.query<CachedIdeaRow>(
        `
        SELECT
            ic.run_id,
            ir.run_date::text AS run_date,
            ic.symbol,
            u.exchange,
            u.company_name,
            ic.overall_grade,
            ic.composite_score,
            ic.recommended_strike,
            ic.recommended_tenor_days,
            ic.ref_coupon_pct,
            ic.moneyness_pct,
            ic.why_now,
            ic.risk_note,
            ic.sentiment_score,
            ic.reasoning_text,
            ic.current_price,
            ic.ma20,
            ic.ma50,
            ic.ma200,
            ic.pct_from_52w_high,
            ic.selected_implied_volatility,
            ec.report_date::text AS earnings_date,
            CASE
                WHEN ec.report_date IS NOT NULL THEN (ec.report_date - CURRENT_DATE)
                ELSE NULL
            END AS days_to_earnings
        FROM idea_candidates ic
        JOIN idea_runs ir
            ON ir.run_id = ic.run_id
        JOIN underlyings u
            ON u.symbol = ic.symbol
        LEFT JOIN LATERAL (
            SELECT report_date
            FROM earnings_calendar
            WHERE symbol = ic.symbol
              AND report_date >= CURRENT_DATE
            ORDER BY report_date ASC
            LIMIT 1
        ) ec ON TRUE
        WHERE ic.symbol = $1
          AND ic.run_id = $2
        LIMIT 1
        `,
        [symbol, runId]
    );

    return result.rows[0] ?? null;
}

export async function deleteTodayIdeaCandidate(symbol: string): Promise<void> {
    await pool.query(
        `
        DELETE FROM idea_candidates
        WHERE symbol = $1
          AND created_at::date = CURRENT_DATE
        `,
        [symbol]
    );
}

export async function getPriceContextBySymbol(symbol: string): Promise<PriceContextRow | null> {
    const result = await pool.query<PriceContextRow>(
        `
        SELECT
            latest_price.close AS current_price,
            ma.ma20,
            ma.ma50,
            ma.ma200,
            CASE
                WHEN highs.high_52w IS NOT NULL AND highs.high_52w <> 0 AND latest_price.close IS NOT NULL
                THEN ROUND(((latest_price.close - highs.high_52w) / highs.high_52w) * 100, 2)
                ELSE NULL
            END AS pct_from_52w_high,
            NULL::numeric AS implied_volatility,
            latest_price.data_date,
            ec.report_date::text AS earnings_date,
            CASE
                WHEN ec.report_date IS NOT NULL THEN (ec.report_date - CURRENT_DATE)
                ELSE NULL
            END AS days_to_earnings
        FROM underlyings u
        LEFT JOIN LATERAL (
            SELECT close, trade_date::text AS data_date
            FROM price_history
            WHERE symbol = u.symbol
            ORDER BY trade_date DESC
            LIMIT 1
        ) latest_price ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                AVG(close) FILTER (WHERE rn <= 20) AS ma20,
                AVG(close) FILTER (WHERE rn <= 50) AS ma50,
                AVG(close) FILTER (WHERE rn <= 200) AS ma200
            FROM (
                SELECT close, ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM price_history
                WHERE symbol = u.symbol
            ) ranked
        ) ma ON TRUE
        LEFT JOIN LATERAL (
            SELECT MAX(high) AS high_52w
            FROM price_history
            WHERE symbol = u.symbol
              AND trade_date >= (CURRENT_DATE - INTERVAL '365 days')
        ) highs ON TRUE
        LEFT JOIN LATERAL (
            SELECT report_date
            FROM earnings_calendar
            WHERE symbol = u.symbol
              AND report_date >= CURRENT_DATE
            ORDER BY report_date ASC
            LIMIT 1
        ) ec ON TRUE
        WHERE u.symbol = $1
        LIMIT 1
        `,
        [symbol]
    );

    return result.rows[0] ?? null;
}

export async function getRiskFlagsByRunAndSymbol(runId: string, symbol: string): Promise<Flag[]> {
    const result = await pool.query<RiskFlagRow>(
        `
        SELECT
            run_id,
            symbol,
            flag_type,
            severity,
            detail_text
        FROM risk_flags
        WHERE run_id = $1
          AND symbol = $2
        ORDER BY created_at ASC
        `,
        [runId, symbol]
    );

    return result.rows.map((row) => ({
        type: row.flag_type,
        severity: row.severity,
        message: row.detail_text
    }));
}

export async function saveIdeaCandidate(result: SaveIdeaCandidateInput): Promise<void> {
    await pool.query(
        `
        INSERT INTO idea_candidates (
            run_id,
            symbol,
            overall_grade,
            iv_rank_score,
            trend_score,
            skew_score,
            event_risk_score,
            composite_score,
            recommended_strike,
            recommended_tenor_days,
            ref_coupon_pct,
            moneyness_pct,
            selected_implied_volatility,
            current_price,
            ma20,
            ma50,
            ma200,
            pct_from_52w_high,
            why_now,
            risk_note,
            sentiment_score,
            reasoning_text
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
        )
        ON CONFLICT (run_id, symbol) DO UPDATE
        SET overall_grade = EXCLUDED.overall_grade,
            iv_rank_score = EXCLUDED.iv_rank_score,
            trend_score = EXCLUDED.trend_score,
            skew_score = EXCLUDED.skew_score,
            event_risk_score = EXCLUDED.event_risk_score,
            composite_score = EXCLUDED.composite_score,
            recommended_strike = EXCLUDED.recommended_strike,
            recommended_tenor_days = EXCLUDED.recommended_tenor_days,
            ref_coupon_pct = EXCLUDED.ref_coupon_pct,
            moneyness_pct = EXCLUDED.moneyness_pct,
            selected_implied_volatility = EXCLUDED.selected_implied_volatility,
            current_price = EXCLUDED.current_price,
            ma20 = EXCLUDED.ma20,
            ma50 = EXCLUDED.ma50,
            ma200 = EXCLUDED.ma200,
            pct_from_52w_high = EXCLUDED.pct_from_52w_high,
            why_now = EXCLUDED.why_now,
            risk_note = EXCLUDED.risk_note,
            sentiment_score = EXCLUDED.sentiment_score,
            reasoning_text = EXCLUDED.reasoning_text
        `,
        [
            result.runId,
            result.symbol,
            result.overallGrade,
            result.ivRankScore,
            result.trendScore,
            result.skewScore,
            result.eventRiskScore,
            result.compositeScore,
            result.recommendedStrike,
            result.recommendedTenorDays,
            result.refCouponPct,
            result.moneynessPct,
            result.selectedImpliedVolatility ?? null,
            result.currentPrice ?? null,
            result.ma20 ?? null,
            result.ma50 ?? null,
            result.ma200 ?? null,
            result.pctFrom52wHigh ?? null,
            result.whyNow ?? null,
            result.riskNote ?? null,
            result.sentimentScore ?? null,
            result.reasoningText
        ]
    );
}

export async function ensureIdeaCandidatePriceColumns(): Promise<void> {
    await pool.query(`
        ALTER TABLE idea_candidates
        ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC(10, 4),
        ADD COLUMN IF NOT EXISTS selected_implied_volatility NUMERIC(10, 6),
        ADD COLUMN IF NOT EXISTS current_price NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma20 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma50 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma200 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS pct_from_52w_high NUMERIC(10, 4)
    `);
}

export async function ensureUnderlyingCompanyNameColumn(): Promise<void> {
    await pool.query(`
        ALTER TABLE underlyings
        ADD COLUMN IF NOT EXISTS company_name VARCHAR(100)
    `);
}

export async function ensureEarningsCalendarColumns(): Promise<void> {
    await pool.query(`
        ALTER TABLE earnings_calendar
        ADD COLUMN IF NOT EXISTS days_until INTEGER
    `);

    await pool.query(`
        DELETE FROM earnings_calendar a
        USING earnings_calendar b
        WHERE a.earnings_calendar_id < b.earnings_calendar_id
          AND a.symbol = b.symbol
    `);

    await pool.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS idx_earnings_calendar_symbol_unique
        ON earnings_calendar (symbol)
    `);
}

export async function upsertEarningsCalendar(
    rows: Array<{ symbol: string; report_date: string; days_until: number }>
): Promise<void> {
    if (rows.length === 0) {
        return;
    }

    const valuesSql: string[] = [];
    const params: Array<string | number | boolean> = [];

    for (const [index, row] of rows.entries()) {
        const offset = index * 5;
        valuesSql.push(`($${offset + 1}, $${offset + 2}::date, $${offset + 3}, $${offset + 4}, $${offset + 5})`);
        params.push(row.symbol, row.report_date, row.days_until, false, 'finnhub');
    }

    await pool.query(
        `
        INSERT INTO earnings_calendar (
            symbol,
            report_date,
            days_until,
            confirmed,
            source
        ) VALUES ${valuesSql.join(', ')}
        ON CONFLICT (symbol) DO UPDATE
        SET report_date = EXCLUDED.report_date,
            days_until = EXCLUDED.days_until,
            confirmed = EXCLUDED.confirmed,
            source = EXCLUDED.source
        `,
        params
    );
}

export async function updateIdeaCandidateNarrative(
    runId: string,
    symbol: string,
    narrative: {
        why_now: string;
        risk_note: string;
        sentiment_score: number;
    }
): Promise<void> {
    await pool.query(
        `
        UPDATE idea_candidates
        SET why_now = $3,
            risk_note = $4,
            sentiment_score = $5
        WHERE run_id = $1
          AND symbol = $2
        `,
        [runId, symbol, narrative.why_now, narrative.risk_note, narrative.sentiment_score]
    );
}

export async function saveRiskFlags(runId: string, symbol: string, flags: Flag[]): Promise<void> {
    if (flags.length === 0) {
        return;
    }

    const valuesSql: string[] = [];
    const params: unknown[] = [];

    for (const [index, flag] of flags.entries()) {
        const offset = index * 5;
        valuesSql.push(`($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5})`);
        params.push(runId, symbol, flag.type, flag.severity.toLowerCase(), flag.message);
    }

    await pool.query(
        `
        INSERT INTO risk_flags (
            run_id,
            symbol,
            flag_type,
            severity,
            detail_text
        ) VALUES ${valuesSql.join(', ')}
        `,
        params
    );
}

export async function ensureRiskFlagEnumValues(): Promise<void> {
    const values: Flag['type'][] = [
        'EARNINGS_PROXIMITY',
        'BROKEN_TREND',
        'HIGH_VOL_LOW_STRIKE',
        'BEARISH_STRUCTURE',
        'LOWER_HIGH_RISK',
        'LOW_COUPON',
        'LOW_LIQUIDITY',
        'NO_APPROVED_TENOR',
        'NO_APPROVED_STRIKE',
        'HOUSE_OVERRIDE'
    ];

    for (const value of values) {
        await pool.query(`ALTER TYPE risk_flag_type ADD VALUE IF NOT EXISTS '${value}'`);
    }
}

export async function ensureDailyBestHistoryTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS daily_best_history (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
            run_date DATE NOT NULL,
            composite_score NUMERIC,
            theme TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_daily_best_history_run_date
        ON daily_best_history (run_date DESC)
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_daily_best_history_symbol_run_date
        ON daily_best_history (symbol, run_date DESC)
    `);
}

export async function upsertUnderlyingCompanyName(symbol: string, companyName: string | null): Promise<void> {
    if (!companyName) {
        return;
    }

    await pool.query(
        `
        UPDATE underlyings
        SET company_name = $2
        WHERE symbol = $1
        `,
        [symbol, companyName]
    );
}

export async function ensureRecommendationTrackerTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS recommendation_tracker (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
            grade VARCHAR(10) NOT NULL,
            recommended_strike NUMERIC,
            recommended_tenor_days INTEGER,
            moneyness_pct NUMERIC,
            entry_price NUMERIC,
            recommendation_date DATE NOT NULL,
            expiry_date DATE,
            current_price NUMERIC,
            pct_above_strike NUMERIC,
            status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
            last_checked DATE,
            breached_date DATE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT unique_symbol_date UNIQUE (symbol, recommendation_date)
        )
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_recommendation_tracker_status_expiry
        ON recommendation_tracker (status, expiry_date, recommendation_date DESC)
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_recommendation_tracker_symbol_recommendation_date
        ON recommendation_tracker (symbol, recommendation_date DESC)
    `);
}

export async function saveDailyBest(symbol: string, runId: string, theme: string): Promise<void> {
    await pool.query(
        `
        INSERT INTO daily_best_history (
            symbol,
            run_date,
            composite_score,
            theme
        )
        SELECT
            ic.symbol,
            ir.run_date,
            ic.composite_score,
            $3
        FROM idea_candidates ic
        JOIN idea_runs ir
            ON ir.run_id = ic.run_id
        WHERE ic.run_id = $1
          AND ic.symbol = $2
        `,
        [runId, symbol, theme]
    );
}

export async function upsertRecommendationTracker(input: UpsertRecommendationTrackerInput): Promise<void> {
    await pool.query(
        `
        INSERT INTO recommendation_tracker (
            symbol,
            grade,
            recommended_strike,
            recommended_tenor_days,
            moneyness_pct,
            entry_price,
            recommendation_date,
            expiry_date
        ) VALUES (
            $1::varchar,
            $2::varchar,
            $3::numeric,
            $4::integer,
            $5::numeric,
            $6::numeric,
            $7::date,
            CASE
                WHEN $4 IS NOT NULL THEN ($7::date + ($4::int * INTERVAL '1 day'))::date
                ELSE NULL
            END
        )
        ON CONFLICT (symbol, recommendation_date) DO UPDATE
        SET grade = EXCLUDED.grade,
            recommended_strike = EXCLUDED.recommended_strike,
            recommended_tenor_days = EXCLUDED.recommended_tenor_days,
            moneyness_pct = EXCLUDED.moneyness_pct,
            entry_price = EXCLUDED.entry_price,
            expiry_date = EXCLUDED.expiry_date
        `,
        [
            input.symbol,
            input.grade,
            input.recommendedStrike,
            input.recommendedTenorDays,
            input.moneynessPct,
            input.entryPrice,
            input.recommendationDate
        ]
    );
}

export async function deleteRecommendationTrackerForDate(symbol: string, recommendationDate: string): Promise<void> {
    await pool.query(
        `
        DELETE FROM recommendation_tracker
        WHERE symbol = $1
          AND recommendation_date = $2::date
        `,
        [symbol, recommendationDate]
    );
}

export async function getActiveRecommendationTrackers(): Promise<RecommendationTrackerRow[]> {
    const result = await pool.query<RecommendationTrackerRow>(
        `
        SELECT
            id,
            symbol,
            grade,
            recommended_strike,
            recommended_tenor_days,
            moneyness_pct,
            entry_price,
            recommendation_date::text,
            expiry_date::text,
            current_price,
            pct_above_strike,
            status,
            last_checked::text,
            breached_date::text
        FROM recommendation_tracker
        WHERE expiry_date >= CURRENT_DATE
          AND status IN ('ACTIVE', 'BREACHED')
        ORDER BY recommendation_date DESC, symbol ASC
        `
    );

    return result.rows;
}

export async function updateRecommendationTrackerStatus(input: {
    id: number;
    currentPrice: number | null;
    pctAboveStrike: number | null;
    status: 'ACTIVE' | 'BREACHED' | 'EXPIRED_SAFE' | 'EXPIRED_BREACHED';
    breachedDate: string | null;
}): Promise<void> {
    await pool.query(
        `
        UPDATE recommendation_tracker
        SET current_price = $2,
            pct_above_strike = $3,
            status = $4,
            last_checked = CURRENT_DATE,
            breached_date = COALESCE(breached_date, $5::date)
        WHERE id = $1
        `,
        [input.id, input.currentPrice, input.pctAboveStrike, input.status, input.breachedDate]
    );
}

export async function getRecommendationTrackerSummaryRows(): Promise<RecommendationTrackerRow[]> {
    const result = await pool.query<RecommendationTrackerRow>(
        `
        SELECT
            id,
            symbol,
            grade,
            recommended_strike,
            recommended_tenor_days,
            moneyness_pct,
            entry_price,
            recommendation_date::text,
            expiry_date::text,
            current_price,
            pct_above_strike,
            status,
            last_checked::text,
            breached_date::text
        FROM recommendation_tracker
        ORDER BY recommendation_date DESC, symbol ASC
        `
    );

    return result.rows;
}

export async function getRecommendationTrackerHistoryRows(): Promise<RecommendationTrackerRow[]> {
    const result = await pool.query<RecommendationTrackerRow>(
        `
        SELECT
            id,
            symbol,
            grade,
            recommended_strike,
            recommended_tenor_days,
            moneyness_pct,
            entry_price,
            recommendation_date::text,
            expiry_date::text,
            current_price,
            pct_above_strike,
            status,
            last_checked::text,
            breached_date::text
        FROM recommendation_tracker
        WHERE status IN ('EXPIRED_SAFE', 'EXPIRED_BREACHED')
        ORDER BY expiry_date DESC NULLS LAST, recommendation_date DESC, symbol ASC
        `
    );

    return result.rows;
}

export async function createIdeaRun(
    _symbol: string,
    trigger: 'scheduled' | 'manual'
): Promise<string> {
    const result = await pool.query<{ run_id: string }>(
        `
        INSERT INTO idea_runs (
            run_date,
            triggered_by,
            started_at,
            status,
            total_screened,
            total_recommended
        ) VALUES (
            CURRENT_DATE,
            $1::trigger_source,
            NOW(),
            'running'::run_status,
            1,
            0
        )
        RETURNING run_id
        `,
        [trigger]
    );

    return result.rows[0].run_id;
}

export async function updateIdeaRunStatus(
    runId: string,
    status: 'running' | 'completed' | 'failed'
): Promise<void> {
    await pool.query(
        `
        UPDATE idea_runs
        SET status = $2::run_status,
            completed_at = CASE
                WHEN $2::run_status = 'completed'::run_status THEN NOW()
                WHEN $2::run_status = 'failed'::run_status THEN NOW()
                ELSE completed_at
            END
        WHERE run_id = $1
        `,
        [runId, status]
    );
}

export function mapTodayIdeasResponse(
    run: LatestCompletedRun,
    ideas: TodayIdeaRow[],
    riskFlags: RiskFlagRow[],
    dailyBest: DailyBestCard | null = null
): TodayIdeasResponse {
    const flagsBySymbol = new Map<string, Flag[]>();

    for (const flag of riskFlags) {
        const existing = flagsBySymbol.get(flag.symbol) ?? [];
        existing.push({
            type: flag.flag_type,
            severity: flag.severity,
            message: flag.detail_text
        });
        flagsBySymbol.set(flag.symbol, existing);
    }

    return {
        run_date: run.run_date,
        run_id: run.run_id,
        market_context: {
            vix: 0,
            notable_macro: 'TODO: populate market context from macro snapshot source'
        },
        daily_best: dailyBest,
        recommended: ideas
            .filter((idea) => idea.overall_grade === 'GO')
            .map((idea) => ({
                symbol: idea.symbol,
                exchange: idea.exchange,
                company_name: idea.company_name,
                sector: idea.sector,
                themes: idea.themes,
                tier: idea.tier,
                grade: 'GO',
                composite_score: Number(idea.composite_score),
                recommended_strike: parseNumeric(idea.recommended_strike) ?? 0,
                recommended_tenor_days: parseNumeric(idea.recommended_tenor_days) ?? 0,
                estimated_coupon_range: formatEstimatedCouponRange(idea.ref_coupon_pct),
                coupon_note: '实际票息请向交易台询价',
                moneyness_pct: parseNumeric(idea.moneyness_pct) ?? 0,
                reasoning_text: idea.reasoning_text,
                narrative: idea.why_now
                    ? {
                          why_now: idea.why_now,
                          risk_note: idea.risk_note ?? '',
                          sentiment_score: parseNumeric(idea.sentiment_score) ?? 0.5
                      }
                    : null,
                news_items: [],
                flags: flagsBySymbol.get(idea.symbol) ?? [],
                current_price: parseNumeric(idea.current_price),
                pct_from_52w_high: parseNumeric(idea.pct_from_52w_high),
                ma50: parseNumeric(idea.ma50),
                ma200: parseNumeric(idea.ma200),
                implied_volatility: parseNumeric(idea.selected_implied_volatility),
                sentiment_score: parseNumeric(idea.sentiment_score)
            })),
        caution: ideas
            .filter((idea) => idea.overall_grade === 'CAUTION')
            .map((idea) => ({
                symbol: idea.symbol,
                exchange: idea.exchange,
                company_name: idea.company_name,
                sector: idea.sector,
                themes: idea.themes,
                tier: idea.tier,
                grade: 'CAUTION',
                composite_score: Number(idea.composite_score),
                recommended_strike: parseNumeric(idea.recommended_strike) ?? 0,
                recommended_tenor_days: parseNumeric(idea.recommended_tenor_days) ?? 0,
                estimated_coupon_range: formatEstimatedCouponRange(idea.ref_coupon_pct),
                coupon_note: '实际票息请向交易台询价',
                moneyness_pct: parseNumeric(idea.moneyness_pct) ?? 0,
                reasoning_text: idea.reasoning_text,
                narrative: idea.why_now
                    ? {
                          why_now: idea.why_now,
                          risk_note: idea.risk_note ?? '',
                          sentiment_score: parseNumeric(idea.sentiment_score) ?? 0.5
                      }
                    : null,
                news_items: [],
                flags: flagsBySymbol.get(idea.symbol) ?? [],
                current_price: parseNumeric(idea.current_price),
                pct_from_52w_high: parseNumeric(idea.pct_from_52w_high),
                ma50: parseNumeric(idea.ma50),
                ma200: parseNumeric(idea.ma200),
                implied_volatility: parseNumeric(idea.selected_implied_volatility),
                sentiment_score: parseNumeric(idea.sentiment_score)
            })),
        not_recommended: ideas
            .filter((idea) => idea.overall_grade === 'AVOID')
            .map((idea) => {
                const primaryFlag = (flagsBySymbol.get(idea.symbol) ?? [])[0];
                return {
                    symbol: idea.symbol,
                    primary_flag_type: primaryFlag?.type ?? 'NO_APPROVED_STRIKE',
                    primary_flag_detail: primaryFlag?.message ?? 'No primary block reason recorded'
                };
            })
    };
}

function parseNumeric(value: number | string | null): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function formatEstimatedCouponRange(lowerBound: number | string | null): string | null {
    if (lowerBound === null) {
        return null;
    }

    const lowerBoundNumber = Number(lowerBound);
    if (!Number.isFinite(lowerBoundNumber)) {
        return null;
    }

    const upperBound = lowerBoundNumber * (1.15 / 0.85);
    return `${lowerBoundNumber.toFixed(1)}%-${upperBound.toFixed(1)}%`;
}
