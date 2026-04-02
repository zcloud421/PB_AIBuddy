BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE trigger_source AS ENUM ('scheduled', 'manual');
CREATE TYPE run_status AS ENUM ('queued', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE option_side AS ENUM ('put', 'call');
CREATE TYPE overall_grade AS ENUM ('GO', 'CAUTION', 'AVOID');
CREATE TYPE risk_flag_type AS ENUM (
    'EARNINGS_PROXIMITY',
    'BROKEN_TREND',
    'HIGH_VOL_LOW_STRIKE',
    'BEARISH_STRUCTURE',
    'LOWER_HIGH_RISK',
    'LOW_COUPON',
    'EXTREME_SKEW',
    'EVENT_RISK',
    'LIQUIDITY',
    'LOW_LIQUIDITY',
    'NO_APPROVED_TENOR',
    'NO_APPROVED_STRIKE',
    'HOUSE_OVERRIDE'
);
CREATE TYPE risk_flag_severity AS ENUM ('warn', 'block');
CREATE TYPE override_type AS ENUM ('FORCE_AVOID', 'FORCE_CAUTION', 'WHITELIST_ONLY');

CREATE TABLE underlyings (
    symbol TEXT PRIMARY KEY,
    exchange TEXT NOT NULL,
    name TEXT NOT NULL,
    company_name VARCHAR(100),
    sector TEXT,
    currency CHAR(3) NOT NULL,
    themes TEXT[] NOT NULL DEFAULT '{}',
    tier INTEGER NOT NULL DEFAULT 1,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT underlyings_symbol_format_chk CHECK (symbol = UPPER(symbol)),
    CONSTRAINT underlyings_currency_format_chk CHECK (currency = UPPER(currency)),
    CONSTRAINT underlyings_tier_chk CHECK (tier IN (1, 2))
);

COMMENT ON TABLE underlyings IS
'Master list of equity underlyings that the FCN idea engine screens and tracks.';

CREATE TABLE price_history (
    price_history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    open NUMERIC(18, 6) NOT NULL CHECK (open >= 0),
    high NUMERIC(18, 6) NOT NULL CHECK (high >= 0),
    low NUMERIC(18, 6) NOT NULL CHECK (low >= 0),
    close NUMERIC(18, 6) NOT NULL CHECK (close >= 0),
    volume BIGINT NOT NULL CHECK (volume >= 0),
    adj_close NUMERIC(18, 6),
    CONSTRAINT price_history_symbol_trade_date_uniq UNIQUE (symbol, trade_date),
    CONSTRAINT price_history_ohlc_range_chk CHECK (
        high >= GREATEST(open, close, low)
        AND low <= LEAST(open, close, high)
    )
);

COMMENT ON TABLE price_history IS
'Daily OHLCV history per underlying, used for trend, drawdown, and realized-volatility calculations.';

CREATE TABLE option_snapshots (
    option_snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    strike NUMERIC(18, 6) NOT NULL CHECK (strike > 0),
    option_type option_side NOT NULL,
    iv NUMERIC(10, 6) CHECK (iv IS NULL OR iv >= 0),
    delta NUMERIC(10, 6),
    gamma NUMERIC(12, 8),
    vega NUMERIC(12, 8),
    theta NUMERIC(12, 8),
    volume BIGINT NOT NULL DEFAULT 0 CHECK (volume >= 0),
    open_interest BIGINT NOT NULL DEFAULT 0 CHECK (open_interest >= 0),
    mid_price NUMERIC(18, 6) CHECK (mid_price IS NULL OR mid_price >= 0),
    CONSTRAINT option_snapshots_snapshot_expiry_chk CHECK (expiry_date >= snapshot_date),
    CONSTRAINT option_snapshots_unique_contract_uniq UNIQUE (
        symbol,
        snapshot_date,
        expiry_date,
        strike,
        option_type
    )
);

COMMENT ON TABLE option_snapshots IS
'Raw daily listed-options chain snapshots from the Massive API, used as the FCN reference market input.';

CREATE TABLE idea_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_date DATE NOT NULL,
    triggered_by trigger_source NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status run_status NOT NULL DEFAULT 'queued',
    total_screened INTEGER NOT NULL DEFAULT 0 CHECK (total_screened >= 0),
    total_recommended INTEGER NOT NULL DEFAULT 0 CHECK (total_recommended >= 0),
    CONSTRAINT idea_runs_completion_chk CHECK (
        (status = 'completed' AND completed_at IS NOT NULL)
        OR (status <> 'completed')
    )
);

COMMENT ON TABLE idea_runs IS
'One record per daily scoring job, capturing execution metadata and top-level run statistics.';

CREATE TABLE idea_candidates (
    candidate_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES idea_runs(run_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    overall_grade overall_grade NOT NULL,
    iv_rank_score NUMERIC(8, 4) NOT NULL CHECK (iv_rank_score >= 0),
    trend_score NUMERIC(8, 4) NOT NULL CHECK (trend_score >= 0),
    skew_score NUMERIC(8, 4) NOT NULL CHECK (skew_score >= 0),
    event_risk_score NUMERIC(8, 4) NOT NULL CHECK (event_risk_score >= 0),
    composite_score NUMERIC(10, 4) NOT NULL CHECK (composite_score >= 0),
    risk_reward_score NUMERIC(10, 4) CHECK (risk_reward_score IS NULL OR risk_reward_score >= 0),
    recommended_strike NUMERIC(18, 6) CHECK (recommended_strike IS NULL OR recommended_strike > 0),
    recommended_tenor_days INTEGER CHECK (recommended_tenor_days IS NULL OR recommended_tenor_days > 0),
    ref_coupon_pct NUMERIC(10, 4) CHECK (ref_coupon_pct IS NULL OR ref_coupon_pct >= 0),
    moneyness_pct NUMERIC(10, 4),
    selected_implied_volatility NUMERIC(10, 6),
    current_price NUMERIC(18, 6),
    ma20 NUMERIC(18, 6),
    ma50 NUMERIC(18, 6),
    ma200 NUMERIC(18, 6),
    pct_from_52w_high NUMERIC(10, 4),
    why_now TEXT,
    risk_note TEXT,
    sentiment_score NUMERIC(10, 4),
    reasoning_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT idea_candidates_run_symbol_uniq UNIQUE (run_id, symbol)
);

COMMENT ON TABLE idea_candidates IS
'Per-run scoring output by symbol, including the final grade, component scores, and recommended FCN reference terms.';

CREATE TABLE risk_flags (
    flag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES idea_runs(run_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    flag_type risk_flag_type NOT NULL,
    severity risk_flag_severity NOT NULL,
    detail_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT risk_flags_candidate_fk
        FOREIGN KEY (run_id, symbol)
        REFERENCES idea_candidates(run_id, symbol)
        ON DELETE CASCADE
);

COMMENT ON TABLE risk_flags IS
'Diagnostic flags attached to a candidate for transparency when the engine downgrades or blocks an idea.';

CREATE TABLE house_overrides (
    override_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    override_type override_type NOT NULL,
    reason TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE house_overrides IS
'Admin-driven policy overrides that can force caution, block a symbol, or restrict recommendations to whitelisted names.';

CREATE TABLE earnings_calendar (
    earnings_calendar_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    report_date DATE NOT NULL,
    days_until INTEGER,
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT earnings_calendar_symbol_report_date_uniq UNIQUE (symbol, report_date, source)
);

COMMENT ON TABLE earnings_calendar IS
'Upcoming earnings events per symbol, used to block or downgrade ideas with event risk inside the intended tenor.';

CREATE TABLE daily_best_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL REFERENCES underlyings(symbol) ON UPDATE CASCADE ON DELETE CASCADE,
    run_date DATE NOT NULL,
    composite_score NUMERIC,
    theme TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE daily_best_history IS
'Daily selected best FCN combo candidate, stored with freshness history to avoid repeating the same name too often.';

CREATE INDEX idx_underlyings_active_symbol
    ON underlyings (active, symbol);

CREATE INDEX idx_price_history_symbol_trade_date
    ON price_history (symbol, trade_date DESC);

CREATE INDEX idx_price_history_trade_date
    ON price_history (trade_date DESC);

CREATE INDEX idx_option_snapshots_symbol_snapshot_date
    ON option_snapshots (symbol, snapshot_date DESC);

CREATE INDEX idx_option_snapshots_symbol_snapshot_expiry
    ON option_snapshots (symbol, snapshot_date DESC, expiry_date, option_type);

CREATE INDEX idx_option_snapshots_snapshot_date
    ON option_snapshots (snapshot_date DESC);

CREATE INDEX idx_idea_runs_run_date
    ON idea_runs (run_date DESC);

CREATE INDEX idx_idea_runs_status_completed_at
    ON idea_runs (status, completed_at DESC);

CREATE INDEX idx_idea_candidates_run_id
    ON idea_candidates (run_id);

CREATE INDEX idx_idea_candidates_symbol_created_at
    ON idea_candidates (symbol, created_at DESC);

CREATE INDEX idx_idea_candidates_run_grade
    ON idea_candidates (run_id, overall_grade, composite_score DESC);

CREATE INDEX idx_risk_flags_run_id
    ON risk_flags (run_id);

CREATE INDEX idx_risk_flags_symbol_created_at
    ON risk_flags (symbol, created_at DESC);

CREATE INDEX idx_risk_flags_run_symbol
    ON risk_flags (run_id, symbol);

CREATE INDEX idx_house_overrides_symbol
    ON house_overrides (symbol);

CREATE INDEX idx_house_overrides_symbol_expires_at
    ON house_overrides (symbol, expires_at);

CREATE INDEX idx_earnings_calendar_symbol_report_date
    ON earnings_calendar (symbol, report_date);

CREATE INDEX idx_earnings_calendar_report_date
    ON earnings_calendar (report_date);

CREATE UNIQUE INDEX idx_earnings_calendar_symbol_unique
    ON earnings_calendar (symbol);

CREATE INDEX idx_daily_best_history_run_date
    ON daily_best_history (run_date DESC);

CREATE INDEX idx_daily_best_history_symbol_run_date
    ON daily_best_history (symbol, run_date DESC);

CREATE OR REPLACE VIEW v_today_ideas AS
WITH latest_completed_run AS (
    SELECT ir.run_id, ir.run_date, ir.completed_at
    FROM idea_runs ir
    WHERE ir.status = 'completed'
    ORDER BY ir.run_date DESC, ir.completed_at DESC, ir.started_at DESC
    LIMIT 1
),
active_overrides AS (
    SELECT
        ho.symbol,
        ho.override_id,
        ho.override_type,
        ho.reason,
        ho.expires_at,
        ho.created_by,
        ho.created_at
    FROM (
        SELECT
            ho.*,
            ROW_NUMBER() OVER (
                PARTITION BY ho.symbol
                ORDER BY ho.created_at DESC, ho.override_id DESC
            ) AS rn
        FROM house_overrides ho
        WHERE ho.expires_at IS NULL OR ho.expires_at >= NOW()
    ) ho
    WHERE ho.rn = 1
),
flag_rollup AS (
    SELECT
        rf.run_id,
        rf.symbol,
        ARRAY_AGG(rf.flag_type ORDER BY rf.severity DESC, rf.created_at ASC) AS risk_flag_types,
        ARRAY_AGG(rf.severity ORDER BY rf.severity DESC, rf.created_at ASC) AS risk_flag_severities,
        ARRAY_AGG(rf.detail_text ORDER BY rf.severity DESC, rf.created_at ASC) AS risk_flag_details,
        COUNT(*) AS risk_flag_count,
        BOOL_OR(rf.severity = 'block') AS has_blocking_flag
    FROM risk_flags rf
    GROUP BY rf.run_id, rf.symbol
)
SELECT
    lcr.run_id,
    lcr.run_date,
    lcr.completed_at AS run_completed_at,
    ic.candidate_id,
    ic.symbol,
    u.exchange,
    u.name,
    u.sector,
    u.currency,
    ic.overall_grade,
    ic.iv_rank_score,
    ic.trend_score,
    ic.skew_score,
    ic.event_risk_score,
    ic.composite_score,
    ic.recommended_strike,
    ic.recommended_tenor_days,
    ic.ref_coupon_pct,
    ic.moneyness_pct,
    ic.reasoning_text,
    ic.created_at AS candidate_created_at,
    COALESCE(fr.risk_flag_types, ARRAY[]::risk_flag_type[]) AS risk_flag_types,
    COALESCE(fr.risk_flag_severities, ARRAY[]::risk_flag_severity[]) AS risk_flag_severities,
    COALESCE(fr.risk_flag_details, ARRAY[]::TEXT[]) AS risk_flag_details,
    COALESCE(fr.risk_flag_count, 0) AS risk_flag_count,
    COALESCE(fr.has_blocking_flag, FALSE) AS has_blocking_flag,
    ao.override_id,
    ao.override_type,
    ao.reason AS override_reason,
    ao.expires_at AS override_expires_at,
    ao.created_by AS override_created_by,
    ao.created_at AS override_created_at
FROM latest_completed_run lcr
JOIN idea_candidates ic
    ON ic.run_id = lcr.run_id
JOIN underlyings u
    ON u.symbol = ic.symbol
LEFT JOIN flag_rollup fr
    ON fr.run_id = ic.run_id
   AND fr.symbol = ic.symbol
LEFT JOIN active_overrides ao
    ON ao.symbol = ic.symbol;

COMMENT ON VIEW v_today_ideas IS
'Latest completed daily idea set, enriched with aggregated risk flags and any currently active house override.';

CREATE TABLE recommendation_tracker (
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
);

COMMENT ON TABLE recommendation_tracker IS
'Tracks GO and CAUTION FCN recommendations over time, including strike safety buffer and post-recommendation breach outcomes.';

CREATE INDEX idx_recommendation_tracker_status_expiry
    ON recommendation_tracker (status, expiry_date, recommendation_date DESC);

CREATE INDEX idx_recommendation_tracker_symbol_recommendation_date
    ON recommendation_tracker (symbol, recommendation_date DESC);

COMMIT;
