import { pool } from '../db/client';

async function main(): Promise<void> {
    await pool.query(`
        ALTER TYPE risk_flag_type ADD VALUE IF NOT EXISTS 'HIGH_VOL_LOW_STRIKE'
    `);

    await pool.query(`
        ALTER TABLE idea_candidates
        ADD COLUMN IF NOT EXISTS why_now TEXT,
        ADD COLUMN IF NOT EXISTS risk_note TEXT,
        ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC(10, 4),
        ADD COLUMN IF NOT EXISTS current_price NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma20 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma50 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS ma200 NUMERIC(18, 6),
        ADD COLUMN IF NOT EXISTS pct_from_52w_high NUMERIC(10, 4)
    `);

    await pool.query(`
        ALTER TABLE idea_candidates
        DROP COLUMN IF EXISTS client_opener
    `);

    await pool.query(`
        ALTER TABLE earnings_calendar
        ADD COLUMN IF NOT EXISTS days_until INTEGER
    `);

    await pool.query(`
        ALTER TABLE underlyings
        ADD COLUMN IF NOT EXISTS company_name VARCHAR(100)
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

    console.log('Idea candidate narrative, price-context, and recommendation tracker migration completed');
    await pool.end();
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
});
