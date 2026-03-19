import { pool } from '../db/client';

async function main(): Promise<void> {
    await pool.query(`
        ALTER TABLE idea_candidates
        ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC,
        ADD COLUMN IF NOT EXISTS selected_implied_volatility NUMERIC,
        ADD COLUMN IF NOT EXISTS current_price NUMERIC,
        ADD COLUMN IF NOT EXISTS ma20 NUMERIC,
        ADD COLUMN IF NOT EXISTS ma50 NUMERIC,
        ADD COLUMN IF NOT EXISTS ma200 NUMERIC,
        ADD COLUMN IF NOT EXISTS pct_from_52w_high NUMERIC
    `);

    console.log('Idea candidate price-context migration completed');
    await pool.end();
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
});
