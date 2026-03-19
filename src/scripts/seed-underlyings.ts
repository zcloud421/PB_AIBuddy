interface SeedUnderlying {
    symbol: string;
    exchange: string;
    sector: string;
    currency: string;
    themes: string[];
    tier: 1 | 2;
    active: boolean;
}

const UNDERLYINGS: SeedUnderlying[] = [
    { symbol: 'NVDA', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure', 'Semiconductors'], tier: 1, active: true },
    { symbol: 'MU', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure', 'Semiconductors'], tier: 2, active: true },
    { symbol: 'TSM', exchange: 'NYSE', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure', 'Semiconductors'], tier: 1, active: true },
    { symbol: 'AVGO', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure', 'Semiconductors'], tier: 2, active: true },
    { symbol: 'AMD', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure', 'Semiconductors'], tier: 1, active: true },
    { symbol: 'ORCL', exchange: 'NYSE', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure'], tier: 2, active: true },
    { symbol: 'AMZN', exchange: 'NASDAQ', sector: 'Consumer Discretionary', currency: 'USD', themes: ['Mag7'], tier: 1, active: true },
    { symbol: 'MSFT', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Mag7'], tier: 1, active: true },
    { symbol: 'META', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Mag7'], tier: 1, active: true },
    { symbol: 'GOOG', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Mag7'], tier: 1, active: true },
    { symbol: 'AAPL', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Mag7'], tier: 1, active: true },
    { symbol: 'TSLA', exchange: 'NASDAQ', sector: 'Consumer Discretionary', currency: 'USD', themes: ['Mag7', 'High Volatility'], tier: 1, active: true },
    { symbol: 'PLTR', exchange: 'NYSE', sector: 'Technology', currency: 'USD', themes: ['AI Infrastructure'], tier: 2, active: true },
    { symbol: 'NFLX', exchange: 'NASDAQ', sector: 'Communication', currency: 'USD', themes: ['Consumer Tech'], tier: 1, active: true },
    { symbol: 'V', exchange: 'NYSE', sector: 'Financials', currency: 'USD', themes: ['Payments'], tier: 1, active: true },
    { symbol: 'MA', exchange: 'NYSE', sector: 'Financials', currency: 'USD', themes: ['Payments'], tier: 1, active: true },
    { symbol: 'AXP', exchange: 'NYSE', sector: 'Financials', currency: 'USD', themes: ['Payments'], tier: 1, active: false },
    { symbol: 'HOOD', exchange: 'NASDAQ', sector: 'Financials', currency: 'USD', themes: ['High Volatility', 'Crypto'], tier: 2, active: true },
    { symbol: 'BABA', exchange: 'NYSE', sector: 'Consumer Discretionary', currency: 'USD', themes: ['China Tech'], tier: 1, active: true },
    { symbol: 'PDD', exchange: 'NASDAQ', sector: 'Consumer Discretionary', currency: 'USD', themes: ['China Tech'], tier: 2, active: true },
    { symbol: 'BIDU', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['China Tech'], tier: 2, active: true },
    { symbol: 'COIN', exchange: 'NASDAQ', sector: 'Financials', currency: 'USD', themes: ['Crypto', 'High Volatility'], tier: 1, active: true },
    { symbol: 'MSTR', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Crypto', 'High Volatility'], tier: 2, active: true },
    { symbol: 'CRCL', exchange: 'NYSE', sector: 'Technology', currency: 'USD', themes: ['Crypto', 'High Volatility'], tier: 2, active: true },
    { symbol: 'GLD', exchange: 'NYSE', sector: 'Commodities', currency: 'USD', themes: ['Gold', 'Defensive'], tier: 1, active: false },
    { symbol: 'NEM', exchange: 'NYSE', sector: 'Materials', currency: 'USD', themes: ['Gold'], tier: 2, active: false },
    { symbol: 'CEG', exchange: 'NASDAQ', sector: 'Energy', currency: 'USD', themes: ['Nuclear', 'Energy'], tier: 2, active: false },
    { symbol: 'UNH', exchange: 'NYSE', sector: 'Healthcare', currency: 'USD', themes: ['Defensive'], tier: 1, active: true },
    { symbol: 'INTC', exchange: 'NASDAQ', sector: 'Technology', currency: 'USD', themes: ['Semiconductors'], tier: 1, active: true }
];

async function main(): Promise<void> {
    const { fetchTickerCompanyName } = await import('../data/massive-fetcher');
    const { pool } = await import('../db/client');
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        await client.query(`
            ALTER TABLE underlyings
            ADD COLUMN IF NOT EXISTS themes TEXT[] NOT NULL DEFAULT '{}'
        `);

        await client.query(`
            ALTER TABLE underlyings
            ADD COLUMN IF NOT EXISTS tier INTEGER NOT NULL DEFAULT 1
        `);

        await client.query(`
            ALTER TABLE underlyings
            ADD COLUMN IF NOT EXISTS company_name VARCHAR(100)
        `);

        await client.query(`
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'underlyings_tier_chk'
                ) THEN
                    ALTER TABLE underlyings
                    ADD CONSTRAINT underlyings_tier_chk CHECK (tier IN (1, 2));
                END IF;
            END
            $$;
        `);

        for (const underlying of UNDERLYINGS) {
            const companyName = await fetchTickerCompanyName(underlying.symbol).catch(() => null);
            await client.query(
                `
                    INSERT INTO underlyings (
                        symbol,
                        exchange,
                        name,
                        company_name,
                        sector,
                        currency,
                        themes,
                        tier,
                        active
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9)
                    ON CONFLICT (symbol) DO UPDATE
                    SET exchange = EXCLUDED.exchange,
                        name = EXCLUDED.name,
                        company_name = EXCLUDED.company_name,
                        sector = EXCLUDED.sector,
                        currency = EXCLUDED.currency,
                        themes = EXCLUDED.themes,
                        tier = EXCLUDED.tier,
                        active = EXCLUDED.active
                `,
                [
                    underlying.symbol,
                    underlying.exchange,
                    underlying.symbol,
                    companyName,
                    underlying.sector,
                    underlying.currency,
                    underlying.themes,
                    underlying.tier,
                    underlying.active
                ]
            );
        }

        await client.query('COMMIT');
        console.log(`Successfully seeded ${UNDERLYINGS.length} underlyings`);
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release();
        await pool.end();
    }
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
});
