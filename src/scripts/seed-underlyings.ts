export type UnderlyingClassification = 'blue_chip' | 'theme' | 'both';

export interface SeedUnderlying {
    symbol: string;
    exchange: string;
    sector: string;
    currency: string;
    themes: string[];
    tier: 1 | 2;
    active: boolean;
    status: 'active' | 'suspended' | 'under_review' | 'deprecated';
    status_reason?: string | null;
    reviewed_by?: string | null;
    reviewed_at?: string | null;
    classification: UnderlyingClassification;
    adr_risk?: boolean;
    turnaround_watch?: boolean;
    holdable_concern?: string | null;
}

const ADR_RISK = new Set(['BABA', 'PDD', 'JD', 'BIDU', 'NTES', 'FUTU']);
const TURNAROUND_WATCH = new Set(['INTC', 'NKE', 'LULU', 'NVO']);

function u(input: Omit<SeedUnderlying, 'currency' | 'active' | 'status' | 'adr_risk' | 'turnaround_watch' | 'holdable_concern'> & {
    active?: boolean;
    status?: SeedUnderlying['status'];
    adr_risk?: boolean;
    turnaround_watch?: boolean;
    holdable_concern?: string | null;
}): SeedUnderlying {
    const status = input.status ?? (input.active === false ? 'deprecated' : 'active');
    return {
        ...input,
        currency: 'USD',
        active: status === 'active',
        status,
        adr_risk: input.adr_risk ?? ADR_RISK.has(input.symbol),
        turnaround_watch: input.turnaround_watch ?? TURNAROUND_WATCH.has(input.symbol),
        holdable_concern: input.holdable_concern ?? null
    };
}

export const UNDERLYINGS: SeedUnderlying[] = [
    // T1 Core PB Names (36)
    u({ symbol: 'NVDA', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'AI Infrastructure', 'Semiconductors'], tier: 1, classification: 'both' }),
    u({ symbol: 'MSFT', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'Mag7', 'AI Software'], tier: 1, classification: 'both' }),
    u({ symbol: 'AAPL', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'Mag7', 'Consumer Tech'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'AMZN', exchange: 'NASDAQ', sector: 'Consumer Discretionary', themes: ['Mega-cap Tech / AI', 'Mag7', 'Cloud'], tier: 1, classification: 'both' }),
    u({ symbol: 'GOOG', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'Mag7', 'AI Cloud'], tier: 1, classification: 'both' }),
    u({ symbol: 'META', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'Mag7', 'Digital Ads'], tier: 1, classification: 'both' }),
    u({ symbol: 'TSLA', exchange: 'NASDAQ', sector: 'Consumer Discretionary', themes: ['Mega-cap Tech / AI', 'EV', 'High Volatility'], tier: 1, classification: 'both' }),
    u({ symbol: 'TSM', exchange: 'NYSE', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'AI Semi', 'Foundry'], tier: 1, classification: 'both' }),
    u({ symbol: 'AVGO', exchange: 'NASDAQ', sector: 'Technology', themes: ['Mega-cap Tech / AI', 'AI ASIC', 'Semiconductors'], tier: 1, classification: 'both' }),
    u({ symbol: 'AMD', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Semiconductors'], tier: 1, classification: 'both' }),
    u({ symbol: 'MU', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Memory', 'HBM'], tier: 1, classification: 'both' }),
    u({ symbol: 'AMAT', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Semiconductor Equipment'], tier: 1, classification: 'theme' }),
    u({ symbol: 'LRCX', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Semiconductor Equipment'], tier: 1, classification: 'theme' }),
    u({ symbol: 'QCOM', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Connectivity'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'TXN', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Analog Semiconductors'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'ASML', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'EUV Lithography'], tier: 1, classification: 'both' }),
    u({ symbol: 'KLAC', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Semi', 'Semiconductor Equipment'], tier: 1, classification: 'theme' }),
    u({ symbol: 'ANET', exchange: 'NYSE', sector: 'Technology', themes: ['AI Infrastructure', 'Networking'], tier: 1, classification: 'theme' }),
    u({ symbol: 'ORCL', exchange: 'NYSE', sector: 'Technology', themes: ['AI Infrastructure + Database Software'], tier: 1, classification: 'both' }),
    u({ symbol: 'VRT', exchange: 'NYSE', sector: 'Industrials', themes: ['AI Infrastructure', 'Data Center Power & Cooling'], tier: 1, classification: 'theme' }),
    u({ symbol: 'ETN', exchange: 'NYSE', sector: 'Industrials', themes: ['AI Infrastructure', 'Electrical Equipment'], tier: 1, classification: 'both' }),
    u({ symbol: 'GEV', exchange: 'NYSE', sector: 'Industrials', themes: ['Power Infrastructure', 'Grid'], tier: 1, classification: 'theme' }),
    u({ symbol: 'JPM', exchange: 'NYSE', sector: 'Financials', themes: ['Financials', 'Money Center Bank'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'GS', exchange: 'NYSE', sector: 'Financials', themes: ['Financials', 'Investment Banking'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'V', exchange: 'NYSE', sector: 'Financials', themes: ['Payments', 'Consumer Finance'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'MA', exchange: 'NYSE', sector: 'Financials', themes: ['Payments', 'Consumer Finance'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'COST', exchange: 'NASDAQ', sector: 'Consumer Defensive', themes: ['Consumer Defensive', 'Retail'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'WMT', exchange: 'NYSE', sector: 'Consumer Defensive', themes: ['Consumer Defensive', 'Retail'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'NFLX', exchange: 'NASDAQ', sector: 'Communication', themes: ['Consumer Tech', 'Streaming'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'LLY', exchange: 'NYSE', sector: 'Healthcare', themes: ['Healthcare', 'GLP-1'], tier: 1, classification: 'both' }),
    u({ symbol: 'UNH', exchange: 'NYSE', sector: 'Healthcare', themes: ['Healthcare', 'Defensive'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'JNJ', exchange: 'NYSE', sector: 'Healthcare', themes: ['Healthcare', 'Defensive'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'ISRG', exchange: 'NASDAQ', sector: 'Healthcare', themes: ['Medical Robotics', 'AI-enabled Healthcare'], tier: 1, classification: 'both' }),
    u({ symbol: 'RTX', exchange: 'NYSE', sector: 'Industrials', themes: ['Aerospace & Defense', 'Security Infrastructure'], tier: 1, classification: 'both' }),
    u({ symbol: 'XOM', exchange: 'NYSE', sector: 'Energy', themes: ['Energy', 'Macro'], tier: 1, classification: 'blue_chip' }),
    u({ symbol: 'GLD', exchange: 'NYSEARCA', sector: 'Commodities', themes: ['Gold', 'Defensive'], tier: 1, classification: 'theme' }),

    // T2 Theme / Tactical (20)
    u({ symbol: 'PLTR', exchange: 'NYSE', sector: 'Technology', themes: ['AI Software / Government Tech'], tier: 2, classification: 'theme' }),
    u({ symbol: 'DELL', exchange: 'NYSE', sector: 'Technology', themes: ['AI Infra Beta', 'Servers'], tier: 2, classification: 'theme' }),
    u({ symbol: 'LITE', exchange: 'NASDAQ', sector: 'Technology', themes: ['AI Infra Beta', 'Optical Networking'], tier: 2, classification: 'theme' }),
    u({ symbol: 'CEG', exchange: 'NASDAQ', sector: 'Energy', themes: ['AI Power / Nuclear', 'Nuclear'], tier: 2, classification: 'theme' }),
    u({ symbol: 'VST', exchange: 'NYSE', sector: 'Utilities', themes: ['Nuclear + Power Demand + Energy'], tier: 2, classification: 'theme' }),
    u({ symbol: 'PWR', exchange: 'NYSE', sector: 'Industrials', themes: ['AI Power', 'Grid Engineering'], tier: 2, classification: 'theme' }),
    u({ symbol: 'GDX', exchange: 'NYSEARCA', sector: 'Commodities', themes: ['Gold Miners', 'Gold Beta'], tier: 2, classification: 'theme', holdable_concern: 'Gold-miner operating leverage requires a deeper buffer than physical-gold exposure' }),
    u({ symbol: 'BABA', exchange: 'NYSE', sector: 'Consumer Discretionary', themes: ['China ADR', 'China Tech'], tier: 2, classification: 'blue_chip' }),
    u({
        symbol: 'PDD',
        exchange: 'NASDAQ',
        sector: 'Consumer Discretionary',
        themes: ['China ADR', 'China Tech'],
        tier: 2,
        classification: 'theme',
        status: 'under_review',
        status_reason: 'China internet sector trend remains weak; require sector and company-specific re-confirmation',
        reviewed_by: 'IC-2026Q3',
        reviewed_at: '2026-07-11T00:00:00Z',
        holdable_concern: 'Price-led competition and cross-border policy uncertainty require tighter PB suitability review'
    }),
    u({
        symbol: 'JD',
        exchange: 'NASDAQ',
        sector: 'Consumer Discretionary',
        themes: ['China ADR', 'E-Commerce'],
        tier: 2,
        classification: 'theme',
        status: 'under_review',
        status_reason: 'China consumer and e-commerce regime remains weak; require trend and earnings re-confirmation',
        reviewed_by: 'IC-2026Q3',
        reviewed_at: '2026-07-11T00:00:00Z',
        holdable_concern: 'Margin pressure and intense e-commerce competition; search remains available for RM review'
    }),
    u({
        symbol: 'BIDU',
        exchange: 'NASDAQ',
        sector: 'Technology',
        themes: ['China ADR', 'China Tech'],
        tier: 2,
        classification: 'theme',
        status: 'under_review',
        status_reason: 'Legacy advertising weakness offsets AI cloud growth; require trend and earnings re-confirmation',
        reviewed_by: 'IC-2026Q3',
        reviewed_at: '2026-07-11T00:00:00Z',
        holdable_concern: 'AI transition is promising but legacy advertising remains under pressure'
    }),
    u({ symbol: 'COIN', exchange: 'NASDAQ', sector: 'Financials', themes: ['Bitcoin Proxy + High Volatility', 'Crypto'], tier: 2, classification: 'theme', holdable_concern: 'Crypto beta and regulatory volatility; size discipline required' }),
    u({ symbol: 'CRCL', exchange: 'NYSE', sector: 'Technology', themes: ['Stablecoin Infrastructure + Fintech + High Volatility'], tier: 2, classification: 'theme', holdable_concern: 'Stablecoin infrastructure beta; very high volatility' }),
    u({ symbol: 'MSTR', exchange: 'NASDAQ', sector: 'Technology', themes: ['Bitcoin Proxy + High Volatility'], tier: 2, classification: 'theme', holdable_concern: 'Bitcoin proxy; client suitability must be explicit' }),
    u({ symbol: 'NVO', exchange: 'NYSE', sector: 'Healthcare', themes: ['GLP-1 / Turnaround'], tier: 2, classification: 'both' }),
    u({ symbol: 'CRWD', exchange: 'NASDAQ', sector: 'Technology', themes: ['Cybersecurity'], tier: 2, classification: 'theme' }),
    u({ symbol: 'PANW', exchange: 'NASDAQ', sector: 'Technology', themes: ['Cybersecurity'], tier: 2, classification: 'theme' }),
    u({ symbol: 'NKE', exchange: 'NYSE', sector: 'Consumer Discretionary', themes: ['Consumer Tactical', 'Turnaround'], tier: 2, classification: 'blue_chip' }),
    u({ symbol: 'LULU', exchange: 'NASDAQ', sector: 'Consumer Discretionary', themes: ['Consumer Tactical', 'Turnaround'], tier: 2, classification: 'theme' }),
    u({ symbol: 'INTC', exchange: 'NASDAQ', sector: 'Technology', themes: ['Legacy Turnaround', 'Semiconductors'], tier: 2, classification: 'theme' })
];

export const DEPRECATED_SYMBOLS = ['USO', 'NEM', 'BILI', 'LI', 'XPEV'];

export async function seedUnderlyingsInline(options: { skipCompanyNameFetch?: boolean } = {}): Promise<{
    status: 'ok';
    active_count: number;
    deprecated_symbols: string[];
}> {
    const { fetchTickerCompanyName } = await import('../data/massive-fetcher');
    const { pool } = await import('../db/client');
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        await client.query(`
            ALTER TABLE underlyings
            ADD COLUMN IF NOT EXISTS themes TEXT[] NOT NULL DEFAULT '{}',
            ADD COLUMN IF NOT EXISTS tier INTEGER NOT NULL DEFAULT 1,
            ADD COLUMN IF NOT EXISTS company_name VARCHAR(100),
            ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
            ADD COLUMN IF NOT EXISTS status_reason TEXT,
            ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
            ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS classification TEXT,
            ADD COLUMN IF NOT EXISTS adr_risk BOOLEAN DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS turnaround_watch BOOLEAN DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS holdable_concern TEXT
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

                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'underlyings_status_chk'
                ) THEN
                    ALTER TABLE underlyings
                    ADD CONSTRAINT underlyings_status_chk CHECK (status IN ('active', 'suspended', 'under_review', 'deprecated'));
                END IF;

                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'underlyings_classification_chk'
                ) THEN
                    ALTER TABLE underlyings
                    ADD CONSTRAINT underlyings_classification_chk CHECK (classification IN ('blue_chip', 'theme', 'both'));
                END IF;
            END
            $$;
        `);

        for (const underlying of UNDERLYINGS) {
            const companyName = options.skipCompanyNameFetch
                ? null
                : await fetchTickerCompanyName(underlying.symbol).catch(() => null);
            const status = underlying.status;
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
                        active,
                        status,
                        classification,
                        adr_risk,
                        turnaround_watch,
                        holdable_concern,
                        status_reason,
                        reviewed_by,
                        reviewed_at,
                        removed_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7::text[], $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17::timestamptz,
                        CASE WHEN $10 = 'deprecated' THEN NOW() ELSE NULL END
                    )
                    ON CONFLICT (symbol) DO UPDATE
                    SET exchange = EXCLUDED.exchange,
                        name = EXCLUDED.name,
                        company_name = COALESCE(EXCLUDED.company_name, underlyings.company_name),
                        sector = EXCLUDED.sector,
                        currency = EXCLUDED.currency,
                        themes = EXCLUDED.themes,
                        tier = EXCLUDED.tier,
                        active = EXCLUDED.active,
                        status = EXCLUDED.status,
                        classification = EXCLUDED.classification,
                        adr_risk = EXCLUDED.adr_risk,
                        turnaround_watch = EXCLUDED.turnaround_watch,
                        holdable_concern = EXCLUDED.holdable_concern,
                        status_reason = EXCLUDED.status_reason,
                        reviewed_by = EXCLUDED.reviewed_by,
                        reviewed_at = EXCLUDED.reviewed_at,
                        removed_at = CASE
                            WHEN EXCLUDED.status = 'deprecated' THEN COALESCE(underlyings.removed_at, NOW())
                            ELSE NULL
                        END
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
                    underlying.active,
                    status,
                    underlying.classification,
                    underlying.adr_risk ?? false,
                    underlying.turnaround_watch ?? false,
                    underlying.holdable_concern ?? null,
                    underlying.status_reason ?? null,
                    underlying.reviewed_by ?? null,
                    underlying.reviewed_at ?? null
                ]
            );
        }

        await client.query(
            `
            UPDATE underlyings
            SET active = FALSE,
                status = 'deprecated',
                status_reason = COALESCE(status_reason, 'Deprecated by Phase 5.5 PB FCN universe restructure'),
                removed_at = COALESCE(removed_at, NOW())
            WHERE symbol = ANY($1::text[])
               OR symbol <> ALL($2::text[])
            `,
            [DEPRECATED_SYMBOLS, UNDERLYINGS.map((entry) => entry.symbol)]
        );

        await client.query('COMMIT');
        const activeCount = UNDERLYINGS.filter((u) => u.active).length;
        console.log(`Successfully seeded ${activeCount} active underlyings; deprecated ${DEPRECATED_SYMBOLS.join(', ')}`);
        return { status: 'ok', active_count: activeCount, deprecated_symbols: DEPRECATED_SYMBOLS };
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release();
    }
}

if (require.main === module) {
    seedUnderlyingsInline()
        .then(async () => {
            const { pool } = await import('../db/client');
            await pool.end();
        })
        .catch(async (error: unknown) => {
            const message = error instanceof Error ? error.message : String(error);
            console.error(message);
            process.exitCode = 1;
            const { pool } = await import('../db/client');
            await pool.end();
        });
}
