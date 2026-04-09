import express from 'express';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

import { errorHandler } from './middleware/error-handler';
import { apiRateLimit } from './middleware/rate-limit';
import { requestIdMiddleware } from './middleware/request-id';
import { pool } from './db/client';
import {
    deleteTodayIdeaCandidate,
    ensureDailyBestHistoryTable,
    ensureDailyRecommendationHistoryTable,
    ensureEarningsCalendarColumns,
    ensureIdeaCandidatePriceColumns,
    ensureRecommendationTrackerTable,
    ensureRiskFlagEnumValues,
    ensureUnderlyingCompanyNameColumn
} from './db/queries/ideas';
import { ideasRouter } from './routes/ideas';
import { deviceRouter } from './routes/device';
import { trackerRouter } from './routes/tracker';
import { pairAnalysisRouter } from './routes/pair-analysis';
import { ensureDeviceTables } from './db/queries/devices';
import { getClientFocusList } from './services/client-focus-service';

dotenv.config();

export function createApp() {
    const app = express();

    app.use(express.json());
    app.use(requestIdMiddleware);
    app.use(apiRateLimit);

    app.get('/health', (_req, res) => {
        res.status(200).json({
            status: 'ok'
        });
    });

    app.get('/setup', async (req, res) => {
        try {
            const rawSetupToken = process.env.SETUP_TOKEN;
            const rawProvidedToken =
                (typeof req.query.token === 'string' ? req.query.token : null) ??
                req.header('x-setup-token') ??
                null;
            const setupToken = rawSetupToken?.trim() ?? null;
            const providedToken = rawProvidedToken?.trim() ?? null;

            if (!setupToken || providedToken !== setupToken) {
                res.status(403).json({
                    error: 'forbidden',
                    debug: {
                        hasSetupToken: Boolean(setupToken),
                        providedLength: providedToken?.length ?? 0,
                        expectedLength: setupToken?.length ?? 0
                    }
                });
                return;
            }

            const sql = fs.readFileSync(path.join(__dirname, '../schema.sql'), 'utf8');
            await pool.query(sql);
            res.status(200).json({ status: 'schema imported' });
        } catch (error) {
            res.status(500).json({
                error: error instanceof Error ? error.message : String(error)
            });
        }
    });

    app.delete('/cache/:symbol', async (req, res) => {
        const rawSetupToken = process.env.SETUP_TOKEN;
        const rawProvidedToken = typeof req.query.token === 'string' ? req.query.token : null;
        const setupToken = rawSetupToken?.trim() ?? null;
        const providedToken = rawProvidedToken?.trim() ?? null;

        if (!setupToken || providedToken !== setupToken) {
            res.status(403).json({ error: 'forbidden' });
            return;
        }

        const symbol = req.params.symbol.toUpperCase();
        await deleteTodayIdeaCandidate(symbol);
        res.json({ status: 'cleared', symbol });
    });

    app.use('/ideas', ideasRouter);
    app.use('/device', deviceRouter);
    app.use('/tracker', trackerRouter);
    app.use('/api/pair-analysis', pairAnalysisRouter);
    app.use(errorHandler);

    return app;
}

async function ensureSchemaGuards(): Promise<void> {
    await ensureDailyBestHistoryTable();
    await ensureDailyRecommendationHistoryTable();
    await ensureIdeaCandidatePriceColumns();
    await ensureEarningsCalendarColumns();
    await ensureRiskFlagEnumValues();
    await ensureRecommendationTrackerTable();
    await ensureUnderlyingCompanyNameColumn();
    await ensureDeviceTables();
}

if (require.main === module) {
    const PORT = parseInt(process.env.PORT || '3000', 10);
    const app = createApp();

    ensureSchemaGuards()
        .then(() => {
            app.listen(PORT, '0.0.0.0', () => {
                // eslint-disable-next-line no-console
                console.log(`FCN API listening on port ${PORT}`);
                // Pre-warm focus cache in background — non-blocking
                setTimeout(() => {
                    getClientFocusList()
                        .then(() => console.log('[focus-prewarm] cache warmed successfully'))
                        .catch((err) => console.warn('[focus-prewarm] failed:', err instanceof Error ? err.message : String(err)));
                }, 5000);
            });
        })
        .catch((error) => {
            // eslint-disable-next-line no-console
            console.error('Schema guard initialization failed:', error);
            process.exit(1);
        });
}
