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
    ensureRowLevelSecurity,
    ensureSourceQualityColumn,
    ensureUnderlyingCompanyNameColumn,
    ensureUnderlyingsGovernanceColumns
} from './db/queries/ideas';
import { ideasRouter } from './routes/ideas';
import { deviceRouter } from './routes/device';
import { trackerRouter } from './routes/tracker';
import { pairAnalysisRouter } from './routes/pair-analysis';
import { macroRegimeRouter } from './routes/macro-regime';
import { adminRouter } from './routes/admin';
import { ensureDeviceTables } from './db/queries/devices';
import {
    ensureIndicatorPersistenceTable,
    ensureIndicatorHistoryTable,
    ensureLateCyclePillarHistoryTable,
    ensureMacroRegimeSnapshotsTable
} from './db/queries/macro-regime';
import { asyncHandler } from './lib/async-handler';

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

    app.post('/screener/refresh', asyncHandler(async (req, res) => {
        const rawSetupToken = process.env.SETUP_TOKEN?.trim() ?? null;
        const rawProvided = req.header('x-setup-token')?.trim() ?? null;
        if (!rawSetupToken || rawProvided !== rawSetupToken) {
            res.status(403).json({ error: 'forbidden' });
            return;
        }

        const { runDailyScreener } = await import('./scripts/run-daily-screener');
        runDailyScreener()
            .then((result) => {
                console.log('[screener-cron] completed', result);
            })
            .catch((error) => {
                console.error('[screener-cron] failed:', error);
            });

        res.status(202).json({ status: 'screener triggered (async)' });
    }));

    app.post('/attribution-health/check', asyncHandler(async (req, res) => {
        const rawSetupToken = process.env.SETUP_TOKEN?.trim() ?? null;
        const rawProvided = req.header('x-setup-token')?.trim() ?? null;
        if (!rawSetupToken || rawProvided !== rawSetupToken) {
            res.status(403).json({ error: 'forbidden' });
            return;
        }

        const { runAttributionHealthCheck } = await import('./scripts/check-attribution-health');
        runAttributionHealthCheck()
            .then((result) => {
                console.log('[attrib-health-cron] completed', result);
            })
            .catch((error) => {
                console.error('[attrib-health-cron] failed:', error);
            });

        res.status(202).json({ status: 'attribution health check triggered (async)' });
    }));

    app.post('/narrative-health/check', asyncHandler(async (req, res) => {
        const rawSetupToken = process.env.SETUP_TOKEN?.trim() ?? null;
        const rawProvided = req.header('x-setup-token')?.trim() ?? null;
        if (!rawSetupToken || rawProvided !== rawSetupToken) {
            res.status(403).json({ error: 'forbidden' });
            return;
        }

        const { runNarrativeHealthCheck } = await import('./scripts/check-narrative-health');
        runNarrativeHealthCheck()
            .then((result) => {
                console.log('[narrative-health-cron] completed', result);
            })
            .catch((error) => {
                console.error('[narrative-health-cron] failed:', error);
            });

        res.status(202).json({ status: 'narrative health check triggered (async)' });
    }));

    app.use('/ideas', ideasRouter);
    app.use('/device', deviceRouter);
    app.use('/tracker', trackerRouter);
    app.use('/api/pair-analysis', pairAnalysisRouter);
    app.use('/macro-regime', macroRegimeRouter);
    app.use('/admin', adminRouter);
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
    await ensureUnderlyingsGovernanceColumns();
    await ensureSourceQualityColumn();
    await ensureDeviceTables();
    await ensureMacroRegimeSnapshotsTable();
    await ensureLateCyclePillarHistoryTable();
    await ensureIndicatorPersistenceTable();
    await ensureIndicatorHistoryTable();
    // Must run LAST: secures every table created by the guards above, plus any
    // future table, against Supabase's public PostgREST API.
    await ensureRowLevelSecurity();
}

if (require.main === module) {
    const PORT = parseInt(process.env.PORT || '3000', 10);
    const app = createApp();

    ensureSchemaGuards()
        .then(() => {
            app.listen(PORT, '0.0.0.0', () => {
                // eslint-disable-next-line no-console
                console.log(`FCN API listening on port ${PORT}`);
            });
        })
        .catch((error) => {
            // eslint-disable-next-line no-console
            console.error('Schema guard initialization failed:', error);
            process.exit(1);
        });
}
