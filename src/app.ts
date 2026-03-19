import express from 'express';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

import { errorHandler } from './middleware/error-handler';
import { apiRateLimit } from './middleware/rate-limit';
import { requestIdMiddleware } from './middleware/request-id';
import { pool } from './db/client';
import { ideasRouter } from './routes/ideas';
import { trackerRouter } from './routes/tracker';

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

    app.use('/ideas', ideasRouter);
    app.use('/tracker', trackerRouter);
    app.use(errorHandler);

    return app;
}

if (require.main === module) {
    const PORT = parseInt(process.env.PORT || '3000', 10);
    const app = createApp();

    app.listen(PORT, '0.0.0.0', () => {
        // eslint-disable-next-line no-console
        console.log(`FCN API listening on port ${PORT}`);
    });
}
