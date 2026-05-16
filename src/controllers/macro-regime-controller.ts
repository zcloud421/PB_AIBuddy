import type { Request, Response } from 'express';

import { buildMacroRegimeSnapshot } from '../services/macro-regime/snapshot-builder';
import {
    ensureMacroRegimeSnapshotsTable,
    getLatestMacroRegimeSnapshot,
    upsertMacroRegimeSnapshot
} from '../db/queries/macro-regime';

/**
 * GET /macro-regime/latest
 *
 * Returns the most recently persisted snapshot. If nothing in DB yet (first
 * deploy, before the daily cron has run), responds 503 to make it explicit
 * that data is missing rather than returning a misleading empty object.
 */
export async function getLatestMacroRegimeController(_req: Request, res: Response): Promise<void> {
    await ensureMacroRegimeSnapshotsTable();
    const snapshot = await getLatestMacroRegimeSnapshot();
    if (!snapshot) {
        res.status(503).json({
            error: 'NO_SNAPSHOT_YET',
            message: 'Macro regime snapshot has not been generated yet.'
        });
        return;
    }
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(snapshot);
}

/**
 * POST /macro-regime/refresh — admin-only, runs the snapshot pipeline now
 * and writes to DB. Guarded by SETUP_TOKEN.
 *
 * Useful for testing without waiting for the cron, and as a recovery hook
 * if a scheduled run failed.
 */
export async function refreshMacroRegimeController(req: Request, res: Response): Promise<void> {
    const rawSetupToken = process.env.SETUP_TOKEN;
    const rawProvidedToken =
        (typeof req.query.token === 'string' ? req.query.token : null) ??
        req.header('x-setup-token') ??
        null;
    const setupToken = rawSetupToken?.trim() ?? null;
    const providedToken = rawProvidedToken?.trim() ?? null;
    if (!setupToken || providedToken !== setupToken) {
        res.status(403).json({ error: 'forbidden' });
        return;
    }

    await ensureMacroRegimeSnapshotsTable();
    const snapshot = await buildMacroRegimeSnapshot();
    await upsertMacroRegimeSnapshot(snapshot);
    res.setHeader('Cache-Control', 'private, no-cache');
    res.status(200).json(snapshot);
}
