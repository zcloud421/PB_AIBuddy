import type { Request, Response } from 'express';

import { buildMacroRegimeSnapshot } from '../services/macro-regime/snapshot-builder';
import {
    ensureIndicatorPersistenceTable,
    ensureIndicatorHistoryTable,
    ensureLateCyclePillarHistoryTable,
    ensureMacroRegimeSnapshotsTable,
    getLatestMacroRegimeSnapshot,
    insertLateCyclePillarHistory,
    upsertMacroRegimeSnapshot
} from '../db/queries/macro-regime';

function requireSetupToken(req: Request, res: Response): boolean {
    const rawSetupToken = process.env.SETUP_TOKEN;
    const rawProvidedToken =
        (typeof req.query.token === 'string' ? req.query.token : null) ??
        req.header('x-setup-token') ??
        null;
    const setupToken = rawSetupToken?.trim() ?? null;
    const providedToken = rawProvidedToken?.trim() ?? null;
    if (!setupToken || providedToken !== setupToken) {
        res.status(403).json({ error: 'forbidden' });
        return false;
    }
    return true;
}

function warnStaleLateCyclePillars(snapshot: Awaited<ReturnType<typeof buildMacroRegimeSnapshot>>): void {
    const pillars = snapshot.late_cycle_context.pillars;
    for (const [key, pillar] of Object.entries(pillars)) {
        if (pillar.stale_warning) {
            console.warn(`[late-cycle-context] pillar ${key} stale: ${pillar.days_since_review} days`);
        }
    }
}

/**
 * GET /macro-regime/latest
 *
 * Returns the most recently persisted snapshot. If nothing in DB yet (first
 * deploy, before the daily cron has run), responds 503 to make it explicit
 * that data is missing rather than returning a misleading empty object.
 */
export async function getLatestMacroRegimeController(_req: Request, res: Response): Promise<void> {
    await ensureMacroRegimeSnapshotsTable();
    await ensureIndicatorPersistenceTable();
    await ensureIndicatorHistoryTable();
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
    if (!requireSetupToken(req, res)) return;

    await ensureMacroRegimeSnapshotsTable();
    await ensureLateCyclePillarHistoryTable();
    await ensureIndicatorPersistenceTable();
    await ensureIndicatorHistoryTable();
    const snapshot = await buildMacroRegimeSnapshot();
    warnStaleLateCyclePillars(snapshot);
    await upsertMacroRegimeSnapshot(snapshot);
    res.setHeader('Cache-Control', 'private, no-cache');
    res.status(200).json(snapshot);
}

export async function logLateCyclePillarReviewController(req: Request, res: Response): Promise<void> {
    if (!requireSetupToken(req, res)) return;

    const { pillar, old_state, new_state, evidence_snapshot, reviewed_at } = req.body ?? {};
    if (
        typeof pillar !== 'string' ||
        typeof old_state !== 'string' ||
        typeof new_state !== 'string'
    ) {
        res.status(400).json({
            error: 'invalid_request',
            message: 'pillar, old_state, and new_state are required strings'
        });
        return;
    }

    await insertLateCyclePillarHistory({
        reviewed_at: typeof reviewed_at === 'string' ? reviewed_at : new Date().toISOString(),
        pillar,
        old_state,
        new_state,
        evidence_snapshot: evidence_snapshot ?? {}
    });
    res.status(200).json({ ok: true });
}
