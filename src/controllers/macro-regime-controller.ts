import type { Request, Response } from 'express';

import { buildMacroRegimeSnapshot } from '../services/macro-regime/snapshot-builder';
import {
    ensureIndicatorPersistenceTable,
    ensureIndicatorHistoryTable,
    ensureLateCyclePillarHistoryTable,
    ensureMacroRegimeSnapshotsTable,
    getLatestMacroRegimeSnapshot,
    getRecentVerdictHistory,
    insertLateCyclePillarHistory,
    upsertMacroRegimeSnapshot,
    type VerdictHistoryRow
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
    const history = await getRecentVerdictHistory(14);
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json({
        ...snapshot,
        verdict_history: history.map((row) => ({ date: row.run_date, state: row.state })),
        verdict_days_in_state: computeDaysInState(history),
        verdict_direction: computeVerdictDirection(history)
    });
}

// 刹车档位排序:任一机制升档 → 恶化;无升档且任一降档 → 缓和;否则持平。
// crowding 只有 quiet/elevated 两档,同样参与比较。
const BRAKE_RANK: Record<string, number> = { quiet: 0, elevated: 1, watch: 1, forming: 2, confirmed: 3 };

export function computeDaysInState(history: VerdictHistoryRow[]): number | null {
    const latest = history[history.length - 1];
    if (!latest?.state) return null;
    let days = 0;
    for (let i = history.length - 1; i >= 0; i -= 1) {
        if (history[i].state !== latest.state) break;
        days += 1;
    }
    return days;
}

export function computeVerdictDirection(history: VerdictHistoryRow[]): 'worse' | 'same' | 'better' | null {
    const latest = history[history.length - 1];
    const prev = history[history.length - 2];
    if (!latest?.brakes || !prev?.brakes) return null;
    let anyUp = false;
    let anyDown = false;
    // 只比较三个确认机制;拥挤(crowding)是置信度调节项,不上屏,若参与比较会出现
    // 「显示恶化但页面上三行机制全没变」的不可解释状态。
    for (const key of ['credit', 'rates', 'fundamental']) {
        const now = BRAKE_RANK[latest.brakes[key] ?? ''] ?? null;
        const before = BRAKE_RANK[prev.brakes[key] ?? ''] ?? null;
        if (now === null || before === null) continue;
        if (now > before) anyUp = true;
        if (now < before) anyDown = true;
    }
    if (anyUp) return 'worse';
    if (anyDown) return 'better';
    return 'same';
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
