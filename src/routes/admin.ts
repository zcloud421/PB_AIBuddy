import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    createHouseOverride,
    listActiveHouseOverrides,
    listUniverse,
    revokeHouseOverride,
    updateUnderlyingStatus
} from '../db/queries/ideas';

export const adminRouter = Router();

adminRouter.use((req, res, next) => {
    const expected = process.env.SETUP_TOKEN?.trim() ?? null;
    const provided = req.header('x-setup-token')?.trim() ?? null;
    if (!expected || provided !== expected) {
        res.status(403).json({ error: 'forbidden' });
        return;
    }
    next();
});

adminRouter.get('/overrides', asyncHandler(async (_req, res) => {
    res.json({ overrides: await listActiveHouseOverrides() });
}));

adminRouter.post('/overrides', asyncHandler(async (req, res) => {
    const symbol = normalizeSymbol(req.body?.symbol);
    const action = req.body?.action;
    const reason = normalizeText(req.body?.reason);
    const setBy = normalizeText(req.body?.set_by);
    const expiresAt = normalizeText(req.body?.expires_at);

    if (!symbol || !isOverrideAction(action) || !reason || !setBy) {
        res.status(400).json({ error: 'invalid_override_payload' });
        return;
    }

    const override = await createHouseOverride({
        symbol,
        action,
        reason,
        setBy,
        expiresAt: expiresAt ?? null
    });
    res.status(201).json({ override });
}));

adminRouter.delete('/overrides/:symbol', asyncHandler(async (req, res) => {
    const symbol = normalizeSymbol(req.params.symbol);
    if (!symbol) {
        res.status(400).json({ error: 'invalid_symbol' });
        return;
    }
    const revoked = await revokeHouseOverride(symbol);
    res.json({ symbol, revoked });
}));

adminRouter.get('/universe', asyncHandler(async (_req, res) => {
    res.json({ underlyings: await listUniverse() });
}));

adminRouter.patch('/universe/:symbol/status', asyncHandler(async (req, res) => {
    const symbol = normalizeSymbol(req.params.symbol);
    const newStatus = req.body?.new_status;
    const reason = normalizeText(req.body?.reason);
    const changedBy = normalizeText(req.body?.changed_by);

    if (!symbol || !isUnderlyingStatus(newStatus) || !changedBy) {
        res.status(400).json({ error: 'invalid_status_payload' });
        return;
    }

    const underlying = await updateUnderlyingStatus({
        symbol,
        newStatus,
        reason: reason ?? null,
        changedBy
    });
    if (!underlying) {
        res.status(404).json({ error: 'symbol_not_found' });
        return;
    }
    res.json({ underlying });
}));

function normalizeSymbol(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const normalized = value.trim().toUpperCase();
    return /^[A-Z0-9.\-]{1,12}$/.test(normalized) ? normalized : null;
}

function normalizeText(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const normalized = value.trim();
    return normalized.length > 0 ? normalized : null;
}

function isOverrideAction(value: unknown): value is 'FORCE_AVOID' | 'FORCE_CAUTION' | 'WHITELIST_ONLY' {
    return value === 'FORCE_AVOID' || value === 'FORCE_CAUTION' || value === 'WHITELIST_ONLY';
}

function isUnderlyingStatus(value: unknown): value is 'active' | 'suspended' | 'under_review' | 'deprecated' {
    return value === 'active' || value === 'suspended' || value === 'under_review' || value === 'deprecated';
}
