import type { Request, Response } from 'express';

import {
    addDeviceFavorite,
    bulkAddDeviceFavorites,
    getDeviceFavorites,
    removeDeviceFavorite,
    upsertDeviceToken,
} from '../db/queries/devices';

export async function getFavoritesController(req: Request, res: Response): Promise<void> {
    const deviceId = typeof req.query.device_id === 'string' ? req.query.device_id.trim() : null;
    if (!deviceId) {
        res.status(400).json({ error: 'device_id required' });
        return;
    }
    const symbols = await getDeviceFavorites(deviceId);
    res.json({ symbols });
}

export async function addFavoriteController(req: Request, res: Response): Promise<void> {
    const { device_id, symbol } = req.body as { device_id?: string; symbol?: string };
    if (!device_id || !symbol) {
        res.status(400).json({ error: 'device_id and symbol required' });
        return;
    }
    await addDeviceFavorite(device_id.trim(), symbol.trim());
    res.json({ ok: true });
}

export async function removeFavoriteController(req: Request, res: Response): Promise<void> {
    const { device_id, symbol } = req.body as { device_id?: string; symbol?: string };
    if (!device_id || !symbol) {
        res.status(400).json({ error: 'device_id and symbol required' });
        return;
    }
    await removeDeviceFavorite(device_id.trim(), symbol.trim());
    res.json({ ok: true });
}

export async function bulkSyncFavoritesController(req: Request, res: Response): Promise<void> {
    const { device_id, symbols } = req.body as { device_id?: string; symbols?: unknown };
    if (!device_id || !Array.isArray(symbols)) {
        res.status(400).json({ error: 'device_id and symbols[] required' });
        return;
    }
    const validSymbols = symbols.filter((s): s is string => typeof s === 'string' && s.trim().length > 0);
    await bulkAddDeviceFavorites(device_id.trim(), validSymbols);
    res.json({ ok: true, synced: validSymbols.length });
}

export async function upsertTokenController(req: Request, res: Response): Promise<void> {
    const { device_id, push_token } = req.body as { device_id?: string; push_token?: string };
    if (!device_id || !push_token) {
        res.status(400).json({ error: 'device_id and push_token required' });
        return;
    }
    await upsertDeviceToken(device_id.trim(), push_token.trim());
    res.json({ ok: true });
}
