import type { Request, Response } from 'express';

import {
    getClientFocusDetail,
    getClientFocusList,
    getDailyMarketNarrative,
    refreshDailyMarketNarrativeNow,
    getClientFocusMarketState,
    getMiddleEastPolymarket,
    getSymbolIdea,
    getSymbolNarrative,
    getSymbolPriceHistory,
    getSymbolIdeaStatus,
    getTodayIdeas
} from '../services/ideas-service';
import { HttpError } from '../lib/http-error';

export async function getTodayIdeasController(_req: Request, res: Response): Promise<void> {
    const payload = await getTodayIdeas();
    res.setHeader('Cache-Control', 'private, max-age=60');
    res.status(200).json(payload);
}

export async function getClientFocusListController(_req: Request, res: Response): Promise<void> {
    const payload = await getClientFocusList();
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function getClientFocusDetailController(req: Request, res: Response): Promise<void> {
    const payload = await getClientFocusDetail(req.params.slug);
    if (!payload) {
        throw new HttpError(404, 'NOT_FOUND', 'Client focus topic not found.');
    }

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function getClientFocusMarketStateController(_req: Request, res: Response): Promise<void> {
    const payload = await getClientFocusMarketState();
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function getDailyMarketNarrativeController(_req: Request, res: Response): Promise<void> {
    const payload = await getDailyMarketNarrative();
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function refreshDailyMarketNarrativeController(req: Request, res: Response): Promise<void> {
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

    const payload = await refreshDailyMarketNarrativeNow();
    res.setHeader('Cache-Control', 'private, no-cache');
    res.status(200).json(payload);
}

export async function getMiddleEastPolymarketController(_req: Request, res: Response): Promise<void> {
    const payload = await getMiddleEastPolymarket();
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function getSymbolIdeaController(req: Request, res: Response): Promise<void> {
    const payload = await getSymbolIdea(req.params.symbol);

    if ('job_id' in payload) {
        res.setHeader('Cache-Control', 'private, no-cache');
        res.setHeader('Retry-After', '2');
        res.status(202).json(payload);
        return;
    }

    res.setHeader('Cache-Control', payload.cached ? 'private, max-age=300' : 'private, no-cache');
    res.setHeader('X-Cache', payload.cached ? 'HIT' : 'MISS');
    res.status(200).json(payload);
}

export async function getSymbolNarrativeController(req: Request, res: Response): Promise<void> {
    const payload = await getSymbolNarrative(req.params.symbol);
    res.setHeader('Cache-Control', 'private, max-age=60');
    res.status(200).json(payload);
}

export async function getSymbolPriceHistoryController(req: Request, res: Response): Promise<void> {
    const strikePctRaw = typeof req.query.strike_pct === 'string' ? Number(req.query.strike_pct) : null;
    const strikePct =
        strikePctRaw !== null && Number.isFinite(strikePctRaw) && strikePctRaw > 0 && strikePctRaw <= 100
            ? strikePctRaw
            : null;
    const payload = await getSymbolPriceHistory(req.params.symbol, strikePct);
    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(payload);
}

export async function getSymbolIdeaStatusController(req: Request, res: Response): Promise<void> {
    const payload = await getSymbolIdeaStatus(req.params.symbol, req.params.job_id);
    res.setHeader('Cache-Control', 'private, no-cache');

    if (payload.status === 'PENDING' || payload.status === 'RUNNING') {
        res.setHeader('Retry-After', '2');
    }

    res.status(200).json(payload);
}
