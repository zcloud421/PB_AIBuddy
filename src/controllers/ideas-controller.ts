import type { Request, Response } from 'express';

import {
    getSymbolIdea,
    getSymbolNarrative,
    getSymbolPriceHistory,
    getSymbolIdeaStatus,
    getTodayIdeas,
    rescoreSymbolIdea
} from '../services/ideas-service';

export async function getTodayIdeasController(_req: Request, res: Response): Promise<void> {
    const payload = await getTodayIdeas();
    res.setHeader('Cache-Control', 'private, max-age=60');
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

export async function rescoreSymbolIdeaController(req: Request, res: Response): Promise<void> {
    const strike = Number(req.body?.strike);
    const tenorDays =
        req.body?.tenor_days === undefined || req.body?.tenor_days === null
            ? null
            : Number(req.body.tenor_days);

    if (!Number.isFinite(strike) || strike <= 0) {
        res.status(400).json({
            error: {
                code: 'SCORING_ENGINE_UNAVAILABLE',
                message: 'Body field "strike" must be a positive number.',
                request_id: (req as Request & { requestId?: string }).requestId ?? 'unknown'
            }
        });
        return;
    }

    if (tenorDays !== null && (!Number.isFinite(tenorDays) || tenorDays <= 0)) {
        res.status(400).json({
            error: {
                code: 'SCORING_ENGINE_UNAVAILABLE',
                message: 'Body field "tenor_days" must be a positive number when provided.',
                request_id: (req as Request & { requestId?: string }).requestId ?? 'unknown'
            }
        });
        return;
    }

    const payload = await rescoreSymbolIdea({
        symbol: req.params.symbol,
        strike,
        tenorDays
    });

    res.setHeader('Cache-Control', 'private, no-cache');
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
