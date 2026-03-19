import type { Request, Response } from 'express';

import {
    getSymbolIdea,
    getSymbolIdeaStatus,
    getTodayIdeas
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

export async function getSymbolIdeaStatusController(req: Request, res: Response): Promise<void> {
    const payload = await getSymbolIdeaStatus(req.params.symbol, req.params.job_id);
    res.setHeader('Cache-Control', 'private, no-cache');

    if (payload.status === 'PENDING' || payload.status === 'RUNNING') {
        res.setHeader('Retry-After', '2');
    }

    res.status(200).json(payload);
}

