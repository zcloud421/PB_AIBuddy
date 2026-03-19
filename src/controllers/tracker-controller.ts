import type { Request, Response } from 'express';

import { getTrackerHistory, getTrackerSummary } from '../services/tracker-service';

export async function getTrackerSummaryController(_req: Request, res: Response): Promise<void> {
    const payload = await getTrackerSummary();
    res.setHeader('Cache-Control', 'private, max-age=60');
    res.status(200).json(payload);
}

export async function getTrackerHistoryController(_req: Request, res: Response): Promise<void> {
    const payload = await getTrackerHistory();
    res.setHeader('Cache-Control', 'private, max-age=60');
    res.status(200).json(payload);
}
