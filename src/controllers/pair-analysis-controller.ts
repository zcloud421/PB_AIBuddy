import type { Request, Response } from 'express';

import { analyzePairSuitability } from '../services/pair-analysis-service';

interface PairAnalysisRequestBody {
    symbolA?: string;
    symbolB?: string;
}

export async function postPairAnalysisController(
    req: Request<unknown, unknown, PairAnalysisRequestBody>,
    res: Response
): Promise<void> {
    const symbolA = req.body?.symbolA?.trim();
    const symbolB = req.body?.symbolB?.trim();

    if (!symbolA || !symbolB) {
        res.status(400).json({ message: 'symbolA and symbolB are required' });
        return;
    }

    const result = await analyzePairSuitability(symbolA, symbolB);

    if (result.kind === 'not_found') {
        res.status(404).json({ message: 'Symbol not found in historical price data' });
        return;
    }

    if (result.kind === 'insufficient_data') {
        res.status(400).json({ message: '历史数据不足，无法计算配对适配度' });
        return;
    }

    res.setHeader('Cache-Control', 'private, max-age=300');
    res.status(200).json(result.data);
}
