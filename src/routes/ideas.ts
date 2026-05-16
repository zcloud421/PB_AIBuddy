import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    getSymbolIdeaController,
    getSymbolNarrativeController,
    getSymbolPriceHistoryController,
    getSymbolIdeaStatusController,
    getTodayIdeasController
} from '../controllers/ideas-controller';

export const ideasRouter = Router();

ideasRouter.get('/today', asyncHandler(getTodayIdeasController));
ideasRouter.get('/:symbol/price-history', asyncHandler(getSymbolPriceHistoryController));
ideasRouter.get('/:symbol/narrative', asyncHandler(getSymbolNarrativeController));
ideasRouter.get('/:symbol', asyncHandler(getSymbolIdeaController));
ideasRouter.get('/:symbol/status/:job_id', asyncHandler(getSymbolIdeaStatusController));
