import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    getClientFocusDetailController,
    getClientFocusListController,
    getDailyMarketNarrativeController,
    getClientFocusMarketStateController,
    getMiddleEastPolymarketController,
    getSymbolIdeaController,
    getSymbolNarrativeController,
    getSymbolPriceHistoryController,
    getSymbolIdeaStatusController,
    getTodayIdeasController
} from '../controllers/ideas-controller';

export const ideasRouter = Router();

ideasRouter.get('/today', asyncHandler(getTodayIdeasController));
ideasRouter.get('/focus', asyncHandler(getClientFocusListController));
ideasRouter.get('/focus/daily-narrative', asyncHandler(getDailyMarketNarrativeController));
ideasRouter.get('/focus/market-state', asyncHandler(getClientFocusMarketStateController));
ideasRouter.get('/focus/middle-east-polymarket', asyncHandler(getMiddleEastPolymarketController));
ideasRouter.get('/focus/:slug', asyncHandler(getClientFocusDetailController));
ideasRouter.get('/:symbol/price-history', asyncHandler(getSymbolPriceHistoryController));
ideasRouter.get('/:symbol/narrative', asyncHandler(getSymbolNarrativeController));
ideasRouter.get('/:symbol', asyncHandler(getSymbolIdeaController));
ideasRouter.get('/:symbol/status/:job_id', asyncHandler(getSymbolIdeaStatusController));
