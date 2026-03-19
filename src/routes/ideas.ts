import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    getSymbolIdeaController,
    getSymbolIdeaStatusController,
    getTodayIdeasController
} from '../controllers/ideas-controller';

export const ideasRouter = Router();

ideasRouter.get('/today', asyncHandler(getTodayIdeasController));
ideasRouter.get('/:symbol', asyncHandler(getSymbolIdeaController));
ideasRouter.get('/:symbol/status/:job_id', asyncHandler(getSymbolIdeaStatusController));

