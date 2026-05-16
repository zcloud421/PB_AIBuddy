import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    getLatestMacroRegimeController,
    logLateCyclePillarReviewController,
    refreshMacroRegimeController
} from '../controllers/macro-regime-controller';

export const macroRegimeRouter = Router();

macroRegimeRouter.get('/latest', asyncHandler(getLatestMacroRegimeController));
macroRegimeRouter.post('/refresh', asyncHandler(refreshMacroRegimeController));
macroRegimeRouter.post('/pillar-review-log', asyncHandler(logLateCyclePillarReviewController));
