import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    getLatestMacroRegimeController,
    refreshMacroRegimeController
} from '../controllers/macro-regime-controller';

export const macroRegimeRouter = Router();

macroRegimeRouter.get('/latest', asyncHandler(getLatestMacroRegimeController));
macroRegimeRouter.post('/refresh', asyncHandler(refreshMacroRegimeController));
