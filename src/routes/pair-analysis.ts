import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import { postPairAnalysisController } from '../controllers/pair-analysis-controller';

export const pairAnalysisRouter = Router();

pairAnalysisRouter.post('/', asyncHandler(postPairAnalysisController));
