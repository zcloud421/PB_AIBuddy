import { Router } from 'express';

import { getTrackerHistoryController, getTrackerSummaryController } from '../controllers/tracker-controller';
import { asyncHandler } from '../lib/async-handler';

export const trackerRouter = Router();

trackerRouter.get('/summary', asyncHandler(getTrackerSummaryController));
trackerRouter.get('/history', asyncHandler(getTrackerHistoryController));
