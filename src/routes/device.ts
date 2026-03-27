import { Router } from 'express';

import { asyncHandler } from '../lib/async-handler';
import {
    addFavoriteController,
    bulkSyncFavoritesController,
    getFavoritesController,
    removeFavoriteController,
    upsertTokenController,
} from '../controllers/device-controller';

export const deviceRouter = Router();

deviceRouter.get('/favorites', asyncHandler(getFavoritesController));
deviceRouter.post('/favorites', asyncHandler(addFavoriteController));
deviceRouter.delete('/favorites', asyncHandler(removeFavoriteController));
deviceRouter.post('/favorites/sync', asyncHandler(bulkSyncFavoritesController));
deviceRouter.post('/token', asyncHandler(upsertTokenController));
