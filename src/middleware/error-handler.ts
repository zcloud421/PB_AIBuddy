import type { NextFunction, Request, Response } from 'express';

import { HttpError } from '../lib/http-error';
import type { ErrorResponse } from '../types/api';

type RequestWithId = Request & { requestId?: string };

export function errorHandler(err: unknown, req: RequestWithId, res: Response, _next: NextFunction): void {
    const requestId = req.requestId ?? 'unknown';

    if (err instanceof HttpError) {
        const body: ErrorResponse = {
            error: {
                code: err.code,
                message: err.message,
                request_id: requestId
            }
        };

        res.status(err.statusCode).json(body);
        return;
    }

    const fallback: ErrorResponse = {
        error: {
            code: 'SCORING_ENGINE_UNAVAILABLE',
            message: 'The scoring engine is temporarily unavailable. Please retry shortly.',
            request_id: requestId
        }
    };

    res.status(503).json(fallback);
}
