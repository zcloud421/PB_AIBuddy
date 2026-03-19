import type { NextFunction, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';

type RequestWithId = Request & { requestId?: string };

export function requestIdMiddleware(req: RequestWithId, res: Response, next: NextFunction): void {
    const headerValue = req.header('x-request-id');
    const requestId = headerValue && headerValue.trim().length > 0 ? headerValue : uuidv4();

    req.requestId = requestId;
    res.setHeader('X-Request-Id', requestId);
    next();
}
