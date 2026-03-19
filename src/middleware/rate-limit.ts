import rateLimit from 'express-rate-limit';

export const apiRateLimit = rateLimit({
    windowMs: 60 * 1000,
    limit: 60,
    standardHeaders: true,
    legacyHeaders: false
});

