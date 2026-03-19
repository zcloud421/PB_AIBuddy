export class HttpError extends Error {
    public readonly statusCode: number;
    public readonly code: 'NOT_FOUND' | 'SCORING_ENGINE_UNAVAILABLE';

    constructor(statusCode: number, code: 'NOT_FOUND' | 'SCORING_ENGINE_UNAVAILABLE', message: string) {
        super(message);
        this.statusCode = statusCode;
        this.code = code;
    }
}

