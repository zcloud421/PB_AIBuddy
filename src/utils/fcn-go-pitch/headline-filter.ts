const CLICKBAIT_PATTERNS = [
    /^Why .+\??$/i,
    /\bshould (investors|you)\b/i,
    /\bis .+ a (buy|sell|hold)\b/i,
    /\bbuy,\s*sell,\s*or\s*hold\b/i,
    /\b(best|better) .+ to buy\b/i,
    /^.+\s+vs\.?\s+.+\?$/i,
    /^prediction:/i,
    /\bsecret weapon\b/i,
    /\bskyrocket\b/i,
    /\beveryone is talking\b/i
];

const NUMBER_TOKEN_REGEX = /\d+(?:\.\d+)?\s*(?:%|\$|bp|x|倍)?/gi;

export function isClickbait(title: string): boolean {
    return CLICKBAIT_PATTERNS.some((pattern) => pattern.test(title));
}

export function isSafeTemplateHeadline(title: string): boolean {
    if (isClickbait(title)) return false;
    const matches = title.match(NUMBER_TOKEN_REGEX) ?? [];
    return matches.every((raw) => {
        const normalized = raw.trim();
        if (/^20\d{2}$/.test(normalized)) return true;
        if (/^Q[1-4]$/i.test(normalized)) return true;
        return false;
    });
}
