export interface StyleRepetitionHit {
    phrase: string;
    count_7d: number;
}

const REPETITION_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const WATCH_PHRASES = [
    '订单可见度较高',
    '支撑未来收入',
    '技术形态健康',
    '均线多头排列',
    '动量未破',
    'AI 需求支撑',
    '当前卖 put 条件不够友好',
    '短期事件风险未落地',
    '趋势结构转弱',
    '结构优势不够清晰'
];

const usage = new Map<string, number[]>();

export function checkRepetitionStyle(text: string): StyleRepetitionHit[] {
    const now = Date.now();
    const hits: StyleRepetitionHit[] = [];

    for (const phrase of WATCH_PHRASES) {
        const existing = (usage.get(phrase) ?? []).filter((ts) => now - ts <= REPETITION_WINDOW_MS);
        if (text.includes(phrase)) {
            existing.push(now);
            if (existing.length > 3) {
                hits.push({ phrase, count_7d: existing.length });
            }
        }
        usage.set(phrase, existing);
    }

    return hits;
}

export function logStyleRepetitionWarning(symbol: string, text: string, hits: StyleRepetitionHit[]): void {
    if (hits.length === 0) return;
    console.log(JSON.stringify({
        tag: 'style_repetition_warning',
        symbol,
        hits,
        text_preview: text.slice(0, 120),
        ts: new Date().toISOString()
    }));
}
