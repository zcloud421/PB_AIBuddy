import { getTokensForFavoritedSymbols } from '../db/queries/devices';

interface GradeResult {
    symbol: string;
    grade: 'GO' | 'CAUTION' | 'AVOID';
}

interface PreviousGrade {
    symbol: string;
    grade: string;
}

const EXPO_PUSH_URL = 'https://exp.host/--/api/v2/push/send';
const PUSH_BATCH_SIZE = 100;

function isDowngrade(prev: string, next: string): boolean {
    const rank: Record<string, number> = { GO: 2, CAUTION: 1, AVOID: 0 };
    return (rank[next] ?? 0) < (rank[prev] ?? 0);
}

function gradeLabel(grade: string): string {
    if (grade === 'AVOID') return '不推荐';
    if (grade === 'CAUTION') return '需谨慎';
    return grade;
}

async function sendPushBatch(
    messages: Array<{ to: string; title: string; body: string; data?: Record<string, string> }>,
): Promise<void> {
    await fetch(EXPO_PUSH_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(messages),
    });
}

export async function sendDowngradeNotifications(
    todayResults: GradeResult[],
    previousGrades: PreviousGrade[],
): Promise<void> {
    const prevMap = new Map(previousGrades.map((r) => [r.symbol, r.grade]));

    const downgradedSymbols = todayResults
        .filter((r) => {
            const prev = prevMap.get(r.symbol);
            return prev !== undefined && isDowngrade(prev, r.grade);
        })
        .map((r) => ({ symbol: r.symbol, newGrade: r.grade, prevGrade: prevMap.get(r.symbol)! }));

    if (downgradedSymbols.length === 0) {
        return;
    }

    const symbolList = downgradedSymbols.map((d) => d.symbol);
    const tokenRows = await getTokensForFavoritedSymbols(symbolList);

    if (tokenRows.length === 0) {
        return;
    }

    const gradeMap = new Map(downgradedSymbols.map((d) => [d.symbol, d.newGrade]));

    const messages = tokenRows.map((row) => ({
        to: row.push_token,
        title: `${row.symbol} 评级变化`,
        body: `${row.symbol} 已调整为${gradeLabel(gradeMap.get(row.symbol) ?? '')}，建议重新评估`,
        data: { symbol: row.symbol },
    }));

    for (let i = 0; i < messages.length; i += PUSH_BATCH_SIZE) {
        await sendPushBatch(messages.slice(i, i + PUSH_BATCH_SIZE));
    }

    console.log(`[push] Sent downgrade notifications for ${downgradedSymbols.length} symbol(s) to ${tokenRows.length} device(s)`);
}
