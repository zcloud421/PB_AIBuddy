import type { NarrativeInput } from '../narrative-generator';

export function parseCouponRange(s: string): { low: number; high: number } | null {
    const match = s.match(/(\d+(?:\.\d+)?)\s*%?\s*[-~–]\s*(\d+(?:\.\d+)?)\s*%?/);
    if (!match) return null;

    const low = Number(match[1]);
    const high = Number(match[2]);
    if (!Number.isFinite(low) || !Number.isFinite(high)) return null;
    return { low, high };
}

export function parseTenorMonths(tenorDays: number): string {
    const months = Math.max(1, Math.round(tenorDays / 30));
    return `${months} 个月`;
}

export function isHighIVString(level: string): boolean {
    return /high|extreme/i.test(level) || level.includes('高');
}

export function inferEarningsBeat(input: NarrativeInput): boolean | null {
    if (!input.has_recent_earnings) return null;

    const joined = (input.news_headlines || []).join(' ').toLowerCase();
    if (/beat|tops|exceed|crush|超预期|大超|强劲指引|raise.*guide/i.test(joined)) return true;
    if (/miss|disappoint|cut.*guide|低于预期|下调指引/i.test(joined)) return false;
    return null;
}
