import { fetchFearGreedIndex, type FearGreedReading } from '../../data/cnn-fear-greed-fetcher';
import type { LateCycleContext, LateCyclePillar, PillarState } from './types';

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function formatFearGreedScore(value: number): string {
    return Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1);
}

function fearGreedChineseLabel(score: number): { state: PillarState; label: string } {
    if (score >= 75) return { state: 'extreme', label: '极度贪婪' };
    if (score >= 60) return { state: 'elevated', label: '贪婪' };
    if (score >= 45) return { state: 'normal', label: '中性' };
    if (score >= 25) return { state: 'normal', label: '恐慌' };
    return { state: 'normal', label: '极度恐慌' };
}

function buildSentimentFromFearGreed(reading: FearGreedReading): LateCyclePillar {
    const { state, label } = fearGreedChineseLabel(reading.score);
    const score = formatFearGreedScore(reading.score);
    return {
        state,
        summary: `F&G ${score} · ${label}`,
        evidence: [
            `当前分值 ${score} (${reading.rating})`,
            `上周 ${formatFearGreedScore(reading.previous_1_week)} · 上月 ${formatFearGreedScore(reading.previous_1_month)}`,
            '来源 CNN Fear & Greed Index · 7 个子指标加权'
        ],
        last_reviewed_at: todayIsoDate(),
        days_since_review: 0,
        stale_warning: false
    };
}

function buildSentimentFallback(): LateCyclePillar {
    return {
        state: 'normal',
        summary: 'F&G 数据暂不可用',
        evidence: ['CNN 数据源暂时不可达,稍后重试'],
        last_reviewed_at: todayIsoDate(),
        days_since_review: 0,
        stale_warning: false
    };
}

export async function buildLateCycleContext(): Promise<LateCycleContext> {
    const fearGreed = await fetchFearGreedIndex();
    const sentimentManual = fearGreed
        ? buildSentimentFromFearGreed(fearGreed)
        : buildSentimentFallback();

    const pillars = {
        sentiment_manual: sentimentManual
    };
    return {
        pillars
    };
}
