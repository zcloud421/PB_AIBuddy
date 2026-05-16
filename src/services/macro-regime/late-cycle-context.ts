import fs from 'fs';
import path from 'path';

import { fetchFearGreedIndex, type FearGreedReading } from '../../data/cnn-fear-greed-fetcher';
import type { SpyHolding } from '../../data/spy-holdings-fetcher';
import type { LateCycleContext, LateCyclePillar, PillarState } from './types';

interface ManualPillarState {
    state: PillarState;
    summary: string;
    evidence: string[];
    last_reviewed_at: string;
}

interface ManualStateFile {
    valuation?: ManualPillarState;
}

const MANUAL_STATE_PATH = path.resolve(process.cwd(), 'data', 'late_cycle_pillars_state.json');
const STALE_REVIEW_DAYS = 100;

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function daysSince(dateIso: string): number {
    const date = new Date(`${dateIso}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return 9999;
    const today = new Date(`${todayIsoDate()}T00:00:00Z`);
    return Math.max(0, Math.floor((today.getTime() - date.getTime()) / (24 * 60 * 60 * 1000)));
}

function toPillar(state: ManualPillarState | undefined, fallbackSummary: string): LateCyclePillar {
    const lastReviewed = state?.last_reviewed_at ?? '1970-01-01';
    const reviewAge = daysSince(lastReviewed);
    return {
        state: state?.state ?? 'normal',
        summary: state?.summary ?? fallbackSummary,
        evidence: Array.isArray(state?.evidence) ? state.evidence : [],
        last_reviewed_at: lastReviewed,
        days_since_review: reviewAge,
        stale_warning: reviewAge > STALE_REVIEW_DAYS
    };
}

function readManualState(): ManualStateFile {
    try {
        const raw = fs.readFileSync(MANUAL_STATE_PATH, 'utf8');
        return JSON.parse(raw) as ManualStateFile;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`[late-cycle-context] manual state unavailable: ${message}`);
        return {};
    }
}

function topHoldings(holdings: SpyHolding[]): SpyHolding[] {
    return [...holdings]
        .filter((holding) => Number.isFinite(holding.weight_pct) && holding.weight_pct > 0)
        .sort((left, right) => right.weight_pct - left.weight_pct)
        .slice(0, 10);
}

function computeHhi(holdings: SpyHolding[]): number {
    return Math.round(
        holdings.reduce((sum, holding) => sum + holding.weight_pct * holding.weight_pct, 0)
    );
}

function buildConcentrationPillar(spyHoldings: SpyHolding[] | null): LateCyclePillar {
    const reviewDate = todayIsoDate();
    if (!spyHoldings || spyHoldings.length === 0) {
        return {
            state: 'normal',
            summary: 'Top-10 concentration unavailable',
            evidence: ['SPY holdings fetch failed; context pillar skipped'],
            last_reviewed_at: reviewDate,
            days_since_review: 0,
            stale_warning: false
        };
    }

    const top10 = topHoldings(spyHoldings);
    const share = top10.reduce((sum, holding) => sum + holding.weight_pct, 0);
    const state: PillarState = share > 38 ? 'extreme' : share >= 32 ? 'elevated' : 'normal';
    const hhi = computeHhi(spyHoldings);

    return {
        state,
        summary: `Top-10 ${share.toFixed(1)}% (${state}; dot-com peak 18%)`,
        evidence: [
            `Top-10 S&P 500 weight ${share.toFixed(1)}%`,
            `HHI ${hhi}`,
            `Largest weights: ${top10.slice(0, 5).map((holding) => `${holding.ticker} ${holding.weight_pct.toFixed(1)}%`).join(', ')}`
        ],
        last_reviewed_at: reviewDate,
        days_since_review: 0,
        stale_warning: false
    };
}

function buildOasComplacencyPillar(hyOasSeries: number[] | null): LateCyclePillar {
    const reviewDate = todayIsoDate();
    const latest = hyOasSeries && hyOasSeries.length > 0 ? hyOasSeries[hyOasSeries.length - 1] : null;
    if (latest === null || !Number.isFinite(latest)) {
        return {
            state: 'normal',
            summary: 'HY OAS unavailable',
            evidence: ['FRED HY OAS series unavailable; context pillar skipped'],
            last_reviewed_at: reviewDate,
            days_since_review: 0,
            stale_warning: false
        };
    }

    const state: PillarState = latest < 250 ? 'extreme' : latest < 300 ? 'elevated' : 'normal';
    const label =
        state === 'extreme'
            ? 'extreme complacency'
            : state === 'elevated'
                ? 'elevated; investors not pricing tail risk'
                : 'normal risk pricing';

    return {
        state,
        summary: `HY OAS ${latest.toFixed(0)}bp (${label})`,
        evidence: [
            `HY OAS latest ${latest.toFixed(0)}bp`,
            'Display-only context: tight spreads can persist and are not timing triggers',
            'Kept outside severity aggregator to avoid late-cycle alert fatigue'
        ],
        last_reviewed_at: reviewDate,
        days_since_review: 0,
        stale_warning: false
    };
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
            '来源 CNN Fear & Greed Index · 7 个 sub-indicator 加权'
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
        evidence: ['数据源 CNN dataviz 暂时不可达,稍后重试'],
        last_reviewed_at: todayIsoDate(),
        days_since_review: 0,
        stale_warning: false
    };
}

export async function buildLateCycleContext(
    spyHoldings: SpyHolding[] | null,
    hyOasSeries: number[] | null
): Promise<LateCycleContext> {
    const manualState = readManualState();
    const valuation = toPillar(manualState.valuation, 'Valuation review unavailable');
    const fearGreed = await fetchFearGreedIndex();
    const sentimentManual = fearGreed
        ? buildSentimentFromFearGreed(fearGreed)
        : buildSentimentFallback();
    const concentrationDisplay = buildConcentrationPillar(spyHoldings);
    const oasComplacency = buildOasComplacencyPillar(hyOasSeries);

    const pillars = {
        valuation,
        concentration_display: concentrationDisplay,
        oas_complacency: oasComplacency,
        sentiment_manual: sentimentManual
    };
    const elevatedCount = Object.values(pillars).filter((pillar) => pillar.state !== 'normal').length;
    const softPauseActive = elevatedCount >= 2;

    return {
        elevated_count: elevatedCount,
        soft_pause_active: softPauseActive,
        headline_suffix: softPauseActive ? '; late-cycle elevated · soft-pause aggressive AI adds' : '',
        consecutive_days_active: 0,
        fatigue_warning: false,
        pillars
    };
}
