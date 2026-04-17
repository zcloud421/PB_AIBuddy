import dotenv from 'dotenv';

import type { DrawdownAttribution, SymbolPriceHistoryResponse } from '../types/api';
import { getSymbolPriceHistory } from '../services/ideas-service';

dotenv.config();

const DEFAULT_SYMBOLS = [
    'BABA',
    'PDD',
    'JD',
    'BIDU',
    'GOOG',
    'META',
    'MSFT',
    'AMZN',
    'AAPL',
    'TSLA',
    'UNH',
    'LLY',
    'NVO',
    'TSM',
    'AMD',
    'MU',
    'NVDA',
    'LITE',
    'VRT',
    'COIN',
    'HOOD',
    'MSTR',
    'JPM',
    'GS',
    'V',
    'MA',
    'XOM',
    'CAT',
    'FCX'
];

type SymbolEvaluation = {
    symbol: string;
    attributionCount: number;
    cycleFamily: string | null;
    businessArchetype: string | null;
    orderValid: boolean;
    likelyFallbackCount: number;
    signalBackedCount: number;
    companyPrimaryCount: number;
    sectorPrimaryCount: number;
    macroPrimaryCount: number;
    policyPrimaryCount: number;
    geopoliticalPrimaryCount: number;
    mixedPrimaryCount: number;
};

const WARMUP_WAIT_MS = 8000;
const EMPTY_ATTRIBUTION_RETRY_COUNT = 2;
const EMPTY_ATTRIBUTION_RETRY_DELAY_MS = 3000;

function parseSymbols(argv: string[]): string[] {
    const requested = argv
        .flatMap((arg) => arg.split(','))
        .map((value) => value.trim().toUpperCase())
        .filter(Boolean);
    return requested.length > 0 ? [...new Set(requested)] : DEFAULT_SYMBOLS;
}

function getLikelyFallback(attribution: DrawdownAttribution): boolean {
    if (attribution.reason_zh?.includes('暂无明确')) {
        return true;
    }

    return (
        attribution.reason_family === 'company-fundamental' &&
        attribution.primary_driver_type === 'company' &&
        attribution.secondary_driver === '市场风险偏好回落放大跌幅' &&
        !attribution.background_regime
    );
}

function isDescendingByPeakDate(attributions: DrawdownAttribution[]): boolean {
    for (let index = 1; index < attributions.length; index += 1) {
        const prevTime = new Date(attributions[index - 1].peak_date).getTime();
        const nextTime = new Date(attributions[index].peak_date).getTime();
        if (prevTime < nextTime) {
            return false;
        }
    }
    return true;
}

function summarizeSymbol(symbol: string, attributions: DrawdownAttribution[]): SymbolEvaluation {
    const cycleFamily = attributions.find((item) => item.cycle_family)?.cycle_family ?? null;
    const businessArchetype = attributions.find((item) => item.business_archetype)?.business_archetype ?? null;
    const likelyFallbackCount = attributions.filter(getLikelyFallback).length;
    const signalBackedCount = attributions.filter((item) => (item.event_signal_details?.length ?? 0) > 0).length;

    return {
        symbol,
        attributionCount: attributions.length,
        cycleFamily,
        businessArchetype,
        orderValid: isDescendingByPeakDate(attributions),
        likelyFallbackCount,
        signalBackedCount,
        companyPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'company').length,
        sectorPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'sector').length,
        macroPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'macro').length,
        policyPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'policy').length,
        geopoliticalPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'geopolitical').length,
        mixedPrimaryCount: attributions.filter((item) => item.primary_driver_type === 'mixed').length
    };
}

function renderPct(numerator: number, denominator: number): string {
    if (denominator <= 0) return '0%';
    return `${Math.round((numerator / denominator) * 100)}%`;
}

function hasMaterialPriceHistory(response: SymbolPriceHistoryResponse): boolean {
    return (response.price_history?.length ?? 0) > 30;
}

function shouldRetryEmptyAttribution(response: SymbolPriceHistoryResponse): boolean {
    return hasMaterialPriceHistory(response) && (response.drawdown_attributions?.length ?? 0) === 0;
}

async function loadEvaluatedPriceHistory(symbol: string): Promise<SymbolPriceHistoryResponse> {
    let response = await getSymbolPriceHistory(symbol);

    for (let attempt = 0; attempt < EMPTY_ATTRIBUTION_RETRY_COUNT && shouldRetryEmptyAttribution(response); attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, EMPTY_ATTRIBUTION_RETRY_DELAY_MS));
        response = await getSymbolPriceHistory(symbol);
    }

    return response;
}

function printGroupSummary(label: string, evaluations: SymbolEvaluation[], selector: (item: SymbolEvaluation) => string | null) {
    const buckets = new Map<string, SymbolEvaluation[]>();
    for (const evaluation of evaluations) {
        const key = selector(evaluation) ?? 'unknown';
        const current = buckets.get(key) ?? [];
        current.push(evaluation);
        buckets.set(key, current);
    }

    console.log(`\n${label}`);
    console.log('-'.repeat(label.length));
    for (const [key, items] of [...buckets.entries()].sort((left, right) => left[0].localeCompare(right[0]))) {
        const attributionCount = items.reduce((sum, item) => sum + item.attributionCount, 0);
        const fallbackCount = items.reduce((sum, item) => sum + item.likelyFallbackCount, 0);
        const signalBackedCount = items.reduce((sum, item) => sum + item.signalBackedCount, 0);
        console.log(
            `${key}: ${items.length} symbols, ${attributionCount} episodes, fallback ${renderPct(
                fallbackCount,
                attributionCount
            )}, signal-backed ${renderPct(signalBackedCount, attributionCount)}`
        );
    }
}

async function main(): Promise<void> {
    const symbols = parseSymbols(process.argv.slice(2));
    const evaluations: SymbolEvaluation[] = [];

    console.log(`Evaluating drawdown attribution coverage for ${symbols.length} symbols...\n`);

    console.log(`Warm-up pass: triggering enrichment for ${symbols.length} symbols...`);
    await Promise.allSettled(symbols.map((symbol) => getSymbolPriceHistory(symbol)));
    console.log(`Waiting ${Math.round(WARMUP_WAIT_MS / 1000)}s for background enrichment to settle...\n`);
    await new Promise((resolve) => setTimeout(resolve, WARMUP_WAIT_MS));

    for (const symbol of symbols) {
        try {
            const response = await loadEvaluatedPriceHistory(symbol);
            const attributions = response.drawdown_attributions ?? [];
            const evaluation = summarizeSymbol(symbol, attributions);
            evaluations.push(evaluation);
        } catch (error) {
            console.log(`${symbol}: failed to evaluate (${error instanceof Error ? error.message : String(error)})`);
        }
    }

    const totalEpisodes = evaluations.reduce((sum, item) => sum + item.attributionCount, 0);
    const totalFallback = evaluations.reduce((sum, item) => sum + item.likelyFallbackCount, 0);
    const totalSignalBacked = evaluations.reduce((sum, item) => sum + item.signalBackedCount, 0);
    const orderFailures = evaluations.filter((item) => !item.orderValid).map((item) => item.symbol);

    console.log('Portfolio-level summary');
    console.log('-----------------------');
    console.log(`Symbols evaluated: ${evaluations.length}`);
    console.log(`Attributed episodes: ${totalEpisodes}`);
    console.log(`Likely fallback episodes: ${totalFallback} (${renderPct(totalFallback, totalEpisodes)})`);
    console.log(`Signal-backed episodes: ${totalSignalBacked} (${renderPct(totalSignalBacked, totalEpisodes)})`);
    console.log(`Ordering failures: ${orderFailures.length > 0 ? orderFailures.join(', ') : 'none'}`);

    console.log('\nPer-symbol summary');
    console.log('------------------');
    for (const item of evaluations) {
        console.log(
            [
                item.symbol.padEnd(6),
                `episodes=${String(item.attributionCount).padStart(2)}`,
                `fallback=${String(item.likelyFallbackCount).padStart(2)}`,
                `signal=${String(item.signalBackedCount).padStart(2)}`,
                `cycle=${(item.cycleFamily ?? 'unknown').padEnd(26)}`,
                `archetype=${item.businessArchetype ?? 'unknown'}`
            ].join(' | ')
        );
    }

    printGroupSummary('By cycle family', evaluations, (item) => item.cycleFamily);
    printGroupSummary('By business archetype', evaluations, (item) => item.businessArchetype);

    console.log('\nPrimary driver mix');
    console.log('------------------');
    const driverMix = {
        company: evaluations.reduce((sum, item) => sum + item.companyPrimaryCount, 0),
        sector: evaluations.reduce((sum, item) => sum + item.sectorPrimaryCount, 0),
        macro: evaluations.reduce((sum, item) => sum + item.macroPrimaryCount, 0),
        policy: evaluations.reduce((sum, item) => sum + item.policyPrimaryCount, 0),
        geopolitical: evaluations.reduce((sum, item) => sum + item.geopoliticalPrimaryCount, 0),
        mixed: evaluations.reduce((sum, item) => sum + item.mixedPrimaryCount, 0)
    };
    for (const [driverType, count] of Object.entries(driverMix)) {
        console.log(`${driverType}: ${count} (${renderPct(count, totalEpisodes)})`);
    }
}

void main().catch((error) => {
    console.error(error);
    process.exit(1);
});
