import dotenv from 'dotenv';

import { fetchEarningsCalendar } from '../data/earnings-fetcher';
import { fetchTickerCompanyName, MassiveDataFetcher } from '../data/massive-fetcher';
import { fetchStockNewsContext } from '../data/news-fetcher';
import { pool } from '../db/client';
import {
    ensureClientFocusDailyVerdictsTable,
    createIdeaRun,
    ensureDailyBestHistoryTable,
    ensureDailyRecommendationHistoryTable,
    ensureEarningsCalendarColumns,
    ensureIdeaCandidatePriceColumns,
    ensureRecommendationTrackerTable,
    ensureRiskFlagEnumValues,
    ensureThemeBasketResultsTable,
    ensureUnderlyingCompanyNameColumn,
    deleteRecommendationTrackerForDate,
    getUnderlyingBySymbol,
    saveIdeaCandidate,
    saveDailyBest,
    saveDailyRecommendations,
    saveRiskFlags,
    upsertClientFocusDailyVerdict,
    upsertRecommendationTracker,
    upsertUnderlyingCompanyName,
    upsertEarningsCalendar,
    updateIdeaRunStatus
} from '../db/queries/ideas';
import type { ScoringResult } from '../scoring-engine';
import { runDailyScreener } from '../scoring-engine';
import { selectDailyBest, selectDailyRecommendationShowcase } from '../services/ideas-service';
import { runPriceTracker } from '../services/tracker-service';
import { runThemeBasketDaily } from '../services/theme-basket-service';
import {
    generateClientFocusDailyVerdictSnapshot,
    getClientFocusList,
    getDailyMarketNarrative
} from '../services/client-focus-service';
import { generateNarrative } from '../utils/narrative-generator';
import { sendDowngradeNotifications } from '../utils/push-notifications';
import { ensureDeviceTables } from '../db/queries/devices';

dotenv.config();

interface ActiveUnderlyingRow {
    symbol: string;
}

const PER_SYMBOL_DELAY_MS = 3000;
const BATCH_COOLDOWN_MS = 15000;
const BATCH_SIZE = 5;
const FAILURE_COOLDOWN_MS = 60000;

async function main(): Promise<void> {
    const client = await pool.connect();
    let runId: string | null = null;

    try {
        const symbolResult = await client.query<ActiveUnderlyingRow>(
            `
            SELECT symbol
            FROM underlyings
            WHERE active = TRUE
            ORDER BY tier ASC, symbol ASC
            `
        );

        const symbols = symbolResult.rows.map((row) => row.symbol);
        if (symbols.length === 0) {
            throw new Error('No active underlyings found');
        }

        console.log(`[screener] Starting daily run for ${symbols.length} symbols`);
        console.log('[screener] Rate limit mode: standard (3s between symbols)');

        await ensureDailyBestHistoryTable();
        await ensureDailyRecommendationHistoryTable();
        await ensureIdeaCandidatePriceColumns();
        await ensureEarningsCalendarColumns();
        await ensureRiskFlagEnumValues();
        await ensureRecommendationTrackerTable();
        await ensureUnderlyingCompanyNameColumn();
        await ensureThemeBasketResultsTable();
        await ensureClientFocusDailyVerdictsTable();
        await ensureDeviceTables();

        const previousGradesResult = await client.query<{ symbol: string; grade: string }>(
            `
            SELECT ic.symbol, ic.overall_grade AS grade
            FROM idea_candidates ic
            JOIN idea_runs ir ON ir.run_id = ic.run_id
            WHERE ir.status = 'completed'
              AND ir.run_date = (CURRENT_DATE - INTERVAL '1 day')::date
            `
        );
        const previousGrades = previousGradesResult.rows;

        try {
            const earningsRows = await fetchEarningsCalendar(symbols);
            await upsertEarningsCalendar(earningsRows);
            console.log(`[screener] Earnings calendar refreshed for ${earningsRows.length} symbols`);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[screener] Earnings calendar refresh failed (${message})`);
        }

        runId = await createIdeaRun('DAILY_SCREEN', 'scheduled');

        const fetcher = new MassiveDataFetcher();
        const results: ScoringResult[] = [];
        let failedSymbols = 0;

        for (const [index, symbol] of symbols.entries()) {
            try {
                const companyName = await fetchTickerCompanyName(symbol);
                await upsertUnderlyingCompanyName(symbol, companyName);
                const underlying = await getUnderlyingBySymbol(symbol);

                const [result] = await runDailyScreener([symbol], fetcher);
                if (!result) {
                    throw new Error(`No scoring result returned for ${symbol}`);
                }
                const newsContext = await fetchStockNewsContext(symbol, underlying?.company_name ?? undefined);
                const narrative = await generateNarrative({
                    symbol,
                    theme: underlying?.themes?.[0] ?? 'Featured',
                    grade: result.overall_grade,
                    recommended_strike: result.recommended_strike ?? 0,
                    estimated_coupon_range: result.estimated_coupon_range ?? '',
                    current_price: result.current_price,
                    pct_from_52w_high: result.pct_from_52w_high,
                    ma20: result.ma20,
                    ma50: result.ma50,
                    ma200: result.ma200,
                    iv_level:
                        result.selected_implied_volatility !== null &&
                        result.selected_implied_volatility !== undefined
                            ? result.selected_implied_volatility >= 0.6
                                ? '高'
                                : result.selected_implied_volatility >= 0.3
                                  ? '中'
                                  : '低'
                            : '中',
                    flags: result.flags,
                    tenor_days: result.recommended_tenor_days ?? 90,
                    news_headlines: newsContext.narrativeItems.map((item) => item.title),
                    has_recent_earnings: newsContext.hasRecentEarnings,
                    earnings_weight: newsContext.earningsWeight,
                    days_to_earnings: null,
                    days_since_earnings: newsContext.daysSinceEarnings
                });
                results.push(result);
                await saveIdeaCandidate({
                    runId,
                    symbol: result.symbol,
                    overallGrade: result.overall_grade,
                    ivRankScore: result.iv_rank_score,
                    trendScore: result.trend_score,
                    skewScore: result.skew_score,
                    eventRiskScore: result.event_risk_score,
                    compositeScore: result.composite_score,
                    recommendedStrike: result.recommended_strike,
                    recommendedTenorDays: result.recommended_tenor_days,
                    recommendedExpiryDate: result.recommended_expiry_date,
                    refCouponPct: result.ref_coupon_pct,
                    moneynessPct: result.moneyness_pct,
                    selectedImpliedVolatility: result.selected_implied_volatility,
                    currentPrice: result.current_price,
                    ma20: result.ma20,
                    ma50: result.ma50,
                    ma200: result.ma200,
                    pctFrom52wHigh: result.pct_from_52w_high,
                    whyNow: narrative?.why_now ?? null,
                    riskNote: narrative?.risk_note ?? null,
                    sentimentScore: narrative?.sentiment_score ?? null,
                    keyEvents: narrative?.key_events ?? [],
                    newsItems: newsContext.displayItems,
                    reasoningText: result.reasoning_text
                });

                if (result.flags.length > 0) {
                    await saveRiskFlags(runId, result.symbol, result.flags);
                }

                if (result.overall_grade === 'GO' || result.overall_grade === 'CAUTION') {
                    await upsertRecommendationTracker({
                        symbol: result.symbol,
                        grade: result.overall_grade,
                        recommendedStrike: result.recommended_strike,
                        recommendedTenorDays: result.recommended_tenor_days,
                        expiryDate: result.recommended_expiry_date,
                        moneynessPct: result.moneyness_pct,
                        entryPrice: result.current_price,
                        recommendationDate: new Date().toISOString().slice(0, 10)
                    });
                } else {
                    await deleteRecommendationTrackerForDate(result.symbol, new Date().toISOString().slice(0, 10));
                }

                console.log(
                    `[screener] ${index + 1}/${symbols.length} ${result.symbol} -> ${result.overall_grade} (${result.composite_score.toFixed(2)}) ✓`
                );
            } catch (error) {
                failedSymbols += 1;
                const message = error instanceof Error ? error.message : String(error);
                console.warn(`[screener] ${index + 1}/${symbols.length} ${symbol} -> FAILED (${message})`);
                await delay(FAILURE_COOLDOWN_MS);
            }

            if (index < symbols.length - 1) {
                await delay(PER_SYMBOL_DELAY_MS);

                if ((index + 1) % BATCH_SIZE === 0) {
                    await delay(BATCH_COOLDOWN_MS);
                }
            }
        }

        const totalRecommended = results.filter((result) => result.overall_grade === 'GO').length;
        const dailyBest = await selectDailyBest(results);
        await client.query(
            `
            UPDATE idea_runs
            SET total_screened = $2,
                total_recommended = $3
            WHERE run_id = $1
            `,
            [runId, results.length, totalRecommended]
        );

        if (dailyBest) {
            await saveDailyBest(dailyBest.symbol, runId, dailyBest.theme);
            console.log(
                `[screener] Daily best: ${dailyBest.symbol} (adjusted score: ${dailyBest.adjustedScore.toFixed(2)})`
            );
        }

        const showcase = await selectDailyRecommendationShowcase(results, dailyBest?.symbol ?? null);
        if (runId && showcase.length > 0) {
            const persistedRunId = runId;
            await saveDailyRecommendations(
                showcase.map((item) => ({
                    runId: persistedRunId,
                    symbol: item.symbol,
                    slotRank: item.slotRank,
                    placement: item.placement,
                    compositeScore: item.compositeScore,
                    recommendedStrike: item.recommendedStrike,
                    recommendedTenorDays: item.recommendedTenorDays,
                    moneynessPct: item.moneynessPct
                }))
            );
            console.log(
                `[screener] Showcase saved: ${showcase.map((item) => `${item.slotRank}:${item.symbol}`).join(', ')}`
            );
        }

        await updateIdeaRunStatus(runId, 'completed');
        console.log(
            `Daily screener completed for ${results.length} symbols (${totalRecommended} GO ideas, ${failedSymbols} failed), run_id=${runId}`
        );

        try {
            await runPriceTracker();
            console.log('[screener] Recommendation tracker refresh completed');
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[screener] Recommendation tracker refresh failed (${message})`);
        }

        console.log('[theme-baskets] starting daily run...');
        for (const slug of ['middle-east-tensions', 'gold-repricing']) {
            try {
                await runThemeBasketDaily(slug);
                console.log(`[theme-baskets] ${slug} done`);
            } catch (error) {
                console.warn(`[theme-baskets] ${slug} failed:`, error);
            }
        }

        try {
            const middleEastVerdict = await generateClientFocusDailyVerdictSnapshot('middle-east-tensions');
            if (middleEastVerdict) {
                await upsertClientFocusDailyVerdict(
                    'middle-east-tensions',
                    new Date().toISOString().slice(0, 10),
                    middleEastVerdict
                );
                console.log('[focus-daily] middle-east-tensions saved');
            } else {
                console.warn('[focus-daily] middle-east-tensions skipped: no verdict generated');
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[focus-daily] middle-east-tensions failed (${message})`);
        }

        try {
            console.log('[focus-daily] prewarm started');
            await getClientFocusList();
            console.log('[focus-daily] focus topics cache warmed');
            const dailyNarrative = await getDailyMarketNarrative();
            if (dailyNarrative) {
                console.log(
                    `[focus-daily] daily narrative prepared (${dailyNarrative.primary_slug} / ${dailyNarrative.asset_buckets.map((item) => item.bucket).join(', ')})`
                );
            } else {
                console.warn('[focus-daily] daily narrative skipped: no renderable output');
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[focus-daily] daily narrative failed (${message})`);
        }

        try {
            const gradeResults = results.map((r) => ({ symbol: r.symbol, grade: r.overall_grade }));
            await sendDowngradeNotifications(gradeResults, previousGrades);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            console.warn(`[screener] Push notification dispatch failed (${message})`);
        }
    } catch (error) {
        if (runId) {
            try {
                await updateIdeaRunStatus(runId, 'failed');
            } catch {
                // Ignore secondary persistence errors on failure path.
            }
        }

        throw error;
    } finally {
        client.release();
        await pool.end();
    }
}

main()
    .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(message);
        process.exitCode = 1;
    })
    .finally(() => {
        process.exit(process.exitCode ?? 0);
    });

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
