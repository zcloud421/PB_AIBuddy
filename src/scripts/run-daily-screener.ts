import dotenv from 'dotenv';

import { fetchEarningsCalendar } from '../data/earnings-fetcher';
import { fetchTickerCompanyName, MassiveDataFetcher } from '../data/massive-fetcher';
import { fetchStockNewsContext, getCompanyName } from '../data/news-fetcher';
import { pool } from '../db/client';
import {
    createIdeaRun,
    ensureDailyBestHistoryTable,
    ensureEarningsCalendarColumns,
    ensureIdeaCandidatePriceColumns,
    ensureRecommendationTrackerTable,
    ensureRiskFlagEnumValues,
    ensureUnderlyingCompanyNameColumn,
    deleteRecommendationTrackerForDate,
    saveIdeaCandidate,
    saveDailyBest,
    saveRiskFlags,
    upsertRecommendationTracker,
    upsertUnderlyingCompanyName,
    upsertEarningsCalendar,
    updateIdeaCandidateNarrative,
    updateIdeaRunStatus
} from '../db/queries/ideas';
import type { ScoringResult } from '../scoring-engine';
import { runDailyScreener } from '../scoring-engine';
import { selectDailyBest } from '../services/ideas-service';
import { runPriceTracker } from '../services/tracker-service';
import { generateNarrative } from '../utils/narrative-generator';

dotenv.config();

interface ActiveUnderlyingRow {
    symbol: string;
}

const PER_SYMBOL_DELAY_MS = 8000;
const BATCH_COOLDOWN_MS = 30000;
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
        console.log('[screener] Rate limit mode: conservative (8s between symbols)');

        await ensureDailyBestHistoryTable();
        await ensureIdeaCandidatePriceColumns();
        await ensureEarningsCalendarColumns();
        await ensureRiskFlagEnumValues();
        await ensureRecommendationTrackerTable();
        await ensureUnderlyingCompanyNameColumn();

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

                const [result] = await runDailyScreener([symbol], fetcher);
                if (!result) {
                    throw new Error(`No scoring result returned for ${symbol}`);
                }

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
                    refCouponPct: result.ref_coupon_pct,
                    moneynessPct: result.moneyness_pct,
                    selectedImpliedVolatility: result.selected_implied_volatility,
                    currentPrice: result.current_price,
                    ma20: result.ma20,
                    ma50: result.ma50,
                    ma200: result.ma200,
                    pctFrom52wHigh: result.pct_from_52w_high,
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
            const dailyBestResult = results.find((result) => result.symbol === dailyBest.symbol);
            if (dailyBestResult) {
                const newsContext = await fetchStockNewsContext(dailyBest.symbol, getCompanyName(dailyBest.symbol));
                const newsItems = newsContext.items;
                const currentPriceRow = await client.query<{ close: number | null; ma20: number | null; ma50: number | null; ma200: number | null; pct_from_52w_high: number | null; days_to_earnings: number | null }>(
                    `
                    WITH latest_price AS (
                        SELECT close
                        FROM price_history
                        WHERE symbol = $1
                        ORDER BY trade_date DESC
                        LIMIT 1
                    ),
                    ma AS (
                        SELECT
                            AVG(close) FILTER (WHERE rn <= 20) AS ma20,
                            AVG(close) FILTER (WHERE rn <= 50) AS ma50,
                            AVG(close) FILTER (WHERE rn <= 200) AS ma200
                        FROM (
                            SELECT close, ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                            FROM price_history
                            WHERE symbol = $1
                        ) ranked
                    ),
                    highs AS (
                        SELECT MAX(high) AS high_52w
                        FROM price_history
                        WHERE symbol = $1
                          AND trade_date >= (CURRENT_DATE - INTERVAL '365 days')
                    ),
                    earnings AS (
                        SELECT days_until
                        FROM earnings_calendar
                        WHERE symbol = $1
                          AND report_date >= CURRENT_DATE
                        ORDER BY report_date ASC
                        LIMIT 1
                    )
                    SELECT
                        latest_price.close,
                        ma.ma20,
                        ma.ma50,
                        ma.ma200,
                        CASE
                            WHEN highs.high_52w IS NOT NULL AND highs.high_52w <> 0 AND latest_price.close IS NOT NULL
                            THEN ROUND(((latest_price.close - highs.high_52w) / highs.high_52w) * 100, 2)
                            ELSE NULL
                        END AS pct_from_52w_high,
                        earnings.days_until
                    FROM latest_price, ma, highs
                    LEFT JOIN earnings ON TRUE
                    `,
                    [dailyBest.symbol]
                );
                const market = currentPriceRow.rows[0];
                if (market) {
                    const narrative = await generateNarrative({
                        symbol: dailyBest.symbol,
                        theme: dailyBest.theme,
                        grade: dailyBestResult.overall_grade,
                        recommended_strike: dailyBestResult.recommended_strike ?? 0,
                        estimated_coupon_range: dailyBestResult.estimated_coupon_range ?? '',
                        current_price: Number(market.close ?? 0),
                        pct_from_52w_high: Number(market.pct_from_52w_high ?? 0),
                        ma20: Number(market.ma20 ?? 0),
                        ma50: Number(market.ma50 ?? 0),
                        ma200: Number(market.ma200 ?? 0),
                        iv_level:
                            dailyBestResult.selected_implied_volatility !== null &&
                            dailyBestResult.selected_implied_volatility !== undefined
                                ? dailyBestResult.selected_implied_volatility >= 0.6
                                    ? '高'
                                    : dailyBestResult.selected_implied_volatility >= 0.3
                                      ? '中'
                                      : '低'
                                : '中',
                        flags: dailyBestResult.flags,
                        tenor_days: dailyBestResult.recommended_tenor_days ?? 90,
                        news_headlines: newsItems.map((item) => item.title),
                        has_recent_earnings: newsContext.hasRecentEarnings,
                        days_to_earnings: market.days_to_earnings ?? null
                    });
                    await updateIdeaCandidateNarrative(runId, dailyBest.symbol, narrative);
                }
            }
            console.log(
                `[screener] Daily best: ${dailyBest.symbol} (adjusted score: ${dailyBest.adjustedScore.toFixed(2)})`
            );
        }

        await runPriceTracker();

        await updateIdeaRunStatus(runId, 'completed');
        console.log(
            `Daily screener completed for ${results.length} symbols (${totalRecommended} GO ideas, ${failedSymbols} failed), run_id=${runId}`
        );
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

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
});

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}
