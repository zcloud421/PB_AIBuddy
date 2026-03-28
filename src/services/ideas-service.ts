import { fetchTickerMarketCap, fetchTickerReferenceSnapshot, MassiveDataFetcher } from '../data/massive-fetcher';
import { fetchStockNews, fetchStockNewsContext, getCompanyName } from '../data/news-fetcher';
import {
    calculateHistoricalVolatility,
    approveStrikes,
    approveTenors,
    checkEligibility,
    getStrikeSelectionConfig,
    shouldPreferTenorCandidate,
    scoreAndGrade,
    type DataFetcherInterface,
    type ScoringResult,
    type SymbolData
} from '../scoring-engine';
import {
    createIdeaRun,
    deleteTodayIdeaCandidate,
    getDailyBestByDate,
    getIdeaBySymbolAndRunId,
    getIdeaBySymbolAndDate,
    getIdeasByRunId,
    getLatestCompletedRun,
    getLatestCompletedScheduledRun,
    getPriceContextBySymbol,
    getRecentPriceHistoryBySymbol,
    getRecentDailyBest,
    getRecentDailyBestHistory,
    getRiskFlagsByRunAndSymbol,
    getRiskFlagsByRunId,
    getUnderlyingBySymbol,
    mapTodayIdeasResponse,
    saveIdeaCandidate,
    updateIdeaCandidateNarrative,
    saveRiskFlags,
    upsertPriceHistory,
    upsertUnderlyingReference,
    updateIdeaRunStatus
} from '../db/queries/ideas';
import { HttpError } from '../lib/http-error';
import { buildThemeNarrative } from './theme-narrative';
import { getClientFocusDetail as getClientFocusDetailPayload, getClientFocusList as getClientFocusListPayload } from './client-focus-service';
import { generateNarrative, sanitizeNarrativeOutput } from '../utils/narrative-generator';
import type {
    AsyncScoringAcceptedResponse,
    AsyncScoringStatusResponse,
    ClientFocusDetailResponse,
    ClientFocusListItem,
    DailyBestCard,
    Flag,
    NarrativeOutput,
    NewsItem,
    SignalColor,
    SignalRow,
    SymbolIdeaResponse,
    SymbolPriceHistoryResponse,
    TodayIdeasResponse
} from '../types/api';
import { enqueueSymbolScoringJob, getSymbolScoringJob } from './scoring-queue';

interface FreshSymbolAnalysis {
    exchange: string;
    scoring: ScoringResult;
    symbolData: SymbolData;
}

const KNOWN_CONFERENCE_END_DATES: Partial<Record<string, string>> = {
    GTC: '2026-03-19',
    CES: '2026-01-09',
    OFC: '2026-03-19'
};

const COMMODITY_BETA_SYMBOLS = new Set(['GDX', 'USO']);

function headlinesContainMaterialEvent(items: NewsItem[]): boolean {
    const text = items.map((item) => item.title).join(' ').toLowerCase();
    if (!text) {
        return false;
    }

    const keywords = [
        'indict',
        'charged',
        'lawsuit',
        'sued',
        'probe',
        'investigation',
        'export control',
        'sanction',
        'fraud',
        'short seller',
        'short report',
        'downgrade',
        'rating cut',
        'subpoena',
        'accounting',
        'guidance',
        'earnings',
        'partner',
        'contract',
        'regulator',
        '监管',
        '诉讼',
        '起诉',
        '检方',
        '检察官',
        '指控',
        '调查',
        '出口管制',
        '制裁',
        '做空',
        '下调评级',
        '合作',
        '签约',
        '财报',
        '业绩',
        '诈欺',
        '欺诈',
        '审计'
    ];

    return keywords.some((keyword) => text.includes(keyword));
}

function hasStaleConferenceKeyEvent(keyEvents: string[]): boolean {
    const today = new Date().toISOString().slice(0, 10);

    return keyEvents.some((event) => {
        if (!event.includes('正在进行')) {
            return false;
        }

        for (const [conference, endDate] of Object.entries(KNOWN_CONFERENCE_END_DATES)) {
            if (endDate && event.includes(conference) && today > endDate) {
                return true;
            }
        }

        return false;
    });
}

function shouldRefreshCommodityBetaCache(input: {
    symbol: string;
    grade: string;
    moneynessPct: number | null;
    impliedVolatility: number | null;
    pctFrom52wHigh: number | null;
    currentPrice: number | null;
    ma20: number | null;
}): boolean {
    if (!COMMODITY_BETA_SYMBOLS.has(input.symbol.toUpperCase())) {
        return false;
    }

    const violatesCommodityGuardrail =
        (input.moneynessPct !== null && input.moneynessPct > 85) ||
        (input.impliedVolatility !== null && input.impliedVolatility >= 0.4) ||
        (input.pctFrom52wHigh !== null && input.pctFrom52wHigh < -20) ||
        (
            input.currentPrice !== null &&
            input.ma20 !== null &&
            input.currentPrice < input.ma20
        );

    return violatesCommodityGuardrail && input.grade === 'GO';
}

class CachedDataFetcher implements DataFetcherInterface {
    private readonly inner: DataFetcherInterface;
    private readonly symbolCache = new Map<string, Promise<SymbolData>>();
    private readonly chainCache = new Map<string, Promise<Awaited<ReturnType<DataFetcherInterface['fetchChainData']>>>>();

    constructor(inner: DataFetcherInterface) {
        this.inner = inner;
    }

    fetchSymbolData(symbol: string): Promise<SymbolData> {
        const key = symbol.toUpperCase();
        const cached = this.symbolCache.get(key);
        if (cached) {
            return cached;
        }

        const promise = this.inner.fetchSymbolData(key).catch((error) => {
            this.symbolCache.delete(key);
            throw error;
        });
        this.symbolCache.set(key, promise);
        return promise;
    }

    fetchChainData(symbol: string, currentPrice: number): ReturnType<DataFetcherInterface['fetchChainData']> {
        const key = `${symbol.toUpperCase()}:${Math.floor(currentPrice)}`;
        const cached = this.chainCache.get(key);
        if (cached) {
            return cached;
        }

        const promise = this.inner.fetchChainData(symbol.toUpperCase(), currentPrice).catch((error) => {
            this.chainCache.delete(key);
            throw error;
        });
        this.chainCache.set(key, promise);
        return promise;
    }
}

const sharedDataFetcher = new CachedDataFetcher(new MassiveDataFetcher());

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function currentUsIsoDate(): string {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/New_York',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    });
    return formatter.format(new Date());
}

function normalizeUsTradingDate(isoDate: string): string {
    const date = new Date(`${isoDate}T00:00:00Z`);
    const day = date.getUTCDay();

    if (day === 6) {
        date.setUTCDate(date.getUTCDate() - 1);
    } else if (day === 0) {
        date.setUTCDate(date.getUTCDate() - 2);
    }

    return date.toISOString().slice(0, 10);
}

function latestUsTradingDate(): string {
    return normalizeUsTradingDate(currentUsIsoDate());
}

async function getActiveCompletedRun() {
    const [latestScheduledRun, latestCompletedRun] = await Promise.all([
        getLatestCompletedScheduledRun(),
        getLatestCompletedRun()
    ]);

    if (!latestScheduledRun) {
        return latestCompletedRun;
    }

    if (!latestCompletedRun) {
        return latestScheduledRun;
    }

    if (latestCompletedRun.run_date > latestScheduledRun.run_date) {
        return latestCompletedRun;
    }

    return latestScheduledRun;
}

function normalizeCommodityBetaTodayIdea(
    idea: Awaited<ReturnType<typeof getIdeasByRunId>>[number]
): Awaited<ReturnType<typeof getIdeasByRunId>>[number] {
    if (
        !shouldRefreshCommodityBetaCache({
            symbol: idea.symbol,
            grade: idea.overall_grade,
            moneynessPct: toNullableNumber(idea.moneyness_pct),
            impliedVolatility: toNullableNumber(idea.selected_implied_volatility),
            pctFrom52wHigh: toNullableNumber(idea.pct_from_52w_high),
            currentPrice: toNullableNumber(idea.current_price),
            ma20: toNullableNumber(idea.ma20)
        })
    ) {
        return idea;
    }

    return {
        ...idea,
        overall_grade: 'CAUTION'
    };
}

function scoringLikelyExceedsSyncBudget(): boolean {
    // TODO: Replace with a real estimator based on cache state, upstream latency, and queue depth.
    return false;
}

function buildDataFetcher(): DataFetcherInterface {
    return sharedDataFetcher;
}

export async function getClientFocusList(): Promise<ClientFocusListItem[]> {
    return getClientFocusListPayload();
}

export async function getClientFocusDetail(slug: string): Promise<ClientFocusDetailResponse | null> {
    return getClientFocusDetailPayload(slug);
}

export async function getTodayIdeas(): Promise<TodayIdeasResponse> {
    const latestRun = await getActiveCompletedRun();
    console.log('[debug] getActiveCompletedRun result:', latestRun);

    if (!latestRun) {
        throw new HttpError(503, 'SCORING_ENGINE_UNAVAILABLE', 'No completed scoring run is available.');
    }

    const [ideas, riskFlags, dailyBestRow] = await Promise.all([
        getIdeasByRunId(latestRun.run_id),
        getRiskFlagsByRunId(latestRun.run_id),
        getDailyBestByDate(latestRun.run_date)
    ]);
    const normalizedIdeas = ideas.map((idea) => normalizeCommodityBetaTodayIdea(idea));
    const flagsBySymbol = new Map<string, Flag[]>();
    for (const flag of riskFlags) {
        const existing = flagsBySymbol.get(flag.symbol) ?? [];
        existing.push({
            type: flag.flag_type,
            severity: flag.severity,
            message: flag.detail_text
        });
        flagsBySymbol.set(flag.symbol, existing);
    }

    const dailyBest: DailyBestCard | null = dailyBestRow
        ? await mapDailyBestCard(dailyBestRow.symbol, dailyBestRow.theme ?? 'Featured', normalizedIdeas, flagsBySymbol)
        : null;

    return mapTodayIdeasResponse(latestRun, normalizedIdeas, riskFlags, dailyBest);
}

export async function selectDailyBest(candidates: ScoringResult[]): Promise<{
    symbol: string;
    theme: string;
    adjustedScore: number;
} | null> {
    const goCandidates = candidates.filter((candidate) => candidate.overall_grade === 'GO');
    if (goCandidates.length === 0) {
        return null;
    }

    const recentHistory = await getRecentDailyBestHistory(5);
    let bestChoice: { symbol: string; theme: string; adjustedScore: number } | null = null;

    for (const candidate of goCandidates) {
        const underlying = await getUnderlyingBySymbol(candidate.symbol);
        const theme = underlying?.themes?.[0] ?? 'General';
        const tierBonus = underlying?.tier === 1 ? 0.05 : 0;
        const freshnessPenalty = calculateFreshnessPenalty(candidate.symbol, recentHistory);
        const adjustedScore = candidate.composite_score + tierBonus - freshnessPenalty;

        if (!bestChoice || adjustedScore > bestChoice.adjustedScore) {
            bestChoice = {
                symbol: candidate.symbol,
                theme,
                adjustedScore
            };
        }
    }

    return bestChoice;
}

export async function getSymbolIdea(symbol: string): Promise<SymbolIdeaResponse | AsyncScoringAcceptedResponse> {
    const normalizedSymbol = symbol.toUpperCase();
    const runDate = todayIsoDate();

    let cachedRow = null as Awaited<ReturnType<typeof getIdeaBySymbolAndDate>>;
    let cachedFlags: Flag[] = [];

    try {
        const activeRun = await getActiveCompletedRun();
        cachedRow = activeRun
            ? await getIdeaBySymbolAndRunId(normalizedSymbol, activeRun.run_id)
            : await getIdeaBySymbolAndDate(normalizedSymbol, runDate);

        if (!cachedRow) {
            cachedRow = await getIdeaBySymbolAndDate(normalizedSymbol, runDate);
        }

        cachedFlags = cachedRow
            ? await getRiskFlagsByRunAndSymbol(cachedRow.run_id, normalizedSymbol)
            : [];
    } catch {
        cachedRow = null;
        cachedFlags = [];
    }

    if (cachedRow) {
        const normalizedRunDate = normalizeUsTradingDate(cachedRow.run_date);
        const shouldCheckForMissingKeyEvents = (cachedRow.key_events ?? []).length === 0;
        const shouldRefreshStaleConferenceEvent = hasStaleConferenceKeyEvent(cachedRow.key_events ?? []);
        const needsNewsContext = !cachedRow.why_now || shouldCheckForMissingKeyEvents || shouldRefreshStaleConferenceEvent;
        const newsContext = needsNewsContext
            ? await fetchStockNewsContext(
                  normalizedSymbol,
                  cachedRow.company_name ?? getCompanyName(normalizedSymbol)
              )
            : null;
        const shouldRefreshNarrativeForEvents =
            shouldRefreshStaleConferenceEvent ||
            (shouldCheckForMissingKeyEvents && headlinesContainMaterialEvent(newsContext?.narrativeItems ?? []));
        const needsFreshNarrative = !cachedRow.why_now || shouldRefreshNarrativeForEvents;
        const newsItems = cachedRow.news_items ?? newsContext?.displayItems ?? [];
        const priceContext = await getPriceContextBySymbol(normalizedSymbol);
        const shouldUseDbPriceContext =
            priceContext?.data_date !== null &&
            priceContext?.data_date !== undefined &&
            priceContext.data_date >= normalizedRunDate;
        const effectiveCurrentPrice = toNullableNumber(
            shouldUseDbPriceContext ? priceContext?.current_price ?? cachedRow.current_price : cachedRow.current_price
        );
        const effectiveMa20 = toNullableNumber(
            shouldUseDbPriceContext ? priceContext?.ma20 ?? cachedRow.ma20 : cachedRow.ma20
        );
        const effectiveMa50 = toNullableNumber(
            shouldUseDbPriceContext ? priceContext?.ma50 ?? cachedRow.ma50 : cachedRow.ma50
        );
        const effectiveMa200 = toNullableNumber(
            shouldUseDbPriceContext ? priceContext?.ma200 ?? cachedRow.ma200 : cachedRow.ma200
        );
        const effectivePctFrom52wHigh = toNullableNumber(
            shouldUseDbPriceContext
                ? priceContext?.pct_from_52w_high ?? cachedRow.pct_from_52w_high
                : cachedRow.pct_from_52w_high
        );
        const effectiveIv = toNullableNumber(cachedRow.selected_implied_volatility);
        const latestExpectedMarketDate = latestUsTradingDate();
        const effectiveDataAsOfDate =
            (shouldUseDbPriceContext ? priceContext?.data_date ?? latestExpectedMarketDate : latestExpectedMarketDate);

        if (
            shouldRefreshCommodityBetaCache({
                symbol: normalizedSymbol,
                grade: cachedRow.overall_grade,
                moneynessPct: toNullableNumber(cachedRow.moneyness_pct),
                impliedVolatility: effectiveIv,
                pctFrom52wHigh: effectivePctFrom52wHigh,
                currentPrice: effectiveCurrentPrice,
                ma20: effectiveMa20
            })
        ) {
            return await scoreSingleSymbol(normalizedSymbol);
        }

        let narrative =
            cachedRow.why_now
                ? sanitizeNarrativeOutput({
                      why_now: cachedRow.why_now,
                      risk_note: cachedRow.risk_note ?? '',
                      sentiment_score: toNullableNumber(cachedRow.sentiment_score) ?? 0.5,
                      key_events: cachedRow.key_events ?? []
                  }, newsItems, normalizedSymbol, cachedRow.company_name ?? getCompanyName(normalizedSymbol))
                : null;

        if (
            narrative &&
            JSON.stringify(narrative.key_events) !== JSON.stringify(cachedRow.key_events ?? [])
        ) {
            void updateIdeaCandidateNarrative(cachedRow.run_id, normalizedSymbol, narrative);
        }

        if (!narrative || shouldRefreshNarrativeForEvents) {
            const underlying = await getUnderlyingBySymbol(normalizedSymbol);
            const generatedNarrative = await buildNarrative({
                symbol: normalizedSymbol,
                companyName: cachedRow.company_name ?? getCompanyName(normalizedSymbol),
                theme: underlying?.themes?.[0] ?? 'Featured',
                grade: cachedRow.overall_grade,
                recommendedStrike: toNullableNumber(cachedRow.recommended_strike),
                estimatedCouponRange: formatEstimatedCouponRange(cachedRow.ref_coupon_pct),
                currentPrice: effectiveCurrentPrice,
                pctFrom52wHigh: effectivePctFrom52wHigh,
                ma20: effectiveMa20,
                ma50: effectiveMa50,
                ma200: effectiveMa200,
                impliedVolatility: effectiveIv,
                flags: cachedFlags,
                tenorDays: cachedRow.recommended_tenor_days,
                newsItems: newsContext?.narrativeItems ?? [],
                daysToEarnings: priceContext?.days_to_earnings ?? cachedRow.days_to_earnings,
                hasRecentEarnings: newsContext?.hasRecentEarnings ?? false,
                earningsWeight: newsContext?.earningsWeight ?? 0,
                daysSinceEarnings: newsContext?.daysSinceEarnings ?? null
            });

            if (generatedNarrative) {
                narrative = generatedNarrative;
                void updateIdeaCandidateNarrative(cachedRow.run_id, normalizedSymbol, generatedNarrative);
            }
        }

        return {
            symbol: cachedRow.symbol,
            exchange: cachedRow.exchange,
            company_name: cachedRow.company_name,
            run_date: cachedRow.run_date,
            cached: true,
            grade: cachedRow.overall_grade,
            composite_score: toNullableNumber(cachedRow.composite_score) ?? 0,
            verdict_headline: gradeToHeadline(cachedRow.overall_grade),
            verdict_sub: generateVerdictSub(cachedRow.overall_grade, cachedFlags),
            data_as_of_date: effectiveDataAsOfDate,
            recommended_strike: toNullableNumber(cachedRow.recommended_strike),
            recommended_tenor_days: cachedRow.recommended_tenor_days,
            recommended_expiry_date: cachedRow.expiry_date ?? null,
            estimated_coupon_range: formatEstimatedCouponRange(cachedRow.ref_coupon_pct),
            coupon_note: '实际票息请向交易台询价',
            moneyness_pct: toNullableNumber(cachedRow.moneyness_pct),
            reasoning_text: cachedRow.reasoning_text,
            narrative,
            news_items: newsItems,
            flags: cachedFlags,
            sentiment_score: narrative?.sentiment_score ?? toNullableNumber(cachedRow.sentiment_score),
            price_history: [],
            signals: buildSignalsFromCachedRow({
                current_price: effectiveCurrentPrice,
                ma20: effectiveMa20,
                ma50: effectiveMa50,
                ma200: effectiveMa200,
                pct_from_52w_high: effectivePctFrom52wHigh,
                selected_implied_volatility: effectiveIv,
                earnings_date: priceContext?.earnings_date ?? cachedRow.earnings_date,
                days_to_earnings: priceContext?.days_to_earnings ?? cachedRow.days_to_earnings
            }),
            price_context: {
                current_price: effectiveCurrentPrice,
                ma20: effectiveMa20,
                ma50: effectiveMa50,
                ma200: effectiveMa200,
                pct_from_52w_high: effectivePctFrom52wHigh,
                implied_volatility: effectiveIv,
                data_date: effectiveDataAsOfDate,
                earnings_date: priceContext?.earnings_date ?? cachedRow.earnings_date ?? null,
                days_to_earnings: priceContext?.days_to_earnings ?? cachedRow.days_to_earnings ?? null
            }
        };
    }

    if (scoringLikelyExceedsSyncBudget()) {
        const job = enqueueSymbolScoringJob(normalizedSymbol, runDate, async () => scoreSingleSymbol(normalizedSymbol));

        return {
            symbol: normalizedSymbol,
            run_date: runDate,
            cached: false,
            status: job.status === 'RUNNING' ? 'RUNNING' : 'PENDING',
            job_id: job.jobId,
            poll_url: `/ideas/${normalizedSymbol}/status/${job.jobId}`,
            message: 'Fresh scoring has started. Poll the status endpoint for completion.'
        };
    }

    try {
        return await scoreSingleSymbol(normalizedSymbol);
    } catch {
        return buildUnavailableIdeaResponse(normalizedSymbol);
    }
}

export { deleteTodayIdeaCandidate };

export async function getSymbolPriceHistory(symbol: string): Promise<SymbolPriceHistoryResponse> {
    const normalizedSymbol = symbol.toUpperCase();
    let priceHistory = await getRecentPriceHistoryBySymbol(normalizedSymbol).catch(() => []);
    const latestExpectedMarketDate = latestUsTradingDate();
    const latestStoredPriceHistoryDate = priceHistory[priceHistory.length - 1]?.date ?? null;
    const shouldRefreshPriceHistory =
        priceHistory.length < 2 ||
        latestStoredPriceHistoryDate === null ||
        latestStoredPriceHistoryDate < latestExpectedMarketDate;

    if (shouldRefreshPriceHistory) {
        try {
            const fallbackFetcher = new MassiveDataFetcher();
            const fetchedBars = await fallbackFetcher.fetchPriceHistory(normalizedSymbol);

            if (fetchedBars.length > 0) {
                priceHistory = fetchedBars.map((bar) => ({
                    date: bar.date,
                    close: bar.close
                }));

                void upsertPriceHistory({
                    symbol: normalizedSymbol,
                    bars: fetchedBars
                }).catch((error: unknown) => {
                    console.warn(
                        `[ideas] failed to persist fallback price history for ${normalizedSymbol}: ${
                            error instanceof Error ? error.message : String(error)
                        }`
                    );
                });
            }
        } catch (error) {
            console.warn(
                `[ideas] failed to fetch fallback price history for ${normalizedSymbol}: ${
                    error instanceof Error ? error.message : String(error)
                }`
            );
        }
    }

    return {
        symbol: normalizedSymbol,
        data_as_of_date: priceHistory[priceHistory.length - 1]?.date ?? latestExpectedMarketDate,
        price_history: priceHistory
    };
}

export async function getSymbolIdeaStatus(symbol: string, jobId: string): Promise<AsyncScoringStatusResponse> {
    const normalizedSymbol = symbol.toUpperCase();
    const job = getSymbolScoringJob(normalizedSymbol, jobId);

    if (!job) {
        throw new HttpError(404, 'NOT_FOUND', `Scoring job ${jobId} for ${normalizedSymbol} was not found.`);
    }

    return job;
}

async function scoreSingleSymbol(symbol: string): Promise<SymbolIdeaResponse> {
    let runId: string | null = null;
    let shouldPersist = false;

    try {
        let underlying = await getUnderlyingBySymbol(symbol).catch(() => null);
        if (!underlying || !underlying.company_name) {
            const reference = await fetchTickerReferenceSnapshot(symbol).catch(() => null);
            if (reference && reference.companyName) {
                await upsertUnderlyingReference({
                    symbol,
                    exchange: reference.exchange,
                    companyName: reference.companyName,
                    sector: reference.sector
                }).catch(() => undefined);
                underlying = await getUnderlyingBySymbol(symbol).catch(() => underlying);
            }
        }
        shouldPersist = underlying !== null;

        try {
            if (shouldPersist) {
                runId = await createIdeaRun(symbol, 'manual');
                await updateIdeaRunStatus(runId, 'running');
            }
        } catch {
            runId = null;
        }

        const analysis = await runFreshSymbolScoring(symbol);
        const { exchange, scoring, symbolData } = analysis;
        const newsContext = await fetchStockNewsContext(
            symbol,
            underlying?.company_name ?? getCompanyName(symbol)
        );
        const newsItems = newsContext.displayItems;
        const narrative = await buildNarrative({
            symbol,
            companyName: underlying?.company_name ?? getCompanyName(symbol),
            theme: underlying?.themes?.[0] ?? 'Featured',
            grade: scoring.overall_grade,
            recommendedStrike: scoring.recommended_strike,
            estimatedCouponRange: scoring.estimated_coupon_range,
            currentPrice: symbolData.current_price,
            pctFrom52wHigh: symbolData.pct_from_52w_high,
            ma20: symbolData.ma20,
            ma50: symbolData.ma50,
            ma200: symbolData.ma200,
            impliedVolatility: scoring.selected_implied_volatility,
            flags: scoring.flags,
            tenorDays: scoring.recommended_tenor_days,
            newsItems: newsContext.narrativeItems,
            daysToEarnings: symbolData.days_to_earnings ?? null,
            hasRecentEarnings: newsContext.hasRecentEarnings,
            earningsWeight: newsContext.earningsWeight,
            daysSinceEarnings: newsContext.daysSinceEarnings
        });

        if (runId && shouldPersist) {
            try {
                await saveIdeaCandidate({
                    runId,
                    symbol,
                    overallGrade: scoring.overall_grade,
                    ivRankScore: scoring.iv_rank_score,
                    trendScore: scoring.trend_score,
                    skewScore: scoring.skew_score,
                    eventRiskScore: scoring.event_risk_score,
                    compositeScore: scoring.composite_score,
                    recommendedStrike: scoring.recommended_strike,
                    recommendedTenorDays: scoring.recommended_tenor_days,
                    recommendedExpiryDate: scoring.recommended_expiry_date,
                    refCouponPct: scoring.ref_coupon_pct,
                    moneynessPct: scoring.moneyness_pct,
                    selectedImpliedVolatility: scoring.selected_implied_volatility,
                    currentPrice: symbolData.current_price,
                    ma20: symbolData.ma20,
                    ma50: symbolData.ma50,
                    ma200: symbolData.ma200,
                    pctFrom52wHigh: symbolData.pct_from_52w_high,
                    whyNow: narrative?.why_now ?? null,
                    riskNote: narrative?.risk_note ?? null,
                    sentimentScore: narrative?.sentiment_score ?? null,
                    keyEvents: narrative?.key_events ?? [],
                    newsItems,
                    reasoningText: scoring.reasoning_text
                });

                if (scoring.flags.length > 0) {
                    await saveRiskFlags(runId, symbol, scoring.flags);
                }

                if (narrative) {
                    await updateIdeaCandidateNarrative(runId, symbol, narrative);
                }

                await updateIdeaRunStatus(runId, 'completed');
            } catch (persistError) {
                console.error(`[ideas-service] failed to persist fresh scoring for ${symbol}:`, persistError);
                try {
                    await updateIdeaRunStatus(runId, 'failed');
                } catch {
                    // Ignore persistence failures while degrading to an in-memory response.
                }
            }
        }

        return mapScoringResultToSymbolIdea(
            symbol,
            scoring,
            symbolData,
            exchange,
            underlying?.company_name ?? null,
            narrative,
            newsItems
        );
    } catch (error) {
        if (runId && shouldPersist) {
            try {
                await updateIdeaRunStatus(runId, 'failed');
            } catch {
                // Ignore persistence failures during smoke or degraded runs.
            }
        }
        throw error;
    }
}

function buildUnavailableIdeaResponse(symbol: string): SymbolIdeaResponse {
    return {
        symbol,
        exchange: 'UNKNOWN',
        company_name: null,
        run_date: todayIsoDate(),
        cached: false,
        grade: 'AVOID',
        composite_score: 0,
        verdict_headline: '暂不推荐',
        verdict_sub: '该标的数据暂时无法获取，请稍后重试',
        data_as_of_date: null,
        recommended_strike: null,
        recommended_tenor_days: null,
        recommended_expiry_date: null,
        estimated_coupon_range: null,
        coupon_note: '实际票息请向交易台询价',
        moneyness_pct: null,
        reasoning_text: 'Data unavailable for this symbol at the moment.',
        narrative: {
            why_now: '该标的数据暂时无法获取，请稍后重试。',
            risk_note: '',
            sentiment_score: 0.3,
            key_events: []
        },
        news_items: [],
        flags: [],
        signals: [],
        price_history: [],
        sentiment_score: 0.3,
        price_context: {
            current_price: null,
            ma20: null,
            ma50: null,
            ma200: null,
            pct_from_52w_high: null,
            implied_volatility: null,
            data_date: null,
            earnings_date: null,
            days_to_earnings: null
        }
    };
}

async function runFreshSymbolScoring(symbol: string): Promise<FreshSymbolAnalysis> {
    const fetcher = buildDataFetcher();
    const symbolData = await fetcher.fetchSymbolData(symbol);
    const chainData = await fetcher.fetchChainData(symbol, symbolData.current_price);

    let underlying = null as Awaited<ReturnType<typeof getUnderlyingBySymbol>>;
    try {
        underlying = await getUnderlyingBySymbol(symbol);
    } catch {
        underlying = null;
    }

    const newsContext = await fetchStockNewsContext(symbol, underlying?.company_name ?? undefined);

    const eligibility = checkEligibility(symbolData);
    if (!eligibility.eligible) {
        return {
            exchange: underlying?.exchange ?? 'UNKNOWN',
            symbolData,
            scoring: {
                symbol,
                overall_grade: 'AVOID',
                composite_score: 0,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: 1,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: 'The name is not suitable for FCN pitching today due to eligibility blocks.',
                flags: eligibility.flags
            }
        };
    }

    const approvedTenors = approveTenors(symbolData, chainData);
    if (approvedTenors.length === 0) {
        const daysToEarnings = symbolData.days_to_earnings ?? null;
        const flags: Flag[] = [
            ...eligibility.flags,
            ...(daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 14
                ? [
                    {
                        type: 'EARNINGS_PROXIMITY' as const,
                        severity: 'WARN' as const,
                        message:
                            daysToEarnings === 0
                                ? 'Earnings are due today, near-term event risk blocks FCN setup'
                                : `Earnings are due in ${daysToEarnings} day(s), near-term event risk blocks FCN setup`
                    }
                ]
                : []),
            {
                type: 'NO_APPROVED_TENOR' as const,
                severity: 'WARN' as const,
                message:
                    daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 14
                        ? 'No tenor windows passed because the earnings event falls inside the FCN tenor window'
                        : 'No tenor windows passed earnings and richness checks'
            }
        ];

        return {
            exchange: underlying?.exchange ?? 'UNKNOWN',
            symbolData,
            scoring: {
                symbol,
                overall_grade: daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 3 ? 'AVOID' : 'CAUTION',
                composite_score: daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 3 ? 0.2 : 0.35,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 3 ? 0.1 : 0.4,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: 'No tenor and strike combination passed the current screening constraints.',
                flags
            }
        };
    }

    let best90: {
        scoring: ScoringResult;
        couponDistance: number;
        strikeData: Awaited<ReturnType<DataFetcherInterface['fetchChainData']>>[number];
        tenorBucketDays: number;
    } | null = null;
    let best180: {
        scoring: ScoringResult;
        couponDistance: number;
        strikeData: Awaited<ReturnType<DataFetcherInterface['fetchChainData']>>[number];
        tenorBucketDays: number;
    } | null = null;
    const historicalVolatility = calculateHistoricalVolatility(symbolData.price_history);
    const targetCouponPct =
        historicalVolatility > 0.6
            ? ['GDX', 'USO'].includes(symbol) ? 15 : 20
            : historicalVolatility >= 0.3
              ? ['GDX', 'USO'].includes(symbol) ? 12 : 15
              : ['GDX', 'USO'].includes(symbol) ? 8 : 10;

    const shouldReplaceSameTenorChoice = (
        candidate: { scoring: ScoringResult; couponDistance: number },
        current: { scoring: ScoringResult; couponDistance: number } | null
    ) => {
        if (!current) {
            return true;
        }

        return (
            candidate.couponDistance < current.couponDistance ||
            (candidate.couponDistance === current.couponDistance &&
                candidate.scoring.composite_score > current.scoring.composite_score) ||
            (candidate.couponDistance === current.couponDistance &&
                candidate.scoring.composite_score === current.scoring.composite_score &&
                (candidate.scoring.ref_coupon_pct ?? 0) > (current.scoring.ref_coupon_pct ?? 0))
        );
    };

    for (const tenorData of approvedTenors) {
        const approvedStrikes = approveStrikes(symbol, symbolData, tenorData, getStrikeSelectionConfig(symbol));
        for (const strikeData of approvedStrikes) {
            const scoring = scoreAndGrade({
                symbol,
                symbolData,
                tenorData,
                strikeData,
                hasRecentEarnings: newsContext.hasRecentEarnings,
                sentimentProxy: newsContext.sentimentProxy,
                hasMaterialNegativeNews: newsContext.hasMaterialNegativeNews
            });
            scoring.flags = mergeUniqueFlags(eligibility.flags, scoring.flags);

            const couponDistance =
                scoring.ref_coupon_pct === null
                    ? Number.POSITIVE_INFINITY
                    : Math.abs(scoring.ref_coupon_pct - targetCouponPct);

            const candidate = {
                scoring,
                couponDistance,
                strikeData,
                tenorBucketDays: tenorData.preferred_tenor_days
            };
            const tenorDays = tenorData.preferred_tenor_days;

            if (tenorDays === 90 && shouldReplaceSameTenorChoice(candidate, best90)) {
                best90 = candidate;
            } else if (tenorDays === 180 && shouldReplaceSameTenorChoice(candidate, best180)) {
                best180 = candidate;
            }
        }
    }

    let bestChoice = best90;
    if (
        best90 !== null &&
        best180 !== null &&
        shouldPreferTenorCandidate({
            candidateTenorDays: best180.tenorBucketDays,
            candidateCouponDistance: best180.couponDistance,
            candidateCompositeScore: best180.scoring.composite_score,
            candidateRefCouponPct: best180.scoring.ref_coupon_pct,
            bestTenorDays: best90.tenorBucketDays,
            bestCouponDistance: best90.couponDistance,
            bestCompositeScore: best90.scoring.composite_score,
            bestRefCouponPct: best90.scoring.ref_coupon_pct,
            daysToEarnings: symbolData.days_to_earnings ?? null
        })
    ) {
        bestChoice = best180;
    }

    if (!bestChoice) {
        const lowLiquidity = chainData.every((strike) => strike.open_interest < 100);
        const fallbackFlagType: Flag['type'] = lowLiquidity ? 'LOW_LIQUIDITY' : 'NO_APPROVED_STRIKE';
        const flags: Flag[] = [
            ...eligibility.flags,
            {
                type: fallbackFlagType,
                severity: 'WARN' as const,
                message: lowLiquidity
                    ? 'No strikes met the minimum open-interest threshold of 100 contracts'
                    : 'No strike met the delta and liquidity filters.'
            }
        ];
        return {
            exchange: underlying?.exchange ?? 'UNKNOWN',
            symbolData,
            scoring: {
                symbol,
                overall_grade: lowLiquidity ? 'AVOID' : 'CAUTION',
                composite_score: lowLiquidity ? 0.2 : 0.35,
                iv_rank_score: 0,
                trend_score: 0,
                skew_score: 0,
                event_risk_score: 1,
                premium_score: null,
                selected_implied_volatility: null,
                recommended_strike: null,
                recommended_tenor_days: null,
                recommended_expiry_date: null,
                estimated_coupon_range: null,
                ref_coupon_pct: null,
                moneyness_pct: null,
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                reasoning_text: 'No tenor and strike combination passed the current screening constraints.',
                flags
            }
        };
    }

    const hasDrawdownRisk = bestChoice.scoring.flags.some(
        (flag) => flag.type === 'BEARISH_STRUCTURE' || flag.type === 'LOWER_HIGH_RISK'
    );
    const isHardAvoid =
        ((symbolData.days_to_earnings ?? null) !== null && (symbolData.days_to_earnings ?? 99) <= 3) ||
        bestChoice.strikeData.open_interest < 100 ||
        ((bestChoice.scoring.ref_coupon_pct ?? Number.POSITIVE_INFINITY) < 6) ||
        (
            bestChoice.scoring.flags.some((flag) => flag.type === 'BEARISH_STRUCTURE') &&
            (
                bestChoice.scoring.flags.some((flag) => flag.type === 'LOW_COUPON') ||
                bestChoice.scoring.flags.some((flag) => flag.type === 'LOW_LIQUIDITY') ||
                symbolData.pct_from_52w_high < -40
            )
        );

    const scoring =
        historicalVolatility > 0.8 &&
        hasDrawdownRisk &&
        (bestChoice.scoring.ref_coupon_pct ?? 0) >= 15 &&
        !isHardAvoid
            ? {
                  ...bestChoice.scoring,
                  overall_grade: 'CAUTION' as const,
                  flags: mergeUniqueFlags(bestChoice.scoring.flags, [
                      {
                          type: 'HIGH_VOL_LOW_STRIKE' as const,
                          severity: 'WARN' as const,
                          message: 'High-volatility caution applied because spot is in a deeper drawdown regime'
                      }
                  ]),
                  reasoning_text:
                      `The name is usable only with tighter risk discipline. Current best reference is a ` +
                      `${bestChoice.scoring.recommended_tenor_days}-day tenor around strike ` +
                      `${bestChoice.scoring.recommended_strike?.toFixed(2)}, with estimated coupon range ` +
                      `${bestChoice.scoring.estimated_coupon_range}. Key watchpoints: ` +
                      `${mergeUniqueFlags(bestChoice.scoring.flags, [
                          {
                              type: 'HIGH_VOL_LOW_STRIKE' as const,
                              severity: 'WARN' as const,
                              message: 'High-volatility caution applied because spot is in a deeper drawdown regime'
                          }
                      ])
                          .map((flag) => flag.message)
                          .join('; ')}.`
              }
            : bestChoice.scoring;

    return {
        exchange: underlying?.exchange ?? 'UNKNOWN',
        symbolData,
        scoring
    };
}

function mapScoringResultToSymbolIdea(
    symbol: string,
    scoring: ScoringResult,
    symbolData: SymbolData,
    exchange: string,
    companyName: string | null,
    narrative: NarrativeOutput | null,
    newsItems: NewsItem[]
): SymbolIdeaResponse {
    return {
        symbol,
        exchange,
        company_name: companyName,
        run_date: todayIsoDate(),
        cached: false,
        grade: scoring.overall_grade,
        composite_score: scoring.composite_score,
        verdict_headline: gradeToHeadline(scoring.overall_grade),
        verdict_sub: generateVerdictSub(scoring.overall_grade, scoring.flags),
        data_as_of_date: symbolData.price_history[symbolData.price_history.length - 1]?.date ?? todayIsoDate(),
        recommended_strike: scoring.recommended_strike,
        recommended_tenor_days: scoring.recommended_tenor_days,
        recommended_expiry_date: scoring.recommended_expiry_date,
        estimated_coupon_range: scoring.estimated_coupon_range,
        coupon_note: '实际票息请向交易台询价',
        moneyness_pct: scoring.moneyness_pct,
        reasoning_text: scoring.reasoning_text,
        narrative,
        news_items: newsItems,
        flags: scoring.flags,
        sentiment_score: narrative?.sentiment_score ?? null,
        price_history: symbolData.price_history.map((point) => ({
            date: point.date,
            close: point.close
        })),
        signals: buildSignals(symbolData, scoring),
        price_context: {
            current_price: symbolData.current_price,
            ma20: symbolData.ma20,
            ma50: symbolData.ma50,
            ma200: symbolData.ma200,
            pct_from_52w_high: symbolData.pct_from_52w_high,
            implied_volatility: scoring.selected_implied_volatility,
            data_date: symbolData.price_history[symbolData.price_history.length - 1]?.date ?? todayIsoDate(),
            earnings_date: symbolData.earnings_date ?? null,
            days_to_earnings: symbolData.days_to_earnings ?? null
        }
    };
}

function buildSignals(symbolData: SymbolData, scoring: ScoringResult): SignalRow[] {
    const trendColor: SignalColor =
        scoring.trend_score >= 0.75 ? 'green' : scoring.trend_score >= 0.45 ? 'amber' : 'red';
    const positionColor: SignalColor =
        symbolData.pct_from_52w_high >= -15 ? 'green' : symbolData.pct_from_52w_high >= -30 ? 'amber' : 'red';
    const skewColor: SignalColor =
        scoring.skew_score >= 0.7 ? 'green' : scoring.skew_score >= 0.45 ? 'amber' : 'red';
    const ivSignal = buildImpliedVolatilitySignal(scoring.selected_implied_volatility);
    const daysToEarnings = symbolData.days_to_earnings ?? null;

    const signals: SignalRow[] = [
        {
            name: 'Trend structure',
            value:
                trendColor === 'green'
                    ? 'Above 200-day moving average'
                    : trendColor === 'amber'
                      ? 'Mixed trend structure'
                      : 'Below 200-day moving average',
            color: trendColor,
            priority: Math.round((1 - scoring.trend_score) * 100)
        },
        {
            name: '52-week position',
            value: `${symbolData.pct_from_52w_high.toFixed(1)}% from 52-week high`,
            color: positionColor,
            priority: Math.round(Math.abs(symbolData.pct_from_52w_high))
        },
        {
            name: 'IV rank',
            value: ivSignal.value,
            color: ivSignal.color,
            priority: ivSignal.priority
        },
        {
            name: 'Put skew',
            value: skewColor === 'green' ? 'Seller-friendly skew' : skewColor === 'amber' ? 'Moderate skew' : 'Steep downside skew',
            color: skewColor,
            priority: Math.round((1 - scoring.skew_score) * 90)
        },
        {
            name: 'Earnings risk',
            value:
                daysToEarnings !== null && daysToEarnings <= 3
                    ? `${daysToEarnings}天内财报`
                    : daysToEarnings !== null && daysToEarnings <= 14
                      ? `${daysToEarnings}天内财报`
                      : '近14天无财报发布',
            color:
                daysToEarnings !== null && daysToEarnings <= 3
                    ? 'red'
                    : daysToEarnings !== null && daysToEarnings <= 14
                      ? 'amber'
                      : 'gray',
            priority:
                daysToEarnings !== null && daysToEarnings <= 3
                    ? 95
                    : daysToEarnings !== null && daysToEarnings <= 14
                      ? 75
                      : 10
        }
    ];

    return signals.sort((a, b) => b.priority - a.priority);
}

function buildSignalsFromCachedRow(cachedRow: {
    current_price: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    pct_from_52w_high: number | null;
    selected_implied_volatility: number | null;
    earnings_date: string | null;
    days_to_earnings: number | null;
}): SignalRow[] {
    const trendColor: SignalColor =
        (cachedRow.current_price ?? 0) > (cachedRow.ma200 ?? Number.POSITIVE_INFINITY) ? 'green' : 'red';
    const positionColor: SignalColor =
        cachedRow.pct_from_52w_high !== null && cachedRow.pct_from_52w_high >= -15
            ? 'green'
            : cachedRow.pct_from_52w_high !== null && cachedRow.pct_from_52w_high >= -30
              ? 'amber'
              : 'red';
    const ivSignal = buildImpliedVolatilitySignal(cachedRow.selected_implied_volatility);

    const signals: SignalRow[] = [
        {
            name: 'Trend structure',
            value: trendColor === 'green' ? 'Above 200-day moving average' : 'Below 200-day moving average',
            color: trendColor,
            priority: 90
        },
        {
            name: '52-week position',
            value: cachedRow.pct_from_52w_high !== null ? `${cachedRow.pct_from_52w_high.toFixed(1)}% from 52-week high` : 'Unavailable',
            color: positionColor,
            priority: 70
        },
        {
            name: 'IV rank',
            value: ivSignal.value,
            color: ivSignal.color,
            priority: ivSignal.priority
        },
        {
            name: 'Put skew',
            value: 'Derived from stored snapshot',
            color: 'amber',
            priority: 40
        },
        {
            name: 'Earnings risk',
            value:
                cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 3
                    ? `${cachedRow.days_to_earnings}天内财报`
                    : cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 14
                      ? `${cachedRow.days_to_earnings}天内财报`
                      : '近14天无财报发布',
            color:
                cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 3
                    ? 'red'
                    : cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 14
                      ? 'amber'
                      : 'gray',
            priority:
                cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 3
                    ? 95
                    : cachedRow.days_to_earnings !== null && cachedRow.days_to_earnings <= 14
                      ? 75
                      : 10
        }
    ];

    return signals;
}

function buildImpliedVolatilitySignal(iv: number | null): SignalRow {
    if (iv === null || !Number.isFinite(iv)) {
        return {
            name: 'IV rank',
            value: '数据不可用',
            color: 'amber',
            priority: 45
        };
    }

    const pct = Math.round(iv * 100);
    if (iv >= 0.6) {
        return {
            name: 'IV rank',
            value: `波动率偏高（${pct}%）`,
            color: 'green',
            priority: 80
        };
    }

    if (iv >= 0.3) {
        return {
            name: 'IV rank',
            value: `波动率适中（${pct}%）`,
            color: 'amber',
            priority: 60
        };
    }

    return {
        name: 'IV rank',
        value: `波动率偏低（${pct}%）`,
        color: 'red',
        priority: 30
    };
}

function gradeToHeadline(grade: 'GO' | 'CAUTION' | 'AVOID'): string {
    if (grade === 'GO') {
        return 'Recommended';
    }
    if (grade === 'CAUTION') {
        return 'Proceed with caution';
    }
    return 'Not recommended';
}

function generateVerdictSub(grade: 'GO' | 'CAUTION' | 'AVOID', flags: Flag[]): string {
    const hasEarningsProximity = flags.some((flag) => flag.type === 'EARNINGS_PROXIMITY');
    if (flags.some((flag) => flag.severity === 'BLOCK')) {
        return 'Blocked by current risk controls.';
    }

    if (grade === 'AVOID' && hasEarningsProximity) {
        return 'Upcoming earnings risk is too close to support FCN discussion today.';
    }

    if (grade === 'GO') {
        return 'Balanced trend, volatility, and strike setup for PB discussion.';
    }
    if (grade === 'CAUTION') {
        return 'Setup is usable, but requires tighter strike discipline and client suitability screening.';
    }
    return 'Current setup does not meet PB FCN risk standards.';
}

function mergeUniqueFlags(...flagGroups: Flag[][]): Flag[] {
    const byType = new Map<Flag['type'], Flag>();

    for (const flag of flagGroups.flat()) {
        const existing = byType.get(flag.type);
        if (!existing || severityRank(flag.severity) > severityRank(existing.severity)) {
            byType.set(flag.type, flag);
        }
    }

    return [...byType.values()];
}

function severityRank(severity: Flag['severity']): number {
    return severity === 'BLOCK' ? 3 : severity === 'WARN' ? 2 : 1;
}

function formatEstimatedCouponRange(lowerBound: number | string | null): string | null {
    if (lowerBound === null) {
        return null;
    }

    const lowerBoundNumber = Number(lowerBound);
    if (!Number.isFinite(lowerBoundNumber)) {
        return null;
    }

    const upperBound = lowerBoundNumber * (1.15 / 0.85);
    return `${Math.round(lowerBoundNumber)}%-${Math.round(upperBound)}%`;
}

function toNullableNumber(value: number | string | null | undefined): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : null;
}

async function backfillCachedNarrative(
    symbol: string,
    cachedRow: NonNullable<Awaited<ReturnType<typeof getIdeaBySymbolAndDate>>>,
    cachedFlags: Flag[],
    priceContext: Awaited<ReturnType<typeof getPriceContextBySymbol>>,
    newsItems: NewsItem[]
): Promise<void> {
    if (
        cachedRow.recommended_strike === null ||
        cachedRow.recommended_tenor_days === null ||
        !cachedRow.run_id
    ) {
        return;
    }

    const underlying = await getUnderlyingBySymbol(symbol);
    const newsContext = await fetchStockNewsContext(symbol, underlying?.company_name ?? undefined);
    const narrative = await buildNarrative({
        symbol,
        companyName: underlying?.company_name ?? getCompanyName(symbol),
        theme: underlying?.themes?.[0] ?? 'Featured',
        grade: cachedRow.overall_grade,
        recommendedStrike: toNullableNumber(cachedRow.recommended_strike),
        estimatedCouponRange: formatEstimatedCouponRange(cachedRow.ref_coupon_pct),
        currentPrice: toNullableNumber(priceContext?.current_price ?? cachedRow.current_price),
        pctFrom52wHigh: toNullableNumber(priceContext?.pct_from_52w_high ?? cachedRow.pct_from_52w_high),
        ma20: toNullableNumber(priceContext?.ma20 ?? cachedRow.ma20),
        ma50: toNullableNumber(priceContext?.ma50 ?? cachedRow.ma50),
        ma200: toNullableNumber(priceContext?.ma200 ?? cachedRow.ma200),
        impliedVolatility: toNullableNumber(cachedRow.selected_implied_volatility),
        flags: cachedFlags,
        tenorDays: cachedRow.recommended_tenor_days,
        newsItems,
        daysToEarnings: priceContext?.days_to_earnings ?? cachedRow.days_to_earnings,
        hasRecentEarnings: newsContext.hasRecentEarnings,
        earningsWeight: newsContext.earningsWeight,
        daysSinceEarnings: newsContext.daysSinceEarnings
    });

    if (narrative) {
        await updateIdeaCandidateNarrative(cachedRow.run_id, symbol, narrative);
    }
}

async function buildNarrative(input: {
    symbol: string;
    companyName?: string | null;
    theme: string;
    grade: string;
    recommendedStrike: number | null;
    estimatedCouponRange: string | null;
    currentPrice: number | null;
    pctFrom52wHigh: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    impliedVolatility: number | null;
    flags: Flag[];
    tenorDays: number | null;
    newsItems: NewsItem[];
    daysToEarnings: number | null;
    hasRecentEarnings: boolean;
    earningsWeight: number;
    daysSinceEarnings: number | null;
}): Promise<NarrativeOutput | null> {
    if (
        input.recommendedStrike === null ||
        input.estimatedCouponRange === null ||
        input.tenorDays === null
    ) {
        return null;
    }

    const narrative = await generateNarrative({
        symbol: input.symbol,
        company_name: input.companyName,
        theme: input.theme,
        grade: input.grade,
        recommended_strike: input.recommendedStrike,
        estimated_coupon_range: input.estimatedCouponRange,
        current_price: input.currentPrice,
        pct_from_52w_high: input.pctFrom52wHigh,
        ma20: input.ma20,
        ma50: input.ma50,
        ma200: input.ma200,
        iv_level:
            input.impliedVolatility !== null
                ? input.impliedVolatility >= 0.6
                    ? '高'
                    : input.impliedVolatility >= 0.3
                      ? '中'
                      : '低'
                : '中',
        flags: input.flags,
        tenor_days: input.tenorDays,
        news_headlines: input.newsItems.map((item) => item.title),
        news_items: input.newsItems,
        has_recent_earnings: input.hasRecentEarnings,
        earnings_weight: input.earningsWeight,
        days_to_earnings: input.daysToEarnings,
        days_since_earnings: input.daysSinceEarnings
    });

    const marketCap = await fetchTickerMarketCap(input.symbol).catch(() => null);

    return applyNarrativeGuardrails(narrative, {
        ...input,
        marketCap
    });
}

function applyNarrativeGuardrails(
    narrative: NarrativeOutput,
    input: {
        symbol: string;
        currentPrice: number | null;
        flags: Flag[];
        recommendedStrike: number | null;
        marketCap: number | null;
    }
): NarrativeOutput {
    const hasHighVolLowStrike = input.flags.some((flag) => flag.type === 'HIGH_VOL_LOW_STRIKE');
    const isCryptoLinked = CRYPTO_LINKED.includes(input.symbol);
    const isSmallCap = input.marketCap !== null && input.marketCap < 20_000_000_000;
    const riskNoteParts = [narrative.risk_note].filter(Boolean);

    if (isCryptoLinked) {
        riskNoteParts.push('标的与加密货币市场高度相关，建议结合house view及客户风险承受度评估适合性。');
    }

    if (input.symbol === 'USO') {
        riskNoteParts.push('需留意USO持有的是原油期货而非实物原油，若期货曲线处于升水结构，展期过程可能持续侵蚀回报。');
        riskNoteParts.push('若美伊冲突推升油价并抬高通胀预期，短期波动率与曲线结构都可能快速恶化。');
    }

    if (input.symbol === 'GDX') {
        riskNoteParts.push('需留意GDX持有的是黄金矿商而非实物黄金，矿商股对金价、成本与利率预期的波动通常放大。');
        riskNoteParts.push('若油价上行推升通胀与加息预期，黄金矿商股可能较黄金现货承受更明显回撤。');
    }

    if (input.symbol === 'XOM') {
        riskNoteParts.push('若中东局势缓和或供给担忧降温，油价回落可能压缩能源股短期弹性与票息吸引力。');
    }

    if (isSmallCap) {
        riskNoteParts.push('标的流动性及报价差异较大，建议询价前确认交易台可做性。');
    }

    if (!hasHighVolLowStrike) {
        return {
            ...narrative,
            risk_note: dedupeSentences(riskNoteParts.join(''))
        };
    }

    const prefix = '高波动标的，当前价格距高点回撤较深，接股风险较高，建议充分评估客户风险承受能力。';
    riskNoteParts.unshift(prefix);

    return {
        ...narrative,
        risk_note: dedupeSentences(riskNoteParts.join(''))
    };
}

const CRYPTO_LINKED = ['COIN', 'HOOD', 'MSTR'];

function dedupeSentences(value: string): string {
    const parts = value
        .split(/(?<=[。！？])/)
        .map((part) => part.trim())
        .filter(Boolean);
    const seen = new Set<string>();
    const deduped: string[] = [];

    for (const part of parts) {
        if (seen.has(part)) {
            continue;
        }
        seen.add(part);
        deduped.push(part);
    }

    return deduped.join('');
}

function calculateFreshnessPenalty(symbol: string, history: Array<{ symbol: string }>): number {
    let streak = 0;
    for (const entry of history) {
        if (entry.symbol === symbol) {
            streak += 1;
        } else {
            break;
        }
    }

    if (streak <= 1) {
        return 0;
    }
    if (streak === 2) {
        return 0.05;
    }
    if (streak === 3) {
        return 0.10;
    }
    return 0.20;
}

async function mapDailyBestCard(
    symbol: string,
    theme: string,
    ideas: Array<{
        symbol: string;
        overall_grade: 'GO' | 'CAUTION' | 'AVOID';
        company_name?: string | null;
        themes: string[];
        why_now?: string | null;
        risk_note?: string | null;
        sentiment_score?: number | null;
        key_events?: string[] | null;
        news_items?: NewsItem[] | null;
        recommended_strike: number | null;
        recommended_tenor_days: number | null;
        expiry_date?: string | null;
        ref_coupon_pct: number | null;
        moneyness_pct: number | null;
        selected_implied_volatility?: number | null;
        reasoning_text: string;
    }>,
    flagsBySymbol: Map<string, Flag[]>
): Promise<DailyBestCard | null> {
    const idea = ideas.find((entry) => entry.symbol === symbol);
    const recommendedStrike = parseNullableNumber(idea?.recommended_strike ?? null);
    const recommendedTenorDays = parseNullableNumber(idea?.recommended_tenor_days ?? null);
    const moneynessPct = parseNullableNumber(idea?.moneyness_pct ?? null);

    if (
        !idea ||
        idea.overall_grade !== 'GO' ||
        recommendedStrike === null ||
        recommendedTenorDays === null ||
        moneynessPct === null
    ) {
        return null;
    }

    const narrative = idea.why_now
        ? {
              why_now: idea.why_now,
              risk_note: idea.risk_note ?? '',
              sentiment_score: parseNullableNumber(idea.sentiment_score ?? null) ?? 0.5,
              key_events: idea.key_events ?? []
          }
        : await buildNarrative({
              ...(await (async () => {
                  const newsContext = await fetchStockNewsContext(symbol, idea.company_name ?? undefined);
                  return {
                      newsItems: newsContext.narrativeItems,
                      hasRecentEarnings: newsContext.hasRecentEarnings,
                      earningsWeight: newsContext.earningsWeight,
                      daysSinceEarnings: newsContext.daysSinceEarnings
                  };
              })()),
              symbol,
              companyName: idea.company_name ?? getCompanyName(symbol),
              theme,
              grade: 'GO',
              recommendedStrike,
              estimatedCouponRange: formatEstimatedCouponRange(idea.ref_coupon_pct),
              currentPrice: 0,
              pctFrom52wHigh: 0,
              ma20: 0,
              ma50: 0,
              ma200: 0,
              impliedVolatility: parseNullableNumber(idea.selected_implied_volatility ?? null),
              flags: flagsBySymbol.get(symbol) ?? [],
              tenorDays: recommendedTenorDays,
              daysToEarnings: null
          });

    return {
        symbol,
        company_name: idea.company_name ?? null,
        theme,
        theme_narrative: buildThemeNarrative(idea.themes ?? [theme], flagsBySymbol.get(symbol) ?? []),
        grade: 'GO',
        recommended_strike: recommendedStrike,
        recommended_tenor_days: recommendedTenorDays,
        recommended_expiry_date: idea.expiry_date ?? null,
        estimated_coupon_range: formatEstimatedCouponRange(idea.ref_coupon_pct) ?? '—',
        moneyness_pct: moneynessPct,
        reasoning_text: idea.reasoning_text,
        narrative,
        news_items: idea.news_items ?? [],
        flags: flagsBySymbol.get(symbol) ?? [],
        sentiment_score: narrative?.sentiment_score ?? parseNullableNumber(idea.sentiment_score ?? null)
    };
}

function parseNullableNumber(value: number | string | null): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}
