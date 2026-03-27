import { pool } from '../db/client';

export interface NewsItem {
    title: string;
    source: string;
    url: string;
    published_at: string;
}

export interface StockNewsContext {
    items: NewsItem[];
    narrativeItems: NewsItem[];
    displayItems: NewsItem[];
    hasRecentEarnings: boolean;
    earningsWeight: number;
    daysSinceEarnings: number | null;
    sentimentProxy: number | null;
    hasMaterialNegativeNews: boolean;
}

const GOOGLE_NEWS_RSS_BASE_URL = 'https://news.google.com/rss/search';
const NEWS_FETCH_TIMEOUT_MS = 3000;

const ISSUER_NAMES: Record<string, string> = {
    NVDA: 'Nvidia',
    MU: 'Micron',
    TSM: 'TSMC',
    AVGO: 'Broadcom',
    AMD: 'AMD',
    AMZN: 'Amazon',
    MSFT: 'Microsoft',
    META: 'Meta',
    GOOG: 'Google',
    AAPL: 'Apple',
    TSLA: 'Tesla',
    PLTR: 'Palantir',
    NFLX: 'Netflix',
    AXP: 'American Express',
    HOOD: 'Robinhood',
    BE: 'Bloom Energy',
    BABA: 'Alibaba',
    PDD: 'PDD',
    BIDU: 'Baidu',
    APP: 'AppLovin',
    COIN: 'Coinbase',
    CRWV: 'CoreWeave',
    MSTR: 'MicroStrategy',
    CRCL: 'Circle',
    SMCI: 'Super Micro Computer',
    GLD: 'SPDR Gold Shares',
    MP: 'MP Materials',
    NBIS: 'Nebius',
    NEM: 'Newmont',
    CEG: 'Constellation Energy',
    UNH: 'UnitedHealth',
    INTC: 'Intel',
    ORCL: 'Oracle',
    LITE: 'Lumentum',
    VRT: 'Vertiv',
    GDX: 'VanEck Gold Miners ETF',
    XOM: 'Exxon Mobil',
    USO: 'United States Oil Fund'
};

const SPECIAL_QUERIES: Record<string, string> = {
    NVDA: 'Nvidia GTC AI GPU data center',
    MU: 'Micron HBM memory AI supercycle',
    TSM: 'TSMC AI chip foundry',
    AMD: 'AMD AI chip semiconductor',
    AVGO: 'Broadcom AI networking chip',
    INTC: 'Intel foundry 18A NVIDIA partnership',
    GLD: 'gold price market',
    NEM: 'Newmont gold mining earnings',
    CEG: 'nuclear energy Constellation',
    TSLA: 'Tesla AI autonomous driving',
    AMZN: 'Amazon AWS AI cloud retail',
    MSFT: 'Microsoft Azure AI cloud Copilot',
    META: 'Meta AI advertising revenue',
    GOOG: 'Google search AI Gemini advertising',
    PLTR: 'Palantir AI government defense contract',
    APP: 'AppLovin AXON mobile advertising AI',
    BE: 'Bloom Energy fuel cell solid oxide power data center',
    COIN: 'Coinbase crypto regulation',
    CRWV: 'CoreWeave AI cloud GPU infrastructure',
    MSTR: 'MicroStrategy Bitcoin',
    CRCL: 'Circle stablecoin USDC',
    SMCI: 'Super Micro Computer export control indictment DOJ China server shipment',
    BABA: 'Alibaba China AI cloud',
    MP: 'MP Materials rare earth magnet supply chain',
    NBIS: 'Nebius AI cloud GPU infrastructure',
    PDD: 'PDD Temu ecommerce China',
    BIDU: 'Baidu AI China',
    NFLX: 'Netflix streaming subscriber earnings',
    HOOD: 'Robinhood retail crypto trading',
    AAPL: 'Apple iPhone AI services revenue',
    UNH: 'UnitedHealth insurance earnings',
    ORCL: 'Oracle cloud database AI infrastructure',
    LITE: 'Lumentum optical networking AI data center',
    VRT: 'Vertiv data center power cooling AI',
    GDX: 'gold miners ETF gold price mining Iran Middle East inflation Fed rates',
    XOM: 'Exxon oil energy market crude Iran Middle East Hormuz inflation',
    USO: 'WTI crude oil ETF energy market Iran Middle East Hormuz inflation'
};

const RECENT_EARNINGS_QUERIES: Record<string, string> = {
    MU: 'Micron earnings results beat revenue 2026',
    NVDA: 'Nvidia earnings results beat revenue 2026'
};

export function getCompanyName(symbol: string): string {
    return ISSUER_NAMES[symbol.toUpperCase()] ?? symbol.toUpperCase();
}

interface RecentEarningsStatus {
    hasRecentEarnings: boolean;
    daysSinceEarnings: number | null;
    earningsWeight: number;
}

async function getRecentEarningsStatus(symbol: string): Promise<RecentEarningsStatus> {
    const result = await pool.query<{ days_since_earnings: number | null }>(
        `
        SELECT
            (CURRENT_DATE - report_date)::int AS days_since_earnings
        FROM earnings_calendar
        WHERE symbol = $1
          AND report_date BETWEEN (CURRENT_DATE - INTERVAL '14 days') AND CURRENT_DATE
        ORDER BY report_date DESC
        LIMIT 1
        `,
        [symbol]
    );

    const daysSinceEarnings = result.rows[0]?.days_since_earnings ?? null;
    const earningsWeight = getEarningsWeight(daysSinceEarnings);

    return {
        hasRecentEarnings: earningsWeight > 0,
        daysSinceEarnings,
        earningsWeight
    };
}

export async function fetchStockNewsContext(
    symbol: string,
    companyName?: string
): Promise<StockNewsContext> {
    try {
        const normalizedSymbol = symbol.toUpperCase();
        const earningsStatus = await getRecentEarningsStatus(normalizedSymbol);
        const issuerName = ISSUER_NAMES[normalizedSymbol] ?? companyName ?? normalizedSymbol;
        const earningsQuery =
            RECENT_EARNINGS_QUERIES[normalizedSymbol] ??
            `${issuerName} earnings profit revenue miss fall shares drop ${new Date().getFullYear()}`;
        const standardQuery = SPECIAL_QUERIES[normalizedSymbol] ?? `${issuerName} company business outlook`;
        const queryUsed = `primary=${earningsQuery}; secondary=${standardQuery}`;
        console.log(`[news-fetcher] ${normalizedSymbol} hasRecentEarnings=${earningsStatus.hasRecentEarnings}, using query: ${queryUsed}`);

        const primaryItems = await fetchNewsItemsForQuery(earningsQuery);
        const secondaryItems = await fetchNewsItemsForQuery(standardQuery);
        const newsIndicatesRecentEarnings = dedupeNewsItems([...primaryItems, ...secondaryItems]).some((item) =>
            isEarningsHeadline(item.title)
        );
        const hasRecentEarnings = earningsStatus.hasRecentEarnings || newsIndicatesRecentEarnings;
        const effectiveEarningsWeight = hasRecentEarnings
            ? Math.max(earningsStatus.earningsWeight, newsIndicatesRecentEarnings ? 0.6 : 0)
            : 0;

        const displayItems = buildDisplayNewsItems(primaryItems, secondaryItems, hasRecentEarnings);

        let combinedItems = hasRecentEarnings
            ? buildWeightedRecentEarningsNewsItems(primaryItems, secondaryItems, effectiveEarningsWeight)
            : dedupeNewsItems([...secondaryItems]);

        combinedItems = filterStaleEarningsHeadlines(combinedItems, hasRecentEarnings);

        if (hasRecentEarnings) {
            combinedItems = sortNewsItemsForNarrative(combinedItems);

            const leadingItems = combinedItems.slice(0, 3);
            const hasEarningsHeadline = leadingItems.some((item) => scoreNewsPriority(item.title) <= 2);

            if (!hasEarningsHeadline) {
                const fallbackItems = await fetchNewsItemsForQuery(`${issuerName} stock falls earnings ${new Date().getFullYear()}`);
                if (fallbackItems.length > 0) {
                    combinedItems = dedupeNewsItems([fallbackItems[0], ...combinedItems]);
                    combinedItems = sortNewsItemsForNarrative(combinedItems);
                }
            }
        }

        return {
            items: combinedItems.slice(0, 5),
            narrativeItems: combinedItems.slice(0, 5),
            displayItems,
            hasRecentEarnings,
            earningsWeight: effectiveEarningsWeight,
            daysSinceEarnings: earningsStatus.daysSinceEarnings,
            sentimentProxy: hasRecentEarnings ? scoreRecentEarningsHeadlines(primaryItems.length > 0 ? primaryItems : combinedItems) : null,
            hasMaterialNegativeNews: detectMaterialNegativeNews(combinedItems)
        };
    } catch {
        return {
            items: [],
            narrativeItems: [],
            displayItems: [],
            hasRecentEarnings: false,
            earningsWeight: 0,
            daysSinceEarnings: null,
            sentimentProxy: null,
            hasMaterialNegativeNews: false
        };
    }
}

export async function fetchStockNews(symbol: string, companyName?: string): Promise<NewsItem[]> {
    const context = await fetchStockNewsContext(symbol, companyName);
    return context.displayItems;
}

async function fetchNewsItemsForQuery(query: string): Promise<NewsItem[]> {
    return fetchNewsItemsByQuery(query);
}

export async function fetchNewsItemsByQuery(
    query: string,
    options: { excludeEtfAndFunds?: boolean } = {}
): Promise<NewsItem[]> {
    const url = new URL(GOOGLE_NEWS_RSS_BASE_URL);
    url.searchParams.set('q', query);
    url.searchParams.set('hl', 'en-US');
    url.searchParams.set('gl', 'US');
    url.searchParams.set('ceid', 'US:en');

    let response: Response;
    try {
        response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)'
            },
            signal: AbortSignal.timeout(NEWS_FETCH_TIMEOUT_MS)
        });
    } catch {
        return [];
    }

    if (!response.ok) {
        return [];
    }

    let xml: string;
    try {
        xml = await response.text();
    } catch {
        return [];
    }

    const excludeEtfAndFunds = options.excludeEtfAndFunds ?? true;

    return extractItems(xml)
        .filter((article) => !excludeEtfAndFunds || !/ETF|fund/i.test(article.title))
        .filter((article) => article.title.length > 0);
}

function dedupeNewsItems(items: NewsItem[]): NewsItem[] {
    const seen = new Set<string>();
    const deduped: NewsItem[] = [];

    for (const item of items) {
        const key = `${item.title}|${item.url}`;
        if (seen.has(key)) {
            continue;
        }

        seen.add(key);
        deduped.push(item);
    }

    return deduped;
}

function filterStaleEarningsHeadlines(items: NewsItem[], hasRecentEarnings: boolean): NewsItem[] {
    if (hasRecentEarnings) {
        return items;
    }

    return items.filter((item) => {
        if (!isEarningsHeadline(item.title)) {
            return true;
        }

        const publishedAt = Date.parse(item.published_at);
        if (Number.isNaN(publishedAt)) {
            return false;
        }

        const ageInDays = (Date.now() - publishedAt) / (24 * 60 * 60 * 1000);
        return ageInDays <= 14;
    });
}

function buildDisplayNewsItems(
    primaryItems: NewsItem[],
    secondaryItems: NewsItem[],
    hasRecentEarnings: boolean
): NewsItem[] {
    const dedupedItems = dedupeNewsItems([...primaryItems, ...secondaryItems]);

    if (hasRecentEarnings) {
        return sortNewsItemsForNarrative(dedupedItems).slice(0, 5);
    }

    if (secondaryItems.length > 0) {
        return dedupeNewsItems([...secondaryItems, ...primaryItems]).slice(0, 5);
    }

    return dedupedItems.slice(0, 5);
}

function buildWeightedRecentEarningsNewsItems(
    primaryItems: NewsItem[],
    secondaryItems: NewsItem[],
    earningsWeight: number
): NewsItem[] {
    if (earningsWeight >= 1) {
        return dedupeNewsItems([...primaryItems.slice(0, 3)]);
    }

    if (earningsWeight >= 0.6) {
        return dedupeNewsItems([...primaryItems.slice(0, 2), ...secondaryItems.slice(0, 2)]);
    }

    return dedupeNewsItems([...secondaryItems.slice(0, 3), ...primaryItems.slice(0, 1)]);
}

function getEarningsWeight(daysSinceEarnings: number | null): number {
    if (daysSinceEarnings === null || daysSinceEarnings < 0) {
        return 0;
    }
    if (daysSinceEarnings <= 3) {
        return 1.0;
    }
    if (daysSinceEarnings <= 7) {
        return 0.6;
    }
    if (daysSinceEarnings <= 14) {
        return 0.3;
    }
    return 0;
}

function sortNewsItemsForNarrative(items: NewsItem[]): NewsItem[] {
    return [...items].sort((a, b) => scoreNewsPriority(a.title) - scoreNewsPriority(b.title));
}

function scoreNewsPriority(title: string): number {
    const normalizedTitle = title.toLowerCase();
    const negativePhrases = [
        'earnings miss',
        'revenue miss',
        'profit drop',
        'profit falls',
        'profit down',
        'below estimates',
        'disappoints',
        'warns',
        'guidance cut',
        'shares drop',
        'stock falls'
    ];
    const neutralPhrases = ['earnings', 'results', 'quarterly'];
    const industryPhrases = ['ai', 'cloud', 'growth', 'targets'];

    if (negativePhrases.some((phrase) => normalizedTitle.includes(phrase))) {
        return 0;
    }
    if (neutralPhrases.some((phrase) => normalizedTitle.includes(phrase))) {
        return 1;
    }
    if (industryPhrases.some((phrase) => normalizedTitle.includes(phrase))) {
        return 2;
    }
    return 1;
}

function isEarningsHeadline(title: string): boolean {
    const normalizedTitle = title.toLowerCase();
    const earningsSignals = [
        'earnings',
        'results',
        'quarterly',
        'profit down',
        'profit falls',
        'revenue miss',
        'below estimates',
        'shares drop',
        'stock falls',
        'eps miss'
    ];

    return earningsSignals.some((signal) => normalizedTitle.includes(signal));
}

function scoreRecentEarningsHeadlines(items: NewsItem[]): number {
    if (items.length === 0) {
        return 0.5;
    }

    const negativePhrases = [
        'earnings miss',
        'revenue miss',
        'profit drop',
        'profit falls',
        'below estimates',
        'disappoints',
        'warns',
        'guidance cut'
    ];
    const negativeWords = ['slump', 'sink', 'plunge', 'weak', 'decline'];
    const positivePhrases = ['beats estimates', 'tops expectations', 'record revenue', 'strong results'];
    const positiveWords = ['surge', 'jump', 'rise'];

    let score = 0.5;
    for (const item of items) {
        const normalizedTitle = item.title.toLowerCase();
        const negativePhraseMatches = negativePhrases.filter((phrase) => normalizedTitle.includes(phrase)).length;
        const positivePhraseMatches = positivePhrases.filter((phrase) => normalizedTitle.includes(phrase)).length;
        const negativeWordMatches = negativeWords.filter((word) => normalizedTitle.includes(word)).length;
        const positiveWordMatches = positiveWords.filter((word) => normalizedTitle.includes(word)).length;

        if (negativePhraseMatches > 0 && positivePhraseMatches > 0) {
            score -= negativePhraseMatches * 0.25;
            continue;
        }

        score -= negativePhraseMatches * 0.25;
        score -= negativeWordMatches * 0.12;
        score += positivePhraseMatches * 0.15;
        score += positiveWordMatches * 0.08;
    }

    return Math.min(Math.max(Number(score.toFixed(2)), 0), 1);
}

function detectMaterialNegativeNews(items: NewsItem[]): boolean {
    if (items.length === 0) {
        return false;
    }

    const severePhrases = [
        'threatens',
        'would limit',
        'limit rewards',
        'reward restrictions',
        'regulatory risk',
        'draft bill',
        'draft act',
        'senate bill',
        'probe',
        'investigation',
        'lawsuit',
        'sues',
        'indictment',
        'fraud',
        'short seller',
        'short report',
        'ban',
        'sanction'
    ];
    const sharpMoveWords = ['plunge', 'plunges', 'slump', 'slumps', 'sink', 'sinks', 'tumble', 'tumbles', 'drop', 'drops', 'fall', 'falls'];

    return items.slice(0, 3).some((item) => {
        const normalizedTitle = item.title.toLowerCase();
        const hasSeverePhrase = severePhrases.some((phrase) => normalizedTitle.includes(phrase));
        const hasShockMove = sharpMoveWords.some((word) => normalizedTitle.includes(word));
        return hasSeverePhrase || (hasShockMove && /(bill|act|regulat|investigat|lawsuit|probe|fraud|indict)/.test(normalizedTitle));
    });
}

function extractItems(xml: string): NewsItem[] {
    const items = xml.match(/<item\b[\s\S]*?<\/item>/gi) ?? [];
    return items.map((itemXml) => ({
        title: decodeXml(readTag(itemXml, 'title')),
        source: decodeXml(readTag(itemXml, 'source')),
        url: decodeXml(readTag(itemXml, 'link')),
        published_at: decodeXml(readTag(itemXml, 'pubDate'))
    }));
}

function readTag(xml: string, tagName: string): string {
    const pattern = new RegExp(`<${tagName}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tagName}>`, 'i');
    const match = xml.match(pattern);
    return match?.[1]?.trim() ?? '';
}

function decodeXml(value: string): string {
    return value
        .replace(/<!\[CDATA\[(.*?)\]\]>/g, '$1')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");
}
