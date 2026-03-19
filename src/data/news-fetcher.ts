import { pool } from '../db/client';

export interface NewsItem {
    title: string;
    source: string;
    url: string;
    published_at: string;
}

const GOOGLE_NEWS_RSS_BASE_URL = 'https://news.google.com/rss/search';

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
    V: 'Visa',
    MA: 'Mastercard',
    AXP: 'American Express',
    HOOD: 'Robinhood',
    BABA: 'Alibaba',
    PDD: 'PDD',
    BIDU: 'Baidu',
    COIN: 'Coinbase',
    MSTR: 'MicroStrategy',
    CRCL: 'Circle',
    GLD: 'SPDR Gold Shares',
    NEM: 'Newmont',
    CEG: 'Constellation Energy',
    UNH: 'UnitedHealth',
    INTC: 'Intel',
    ORCL: 'Oracle'
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
    COIN: 'Coinbase crypto regulation',
    MSTR: 'MicroStrategy Bitcoin',
    CRCL: 'Circle stablecoin USDC',
    BABA: 'Alibaba China AI cloud',
    PDD: 'PDD Temu ecommerce China',
    BIDU: 'Baidu AI China',
    NFLX: 'Netflix streaming subscriber earnings',
    V: 'Visa payment network earnings',
    MA: 'Mastercard payment consumer spending',
    HOOD: 'Robinhood retail crypto trading',
    AAPL: 'Apple iPhone AI services revenue',
    UNH: 'UnitedHealth insurance earnings',
    ORCL: 'Oracle cloud database AI infrastructure'
};

const RECENT_EARNINGS_QUERIES: Record<string, string> = {
    MU: 'Micron earnings results beat revenue 2026',
    NVDA: 'Nvidia earnings results beat revenue 2026'
};

export function getCompanyName(symbol: string): string {
    return ISSUER_NAMES[symbol.toUpperCase()] ?? symbol.toUpperCase();
}

async function hasRecentEarningsRelease(symbol: string): Promise<boolean> {
    const result = await pool.query<{ has_recent_earnings: boolean }>(
        `
        SELECT EXISTS (
            SELECT 1
            FROM earnings_calendar
            WHERE symbol = $1
              AND report_date BETWEEN (CURRENT_DATE - INTERVAL '3 days') AND CURRENT_DATE
        ) AS has_recent_earnings
        `,
        [symbol]
    );

    return result.rows[0]?.has_recent_earnings ?? false;
}

export async function fetchStockNewsContext(
    symbol: string,
    companyName?: string
): Promise<{ items: NewsItem[]; hasRecentEarnings: boolean }> {
    try {
        const normalizedSymbol = symbol.toUpperCase();
        const hasRecentEarnings = await hasRecentEarningsRelease(normalizedSymbol);
        const issuerName = ISSUER_NAMES[normalizedSymbol] ?? companyName ?? normalizedSymbol;
        const query = hasRecentEarnings
            ? RECENT_EARNINGS_QUERIES[normalizedSymbol] ?? `${issuerName} earnings results 2026`
            : SPECIAL_QUERIES[normalizedSymbol] ?? `${normalizedSymbol} stock earnings news`;

        const url = new URL(GOOGLE_NEWS_RSS_BASE_URL);
        url.searchParams.set('q', query);
        url.searchParams.set('hl', 'en-US');
        url.searchParams.set('gl', 'US');
        url.searchParams.set('ceid', 'US:en');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)'
            }
        });
        if (!response.ok) {
            return { items: [], hasRecentEarnings };
        }

        const xml = await response.text();
        return {
            items: extractItems(xml)
            .filter((article) => !/ETF|fund/i.test(article.title))
            .slice(0, 3)
            .filter((article) => article.title.length > 0),
            hasRecentEarnings
        };
    } catch {
        return { items: [], hasRecentEarnings: false };
    }
}

export async function fetchStockNews(symbol: string, companyName?: string): Promise<NewsItem[]> {
    const context = await fetchStockNewsContext(symbol, companyName);
    return context.items;
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
