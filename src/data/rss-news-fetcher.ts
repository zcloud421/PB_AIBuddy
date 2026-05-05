import axios from 'axios';
import { XMLParser } from 'fast-xml-parser';

import type { NewsItem } from '../types/api';

interface RssSource {
    id: string;
    name: string;
    url: string;
    category: 'central-bank' | 'macro' | 'us-equity' | 'global-news' | 'hk-equity';
    timeoutMs: number;
    supportsKeywordQuery?: boolean;
}

interface ParsedRssItem {
    title: string;
    link: string;
    pubDate: string;
    source: string;
    sourceId: string;
    description?: string;
}

export interface RssFallbackOptions {
    categories?: Array<RssSource['category']>;
    keywords?: string;
    maxItemsPerSource?: number;
    maxTotalItems?: number;
}

const RSS_SOURCES: RssSource[] = [
    { id: 'fed-press', name: 'Federal Reserve Press Releases', url: 'https://www.federalreserve.gov/feeds/press_all.xml', category: 'central-bank', timeoutMs: 8000 },
    { id: 'fed-speeches', name: 'Fed Speeches', url: 'https://www.federalreserve.gov/feeds/speeches.xml', category: 'central-bank', timeoutMs: 8000 },
    { id: 'boj-news', name: 'Bank of Japan News', url: 'https://www.boj.or.jp/en/rss/whatsnew.xml', category: 'central-bank', timeoutMs: 8000 },
    { id: 'ecb-press', name: 'ECB Press Releases', url: 'https://www.ecb.europa.eu/rss/press.html', category: 'central-bank', timeoutMs: 8000 },
    { id: 'cnbc-business', name: 'CNBC Top News', url: 'https://www.cnbc.com/id/100003114/device/rss/rss.html', category: 'global-news', timeoutMs: 6000 },
    { id: 'cnbc-finance', name: 'CNBC Finance', url: 'https://www.cnbc.com/id/10000664/device/rss/rss.html', category: 'us-equity', timeoutMs: 6000 },
    { id: 'cnbc-economy', name: 'CNBC Economy', url: 'https://www.cnbc.com/id/20910258/device/rss/rss.html', category: 'macro', timeoutMs: 6000 },
    { id: 'marketwatch-top', name: 'MarketWatch Top Stories', url: 'http://feeds.marketwatch.com/marketwatch/topstories/', category: 'global-news', timeoutMs: 6000 },
    { id: 'investing-news', name: 'Investing.com News', url: 'https://www.investing.com/rss/news.rss', category: 'global-news', timeoutMs: 6000 },
    { id: 'sec-8k', name: 'SEC 8-K Filings', url: 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K&dateb=&owner=include&count=40&output=atom', category: 'us-equity', timeoutMs: 8000 },
    { id: 'google-news-fed', name: 'Google News: Fed', url: 'https://news.google.com/rss/search?q={q}+when:1d&hl=en-US&gl=US&ceid=US:en', category: 'macro', timeoutMs: 6000, supportsKeywordQuery: true },
];

const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
});

function asArray<T>(value: T | T[] | null | undefined): T[] {
    if (value === null || value === undefined) {
        return [];
    }
    return Array.isArray(value) ? value : [value];
}

function textValue(value: unknown): string {
    if (typeof value === 'string' || typeof value === 'number') {
        return String(value).trim();
    }
    if (value && typeof value === 'object') {
        const record = value as Record<string, unknown>;
        if (typeof record['#text'] === 'string') {
            return record['#text'].trim();
        }
    }
    return '';
}

function parseDateToIso(value: unknown): string | null {
    const raw = textValue(value);
    if (!raw) {
        return null;
    }
    const timestamp = Date.parse(raw);
    if (Number.isNaN(timestamp)) {
        return null;
    }
    return new Date(timestamp).toISOString();
}

function atomLinkValue(link: unknown): string {
    const links = asArray(link);
    for (const entry of links) {
        if (typeof entry === 'string') {
            return entry;
        }
        if (entry && typeof entry === 'object') {
            const record = entry as Record<string, unknown>;
            const href = textValue(record['@_href']);
            if (href) {
                return href;
            }
        }
    }
    return '';
}

function normalizeTitle(title: string): string {
    return title.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
}

function sourceUrl(source: RssSource, keyword?: string): string | null {
    if (!source.supportsKeywordQuery) {
        return source.url;
    }
    if (!keyword?.trim()) {
        return null;
    }
    return source.url.replace('{q}', encodeURIComponent(keyword.trim()));
}

function parseRssItems(xml: string, source: RssSource): ParsedRssItem[] {
    const parsed = parser.parse(xml) as Record<string, unknown>;
    const rssChannel = (parsed.rss as { channel?: { item?: unknown } } | undefined)?.channel;
    const rssItems = asArray(rssChannel?.item);
    if (rssItems.length > 0) {
        return rssItems
            .map((item) => {
                const record = item as Record<string, unknown>;
                const pubDate = parseDateToIso(record.pubDate ?? record.date ?? record['dc:date']);
                return {
                    title: textValue(record.title),
                    link: textValue(record.link),
                    pubDate: pubDate ?? '',
                    source: source.name,
                    sourceId: source.id,
                    description: textValue(record.description),
                };
            })
            .filter((item) => item.title && item.pubDate);
    }

    const atomEntries = asArray((parsed.feed as { entry?: unknown } | undefined)?.entry);
    return atomEntries
        .map((entry) => {
            const record = entry as Record<string, unknown>;
            const pubDate = parseDateToIso(record.updated ?? record.published);
            return {
                title: textValue(record.title),
                link: atomLinkValue(record.link),
                pubDate: pubDate ?? '',
                source: source.name,
                sourceId: source.id,
                description: textValue(record.summary ?? record.content),
            };
        })
        .filter((item) => item.title && item.pubDate);
}

async function fetchRssSource(source: RssSource, keyword?: string): Promise<ParsedRssItem[]> {
    const url = sourceUrl(source, keyword);
    if (!url) {
        return [];
    }

    try {
        const response = await axios.get<string>(url, {
            timeout: source.timeoutMs,
            responseType: 'text',
            headers: {
                'User-Agent': 'Mozilla/5.0 PB-AIBuddy-RSS/1.0',
                Accept: 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
            },
        });
        const cutoff = Date.now() - (24 * 60 * 60 * 1000);
        return parseRssItems(response.data, source).filter((item) => {
            const publishedAt = Date.parse(item.pubDate);
            return Number.isFinite(publishedAt) && publishedAt >= cutoff;
        });
    } catch {
        return [];
    }
}

export async function fetchRssNewsFallback(options: RssFallbackOptions = {}): Promise<NewsItem[]> {
    const categories = new Set(options.categories ?? RSS_SOURCES.map((source) => source.category));
    const maxItemsPerSource = options.maxItemsPerSource ?? 15;
    const maxTotalItems = options.maxTotalItems ?? 60;
    const sources = RSS_SOURCES.filter((source) => categories.has(source.category));
    const results = await Promise.allSettled(
        sources.map(async (source) => (await fetchRssSource(source, options.keywords)).slice(0, maxItemsPerSource))
    );

    const seenTitles = new Set<string>();
    const items: NewsItem[] = [];
    for (const result of results) {
        if (result.status !== 'fulfilled') {
            continue;
        }
        for (const item of result.value) {
            const normalized = normalizeTitle(item.title);
            if (!normalized || seenTitles.has(normalized)) {
                continue;
            }
            seenTitles.add(normalized);
            items.push({
                title: item.title,
                source: item.source,
                url: item.link,
                published_at: item.pubDate,
            });
        }
    }

    return items
        .sort((left, right) => Date.parse(right.published_at) - Date.parse(left.published_at))
        .slice(0, maxTotalItems);
}

