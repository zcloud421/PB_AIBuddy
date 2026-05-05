import dotenv from 'dotenv';

import { fetchRssNewsFallback } from '../data/rss-news-fetcher';

dotenv.config();

async function main(): Promise<void> {
    const items = await fetchRssNewsFallback({
        categories: ['central-bank', 'macro'],
        keywords: 'FOMC Fed rate decision',
        maxTotalItems: 80,
    });

    const bySource = items.reduce<Record<string, number>>((acc, item) => {
        acc[item.source] = (acc[item.source] ?? 0) + 1;
        return acc;
    }, {});

    console.log('[rss-test] total:', items.length);
    console.log('[rss-test] bySource:', JSON.stringify(bySource, null, 2));
    console.log('[rss-test] first10:', JSON.stringify(items.slice(0, 10), null, 2));
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    console.error(`[rss-test] failed: ${message}`);
    process.exitCode = 1;
});
