import dotenv from 'dotenv';
import axios from 'axios';

import { pool } from '../db/client';
import { ensureDailyRecommendationHistoryTable, ensureRecommendationTrackerTable } from '../db/queries/ideas';
import { getTrackerReview } from '../services/tracker-service';

dotenv.config();

async function main(): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();

    if (!botToken || !chatId) {
        throw new Error('TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required');
    }

    await ensureRecommendationTrackerTable();
    await ensureDailyRecommendationHistoryTable();

    const review = await getTrackerReview();
    const message = formatReviewMessage(review);

    await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        chat_id: chatId,
        text: message
    });

    console.log('[tracker-review] telegram message sent');
}

function formatReviewMessage(review: Awaited<ReturnType<typeof getTrackerReview>>): string {
    const lines = [
        'FCN 推荐系统 Review',
        '',
        formatBucket('整体', review.overall),
        '',
        formatWindow(review.rolling_30d),
        '',
        formatWindow(review.rolling_90d),
        '',
        formatWindow(review.matured_3m)
    ];

    return lines.join('\n');
}

function formatWindow(window: Awaited<ReturnType<typeof getTrackerReview>>['rolling_30d']): string {
    return [
        `${window.label}`,
        formatBucket('汇总', window.summary)
    ].join('\n');
}

function formatBucket(
    label: string,
    bucket: Awaited<ReturnType<typeof getTrackerReview>>['overall']
): string {
    return `${label}: total ${bucket.total_recommendations} | matured ${bucket.matured_recommendations} | path breach ${bucket.path_breach_rate} | active below strike ${bucket.active_below_strike_rate} | maturity safe ${bucket.maturity_safe_rate}`;
}

main()
    .catch((error: unknown) => {
        const message = error instanceof Error ? error.stack ?? error.message : String(error);
        console.error(`[tracker-review] failed: ${message}`);
        process.exitCode = 1;
    })
    .finally(async () => {
        await pool.end();
    });
