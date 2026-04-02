import dotenv from 'dotenv';
import axios from 'axios';

import { pool } from '../db/client';
import { getTrackerReview } from '../services/tracker-service';

dotenv.config();

async function main(): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();

    if (!botToken || !chatId) {
        throw new Error('TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required');
    }

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
        formatBucket('Hero', review.hero),
        '',
        formatBucket('Top3', review.recommended),
        '',
        formatWindow(review.recent_expired_window),
        '',
        formatWindow(review.previous_expired_window)
    ];

    return lines.join('\n');
}

function formatWindow(window: Awaited<ReturnType<typeof getTrackerReview>>['recent_expired_window']): string {
    return [
        `${window.label}`,
        formatBucket('整体', window.overall),
        formatBucket('Hero', window.hero),
        formatBucket('Top3', window.recommended)
    ].join('\n');
}

function formatBucket(
    label: string,
    bucket: Awaited<ReturnType<typeof getTrackerReview>>['overall']
): string {
    return `${label}: total ${bucket.total_recommendations} | matured ${bucket.matured_recommendations} | path breach ${bucket.path_breach_rate} | maturity safe ${bucket.maturity_safe_rate}`;
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
