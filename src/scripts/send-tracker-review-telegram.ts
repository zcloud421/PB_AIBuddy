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
    const topSymbolsText =
        review.concentration.top_symbols.length === 0
            ? '无'
            : review.concentration.top_symbols.map((item) => `${item.symbol}(${item.appearances}次)`).join('、');
    const topHeroText = review.concentration.top_hero_symbol
        ? `${review.concentration.top_hero_symbol.symbol}(${review.concentration.top_hero_symbol.appearances}次)`
        : '无';
    const lines = [
        'FCN 推荐系统月度回顾',
        '',
        `回顾月份：${review.report_month}（对比 ${review.baseline_month}）`,
        '',
        `新增推荐：${review.summary.new_recommendations.count} 笔`,
        `期间跌破执行价：${review.summary.path_breach.count} 笔，比例 ${review.summary.path_breach.rate}（较上月 ${review.comparison.path_breach_rate_change_pct}）`,
        `截至报告日仍低于执行价：${review.summary.still_below_strike.count} 笔，比例 ${review.summary.still_below_strike.rate}（较上月 ${review.comparison.still_below_strike_rate_change_pct}）`,
        `已到期3个月期限安全：${review.summary.matured_3m_safe.count} 笔，安全率 ${review.summary.matured_3m_safe.rate}（较上月 ${review.comparison.matured_3m_safe_rate_change_pct}）`,
        '',
        `推荐集中度：本月共涉及 ${review.concentration.unique_symbols} 个标的`,
        `出现次数最多：${topSymbolsText}`,
        `Hero 最多：${topHeroText}`,
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
        formatBucket(window.summary)
    ].join('\n');
}

function formatBucket(bucket: Awaited<ReturnType<typeof getTrackerReview>>['rolling_30d']['summary']): string {
    return `样本 ${bucket.total_recommendations} 笔 | 期间跌破 ${bucket.path_breach_rate} | 当前仍低于执行价 ${bucket.active_below_strike_rate} | 已到期3个月安全率 ${bucket.maturity_safe_rate}`;
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
