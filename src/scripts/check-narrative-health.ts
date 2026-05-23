import dotenv from 'dotenv';
import axios from 'axios';

import { pool } from '../db/client';
import { ensureSourceQualityColumn } from '../db/queries/ideas';

dotenv.config();

type NarrativeSourceQuality =
    | 'llm_validated'
    | 'llm_retry_validated'
    | 'llm_failed_validation'
    | 'template_fallback'
    | 'blocked';

interface NarrativeMetrics {
    total_narratives: number;
    source_quality_distribution: Record<NarrativeSourceQuality, number> & { unknown: number };
    retry_success_rate_pct: number;
    template_fallback_rate_pct: number;
    persistent_template_tickers: Array<{
        symbol: string;
        days_in_template: number;
    }>;
    issues: string[];
}

interface DistributionRow {
    sq: string;
    cnt: string;
}

interface PersistentTemplateRow {
    symbol: string;
    days_in_template: string;
}

function parseInteger(value: string | number | null | undefined): number {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function pct(count: number, total: number): string {
    if (total <= 0) {
        return '0.0%';
    }
    return `${((count / total) * 100).toFixed(1)}%`;
}

function roundPct(value: number): number {
    return Number(value.toFixed(1));
}

async function computeNarrativeMetrics(): Promise<NarrativeMetrics> {
    await ensureSourceQualityColumn();

    const distRes = await pool.query<DistributionRow>(`
        SELECT
            COALESCE(source_quality, 'unknown') AS sq,
            COUNT(*)::text AS cnt
        FROM idea_candidates
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND why_now IS NOT NULL
          AND why_now <> ''
        GROUP BY 1
    `);

    const persistentRes = await pool.query<PersistentTemplateRow>(`
        SELECT
            symbol,
            COUNT(DISTINCT DATE(created_at))::text AS days_in_template
        FROM idea_candidates
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND source_quality IN ('template_fallback', 'llm_failed_validation', 'blocked')
        GROUP BY symbol
        HAVING COUNT(DISTINCT DATE(created_at)) >= 3
        ORDER BY COUNT(DISTINCT DATE(created_at)) DESC, symbol ASC
        LIMIT 10
    `);

    const dist = {
        llm_validated: 0,
        llm_retry_validated: 0,
        llm_failed_validation: 0,
        template_fallback: 0,
        blocked: 0,
        unknown: 0
    };

    for (const row of distRes.rows) {
        const key = row.sq as keyof typeof dist;
        if (key in dist) {
            dist[key] = parseInteger(row.cnt);
        } else {
            dist.unknown += parseInteger(row.cnt);
        }
    }

    const total = Object.values(dist).reduce((sum, value) => sum + value, 0);
    const retryAttempts = dist.llm_retry_validated + dist.llm_failed_validation;
    const retrySuccessRate = retryAttempts > 0 ? (dist.llm_retry_validated / retryAttempts) * 100 : 0;
    const fallbackTotal = dist.template_fallback + dist.llm_failed_validation + dist.blocked;
    const fallbackRate = total > 0 ? (fallbackTotal / total) * 100 : 0;
    const persistentTemplateTickers = persistentRes.rows.map((row) => ({
        symbol: row.symbol,
        days_in_template: parseInteger(row.days_in_template)
    }));

    const issues: string[] = [];
    if (total === 0) {
        issues.push('No narratives in last 7 days - screener or persistence may be down');
    }
    if (fallbackRate > 20) {
        issues.push(`fallback_rate=${fallbackRate.toFixed(1)}% > 20% threshold`);
    }
    if (retryAttempts > 5 && retrySuccessRate < 50) {
        issues.push(`retry_success_rate=${retrySuccessRate.toFixed(1)}% < 50% — LLM 不可教`);
    }
    if (persistentTemplateTickers.length > 0) {
        issues.push(`${persistentTemplateTickers.length} ticker 持续走 template ≥3 天`);
    }
    if (total > 0 && dist.unknown / total > 0.5) {
        issues.push(`unknown_source_quality=${pct(dist.unknown, total)} — 等待 1-2 个 cron cycle 补齐持久化`);
    }

    return {
        total_narratives: total,
        source_quality_distribution: dist,
        retry_success_rate_pct: roundPct(retrySuccessRate),
        template_fallback_rate_pct: roundPct(fallbackRate),
        persistent_template_tickers: persistentTemplateTickers,
        issues
    };
}

function formatReport(metrics: NarrativeMetrics, forceReport: boolean): string | null {
    const dist = metrics.source_quality_distribution;
    const retryAttempts = dist.llm_retry_validated + dist.llm_failed_validation;
    const fallbackTotal = dist.template_fallback + dist.llm_failed_validation + dist.blocked;

    if (metrics.issues.length === 0 && !forceReport) {
        return null;
    }

    if (metrics.issues.length === 0) {
        return [
            '✅ Narrative Health OK (last 7d)',
            `• Total narratives: ${metrics.total_narratives}`,
            `• LLM validated: ${dist.llm_validated} (${pct(dist.llm_validated, metrics.total_narratives)})`,
            `• Retry validated: ${dist.llm_retry_validated} (${pct(dist.llm_retry_validated, metrics.total_narratives)})`,
            `• Template fallback rate: ${metrics.template_fallback_rate_pct.toFixed(1)}%`
        ].join('\n');
    }

    const persistentLines = metrics.persistent_template_tickers.length === 0
        ? ['• None']
        : metrics.persistent_template_tickers.map((item) => `• ${item.symbol}: ${item.days_in_template} days`);

    const actionTickers = metrics.persistent_template_tickers
        .slice(0, 5)
        .map((item) => item.symbol)
        .join(' / ');
    const action = actionTickers
        ? `Action: review LLM prompt or news pipeline for ${actionTickers}`
        : 'Action: review narrative validator logs and source_quality persistence.';

    return [
        '📊 Narrative Health (last 7d)',
        'Stats:',
        `• Total narratives: ${metrics.total_narratives}`,
        `• LLM validated (first try): ${dist.llm_validated} (${pct(dist.llm_validated, metrics.total_narratives)})`,
        `• LLM retry validated: ${dist.llm_retry_validated} (${pct(dist.llm_retry_validated, metrics.total_narratives)})`,
        `• LLM failed → template: ${dist.llm_failed_validation} (${pct(dist.llm_failed_validation, metrics.total_narratives)})`,
        `• Template only (sparse): ${dist.template_fallback} (${pct(dist.template_fallback, metrics.total_narratives)})`,
        `• Blocked: ${dist.blocked} (${pct(dist.blocked, metrics.total_narratives)})`,
        `• Unknown source_quality: ${dist.unknown} (${pct(dist.unknown, metrics.total_narratives)})`,
        `• Retry success rate: ${dist.llm_retry_validated}/${retryAttempts} = ${metrics.retry_success_rate_pct.toFixed(1)}%`,
        `• Template fallback rate: ${fallbackTotal}/${metrics.total_narratives} = ${metrics.template_fallback_rate_pct.toFixed(1)}%`,
        'Persistent template tickers (≥3 days):',
        ...persistentLines,
        'Issues:',
        ...metrics.issues.map((issue) => `- ${issue}`),
        action
    ].join('\n');
}

async function sendTelegramMessage(message: string): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();
    if (!botToken || !chatId) {
        console.log('[narrative-health] telegram env missing, report not sent');
        return;
    }

    await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        chat_id: chatId,
        text: message
    });
    console.log('[narrative-health] telegram alert sent');
}

export async function runNarrativeHealthCheck(): Promise<NarrativeMetrics> {
    const metrics = await computeNarrativeMetrics();
    const forceReport = process.env.NARRATIVE_HEALTH_FORCE_REPORT === 'true';
    const message = formatReport(metrics, forceReport);

    console.log('[narrative-health]', JSON.stringify({ metrics, sent: Boolean(message) }, null, 2));

    if (message) {
        await sendTelegramMessage(message);
    } else {
        console.log('[narrative-health] healthy, no alert sent');
    }

    return metrics;
}

if (require.main === module) {
    runNarrativeHealthCheck()
        .catch((error: unknown) => {
            const message = error instanceof Error ? error.stack ?? error.message : String(error);
            console.error(`[narrative-health] fatal: ${message}`);
            process.exitCode = 1;
        })
        .finally(async () => {
            await pool.end();
        });
}
