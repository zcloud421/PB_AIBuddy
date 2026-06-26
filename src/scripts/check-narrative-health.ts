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
    | 'blocked'
    | 'deterministic'
    | 'go_pitch_llm_validated'
    | 'go_pitch_hybrid_validated'
    | 'go_pitch_template'
    | 'go_pitch_minimal'
    | 'caution_pitch_hybrid_validated'
    | 'caution_pitch_template'
    | 'avoid_pitch_deterministic';

interface NarrativeMetrics {
    total_narratives: number;
    source_quality_distribution: Record<NarrativeSourceQuality, number> & { unknown: number };
    retry_success_rate_pct: number;
    template_fallback_rate_pct: number;
    persistent_template_tickers: Array<{
        symbol: string;
        days_in_template: number;
        source_qualities: string;
        grades: string;
        missing_strike: boolean;
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
    source_qualities: string | null;
    grades: string | null;
    missing_strike: boolean | null;
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

    // 计数口径:只要引擎跑过(source_quality 非空)就算一条 narrative,包含
    // 'blocked'(why_now 为空但确实生成了结果)。早先按 why_now<>'' 过滤会把 blocked
    // 整类隐藏,导致 fallback rate 显示 0% 却同时 flag ticker —— 自相矛盾。
    const distRes = await pool.query<DistributionRow>(`
        SELECT
            COALESCE(ic.source_quality, 'unknown') AS sq,
            COUNT(*)::text AS cnt
        FROM idea_candidates ic
        JOIN idea_runs r ON r.run_id = ic.run_id
        WHERE ic.created_at >= NOW() - INTERVAL '7 days'
          AND r.triggered_by = 'scheduled'::trigger_source
          AND ic.source_quality IS NOT NULL
        GROUP BY 1
    `);

    const persistentRes = await pool.query<PersistentTemplateRow>(`
        SELECT
            ic.symbol,
            COUNT(DISTINCT DATE(ic.created_at))::text AS days_in_template,
            string_agg(DISTINCT ic.source_quality, '/' ORDER BY ic.source_quality) AS source_qualities,
            string_agg(DISTINCT COALESCE(ic.overall_grade::text, '?'), '/' ORDER BY COALESCE(ic.overall_grade::text, '?')) AS grades,
            bool_or(ic.recommended_strike IS NULL OR ic.recommended_strike <= 0) AS missing_strike
        FROM idea_candidates ic
        JOIN idea_runs r ON r.run_id = ic.run_id
        WHERE ic.created_at >= NOW() - INTERVAL '7 days'
          AND r.triggered_by = 'scheduled'::trigger_source
          AND ic.source_quality IN ('template_fallback', 'llm_failed_validation', 'blocked')
        GROUP BY ic.symbol
        HAVING COUNT(DISTINCT DATE(ic.created_at)) >= 3
        ORDER BY COUNT(DISTINCT DATE(ic.created_at)) DESC, ic.symbol ASC
        LIMIT 10
    `);

    const dist = {
        llm_validated: 0,
        llm_retry_validated: 0,
        llm_failed_validation: 0,
        template_fallback: 0,
        blocked: 0,
        deterministic: 0,
        go_pitch_llm_validated: 0,
        go_pitch_hybrid_validated: 0,
        go_pitch_template: 0,
        go_pitch_minimal: 0,
        caution_pitch_hybrid_validated: 0,
        caution_pitch_template: 0,
        avoid_pitch_deterministic: 0,
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
    // fallback rate 只衡量引擎失败(template_fallback + llm_failed);blocked 是"无可卖
    // 结构"的合理拒绝,单独列示,不计入失败率,避免 AVOID 多的日子误触 >20% 告警。
    const fallbackTotal = dist.template_fallback + dist.llm_failed_validation;
    const fallbackRate = total > 0 ? (fallbackTotal / total) * 100 : 0;
    const persistentTemplateTickers = persistentRes.rows.map((row) => ({
        symbol: row.symbol,
        days_in_template: parseInteger(row.days_in_template),
        source_qualities: row.source_qualities ?? 'unknown',
        grades: row.grades ?? '?',
        missing_strike: Boolean(row.missing_strike)
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
    // 只有 template_fallback / llm_failed 才升级为 issue(触发告警);blocked-only
    // 多为 AVOID / 财报窗口无 strike 的预期行为,记录在正文但不刷告警。
    const engineFailPersistent = persistentTemplateTickers.filter((t) =>
        /template_fallback|llm_failed_validation/.test(t.source_qualities)
    );
    if (engineFailPersistent.length > 0) {
        issues.push(`${engineFailPersistent.length} ticker 引擎失败持续走 template ≥3 天`);
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
    const fallbackTotal = dist.template_fallback + dist.llm_failed_validation;

    if (metrics.issues.length === 0 && !forceReport) {
        return null;
    }

    if (metrics.issues.length === 0) {
        return [
            '✅ Narrative Health OK (last 7d)',
            `• Total narratives: ${metrics.total_narratives}`,
            `• LLM validated: ${dist.llm_validated} (${pct(dist.llm_validated, metrics.total_narratives)})`,
            `• Retry validated: ${dist.llm_retry_validated} (${pct(dist.llm_retry_validated, metrics.total_narratives)})`,
            `• GO hybrid validated: ${dist.go_pitch_hybrid_validated} (${pct(dist.go_pitch_hybrid_validated, metrics.total_narratives)})`,
            `• CAUTION hybrid/template: ${dist.caution_pitch_hybrid_validated}/${dist.caution_pitch_template}`,
            `• AVOID deterministic: ${dist.avoid_pitch_deterministic}`,
            `• Template fallback rate: ${metrics.template_fallback_rate_pct.toFixed(1)}%`
        ].join('\n');
    }

    const persistentLines = metrics.persistent_template_tickers.length === 0
        ? ['• None']
        : metrics.persistent_template_tickers.map((item) => {
            const tags = [item.grades, item.source_qualities];
            if (item.missing_strike) tags.push('无strike');
            return `• ${item.symbol}: ${item.days_in_template} days (${tags.join(', ')})`;
        });

    // 区分两类 root cause:blocked + 无strike 多为 AVOID/财报窗口的预期行为(无可卖 put);
    // template_fallback / llm_failed 才是引擎或新闻管道真出问题,需要查 prompt/news。
    const engineFailTickers = metrics.persistent_template_tickers
        .filter((item) => /template_fallback|llm_failed_validation/.test(item.source_qualities))
        .map((item) => item.symbol);
    const blockedOnly = metrics.persistent_template_tickers
        .filter((item) => item.source_qualities === 'blocked')
        .map((item) => item.symbol);
    const actionParts: string[] = [];
    if (engineFailTickers.length > 0) {
        actionParts.push(`查 prompt/news 管道:${engineFailTickers.slice(0, 5).join(' / ')}`);
    }
    if (blockedOnly.length > 0) {
        actionParts.push(`${blockedOnly.slice(0, 5).join(' / ')} 为 blocked(无 strike/价格,多见于 AVOID 或财报窗口),通常为预期`);
    }
    const action = actionParts.length > 0
        ? `Action: ${actionParts.join(';')}`
        : 'Action: review narrative validator logs and source_quality persistence.';

    return [
        '📊 Narrative Health (last 7d)',
        'Stats:',
        `• Total narratives: ${metrics.total_narratives}`,
        `• LLM validated (first try): ${dist.llm_validated} (${pct(dist.llm_validated, metrics.total_narratives)})`,
        `• LLM retry validated: ${dist.llm_retry_validated} (${pct(dist.llm_retry_validated, metrics.total_narratives)})`,
        `• GO hybrid validated: ${dist.go_pitch_hybrid_validated} (${pct(dist.go_pitch_hybrid_validated, metrics.total_narratives)})`,
        `• CAUTION hybrid validated: ${dist.caution_pitch_hybrid_validated} (${pct(dist.caution_pitch_hybrid_validated, metrics.total_narratives)})`,
        `• CAUTION template: ${dist.caution_pitch_template} (${pct(dist.caution_pitch_template, metrics.total_narratives)})`,
        `• AVOID deterministic: ${dist.avoid_pitch_deterministic} (${pct(dist.avoid_pitch_deterministic, metrics.total_narratives)})`,
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
