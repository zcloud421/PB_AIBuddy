import dotenv from 'dotenv';
import axios from 'axios';

import { pool } from '../db/client';
import { ensureDrawdownAttributionDecisionsTable } from '../db/queries/ideas';

dotenv.config();

interface BucketAverages {
    fundamental: number;
    valuation: number;
    macro: number;
    positioning: number;
    idiosyncratic: number;
}

interface RejectionSample {
    symbol: string;
    peak_date: string;
    trough_date: string;
    llm_reason: string;
    reason_summary: string;
}

interface HealthMetrics {
    total_episodes: number;
    llm_attempted: number;
    llm_accepted: number;
    rejected_with_output: number;
    acceptance_rate_pct: number;
    avg_confidence: number | null;
    bucket_averages: BucketAverages;
    top_rejection_samples: RejectionSample[];
}

interface AggregateRow {
    total_episodes: string;
    llm_attempted: string;
    llm_accepted: string;
    rejected_with_output: string;
    avg_confidence: string | null;
    avg_fundamental: string | null;
    avg_valuation: string | null;
    avg_macro: string | null;
    avg_positioning: string | null;
    avg_idiosyncratic: string | null;
}

interface RejectionRow {
    symbol: string;
    peak_date: string;
    trough_date: string;
    llm_reason: string | null;
    confidence: string | null;
}

function parseInteger(value: string | number | null | undefined): number {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function parseNullableNumber(value: string | number | null | undefined): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function roundPct(value: number | null): number {
    if (value === null || !Number.isFinite(value)) {
        return 0;
    }

    return Number(value.toFixed(1));
}

function summarizeRejection(confidence: number | null): string {
    if (confidence !== null && confidence < 0.5) {
        return `low confidence ${confidence.toFixed(2)}`;
    }

    return 'validator rejected, likely no news anchor';
}

function truncateForTelegram(text: string, maxLength = 80): string {
    const normalized = text.replace(/\s+/g, ' ').trim();
    if (normalized.length <= maxLength) {
        return normalized;
    }

    return `${normalized.slice(0, maxLength - 1)}…`;
}

async function fetchHealthMetrics(): Promise<HealthMetrics> {
    const aggregateResult = await pool.query<AggregateRow>(`
        WITH base AS (
            SELECT
                decision_json,
                symbol,
                peak_date::text AS peak_date,
                trough_date::text AS trough_date,
                (decision_json->>'llm_accepted')::boolean AS accepted,
                (decision_json->'llm_raw_output'->>'confidence')::float AS confidence,
                decision_json->'llm_raw_output'->>'reason_zh' AS llm_reason,
                decision_json->'llm_raw_output'->'buckets' AS buckets
            FROM drawdown_attribution_decisions
            WHERE created_at >= NOW() - INTERVAL '7 days'
        )
        SELECT
            COUNT(*) AS total_episodes,
            COUNT(*) FILTER (WHERE decision_json->'llm_raw_output' IS NOT NULL) AS llm_attempted,
            COUNT(*) FILTER (WHERE accepted = true) AS llm_accepted,
            COUNT(*) FILTER (WHERE accepted = false AND decision_json->'llm_raw_output' IS NOT NULL) AS rejected_with_output,
            AVG(confidence) FILTER (WHERE accepted = true) AS avg_confidence,
            AVG((buckets->>'fundamental')::float) FILTER (WHERE accepted = true) AS avg_fundamental,
            AVG((buckets->>'valuation')::float) FILTER (WHERE accepted = true) AS avg_valuation,
            AVG((buckets->>'macro')::float) FILTER (WHERE accepted = true) AS avg_macro,
            AVG((buckets->>'positioning')::float) FILTER (WHERE accepted = true) AS avg_positioning,
            AVG((buckets->>'idiosyncratic')::float) FILTER (WHERE accepted = true) AS avg_idiosyncratic
        FROM base
    `);

    const row = aggregateResult.rows[0];
    const totalEpisodes = parseInteger(row?.total_episodes);
    const llmAccepted = parseInteger(row?.llm_accepted);
    const acceptanceRatePct = totalEpisodes > 0 ? (llmAccepted / totalEpisodes) * 100 : 0;

    const rejectionResult = await pool.query<RejectionRow>(`
        SELECT
            symbol,
            peak_date::text AS peak_date,
            trough_date::text AS trough_date,
            decision_json->'llm_raw_output'->>'reason_zh' AS llm_reason,
            decision_json->'llm_raw_output'->>'confidence' AS confidence
        FROM drawdown_attribution_decisions
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND (decision_json->>'llm_accepted')::boolean = false
          AND decision_json->'llm_raw_output' IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 3
    `);

    return {
        total_episodes: totalEpisodes,
        llm_attempted: parseInteger(row?.llm_attempted),
        llm_accepted: llmAccepted,
        rejected_with_output: parseInteger(row?.rejected_with_output),
        acceptance_rate_pct: roundPct(acceptanceRatePct),
        avg_confidence: parseNullableNumber(row?.avg_confidence),
        bucket_averages: {
            fundamental: roundPct(parseNullableNumber(row?.avg_fundamental)),
            valuation: roundPct(parseNullableNumber(row?.avg_valuation)),
            macro: roundPct(parseNullableNumber(row?.avg_macro)),
            positioning: roundPct(parseNullableNumber(row?.avg_positioning)),
            idiosyncratic: roundPct(parseNullableNumber(row?.avg_idiosyncratic))
        },
        top_rejection_samples: rejectionResult.rows.map((sample) => {
            const confidence = parseNullableNumber(sample.confidence);
            return {
                symbol: sample.symbol,
                peak_date: sample.peak_date,
                trough_date: sample.trough_date,
                llm_reason: truncateForTelegram(sample.llm_reason ?? 'N/A'),
                reason_summary: summarizeRejection(confidence)
            };
        })
    };
}

function evaluateAlerts(metrics: HealthMetrics): string[] {
    const issues: string[] = [];
    if (metrics.total_episodes === 0) {
        issues.push('No attribution decisions in last 7 days - pipeline may be down');
        return issues;
    }

    if (metrics.acceptance_rate_pct < 30) {
        issues.push(`acceptance_rate=${metrics.acceptance_rate_pct.toFixed(1)}% < 30% threshold`);
    }
    if (metrics.acceptance_rate_pct > 90) {
        issues.push(`acceptance_rate=${metrics.acceptance_rate_pct.toFixed(1)}% > 90% (validator may be too lenient)`);
    }

    const bucketEntries = Object.entries(metrics.bucket_averages);
    const maxBucket = bucketEntries.reduce((left, right) => (right[1] > left[1] ? right : left));
    if (maxBucket[1] > 60) {
        issues.push(`bucket imbalance: ${maxBucket[0]}=${maxBucket[1].toFixed(0)}% > 60%`);
    }

    const marketStructureTotal = metrics.bucket_averages.macro + metrics.bucket_averages.positioning;
    if (marketStructureTotal < 20 && metrics.llm_accepted >= 10) {
        issues.push(`macro+positioning=${marketStructureTotal.toFixed(0)}% < 20% (market structure data may be unused)`);
    }

    if (metrics.avg_confidence !== null && metrics.avg_confidence < 0.55) {
        issues.push(`avg confidence=${metrics.avg_confidence.toFixed(2)} < 0.55 (acceptance criteria edge-case)`);
    }

    return issues;
}

function formatBucketLine(bucket: BucketAverages): string {
    return `${bucket.fundamental.toFixed(0)}% / ${bucket.valuation.toFixed(0)}% / ${bucket.macro.toFixed(0)}% / ${bucket.positioning.toFixed(0)}% / ${bucket.idiosyncratic.toFixed(0)}%`;
}

function formatReport(metrics: HealthMetrics, issues: string[], forceReport: boolean): string | null {
    if (issues.length === 0 && !forceReport) {
        return null;
    }

    const avgConfidence = metrics.avg_confidence === null ? 'N/A' : metrics.avg_confidence.toFixed(2);
    if (issues.length === 0) {
        return [
            '✅ Drawdown Attribution Health OK (last 7d)',
            `• Total: ${metrics.total_episodes}, accepted ${metrics.acceptance_rate_pct.toFixed(1)}%, avg conf ${avgConfidence}`,
            `• Buckets balanced (fund ${metrics.bucket_averages.fundamental.toFixed(0)}% / val ${metrics.bucket_averages.valuation.toFixed(0)}% / mac ${metrics.bucket_averages.macro.toFixed(0)}% / pos ${metrics.bucket_averages.positioning.toFixed(0)}% / idio ${metrics.bucket_averages.idiosyncratic.toFixed(0)}%)`
        ].join('\n');
    }

    const rejectionLines = metrics.top_rejection_samples.length === 0
        ? ['None']
        : metrics.top_rejection_samples.map(
            (sample, index) =>
                `${index + 1}. SYMBOL=${sample.symbol}, peak=${sample.peak_date}: "${sample.llm_reason}" (${sample.reason_summary})`
        );

    return [
        '⚠️ Drawdown Attribution Health (last 7d)',
        'Issues:',
        ...issues.map((issue) => `- ${issue}`),
        'Stats:',
        `• Total episodes: ${metrics.total_episodes}`,
        `• LLM accepted: ${metrics.llm_accepted} (${metrics.acceptance_rate_pct.toFixed(1)}%)`,
        `• Avg confidence: ${avgConfidence}`,
        `• Rejected with LLM output: ${metrics.rejected_with_output}`,
        `• Bucket avg (fund/val/mac/pos/idio): ${formatBucketLine(metrics.bucket_averages)}`,
        'Top 3 rejection reasons (fact-anchor failures):',
        ...rejectionLines,
        'Action: review prompt or confidence threshold.'
    ].join('\n');
}

async function main(): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();
    if (!botToken || !chatId) {
        throw new Error('TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required');
    }

    // Ensure table exists before querying. The table is normally created lazily by
    // recordDrawdownAttributionDecision() when the first episode is attributed in
    // production, but if the cron fires before any attribution has run (e.g. fresh
    // deploy or low-traffic window), the SELECT below would crash with "relation
    // does not exist". Idempotent — CREATE TABLE IF NOT EXISTS is a noop after first run.
    await ensureDrawdownAttributionDecisionsTable();

    const metrics = await fetchHealthMetrics();
    const issues = evaluateAlerts(metrics);
    const forceReport = process.env.ATTRIB_HEALTH_FORCE_REPORT === 'true';
    const message = formatReport(metrics, issues, forceReport);

    console.log('[attrib-health]', JSON.stringify({ metrics, issues, sent: Boolean(message) }, null, 2));

    if (message) {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
            chat_id: chatId,
            text: message
        });
        console.log('[attrib-health] telegram alert sent');
    } else {
        console.log('[attrib-health] healthy, no alert sent');
    }
}

main()
    .catch((error: unknown) => {
        const message = error instanceof Error ? error.stack ?? error.message : String(error);
        console.error(`[attrib-health] fatal: ${message}`);
        process.exitCode = 1;
    })
    .finally(async () => {
        await pool.end();
    });
