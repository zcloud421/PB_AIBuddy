import axios from 'axios';
import dotenv from 'dotenv';

import { pool } from '../db/client';

dotenv.config();

type Grade = 'GO' | 'CAUTION' | 'AVOID';

export const ALERT_THRESHOLDS = {
    go_count_change_pct: 40,
    go_share_max: 0.25,
    go_share_min: 0.05,
    hard_fail_change_pct: 50,
    unknown_grade_value: true
};

// Phase 5.0 baseline thresholds. Revisit after 5.2b deploys real gates.

interface RunGradeSnapshot {
    run_id: string;
    run_date: string;
    grade_counts: Record<string, number>;
    hard_fail_count: number;
}

export interface GradeDistributionMetrics {
    latest: RunGradeSnapshot;
    history_average: {
        go_count: number;
        hard_fail_count: number;
        run_count: number;
    };
    total_count: number;
    go_share: number;
    go_count_change_pct: number | null;
    hard_fail_change_pct: number | null;
    unknown_grades: string[];
    issues: string[];
}

export interface GradeDistributionCheckResult {
    status: 'ok';
    distribution: GradeDistributionMetrics | null;
    alerts_fired: boolean;
}

const EXPECTED_GRADES = new Set<Grade>(['GO', 'CAUTION', 'AVOID']);

export function computeGradeDistributionMetrics(
    latest: RunGradeSnapshot,
    history: RunGradeSnapshot[]
): GradeDistributionMetrics {
    const totalCount = Object.values(latest.grade_counts).reduce((sum, count) => sum + count, 0);
    const latestGoCount = latest.grade_counts.GO ?? 0;
    const goShare = totalCount > 0 ? latestGoCount / totalCount : 0;
    const historyGoAverage = average(history.map((row) => row.grade_counts.GO ?? 0));
    const historyHardFailAverage = average(history.map((row) => row.hard_fail_count));
    const goCountChangePct = pctChange(latestGoCount, historyGoAverage);
    const hardFailChangePct = pctChange(latest.hard_fail_count, historyHardFailAverage);
    const unknownGrades = Object.keys(latest.grade_counts).filter((grade) => !EXPECTED_GRADES.has(grade as Grade));

    const issues: string[] = [];
    if (goCountChangePct !== null && Math.abs(goCountChangePct) > ALERT_THRESHOLDS.go_count_change_pct) {
        issues.push(`GO count changed ${formatSigned(goCountChangePct)}% vs 7-run avg`);
    }
    if (totalCount > 0 && goShare > ALERT_THRESHOLDS.go_share_max) {
        issues.push(`GO share ${(goShare * 100).toFixed(1)}% > ${(ALERT_THRESHOLDS.go_share_max * 100).toFixed(0)}%`);
    }
    if (totalCount > 0 && goShare < ALERT_THRESHOLDS.go_share_min) {
        issues.push(`GO share ${(goShare * 100).toFixed(1)}% < ${(ALERT_THRESHOLDS.go_share_min * 100).toFixed(0)}%`);
    }
    if (
        hardFailChangePct !== null &&
        Math.abs(hardFailChangePct) > ALERT_THRESHOLDS.hard_fail_change_pct
    ) {
        issues.push(`Hard fail count changed ${formatSigned(hardFailChangePct)}% vs 7-run avg`);
    }
    if (ALERT_THRESHOLDS.unknown_grade_value && unknownGrades.length > 0) {
        issues.push(`Unknown grade value(s): ${unknownGrades.join(', ')}`);
    }

    return {
        latest,
        history_average: {
            go_count: round1(historyGoAverage),
            hard_fail_count: round1(historyHardFailAverage),
            run_count: history.length
        },
        total_count: totalCount,
        go_share: round1(goShare * 100),
        go_count_change_pct: goCountChangePct === null ? null : round1(goCountChangePct),
        hard_fail_change_pct: hardFailChangePct === null ? null : round1(hardFailChangePct),
        unknown_grades: unknownGrades,
        issues
    };
}

export function formatGradeDistributionReport(metrics: GradeDistributionMetrics): string {
    const gradeCounts = ['GO', 'CAUTION', 'AVOID']
        .map((grade) => `• ${grade}: ${metrics.latest.grade_counts[grade] ?? 0}`)
        .join('\n');
    const issues = metrics.issues.length > 0
        ? metrics.issues.map((issue) => `- ${issue}`).join('\n')
        : 'None';

    return [
        `📊 FCN Grade Distribution (${metrics.latest.run_date})`,
        '',
        'Stats:',
        `• Total screened: ${metrics.total_count}`,
        gradeCounts,
        `• GO share: ${metrics.go_share.toFixed(1)}%`,
        `• GO count vs 7-run avg: ${metrics.go_count_change_pct === null ? 'n/a' : `${formatSigned(metrics.go_count_change_pct)}%`}`,
        `• Hard fail count: ${metrics.latest.hard_fail_count}`,
        `• Hard fail vs 7-run avg: ${metrics.hard_fail_change_pct === null ? 'n/a' : `${formatSigned(metrics.hard_fail_change_pct)}%`}`,
        '',
        'Issues:',
        issues,
        '',
        'Action: review latest screener run if any issue is listed.'
    ].join('\n');
}

export async function runGradeDistributionCheck(): Promise<GradeDistributionCheckResult> {
    const latest = await fetchLatestRunSnapshot();
    if (!latest) {
        console.log('[grade-distribution] no completed run found');
        return {
            status: 'ok',
            distribution: null,
            alerts_fired: false
        };
    }

    const history = await fetchHistorySnapshots(latest.run_id);
    const metrics = computeGradeDistributionMetrics(latest, history);
    console.log('[grade-distribution]', JSON.stringify(metrics, null, 2));

    const alertsFired = metrics.issues.length > 0;
    if (alertsFired) {
        await sendTelegramMessage(formatGradeDistributionReport(metrics));
    } else {
        console.log('[grade-distribution] healthy, no alert sent');
    }

    return {
        status: 'ok',
        distribution: metrics,
        alerts_fired: alertsFired
    };
}

async function fetchLatestRunSnapshot(): Promise<RunGradeSnapshot | null> {
    const result = await pool.query<{ run_id: string; run_date: string }>(`
        SELECT run_id, run_date::text AS run_date
        FROM idea_runs
        WHERE status = 'completed'
          AND triggered_by = 'scheduled'::trigger_source
        ORDER BY run_date DESC, completed_at DESC, started_at DESC
        LIMIT 1
    `);
    const latest = result.rows[0];
    return latest ? fetchRunSnapshot(latest.run_id, latest.run_date) : null;
}

async function fetchHistorySnapshots(excludeRunId: string): Promise<RunGradeSnapshot[]> {
    const result = await pool.query<{ run_id: string; run_date: string }>(
        `
        SELECT run_id, run_date::text AS run_date
        FROM idea_runs
        WHERE status = 'completed'
          AND triggered_by = 'scheduled'::trigger_source
          AND run_id <> $1
        ORDER BY run_date DESC, completed_at DESC, started_at DESC
        LIMIT 7
        `,
        [excludeRunId]
    );
    return Promise.all(result.rows.map((row) => fetchRunSnapshot(row.run_id, row.run_date)));
}

async function fetchRunSnapshot(runId: string, runDate: string): Promise<RunGradeSnapshot> {
    const [gradeResult, hardFailResult] = await Promise.all([
        pool.query<{ grade: string; count: string }>(
            `
            SELECT overall_grade::text AS grade, COUNT(*)::text AS count
            FROM idea_candidates
            WHERE run_id = $1
            GROUP BY 1
            `,
            [runId]
        ),
        pool.query<{ count: string }>(
            `
            SELECT COUNT(DISTINCT symbol)::text AS count
            FROM risk_flags
            WHERE run_id = $1
              AND severity::text = 'block'
            `,
            [runId]
        )
    ]);

    return {
        run_id: runId,
        run_date: runDate,
        grade_counts: Object.fromEntries(
            gradeResult.rows.map((row) => [row.grade, parseInt(row.count, 10) || 0])
        ),
        hard_fail_count: parseInt(hardFailResult.rows[0]?.count ?? '0', 10) || 0
    };
}

async function sendTelegramMessage(message: string): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();
    if (!botToken || !chatId) {
        console.log('[grade-distribution] telegram env missing, report not sent');
        return;
    }
    await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        chat_id: chatId,
        text: message
    });
    console.log('[grade-distribution] telegram alert sent');
}

function average(values: number[]): number {
    if (values.length === 0) return 0;
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function pctChange(current: number, baseline: number): number | null {
    if (baseline === 0) {
        return current === 0 ? null : 100;
    }
    return ((current - baseline) / baseline) * 100;
}

function formatSigned(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}`;
}

function round1(value: number): number {
    return Math.round(value * 10) / 10;
}

if (require.main === module) {
    runGradeDistributionCheck()
        .then(() => pool.end())
        .catch((error) => {
            console.error('[grade-distribution] fatal:', error);
            void pool.end();
            process.exit(1);
        });
}
