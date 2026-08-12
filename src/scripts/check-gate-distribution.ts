import axios from 'axios';
import dotenv from 'dotenv';

import { pool } from '../db/client';
import type { GateDecision, GateDecisionType } from '../utils/fcn-gates/types';
import {
    getHealthTelegramEnableEnv,
    isHealthTelegramEnabled
} from '../utils/health-notification-policy';

dotenv.config();

export interface GateDistributionRow {
    symbol: string;
    gate_decisions: GateDecision[] | null;
    overall_grade: string;
}

export interface GateDistributionMetrics {
    run_id: string;
    run_date: string;
    total_symbols: number;
    type_counts: Array<{
        type: GateDecisionType | string;
        count: number;
        symbols: string[];
    }>;
    grade_drop_reasons: Array<{
        symbol: string;
        reasons: string[];
    }>;
}

export interface GateDistributionCheckResult {
    status: 'ok';
    distribution: GateDistributionMetrics | null;
    alerts_fired: boolean;
}

export function computeGateDistributionMetrics(input: {
    run_id: string;
    run_date: string;
    rows: GateDistributionRow[];
}): GateDistributionMetrics {
    const byType = new Map<string, Set<string>>();
    const gradeDropReasons: GateDistributionMetrics['grade_drop_reasons'] = [];

    for (const row of input.rows) {
        const decisions = Array.isArray(row.gate_decisions) ? row.gate_decisions : [];
        const nonInfoReasons = decisions
            .filter((decision) => decision.severity !== 'INFO')
            .map((decision) => decision.type);

        if (nonInfoReasons.length > 0 && row.overall_grade !== 'GO') {
            gradeDropReasons.push({
                symbol: row.symbol,
                reasons: [...new Set(nonInfoReasons)]
            });
        }

        for (const decision of decisions) {
            const existing = byType.get(decision.type) ?? new Set<string>();
            existing.add(row.symbol);
            byType.set(decision.type, existing);
        }
    }

    return {
        run_id: input.run_id,
        run_date: input.run_date,
        total_symbols: input.rows.length,
        type_counts: [...byType.entries()]
            .map(([type, symbols]) => ({
                type,
                count: symbols.size,
                symbols: [...symbols].sort()
            }))
            .sort((a, b) => b.count - a.count || a.type.localeCompare(b.type)),
        grade_drop_reasons: gradeDropReasons.slice(0, 20)
    };
}

export function formatGateDistributionReport(metrics: GateDistributionMetrics): string {
    const gateRows = metrics.type_counts.length > 0
        ? metrics.type_counts
            .map((row) => `| ${row.type} | ${row.count} | ${row.symbols.slice(0, 8).join(', ')}${row.symbols.length > 8 ? '...' : ''} |`)
            .join('\n')
        : '| none | 0 | — |';
    const dropRows = metrics.grade_drop_reasons.length > 0
        ? metrics.grade_drop_reasons
            .map((row) => `| ${row.symbol} | ${row.reasons.join(', ')} |`)
            .join('\n')
        : '| none | — |';

    return [
        `## Gate Distribution (${metrics.run_date})`,
        '',
        `Run: ${metrics.run_id}`,
        `Symbols: ${metrics.total_symbols}`,
        '',
        '| type | count | symbols |',
        '|---|---:|---|',
        gateRows,
        '',
        '## Top reasons for GO -> CAUTION/AVOID drop',
        '',
        '| symbol | reasons |',
        '|---|---|',
        dropRows
    ].join('\n');
}

export async function runGateDistributionCheck(): Promise<GateDistributionCheckResult> {
    await ensureGateDecisionColumns();
    const latest = await pool.query<{ run_id: string; run_date: string }>(`
        SELECT run_id, run_date::text AS run_date
        FROM idea_runs
        WHERE status = 'completed'
          AND triggered_by = 'scheduled'::trigger_source
        ORDER BY run_date DESC, completed_at DESC, started_at DESC
        LIMIT 1
    `);
    const run = latest.rows[0];
    if (!run) {
        console.log('[gate-distribution] no completed run found');
        return {
            status: 'ok',
            distribution: null,
            alerts_fired: false
        };
    }

    const rows = await pool.query<GateDistributionRow>(
        `
        SELECT symbol, overall_grade::text AS overall_grade, gate_decisions
        FROM idea_candidates
        WHERE run_id = $1
        ORDER BY symbol ASC
        `,
        [run.run_id]
    );
    const metrics = computeGateDistributionMetrics({
        run_id: run.run_id,
        run_date: run.run_date,
        rows: rows.rows
    });
    const report = formatGateDistributionReport(metrics);
    console.log(report);
    const telegramEnabled = isHealthTelegramEnabled('gate_distribution');
    if (telegramEnabled) {
        await sendTelegramMessage(report);
    } else {
        console.log(
            `[gate-distribution] report logged only; set ${getHealthTelegramEnableEnv('gate_distribution')}=true to enable Telegram`
        );
    }
    return {
        status: 'ok',
        distribution: metrics,
        alerts_fired: telegramEnabled
    };
}

async function ensureGateDecisionColumns(): Promise<void> {
    await pool.query(`
        ALTER TABLE idea_candidates
        ADD COLUMN IF NOT EXISTS gate_decisions JSONB DEFAULT '[]'::jsonb,
        ADD COLUMN IF NOT EXISTS shadow_grade TEXT,
        ADD COLUMN IF NOT EXISTS engine_mode TEXT NOT NULL DEFAULT 'gated_shadow'
    `);
}

async function sendTelegramMessage(message: string): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();
    if (!botToken || !chatId) {
        console.log('[gate-distribution] telegram env missing, report not sent');
        return;
    }
    await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        chat_id: chatId,
        text: message
    });
    console.log('[gate-distribution] telegram report sent');
}

if (require.main === module) {
    runGateDistributionCheck()
        .then(() => pool.end())
        .catch(async (error) => {
            console.error('[gate-distribution] failed:', error);
            await pool.end();
            process.exitCode = 1;
        });
}
