import axios from 'axios';
import dotenv from 'dotenv';

import { pool } from '../db/client';
import { MassiveDataFetcher, type DailyPriceBar } from '../data/massive-fetcher';
import {
    ensureMacroRegimeAuditLogTable,
    fetchPendingMacroRegimeAuditRows,
    fetchRecentMacroRegimeAuditRows,
    type MacroRegimeAuditLogRow,
    updateMacroRegimeAuditForwardResult
} from '../db/queries/macro-regime';
import type { CreditRegimeState } from '../services/macro-regime/types';

dotenv.config();

const DEFAULT_HORIZON_TRADING_DAYS = 21;
const DEFAULT_PROXY = 'QQQ';
const STRESS_DRAWDOWN_THRESHOLD_PCT = 5;
const STRESS_REALIZED_VOL_THRESHOLD_PCT = 25;
const FP_CLUSTER_COUNT_THRESHOLD = 2;
const FP_CLUSTER_DAYS = 60;

export interface ForwardStressMetrics {
    start_date: string;
    end_date: string;
    trading_days: number;
    max_drawdown_pct: number;
    realized_vol_pct: number;
    stress_detected: boolean;
}

export type ForwardEvaluation = MacroRegimeAuditLogRow['forward_evaluation'];

export interface MacroCreditModelHealthMetrics {
    pending_checked: number;
    updated: number;
    insufficient_data: number;
    recent_evaluated: number;
    recent_false_positives: number;
    recent_true_positives: number;
    fp_cluster_alert: boolean;
    issues: string[];
    updated_rows: Array<{
        id: number;
        as_of: string;
        state: CreditRegimeState;
        evaluation: ForwardEvaluation;
        max_drawdown_pct: number | null;
        realized_vol_pct: number | null;
    }>;
}

export interface MacroCreditModelHealthResult {
    status: 'ok';
    metrics: MacroCreditModelHealthMetrics;
    alerts_fired: boolean;
}

export function computeForwardStressMetrics(
    bars: DailyPriceBar[],
    asOf: string,
    horizonTradingDays = DEFAULT_HORIZON_TRADING_DAYS,
    thresholds = {
        drawdown_pct: STRESS_DRAWDOWN_THRESHOLD_PCT,
        realized_vol_pct: STRESS_REALIZED_VOL_THRESHOLD_PCT
    }
): ForwardStressMetrics | null {
    const sorted = [...bars].sort((a, b) => a.date.localeCompare(b.date));
    const startIndex = sorted.findIndex((bar) => bar.date >= asOf);
    if (startIndex < 0) return null;

    const window = sorted.slice(startIndex, startIndex + horizonTradingDays + 1);
    if (window.length < horizonTradingDays + 1) return null;

    const startClose = window[0].close;
    if (!Number.isFinite(startClose) || startClose <= 0) return null;

    const forwardBars = window.slice(1);
    const worstLow = Math.min(...forwardBars.map((bar) => Number.isFinite(bar.low) ? bar.low : bar.close));
    const maxDrawdownPct = Math.max(0, (1 - worstLow / startClose) * 100);
    const closes = window.map((bar) => bar.close).filter((close) => Number.isFinite(close) && close > 0);
    if (closes.length < 2) return null;

    const returns: number[] = [];
    for (let i = 1; i < closes.length; i += 1) {
        returns.push(Math.log(closes[i] / closes[i - 1]));
    }
    const realizedVolPct = standardDeviation(returns) * Math.sqrt(252) * 100;

    return {
        start_date: window[0].date,
        end_date: window[window.length - 1].date,
        trading_days: forwardBars.length,
        max_drawdown_pct: round1(maxDrawdownPct),
        realized_vol_pct: round1(realizedVolPct),
        stress_detected:
            maxDrawdownPct >= thresholds.drawdown_pct ||
            realizedVolPct >= thresholds.realized_vol_pct
    };
}

export function classifyForwardEvaluation(
    creditRegimeState: CreditRegimeState,
    stressDetected: boolean
): ForwardEvaluation {
    const warned = creditRegimeState === 'BREAK_FORMING' || creditRegimeState === 'BREAK';
    if (warned && stressDetected) return 'TP';
    if (warned && !stressDetected) return 'FP';
    if (!warned && stressDetected) return 'FN';
    return 'TN';
}

export function computeMacroCreditModelHealthMetrics(input: {
    pending_rows: MacroRegimeAuditLogRow[];
    recent_rows: MacroRegimeAuditLogRow[];
    updated_rows: MacroCreditModelHealthMetrics['updated_rows'];
    insufficient_data: number;
}): MacroCreditModelHealthMetrics {
    const recentEvaluated = input.recent_rows.filter((row) => row.forward_evaluation !== 'PENDING');
    const recentFalsePositives = recentEvaluated.filter((row) => row.forward_evaluation === 'FP').length;
    const recentTruePositives = recentEvaluated.filter((row) => row.forward_evaluation === 'TP').length;
    const fpClusterAlert = recentFalsePositives >= FP_CLUSTER_COUNT_THRESHOLD;
    const issues = fpClusterAlert
        ? [
            `macro credit false positives clustered: ${recentFalsePositives} FP in last ${FP_CLUSTER_DAYS}d`
        ]
        : [];

    return {
        pending_checked: input.pending_rows.length,
        updated: input.updated_rows.length,
        insufficient_data: input.insufficient_data,
        recent_evaluated: recentEvaluated.length,
        recent_false_positives: recentFalsePositives,
        recent_true_positives: recentTruePositives,
        fp_cluster_alert: fpClusterAlert,
        issues,
        updated_rows: input.updated_rows
    };
}

export function formatMacroCreditModelHealthReport(metrics: MacroCreditModelHealthMetrics): string {
    const issues = metrics.issues.length > 0
        ? metrics.issues.map((issue) => `- ${issue}`).join('\n')
        : 'None';
    const updatedRows = metrics.updated_rows.length > 0
        ? metrics.updated_rows
            .slice(0, 8)
            .map((row) =>
                `• ${row.as_of} ${row.state}: ${row.evaluation}` +
                ` (DD ${formatNullablePct(row.max_drawdown_pct)}, RV ${formatNullablePct(row.realized_vol_pct)})`
            )
            .join('\n')
        : '• none';

    return [
        `📊 Macro Credit Model Health (${new Date().toISOString().slice(0, 10)})`,
        '',
        'Stats:',
        `• Pending checked: ${metrics.pending_checked}`,
        `• Newly evaluated: ${metrics.updated}`,
        `• Insufficient data: ${metrics.insufficient_data}`,
        `• Recent evaluated: ${metrics.recent_evaluated}`,
        `• TP / FP: ${metrics.recent_true_positives} / ${metrics.recent_false_positives}`,
        '',
        'New evaluations:',
        updatedRows,
        '',
        'Issues:',
        issues,
        '',
        'Action: advisory only; do not auto-downgrade macro model.'
    ].join('\n');
}

export async function runMacroCreditModelHealthCheck(): Promise<MacroCreditModelHealthResult> {
    await ensureMacroRegimeAuditLogTable();
    const pendingRows = await fetchPendingMacroRegimeAuditRows(100);
    const fetcher = new MassiveDataFetcher();
    const barsByProxy = new Map<string, DailyPriceBar[]>();
    const updatedRows: MacroCreditModelHealthMetrics['updated_rows'] = [];
    let insufficientData = 0;

    for (const row of pendingRows) {
        const proxy = row.forward_proxy || DEFAULT_PROXY;
        if (!barsByProxy.has(proxy)) {
            barsByProxy.set(proxy, await fetcher.fetchPriceHistory(proxy, 420).catch(() => []));
        }
        const metrics = computeForwardStressMetrics(
            barsByProxy.get(proxy) ?? [],
            row.as_of,
            row.forward_horizon_days || DEFAULT_HORIZON_TRADING_DAYS
        );

        if (!metrics) {
            insufficientData += 1;
            continue;
        }

        const evaluation = classifyForwardEvaluation(row.credit_regime_state, metrics.stress_detected);
        await updateMacroRegimeAuditForwardResult({
            id: row.id,
            forward_max_drawdown_pct: metrics.max_drawdown_pct,
            forward_realized_vol_pct: metrics.realized_vol_pct,
            forward_stress_detected: metrics.stress_detected,
            forward_evaluation: evaluation
        });
        updatedRows.push({
            id: row.id,
            as_of: row.as_of,
            state: row.credit_regime_state,
            evaluation,
            max_drawdown_pct: metrics.max_drawdown_pct,
            realized_vol_pct: metrics.realized_vol_pct
        });
    }

    const recentRows = await fetchRecentMacroRegimeAuditRows(FP_CLUSTER_DAYS);
    const metrics = computeMacroCreditModelHealthMetrics({
        pending_rows: pendingRows,
        recent_rows: recentRows,
        updated_rows: updatedRows,
        insufficient_data: insufficientData
    });
    console.log('[macro-credit-model-health]', JSON.stringify(metrics, null, 2));

    if (metrics.fp_cluster_alert) {
        await sendTelegramMessage(formatMacroCreditModelHealthReport(metrics));
    }

    return {
        status: 'ok',
        metrics,
        alerts_fired: metrics.fp_cluster_alert
    };
}

function standardDeviation(values: number[]): number {
    if (values.length <= 1) return 0;
    const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
    const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (values.length - 1);
    return Math.sqrt(variance);
}

function round1(value: number): number {
    return Math.round(value * 10) / 10;
}

function formatNullablePct(value: number | null): string {
    return typeof value === 'number' ? `${value.toFixed(1)}%` : 'n/a';
}

async function sendTelegramMessage(message: string): Promise<void> {
    const botToken = process.env.TELEGRAM_BOT_TOKEN?.trim();
    const chatId = process.env.TELEGRAM_CHAT_ID?.trim();
    if (!botToken || !chatId) {
        console.log('[macro-credit-model-health] telegram env missing, report not sent');
        return;
    }
    await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        chat_id: chatId,
        text: message
    });
    console.log('[macro-credit-model-health] telegram report sent');
}

if (require.main === module) {
    runMacroCreditModelHealthCheck()
        .then(() => pool.end())
        .catch(async (error) => {
            console.error('[macro-credit-model-health] failed:', error);
            await pool.end();
            process.exitCode = 1;
        });
}
