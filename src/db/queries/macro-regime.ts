/**
 * Postgres persistence for Macro Regime daily snapshots.
 *
 * Single-table design: each daily run writes one row keyed on run_date.
 * Mobile reads "latest" via `getLatestMacroRegimeSnapshot()`.
 */

import { pool } from '../client';
import type { CreditRegimeState, MacroRegimeSnapshot, RegimeSeverity } from '../../services/macro-regime/types';

export async function ensureMacroRegimeSnapshotsTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS macro_regime_snapshots (
            run_date     DATE PRIMARY KEY,
            snapshot_json JSONB NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_macro_regime_snapshots_created_at
        ON macro_regime_snapshots (created_at DESC)
    `);
}

export async function ensureMacroRegimeAuditLogTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS macro_regime_audit_log (
            id BIGSERIAL PRIMARY KEY,
            as_of DATE NOT NULL,
            credit_regime_state TEXT NOT NULL,
            leading_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
            overall_severity TEXT NOT NULL,
            base_overall_severity TEXT,
            credit_sub_scores JSONB NOT NULL,
            forward_horizon_days INTEGER NOT NULL DEFAULT 21,
            forward_proxy TEXT NOT NULL DEFAULT 'QQQ',
            forward_max_drawdown_pct NUMERIC,
            forward_realized_vol_pct NUMERIC,
            forward_stress_detected BOOLEAN,
            forward_evaluation TEXT NOT NULL DEFAULT 'PENDING',
            evaluated_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_macro_regime_audit_log_as_of
        ON macro_regime_audit_log (as_of DESC, created_at DESC)
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_macro_regime_audit_log_evaluation
        ON macro_regime_audit_log (forward_evaluation, as_of DESC)
    `);
}

export async function ensureLateCyclePillarHistoryTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS late_cycle_pillar_history (
            id BIGSERIAL PRIMARY KEY,
            reviewed_at TIMESTAMPTZ NOT NULL,
            pillar TEXT NOT NULL,
            old_state TEXT NOT NULL,
            new_state TEXT NOT NULL,
            evidence_snapshot JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_late_cycle_pillar_history_reviewed_at
        ON late_cycle_pillar_history (reviewed_at DESC)
    `);
}

export async function ensureIndicatorPersistenceTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS indicator_persistence (
            indicator_key       TEXT PRIMARY KEY,
            current_severity    TEXT NOT NULL,
            consecutive_days    INTEGER NOT NULL DEFAULT 1,
            severity_started_at DATE NOT NULL,
            last_seen_date      DATE NOT NULL,
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_indicator_persistence_severity
        ON indicator_persistence (current_severity)
    `);
    await pool.query(`
        ALTER TABLE indicator_persistence
        ADD COLUMN IF NOT EXISTS pending_upgrade_severity TEXT,
        ADD COLUMN IF NOT EXISTS pending_upgrade_start_date DATE
    `);
}

export async function ensureIndicatorHistoryTable(): Promise<void> {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS indicator_history (
            indicator_id   TEXT NOT NULL,
            snapshot_date  DATE NOT NULL,
            raw_value      DOUBLE PRECISION NOT NULL,
            severity       TEXT NOT NULL,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (indicator_id, snapshot_date)
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_indicator_history_id_date
        ON indicator_history (indicator_id, snapshot_date DESC)
    `);
}

export interface IndicatorHistoryPoint {
    indicator_id: string;
    snapshot_date: string;
    raw_value: number;
    severity: RegimeSeverity;
}

export async function fetchIndicatorHistory(
    indicatorId: string,
    beforeDate: string,
    daysBack = 7
): Promise<IndicatorHistoryPoint[]> {
    await ensureIndicatorHistoryTable();
    const result = await pool.query<{
        indicator_id: string;
        snapshot_date: string;
        raw_value: number;
        severity: RegimeSeverity;
    }>(
        `
        SELECT
            indicator_id,
            TO_CHAR(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
            raw_value,
            severity
        FROM indicator_history
        WHERE indicator_id = $1
          AND snapshot_date < $2::date
          AND snapshot_date >= $2::date - ($3::int * INTERVAL '1 day')
        ORDER BY snapshot_date ASC
        `,
        [indicatorId, beforeDate, daysBack]
    );
    return result.rows;
}

export async function upsertIndicatorHistory(input: {
    indicator_id: string;
    snapshot_date: string;
    raw_value: number;
    severity: RegimeSeverity;
}): Promise<void> {
    await ensureIndicatorHistoryTable();
    await pool.query(
        `
        INSERT INTO indicator_history (
            indicator_id,
            snapshot_date,
            raw_value,
            severity
        )
        VALUES ($1, $2::date, $3, $4)
        ON CONFLICT (indicator_id, snapshot_date)
        DO UPDATE SET
            raw_value = EXCLUDED.raw_value,
            severity = EXCLUDED.severity,
            created_at = NOW()
        `,
        [input.indicator_id, input.snapshot_date, input.raw_value, input.severity]
    );
}

export async function upsertMacroRegimeSnapshot(snapshot: MacroRegimeSnapshot): Promise<void> {
    await ensureMacroRegimeAuditLogTable();
    await pool.query(
        `
        INSERT INTO macro_regime_snapshots (
            run_date,
            snapshot_json
        )
        VALUES ($1::date, $2::jsonb)
        ON CONFLICT (run_date)
        DO UPDATE SET
            snapshot_json = EXCLUDED.snapshot_json,
            created_at = NOW()
        `,
        [snapshot.as_of, JSON.stringify(snapshot)]
    );
    await appendMacroRegimeAuditRecord(snapshot);
}

export interface MacroRegimeAuditLogRow {
    id: number;
    as_of: string;
    credit_regime_state: CreditRegimeState;
    leading_flags: string[];
    overall_severity: RegimeSeverity;
    base_overall_severity: RegimeSeverity | null;
    credit_sub_scores: Record<string, unknown>;
    forward_horizon_days: number;
    forward_proxy: string;
    forward_max_drawdown_pct: number | null;
    forward_realized_vol_pct: number | null;
    forward_stress_detected: boolean | null;
    forward_evaluation: 'PENDING' | 'TP' | 'FP' | 'TN' | 'FN' | 'INSUFFICIENT_DATA';
    evaluated_at: string | null;
    created_at: string;
}

export function extractCreditSubScores(snapshot: MacroRegimeSnapshot): Record<string, unknown> {
    const credit = snapshot.credit_funding_stress;
    return {
        overall_score: credit.overall_score,
        overall_status: credit.overall_status,
        kbe: credit.kbe_signal.score,
        hy_acceleration: credit.hy_acceleration_signal.score,
        funding_proxy: credit.funding_proxy_signal.score,
        ccc_leads_hy: credit.ccc_leads_hy_signal?.score ?? null,
        credit_equity_divergence: credit.credit_equity_divergence_signal?.score ?? null
    };
}

export async function appendMacroRegimeAuditRecord(snapshot: MacroRegimeSnapshot): Promise<void> {
    const creditRegimeState = snapshot.credit_funding_stress.credit_regime_state ?? 'NOISE';
    const leadingFlags = Array.isArray(snapshot.leading_flags) ? snapshot.leading_flags : [];
    await pool.query(
        `
        INSERT INTO macro_regime_audit_log (
            as_of,
            credit_regime_state,
            leading_flags,
            overall_severity,
            base_overall_severity,
            credit_sub_scores
        )
        VALUES ($1::date, $2, $3::jsonb, $4, $5, $6::jsonb)
        `,
        [
            snapshot.as_of,
            creditRegimeState,
            JSON.stringify(leadingFlags),
            snapshot.overall,
            snapshot.base_overall,
            JSON.stringify(extractCreditSubScores(snapshot))
        ]
    );
}

export async function fetchPendingMacroRegimeAuditRows(limit = 100): Promise<MacroRegimeAuditLogRow[]> {
    await ensureMacroRegimeAuditLogTable();
    const result = await pool.query<MacroRegimeAuditLogRow>(
        `
        SELECT
            id,
            TO_CHAR(as_of, 'YYYY-MM-DD') AS as_of,
            credit_regime_state,
            leading_flags,
            overall_severity,
            base_overall_severity,
            credit_sub_scores,
            forward_horizon_days,
            forward_proxy,
            forward_max_drawdown_pct,
            forward_realized_vol_pct,
            forward_stress_detected,
            forward_evaluation,
            evaluated_at,
            created_at
        FROM macro_regime_audit_log
        WHERE forward_evaluation = 'PENDING'
        ORDER BY as_of ASC, id ASC
        LIMIT $1
        `,
        [limit]
    );
    return result.rows;
}

export async function updateMacroRegimeAuditForwardResult(input: {
    id: number;
    forward_max_drawdown_pct: number | null;
    forward_realized_vol_pct: number | null;
    forward_stress_detected: boolean | null;
    forward_evaluation: MacroRegimeAuditLogRow['forward_evaluation'];
}): Promise<void> {
    await pool.query(
        `
        UPDATE macro_regime_audit_log
        SET
            forward_max_drawdown_pct = $2,
            forward_realized_vol_pct = $3,
            forward_stress_detected = $4,
            forward_evaluation = $5,
            evaluated_at = NOW()
        WHERE id = $1
        `,
        [
            input.id,
            input.forward_max_drawdown_pct,
            input.forward_realized_vol_pct,
            input.forward_stress_detected,
            input.forward_evaluation
        ]
    );
}

export async function fetchRecentMacroRegimeAuditRows(daysBack = 60): Promise<MacroRegimeAuditLogRow[]> {
    await ensureMacroRegimeAuditLogTable();
    const result = await pool.query<MacroRegimeAuditLogRow>(
        `
        SELECT
            id,
            TO_CHAR(as_of, 'YYYY-MM-DD') AS as_of,
            credit_regime_state,
            leading_flags,
            overall_severity,
            base_overall_severity,
            credit_sub_scores,
            forward_horizon_days,
            forward_proxy,
            forward_max_drawdown_pct,
            forward_realized_vol_pct,
            forward_stress_detected,
            forward_evaluation,
            evaluated_at,
            created_at
        FROM macro_regime_audit_log
        WHERE as_of >= CURRENT_DATE - ($1::int * INTERVAL '1 day')
        ORDER BY as_of DESC, id DESC
        `,
        [daysBack]
    );
    return result.rows;
}

export async function getLatestMacroRegimeSnapshot(): Promise<MacroRegimeSnapshot | null> {
    const result = await pool.query<{ snapshot_json: MacroRegimeSnapshot }>(
        `
        SELECT snapshot_json
        FROM macro_regime_snapshots
        ORDER BY run_date DESC, created_at DESC
        LIMIT 1
        `
    );
    return result.rows[0]?.snapshot_json ?? null;
}

export async function insertLateCyclePillarHistory(input: {
    reviewed_at: string;
    pillar: string;
    old_state: string;
    new_state: string;
    evidence_snapshot: unknown;
}): Promise<void> {
    await ensureLateCyclePillarHistoryTable();
    await pool.query(
        `
        INSERT INTO late_cycle_pillar_history (
            reviewed_at,
            pillar,
            old_state,
            new_state,
            evidence_snapshot
        )
        VALUES ($1::timestamptz, $2, $3, $4, $5::jsonb)
        `,
        [
            input.reviewed_at,
            input.pillar,
            input.old_state,
            input.new_state,
            JSON.stringify(input.evidence_snapshot ?? {})
        ]
    );
}
