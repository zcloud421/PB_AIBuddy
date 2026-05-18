/**
 * Postgres persistence for Macro Regime daily snapshots.
 *
 * Single-table design: each daily run writes one row keyed on run_date.
 * Mobile reads "latest" via `getLatestMacroRegimeSnapshot()`.
 */

import { pool } from '../client';
import type { MacroRegimeSnapshot } from '../../services/macro-regime/types';

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
}

export async function upsertMacroRegimeSnapshot(snapshot: MacroRegimeSnapshot): Promise<void> {
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
