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
            late_cycle_soft_pause_first_active_at TIMESTAMPTZ NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        ALTER TABLE macro_regime_snapshots
        ADD COLUMN IF NOT EXISTS late_cycle_soft_pause_first_active_at TIMESTAMPTZ NULL
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

async function resolveLateCycleSoftPauseFirstActiveAt(
    runDate: string,
    softPauseActive: boolean
): Promise<Date | null> {
    if (!softPauseActive) return null;

    const result = await pool.query<{
        soft_pause_active: boolean | null;
        late_cycle_soft_pause_first_active_at: Date | null;
    }>(
        `
        SELECT
            (snapshot_json->'late_cycle_context'->>'soft_pause_active')::boolean AS soft_pause_active,
            late_cycle_soft_pause_first_active_at
        FROM macro_regime_snapshots
        WHERE run_date < $1::date
        ORDER BY run_date DESC, created_at DESC
        LIMIT 1
        `,
        [runDate]
    );
    const previous = result.rows[0];
    if (previous?.soft_pause_active) {
        return previous.late_cycle_soft_pause_first_active_at ?? new Date();
    }
    return new Date();
}

function applyLateCycleSoftPauseCounter(snapshot: MacroRegimeSnapshot, firstActiveAt: Date | null): void {
    if (!snapshot.late_cycle_context.soft_pause_active || !firstActiveAt) {
        snapshot.late_cycle_context.consecutive_days_active = 0;
        snapshot.late_cycle_context.fatigue_warning = false;
        return;
    }

    const days = Math.max(0, Math.floor((Date.now() - firstActiveAt.getTime()) / (24 * 60 * 60 * 1000)));
    snapshot.late_cycle_context.consecutive_days_active = days;
    snapshot.late_cycle_context.fatigue_warning = days > 180;
}

export async function upsertMacroRegimeSnapshot(snapshot: MacroRegimeSnapshot): Promise<void> {
    const firstActiveAt = await resolveLateCycleSoftPauseFirstActiveAt(
        snapshot.as_of,
        snapshot.late_cycle_context.soft_pause_active
    );
    applyLateCycleSoftPauseCounter(snapshot, firstActiveAt);

    await pool.query(
        `
        INSERT INTO macro_regime_snapshots (
            run_date,
            snapshot_json,
            late_cycle_soft_pause_first_active_at
        )
        VALUES ($1::date, $2::jsonb, $3::timestamptz)
        ON CONFLICT (run_date)
        DO UPDATE SET
            snapshot_json = EXCLUDED.snapshot_json,
            late_cycle_soft_pause_first_active_at = EXCLUDED.late_cycle_soft_pause_first_active_at,
            created_at = NOW()
        `,
        [snapshot.as_of, JSON.stringify(snapshot), firstActiveAt]
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
