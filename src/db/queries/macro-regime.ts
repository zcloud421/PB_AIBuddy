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

export async function upsertMacroRegimeSnapshot(snapshot: MacroRegimeSnapshot): Promise<void> {
    await pool.query(
        `
        INSERT INTO macro_regime_snapshots (run_date, snapshot_json)
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
