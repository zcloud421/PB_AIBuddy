/**
 * Daily Macro Regime Dashboard snapshot generator.
 *
 * Runs once per day via Railway cron, fans out 7 indicator + 2 side monitor
 * computations + fundamental modifier load, persists the result to
 * `macro_regime_snapshots`. Mobile app reads via GET /macro-regime/latest.
 *
 * Failure handling: snapshot-builder is internally fail-soft per indicator,
 * so even if FRED is down we still produce a snapshot with skipped readings.
 * The script only exits non-zero on a true unhandled exception.
 */

import dotenv from 'dotenv';

import { pool } from '../db/client';
import {
    ensureMacroRegimeSnapshotsTable,
    upsertMacroRegimeSnapshot
} from '../db/queries/macro-regime';
import { buildMacroRegimeSnapshot } from '../services/macro-regime/snapshot-builder';

dotenv.config();

async function main(): Promise<void> {
    console.log('[macro-regime-cron] starting daily snapshot run');
    await ensureMacroRegimeSnapshotsTable();
    const snapshot = await buildMacroRegimeSnapshot();
    await upsertMacroRegimeSnapshot(snapshot);

    const indicatorSummary = Object.entries(snapshot.indicators)
        .map(([key, reading]) =>
            `${key}=${reading.is_skipped ? 'SKIP' : reading.status}` +
            (reading.value !== null ? `(${reading.value})` : '')
        )
        .join(', ');

    console.log(
        `[macro-regime-cron] snapshot saved: as_of=${snapshot.as_of} ` +
            `overall=${snapshot.overall} base=${snapshot.base_overall}`
    );
    console.log(`[macro-regime-cron] indicators: ${indicatorSummary}`);
    console.log(
        `[macro-regime-cron] side: ai_cloud=${snapshot.ai_cloud_stress.status} ` +
            `credit_funding=${snapshot.credit_funding_stress.overall_status} ` +
            `fundamental=${snapshot.fundamental_modifier.state}`
    );
}

main()
    .catch((error) => {
        console.error('[macro-regime-cron] fatal:', error);
        process.exit(1);
    })
    .finally(async () => {
        await pool.end().catch(() => undefined);
    });
