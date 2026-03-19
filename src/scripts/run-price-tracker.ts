import dotenv from 'dotenv';

import { ensureRecommendationTrackerTable } from '../db/queries/ideas';
import { pool } from '../db/client';
import { runPriceTracker } from '../services/tracker-service';

dotenv.config();

async function main(): Promise<void> {
    try {
        await ensureRecommendationTrackerTable();
        await runPriceTracker();
        console.log('[tracker] Recommendation tracker refresh completed');
    } finally {
        await pool.end();
    }
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
});
