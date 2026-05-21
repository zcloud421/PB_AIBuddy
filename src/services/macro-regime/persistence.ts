/**
 * Persistence tracker for tail-risk vs noise differentiation.
 *
 * Spot severity alone cannot distinguish short-term noise from regime change.
 * We persist how long each indicator has stayed at its current severity so the
 * UI can tell RM users whether a warning is fresh noise or a multi-week shift.
 */

import { pool } from '../../db/client';
import type { IndicatorPersistence, MacroRegimeIndicators, RegimeSeverity } from './types';

export interface PersistenceRecord extends IndicatorPersistence {
    indicator_key: string;
    current_severity: RegimeSeverity;
}

const SEVERITY_RANK: Record<RegimeSeverity, number> = {
    Healthy: 0,
    Neutral: 1,
    Warning: 2,
    Critical: 3
};

export interface HyOasConfirmationResult {
    confirmedSeverity: RegimeSeverity;
    pendingUpgrade?: {
        target_severity: RegimeSeverity;
        confirmation_days_elapsed: number;
        confirmation_days_required: number;
        notes: string;
    };
}

function isUpgrade(previous: RegimeSeverity, next: RegimeSeverity): boolean {
    return SEVERITY_RANK[next] > SEVERITY_RANK[previous];
}

export function countWeekdaysInclusive(startDate: string, endDate: string): number {
    const start = new Date(`${startDate}T00:00:00Z`);
    const end = new Date(`${endDate}T00:00:00Z`);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || end < start) return 0;

    let count = 0;
    const cursor = new Date(start);
    while (cursor <= end) {
        const day = cursor.getUTCDay();
        if (day !== 0 && day !== 6) count += 1;
        cursor.setUTCDate(cursor.getUTCDate() + 1);
    }
    return count;
}

async function setPendingUpgrade(
    indicatorKey: string,
    targetSeverity: RegimeSeverity,
    startDate: string
): Promise<void> {
    await pool.query(
        `
        INSERT INTO indicator_persistence (
            indicator_key,
            current_severity,
            consecutive_days,
            severity_started_at,
            last_seen_date,
            pending_upgrade_severity,
            pending_upgrade_start_date
        )
        VALUES ($1, 'Healthy', 1, $3::date, $3::date, $2, $3::date)
        ON CONFLICT (indicator_key)
        DO UPDATE SET
            pending_upgrade_severity = EXCLUDED.pending_upgrade_severity,
            pending_upgrade_start_date = EXCLUDED.pending_upgrade_start_date,
            updated_at = NOW()
        `,
        [indicatorKey, targetSeverity, startDate]
    );
}

async function clearPendingUpgrade(indicatorKey: string): Promise<void> {
    await pool.query(
        `
        UPDATE indicator_persistence
        SET pending_upgrade_severity = NULL,
            pending_upgrade_start_date = NULL,
            updated_at = NOW()
        WHERE indicator_key = $1
        `,
        [indicatorKey]
    );
}

export async function confirmHyOasSeverity(
    rawSeverity: RegimeSeverity,
    asOfDate: string,
    requiredTradingDays = 10
): Promise<HyOasConfirmationResult> {
    const existing = await pool.query<{
        current_severity: RegimeSeverity;
        pending_upgrade_severity: RegimeSeverity | null;
        pending_upgrade_start_date: string | null;
    }>(
        `
        SELECT
            current_severity,
            pending_upgrade_severity,
            TO_CHAR(pending_upgrade_start_date, 'YYYY-MM-DD') AS pending_upgrade_start_date
        FROM indicator_persistence
        WHERE indicator_key = 'HY_OAS'
        `
    );

    if (existing.rows.length === 0) {
        return { confirmedSeverity: rawSeverity };
    }

    const stored = existing.rows[0];
    const previousSeverity = stored.current_severity;
    if (!isUpgrade(previousSeverity, rawSeverity)) {
        if (stored.pending_upgrade_severity) {
            await clearPendingUpgrade('HY_OAS');
        }
        return { confirmedSeverity: rawSeverity };
    }

    const targetSeverity = rawSeverity;
    const pendingStart =
        stored.pending_upgrade_severity === targetSeverity && stored.pending_upgrade_start_date
            ? stored.pending_upgrade_start_date
            : asOfDate;
    const elapsed = Math.max(1, countWeekdaysInclusive(pendingStart, asOfDate));

    if (elapsed >= requiredTradingDays) {
        await clearPendingUpgrade('HY_OAS');
        return { confirmedSeverity: rawSeverity };
    }

    await setPendingUpgrade('HY_OAS', targetSeverity, pendingStart);
    return {
        confirmedSeverity: previousSeverity,
        pendingUpgrade: {
            target_severity: targetSeverity,
            confirmation_days_elapsed: elapsed,
            confirmation_days_required: requiredTradingDays,
            notes: `${targetSeverity} 升档待确认:已持续 ${elapsed} / ${requiredTradingDays} 个交易日`
        }
    };
}

export async function loadPersistence(): Promise<Map<string, PersistenceRecord>> {
    const result = await pool.query<{
        indicator_key: string;
        current_severity: RegimeSeverity;
        consecutive_days: number;
        severity_started_at: string;
    }>(
        `
        SELECT
            indicator_key,
            current_severity,
            consecutive_days,
            TO_CHAR(severity_started_at, 'YYYY-MM-DD') AS severity_started_at
        FROM indicator_persistence
        `
    );
    return new Map(result.rows.map((row) => [row.indicator_key, row]));
}

/**
 * Upsert persistence for a single indicator key.
 *
 * Idempotent within the same calendar day: re-running the cron or manually
 * refreshing the snapshot will not double-count if severity is unchanged.
 */
export async function upsertIndicatorPersistence(
    indicatorKey: string,
    newSeverity: RegimeSeverity,
    asOfDate: string
): Promise<PersistenceRecord> {
    const existing = await pool.query<{
        current_severity: RegimeSeverity;
        consecutive_days: number;
        severity_started_at: string;
        last_seen_date: string;
    }>(
        `
        SELECT
            current_severity,
            consecutive_days,
            TO_CHAR(severity_started_at, 'YYYY-MM-DD') AS severity_started_at,
            TO_CHAR(last_seen_date, 'YYYY-MM-DD') AS last_seen_date
        FROM indicator_persistence
        WHERE indicator_key = $1
        `,
        [indicatorKey]
    );

    if (existing.rows.length === 0) {
        await pool.query(
            `
            INSERT INTO indicator_persistence (
                indicator_key,
                current_severity,
                consecutive_days,
                severity_started_at,
                last_seen_date
            )
            VALUES ($1, $2, 1, $3::date, $3::date)
            `,
            [indicatorKey, newSeverity, asOfDate]
        );
        return {
            indicator_key: indicatorKey,
            current_severity: newSeverity,
            consecutive_days: 1,
            severity_started_at: asOfDate
        };
    }

    const stored = existing.rows[0];

    if (stored.last_seen_date === asOfDate && stored.current_severity === newSeverity) {
        return {
            indicator_key: indicatorKey,
            current_severity: stored.current_severity,
            consecutive_days: stored.consecutive_days,
            severity_started_at: stored.severity_started_at
        };
    }

    if (stored.current_severity === newSeverity) {
        const today = new Date(`${asOfDate}T00:00:00Z`);
        const startedAt = new Date(`${stored.severity_started_at}T00:00:00Z`);
        const daysSinceStart =
            Math.floor((today.getTime() - startedAt.getTime()) / (24 * 60 * 60 * 1000)) + 1;
        const consecutiveDays = Math.max(1, daysSinceStart);
        await pool.query(
            `
            UPDATE indicator_persistence
            SET consecutive_days = $2,
                last_seen_date = $3::date,
                updated_at = NOW()
            WHERE indicator_key = $1
            `,
            [indicatorKey, consecutiveDays, asOfDate]
        );
        return {
            indicator_key: indicatorKey,
            current_severity: newSeverity,
            consecutive_days: consecutiveDays,
            severity_started_at: stored.severity_started_at
        };
    }

    await pool.query(
        `
        UPDATE indicator_persistence
        SET current_severity = $2,
            consecutive_days = 1,
            severity_started_at = $3::date,
            last_seen_date = $3::date,
            updated_at = NOW()
        WHERE indicator_key = $1
        `,
        [indicatorKey, newSeverity, asOfDate]
    );
    return {
        indicator_key: indicatorKey,
        current_severity: newSeverity,
        consecutive_days: 1,
        severity_started_at: asOfDate
    };
}

function toPersistence(record: PersistenceRecord): IndicatorPersistence {
    return {
        consecutive_days: record.consecutive_days,
        severity_started_at: record.severity_started_at
    };
}

/**
 * Update persistence for all indicators + composites + overall in one pass.
 */
export async function syncAllPersistence(
    asOfDate: string,
    indicators: MacroRegimeIndicators,
    compositeSeverities: {
        overall: RegimeSeverity;
        ai_cloud: RegimeSeverity;
        credit_funding: RegimeSeverity;
        fundamental: RegimeSeverity;
    }
): Promise<Map<string, PersistenceRecord>> {
    const results = new Map<string, PersistenceRecord>();

    for (const [key, reading] of Object.entries(indicators)) {
        const severity: RegimeSeverity = reading.is_skipped ? 'Neutral' : reading.status;
        const record = await upsertIndicatorPersistence(key, severity, asOfDate);
        reading.persistence = toPersistence(record);
        results.set(key, record);
    }

    results.set(
        'OVERALL',
        await upsertIndicatorPersistence('OVERALL', compositeSeverities.overall, asOfDate)
    );
    results.set(
        'AI_CLOUD',
        await upsertIndicatorPersistence('AI_CLOUD', compositeSeverities.ai_cloud, asOfDate)
    );
    results.set(
        'CREDIT_FUNDING',
        await upsertIndicatorPersistence('CREDIT_FUNDING', compositeSeverities.credit_funding, asOfDate)
    );
    results.set(
        'FUNDAMENTAL',
        await upsertIndicatorPersistence('FUNDAMENTAL', compositeSeverities.fundamental, asOfDate)
    );

    return results;
}

export function persistenceFor(
    records: Map<string, PersistenceRecord>,
    key: string
): IndicatorPersistence {
    const record = records.get(key);
    return record
        ? toPersistence(record)
        : {
            consecutive_days: 1,
            severity_started_at: new Date().toISOString().slice(0, 10)
        };
}
