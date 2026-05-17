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
