import assert from 'node:assert/strict';

import type { DailyPriceBar } from '../data/massive-fetcher';
import {
    classifyForwardEvaluation,
    computeForwardStressMetrics,
    computeMacroCreditModelHealthMetrics,
    formatMacroCreditModelHealthReport
} from './check-macro-credit-model-health';
import type { MacroRegimeAuditLogRow } from '../db/queries/macro-regime';

function makeBars(closes: number[], start = '2026-01-02'): DailyPriceBar[] {
    const startDate = new Date(`${start}T00:00:00Z`);
    return closes.map((close, index) => {
        const date = new Date(startDate);
        date.setUTCDate(startDate.getUTCDate() + index);
        const iso = date.toISOString().slice(0, 10);
        return {
            date: iso,
            open: close,
            high: close * 1.01,
            low: close * 0.99,
            close,
            volume: 1_000_000
        };
    });
}

function auditRow(partial: Partial<MacroRegimeAuditLogRow>): MacroRegimeAuditLogRow {
    return {
        id: partial.id ?? 1,
        as_of: partial.as_of ?? '2026-01-02',
        credit_regime_state: partial.credit_regime_state ?? 'NOISE',
        leading_flags: partial.leading_flags ?? [],
        overall_severity: partial.overall_severity ?? 'Neutral',
        base_overall_severity: partial.base_overall_severity ?? 'Neutral',
        credit_sub_scores: partial.credit_sub_scores ?? {},
        forward_horizon_days: partial.forward_horizon_days ?? 21,
        forward_proxy: partial.forward_proxy ?? 'QQQ',
        forward_max_drawdown_pct: partial.forward_max_drawdown_pct ?? null,
        forward_realized_vol_pct: partial.forward_realized_vol_pct ?? null,
        forward_stress_detected: partial.forward_stress_detected ?? null,
        forward_evaluation: partial.forward_evaluation ?? 'PENDING',
        evaluated_at: partial.evaluated_at ?? null,
        created_at: partial.created_at ?? '2026-01-02T00:00:00.000Z'
    };
}

function run(): void {
    {
        const bars = makeBars([100, 99, 97, 94, 95, 96, 97]);
        const metrics = computeForwardStressMetrics(bars, '2026-01-02', 5);
        assert(metrics);
        assert.equal(metrics.max_drawdown_pct >= 6, true);
        assert.equal(metrics.stress_detected, true);
    }

    {
        const bars = makeBars([100, 100.2, 100.1, 100.3, 100.4, 100.5, 100.6]);
        const metrics = computeForwardStressMetrics(bars, '2026-01-02', 5);
        assert(metrics);
        assert.equal(metrics.max_drawdown_pct < 5, true);
        assert.equal(metrics.stress_detected, false);
    }

    {
        const bars = makeBars([100, 99, 98]);
        assert.equal(computeForwardStressMetrics(bars, '2026-01-02', 5), null);
    }

    {
        assert.equal(classifyForwardEvaluation('BREAK_FORMING', true), 'TP');
        assert.equal(classifyForwardEvaluation('BREAK', false), 'FP');
        assert.equal(classifyForwardEvaluation('NOISE', true), 'FN');
        assert.equal(classifyForwardEvaluation('NOISE', false), 'TN');
    }

    {
        const metrics = computeMacroCreditModelHealthMetrics({
            pending_rows: [auditRow({ id: 1 }), auditRow({ id: 2 })],
            recent_rows: [
                auditRow({ id: 1, forward_evaluation: 'FP' }),
                auditRow({ id: 2, forward_evaluation: 'FP' }),
                auditRow({ id: 3, forward_evaluation: 'TP' })
            ],
            updated_rows: [],
            insufficient_data: 0
        });
        assert.equal(metrics.fp_cluster_alert, true);
        assert.equal(metrics.recent_false_positives, 2);
        assert.equal(metrics.recent_true_positives, 1);
        assert.equal(metrics.issues.length, 1);
        assert.match(formatMacroCreditModelHealthReport(metrics), /advisory only/i);
    }

    {
        const metrics = computeMacroCreditModelHealthMetrics({
            pending_rows: [],
            recent_rows: [auditRow({ id: 1, forward_evaluation: 'TP' })],
            updated_rows: [],
            insufficient_data: 1
        });
        assert.equal(metrics.fp_cluster_alert, false);
        assert.equal(metrics.issues.length, 0);
    }

    console.log('macro credit model health tests passed');
}

if (require.main === module) {
    run();
}
