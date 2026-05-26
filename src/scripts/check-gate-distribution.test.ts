import assert from 'node:assert';
import { computeGateDistributionMetrics, formatGateDistributionReport } from './check-gate-distribution';

const metrics = computeGateDistributionMetrics({
    run_id: 'run-1',
    run_date: '2026-05-25',
    rows: [
        {
            symbol: 'TSLA',
            overall_grade: 'AVOID',
            gate_decisions: [
                {
                    type: 'HARD_AVOID_TRIGGERED',
                    failType: 'HARD_FAIL',
                    passed: false,
                    severity: 'BLOCK',
                    message: 'hard avoid'
                }
            ]
        },
        {
            symbol: 'NVDA',
            overall_grade: 'CAUTION',
            gate_decisions: [
                {
                    type: 'GRADE_CAP_OVEREXTENDED',
                    failType: 'SUITABILITY_FAIL',
                    passed: false,
                    severity: 'WARN',
                    message: 'cap'
                }
            ]
        },
        {
            symbol: 'AAPL',
            overall_grade: 'GO',
            gate_decisions: []
        }
    ]
});

assert.equal(metrics.total_symbols, 3);
assert.equal(metrics.type_counts[0].count, 1);
assert(metrics.type_counts.some((row) => row.type === 'HARD_AVOID_TRIGGERED'));
assert(metrics.grade_drop_reasons.some((row) => row.symbol === 'TSLA'));

const report = formatGateDistributionReport(metrics);
assert(report.includes('Gate Distribution'));
assert(report.includes('HARD_AVOID_TRIGGERED'));
assert(report.includes('TSLA'));

console.log('gate-distribution tests passed');
