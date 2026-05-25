import assert from 'node:assert/strict';
import { buildEngineDiffReport } from './engine-diff-report';

const markdown = buildEngineDiffReport({
    baseline_tag: 'current',
    candidate_tag: 'shadow',
    rows: [
        { symbol: 'NVDA', baseline_grade: 'GO', candidate_grade: 'GO', reason: 'unchanged' },
        { symbol: 'LITE', baseline_grade: 'GO', candidate_grade: 'CAUTION', reason: 'bearish_alignment_gate' },
        { symbol: 'XOM', baseline_grade: 'CAUTION', candidate_grade: 'AVOID', reason: 'path_risk_gate' }
    ],
    gate_reasons: [
        { reason: 'bearish_alignment_gate', count: 1 },
        { reason: 'path_risk_gate', count: 1 }
    ]
});

assert.ok(markdown.includes('## Grade Distribution'));
assert.ok(markdown.includes('| GO | 2 | 1 | -1 |'));
assert.ok(markdown.includes('| LITE | GO | CAUTION | bearish_alignment_gate |'));
assert.ok(markdown.includes('| path_risk_gate | 1 |'));

console.log('engine-diff-report tests passed');
