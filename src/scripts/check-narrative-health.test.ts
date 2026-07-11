import assert from 'node:assert/strict';
import { summarizeGoCoverage } from './check-narrative-health';

assert.deepEqual(
    summarizeGoCoverage([
        { source_quality: 'go_pitch_hybrid_validated', count: '8' },
        { source_quality: 'go_pitch_minimal', count: '1' },
        { source_quality: 'go_pitch_template', count: '1' }
    ]),
    { total: 10, hybrid: 8, minimal: 1, template: 1, hybrid_rate_pct: 80 }
);

assert.deepEqual(
    summarizeGoCoverage([]),
    { total: 0, hybrid: 0, minimal: 0, template: 0, hybrid_rate_pct: 0 }
);

console.log('narrative health policy tests passed');
