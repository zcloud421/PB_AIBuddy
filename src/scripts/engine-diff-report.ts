import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';

import { pool } from '../db/client';

dotenv.config();

export interface EngineDiffRow {
    symbol: string;
    baseline_grade: string;
    candidate_grade: string;
    reason?: string | null;
}

export interface EngineDiffInput {
    baseline_tag: string;
    candidate_tag: string;
    rows: EngineDiffRow[];
    gate_reasons?: Array<{ reason: string; count: number }>;
}

const GRADE_ORDER = ['GO', 'CAUTION', 'AVOID', 'WAIT'];

export function buildEngineDiffReport(input: EngineDiffInput): string {
    const baselineCounts = countGrades(input.rows.map((row) => row.baseline_grade));
    const candidateCounts = countGrades(input.rows.map((row) => row.candidate_grade));
    const changed = input.rows
        .filter((row) => row.baseline_grade !== row.candidate_grade)
        .slice(0, 20);
    const gateReasons = input.gate_reasons ?? [];

    return [
        `# Engine Diff Report`,
        '',
        `Baseline: ${input.baseline_tag}`,
        `Candidate: ${input.candidate_tag}`,
        `Generated: ${new Date().toISOString()}`,
        '',
        `## Grade Distribution`,
        '',
        `| Grade | baseline | candidate | delta |`,
        `|---|---:|---:|---:|`,
        ...GRADE_ORDER.map((grade) => {
            const baseline = baselineCounts[grade] ?? 0;
            const candidate = candidateCounts[grade] ?? 0;
            const delta = candidate - baseline;
            return `| ${grade} | ${baseline} | ${candidate} | ${delta >= 0 ? '+' : ''}${delta} |`;
        }),
        '',
        `## Changed Symbols (top 20 by significance)`,
        '',
        `| Symbol | baseline | candidate | reason |`,
        `|---|---|---|---|`,
        ...(changed.length > 0
            ? changed.map((row) => `| ${row.symbol} | ${row.baseline_grade} | ${row.candidate_grade} | ${row.reason ?? 'n/a'} |`)
            : ['| — | — | — | No changes |']),
        '',
        `## Gate Reason Frequency (top reasons)`,
        '',
        `| reason | count |`,
        `|---|---:|`,
        ...(gateReasons.length > 0
            ? gateReasons.slice(0, 20).map((row) => `| ${row.reason} | ${row.count} |`)
            : ['| — | 0 |']),
        ''
    ].join('\n');
}

export async function runEngineDiffReport(options: {
    baselineTag?: string;
    candidateTag?: string;
    outputDir?: string;
} = {}): Promise<{ path: string; markdown: string }> {
    const baselineTag = options.baselineTag ?? 'current';
    const candidateTag = options.candidateTag ?? 'current';
    const rows = await fetchCurrentRows();
    const markdown = buildEngineDiffReport({
        baseline_tag: baselineTag,
        candidate_tag: candidateTag,
        rows
    });
    const outputDir = options.outputDir ?? '/tmp';
    const outputPath = path.join(outputDir, `engine-diff-${new Date().toISOString().replace(/[:.]/g, '-')}.md`);
    fs.writeFileSync(outputPath, markdown);
    console.log(`[engine-diff] wrote ${outputPath}`);
    console.log(markdown);
    return { path: outputPath, markdown };
}

async function fetchCurrentRows(): Promise<EngineDiffRow[]> {
    const result = await pool.query<{
        symbol: string;
        grade: string;
    }>(`
        WITH latest_completed_run AS (
            SELECT run_id
            FROM idea_runs
            WHERE status = 'completed'
            ORDER BY run_date DESC, completed_at DESC, started_at DESC
            LIMIT 1
        )
        SELECT ic.symbol, ic.overall_grade::text AS grade
        FROM idea_candidates ic
        JOIN latest_completed_run lcr ON lcr.run_id = ic.run_id
        ORDER BY ic.symbol ASC
    `);

    // Phase 5.0 has no shadow/candidate column yet, so baseline and candidate are both current.
    return result.rows.map((row) => ({
        symbol: row.symbol,
        baseline_grade: row.grade,
        candidate_grade: row.grade,
        reason: 'phase_5_0_baseline'
    }));
}

function countGrades(grades: string[]): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const grade of grades) {
        counts[grade] = (counts[grade] ?? 0) + 1;
    }
    return counts;
}

function parseArg(name: string): string | undefined {
    const prefix = `--${name}=`;
    const match = process.argv.find((arg) => arg.startsWith(prefix));
    return match ? match.slice(prefix.length) : undefined;
}

if (require.main === module) {
    runEngineDiffReport({
        baselineTag: parseArg('baseline-tag') ?? 'current',
        candidateTag: parseArg('candidate-tag') ?? 'current'
    })
        .then(() => pool.end())
        .catch((error) => {
            console.error('[engine-diff] fatal:', error);
            void pool.end();
            process.exit(1);
        });
}
