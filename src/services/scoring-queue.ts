import { v4 as uuidv4 } from 'uuid';

import type { AsyncScoringStatusResponse, SymbolIdeaResponse } from '../types/api';

export interface ScoringJob {
    jobId: string;
    symbol: string;
    runDate: string;
    status: 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';
    progressPct: number;
    result: SymbolIdeaResponse | null;
}

const jobs = new Map<string, ScoringJob>();

function buildJobKey(symbol: string, jobId: string): string {
    return `${symbol}:${jobId}`;
}

export function enqueueSymbolScoringJob(
    symbol: string,
    runDate: string,
    worker: () => Promise<SymbolIdeaResponse>
): ScoringJob {
    const jobId = uuidv4();
    const key = buildJobKey(symbol, jobId);

    const job: ScoringJob = {
        jobId,
        symbol,
        runDate,
        status: 'PENDING',
        progressPct: 0,
        result: null
    };

    jobs.set(key, job);

    queueMicrotask(() => {
        void runJob(key, worker);
    });

    return job;
}

async function runJob(key: string, worker: () => Promise<SymbolIdeaResponse>): Promise<void> {
    const job = jobs.get(key);
    if (!job) {
        return;
    }

    job.status = 'RUNNING';
    job.progressPct = 25;

    try {
        const result = await worker();
        job.result = result;
        job.status = 'COMPLETED';
        job.progressPct = 100;
    } catch {
        job.status = 'FAILED';
        job.progressPct = 100;
    }
}

export function getSymbolScoringJob(symbol: string, jobId: string): AsyncScoringStatusResponse | null {
    const job = jobs.get(buildJobKey(symbol, jobId));
    if (!job) {
        return null;
    }

    return {
        symbol: job.symbol,
        job_id: job.jobId,
        status: job.status,
        run_date: job.runDate,
        cached: false,
        progress_pct: job.progressPct,
        result: job.result
    };
}
