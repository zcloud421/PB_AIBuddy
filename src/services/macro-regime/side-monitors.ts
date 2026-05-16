/**
 * Side monitors for the Macro Regime Dashboard.
 *
 * Two auxiliary signal sets that escalate the main severity but do not
 * themselves count as primary indicators:
 *   1. AI Cloud Stress (CRWV / NBIS) — equity-side crack in AI capex financing
 *   2. Credit/Funding Stress (KBE + HY accel + funding proxy) — 2008-style risk
 *
 * Both default to Normal and stay dormant unless their subsignals trigger.
 */

import type {
    AiCloudStressReport,
    AiCloudStressStatus,
    AiCloudStressTickerSignal,
    CreditFundingStressReport,
    SideMonitorStatus,
    SideSubSignal
} from './types';
import type { MassiveDataFetcher, DailyPriceBar } from '../../data/massive-fetcher';
import { fetchFredSeries, latestPoint, pointDaysBack } from '../../data/fred-series-fetcher';

function ma(values: number[], window: number): number | null {
    if (values.length < window) return null;
    const slice = values.slice(-window);
    return slice.reduce((acc, v) => acc + v, 0) / window;
}

function statusFromScore(score: 0 | 1 | 2 | 3): SideMonitorStatus {
    if (score === 0) return 'normal';
    if (score === 1) return 'watch';
    if (score === 2) return 'stress';
    return 'crisis';
}

function aiCloudStatusFromScore(score: 0 | 1 | 2 | 3): AiCloudStressStatus {
    if (score === 0) return 'Normal';
    if (score === 1) return 'Watch';
    if (score === 2) return 'Stress';
    return 'Crisis';
}

// ─────────────────────────────────────────────────────────────────────────
// AI Cloud Stress
// ─────────────────────────────────────────────────────────────────────────

interface SubSignalResult {
    signal: 0 | 1 | 2 | 3;
    detail: string | null;
}

async function computeAiCloudTickerSignal(
    ticker: string,
    massiveFetcher: MassiveDataFetcher,
    hyOasDelta4wBp: number | null,
    nvdaSixtyDayReturnPct: number | null
): Promise<AiCloudStressTickerSignal> {
    let bars: DailyPriceBar[] = [];
    try {
        // 100 calendar days ≈ 70 trading bars (need 60 trading bars for return60d)
        bars = await massiveFetcher.fetchPriceHistory(ticker, 100);
    } catch {
        return { ticker, signal: 0, details: [`${ticker} bars unavailable`] };
    }
    if (bars.length < 30) {
        return { ticker, signal: 0, details: [`${ticker} insufficient history`] };
    }

    const closes = bars.map((b) => b.close);
    const volumes = bars.map((b) => b.volume);
    const last = closes[closes.length - 1];

    // 1. 60-day drawdown
    const peak = Math.max(...closes.slice(-60));
    const drawdown = peak > 0 ? (last / peak - 1) * 100 : 0;
    let s1: 0 | 1 | 2 | 3 = 0;
    if (drawdown <= -40) s1 = 3;
    else if (drawdown <= -25) s1 = 2;
    else if (drawdown <= -15) s1 = 1;

    // 2. Relative weakness vs NVDA (60d return spread)
    let s2: 0 | 1 | 2 | 3 = 0;
    let return60d: number | null = null;
    if (closes.length >= 60) {
        const sixtyBack = closes[closes.length - 60];
        if (sixtyBack > 0) {
            return60d = (last / sixtyBack - 1) * 100;
        }
    }
    if (return60d !== null && nvdaSixtyDayReturnPct !== null) {
        const spread = return60d - nvdaSixtyDayReturnPct;
        if (spread <= -40) s2 = 3;
        else if (spread <= -25) s2 = 2;
        else if (spread <= -15) s2 = 1;
    }

    // 3. HY OAS divergence: ticker -20%+ but HY tight
    let s3: 0 | 1 | 2 | 3 = 0;
    if (drawdown <= -20 && hyOasDelta4wBp !== null && hyOasDelta4wBp < 20) {
        s3 = 2;
    }

    // 4. Volume spike + same-day decline
    let s4: 0 | 1 | 2 | 3 = 0;
    if (bars.length >= 21) {
        const ma20Vol = ma(volumes.slice(0, -1), 20);
        const lastBar = bars[bars.length - 1];
        const prevClose = closes[closes.length - 2];
        if (
            ma20Vol !== null &&
            ma20Vol > 0 &&
            lastBar.volume / ma20Vol > 3 &&
            prevClose > 0 &&
            (lastBar.close / prevClose - 1) * 100 <= -5
        ) {
            s4 = 2;
        }
    }

    const signal = Math.max(s1, s2, s3, s4) as 0 | 1 | 2 | 3;
    const details: string[] = [
        `60d drawdown ${drawdown.toFixed(1)}%`,
        return60d !== null
            ? `60d return ${return60d.toFixed(1)}%${nvdaSixtyDayReturnPct !== null ? ` (NVDA ${nvdaSixtyDayReturnPct.toFixed(1)}%)` : ''}`
            : '60d return n/a',
        s3 > 0 ? 'HY tight + equity stress (divergence)' : 'No HY divergence',
        s4 > 0 ? 'Volume spike + -5% day' : 'No volume spike'
    ];

    return { ticker, signal, details };
}

export async function computeAiCloudStress(
    massiveFetcher: MassiveDataFetcher,
    hyOasDelta4wBp: number | null
): Promise<AiCloudStressReport> {
    // Reference: 60d return of NVDA for relative-weakness subsignal
    let nvdaSixtyDayReturnPct: number | null = null;
    try {
        const nvdaBars = await massiveFetcher.fetchPriceHistory('NVDA', 100);
        if (nvdaBars.length >= 60) {
            const last = nvdaBars[nvdaBars.length - 1].close;
            const sixtyBack = nvdaBars[nvdaBars.length - 60].close;
            if (sixtyBack > 0) {
                nvdaSixtyDayReturnPct = (last / sixtyBack - 1) * 100;
            }
        }
    } catch {
        nvdaSixtyDayReturnPct = null;
    }

    const [crwv, nbis] = await Promise.all([
        computeAiCloudTickerSignal('CRWV', massiveFetcher, hyOasDelta4wBp, nvdaSixtyDayReturnPct),
        computeAiCloudTickerSignal('NBIS', massiveFetcher, hyOasDelta4wBp, nvdaSixtyDayReturnPct)
    ]);

    // Weighted composite: 0.65 × CRWV + 0.35 × NBIS, rounded
    const composite = Math.round(0.65 * crwv.signal + 0.35 * nbis.signal);
    const clamped = Math.max(0, Math.min(3, composite)) as 0 | 1 | 2 | 3;

    return {
        score: clamped,
        status: aiCloudStatusFromScore(clamped),
        crwv,
        nbis
    };
}

// ─────────────────────────────────────────────────────────────────────────
// Credit/Funding Stress
// ─────────────────────────────────────────────────────────────────────────

async function computeKbeSubSignal(
    massiveFetcher: MassiveDataFetcher
): Promise<SideSubSignal> {
    let bars: DailyPriceBar[] = [];
    try {
        // 330 calendar days ≈ 230 trading bars (need 200 for MA200 with buffer)
        bars = await massiveFetcher.fetchPriceHistory('KBE', 330);
    } catch {
        return {
            name: 'KBE vs MA200',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['KBE bars unavailable']
        };
    }
    if (bars.length < 200) {
        return {
            name: 'KBE vs MA200',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['KBE insufficient history']
        };
    }
    const closes = bars.map((b) => b.close);
    const ma200 = ma(closes, 200);
    const close = closes[closes.length - 1];
    if (ma200 === null || ma200 === 0) {
        return {
            name: 'KBE vs MA200',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['KBE MA200 unavailable']
        };
    }
    const deviationPct = (close / ma200 - 1) * 100;

    let score: 0 | 1 | 2 | 3;
    if (deviationPct > 0) score = 0;
    else if (deviationPct > -3) score = 1;
    else if (deviationPct > -8) score = 2;
    else score = 3;

    return {
        name: 'KBE vs MA200',
        value: Math.round(deviationPct * 10) / 10,
        score,
        status: statusFromScore(score),
        notes: [`KBE close ${close.toFixed(2)} vs MA200 ${ma200.toFixed(2)}`]
    };
}

async function computeHyAccelerationSubSignal(
    hyOasSeriesBp: number[]
): Promise<SideSubSignal> {
    // Need at least 56 days of history to compute Δ4w - Δ8w
    if (hyOasSeriesBp.length < 56) {
        return {
            name: 'HY OAS Δ4w-Δ8w',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['HY OAS history insufficient']
        };
    }
    const last = hyOasSeriesBp[hyOasSeriesBp.length - 1];
    const fourWeeksBack = hyOasSeriesBp[hyOasSeriesBp.length - 1 - 20];
    const eightWeeksBack = hyOasSeriesBp[hyOasSeriesBp.length - 1 - 40];
    const delta4w = last - fourWeeksBack;
    const delta8w = last - eightWeeksBack;
    const acceleration = delta4w - delta8w;

    let score: 0 | 1 | 2 | 3;
    if (acceleration < 25) score = 0;
    else if (acceleration < 50) score = 1;
    else if (acceleration < 100) score = 2;
    else score = 3;

    return {
        name: 'HY OAS Δ4w-Δ8w',
        value: Math.round(acceleration * 10) / 10,
        score,
        status: statusFromScore(score),
        notes: [`Δ4w ${delta4w.toFixed(0)}bp, Δ8w ${delta8w.toFixed(0)}bp`]
    };
}

async function computeFundingProxySubSignal(): Promise<SideSubSignal> {
    const [dgs3mo, dgs2] = await Promise.all([
        fetchFredSeries('DGS3MO', 10),
        fetchFredSeries('DGS2', 10)
    ]);
    if (!dgs3mo || !dgs2) {
        return {
            name: 'DGS3MO - DGS2',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['FRED DGS3MO or DGS2 unavailable']
        };
    }
    const latest3m = latestPoint(dgs3mo);
    const latest2y = latestPoint(dgs2);
    if (!latest3m || !latest2y) {
        return {
            name: 'DGS3MO - DGS2',
            value: null,
            score: 0,
            status: 'normal',
            notes: ['DGS3MO/DGS2 latest unavailable']
        };
    }
    // Both percent points; convert spread to bp
    const spreadBp = (latest3m.value - latest2y.value) * 100;

    let score: 0 | 1 | 2 | 3;
    if (spreadBp < 25) score = 0;
    else if (spreadBp < 50) score = 1;
    else if (spreadBp < 100) score = 2;
    else score = 3;

    return {
        name: 'DGS3MO - DGS2',
        value: Math.round(spreadBp * 10) / 10,
        score,
        status: statusFromScore(score),
        notes: [`3M ${latest3m.value.toFixed(2)}%, 2Y ${latest2y.value.toFixed(2)}%`]
    };
}

export async function computeCreditFundingStress(
    massiveFetcher: MassiveDataFetcher,
    hyOasSeriesBp: number[]
): Promise<CreditFundingStressReport> {
    const [kbe, hyAccel, funding] = await Promise.all([
        computeKbeSubSignal(massiveFetcher),
        computeHyAccelerationSubSignal(hyOasSeriesBp),
        computeFundingProxySubSignal()
    ]);

    const maxScore = Math.max(kbe.score, hyAccel.score, funding.score) as 0 | 1 | 2 | 3;

    return {
        overall_score: maxScore,
        overall_status: statusFromScore(maxScore),
        kbe_signal: kbe,
        hy_acceleration_signal: hyAccel,
        funding_proxy_signal: funding
    };
}

/**
 * Helper to pull HY OAS history in bp form for the credit-funding stress
 * acceleration sub-signal. Reused by the snapshot builder which already
 * fetches HY OAS for the primary indicator.
 */
export async function fetchHyOasSeriesBp(): Promise<number[]> {
    const series = await fetchFredSeries('BAMLH0A0HYM2', 90);
    if (!series) return [];
    return series.map((p) => p.value * 100);
}

/**
 * Helper used by AI Cloud Stress for the HY divergence subsignal.
 */
export async function computeHyOasDelta4wBp(): Promise<number | null> {
    const series = await fetchFredSeries('BAMLH0A0HYM2', 60);
    if (!series) return null;
    const latest = latestPoint(series);
    const fourWeeksBack = pointDaysBack(series, 28);
    if (!latest || !fourWeeksBack) return null;
    return (latest.value - fourWeeksBack.value) * 100;
}
