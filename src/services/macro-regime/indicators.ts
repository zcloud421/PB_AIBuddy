/**
 * Macro Regime — core indicators.
 *
 * Thresholds and rules sourced directly from the 9-round-audited Portfolio
 * Optimization project spec. Do not introduce new indicators or modify
 * thresholds without re-running that audit.
 *
 * Each indicator returns a `IndicatorReading` with status (Healthy/Neutral/
 * Warning/Critical), the raw value, optional 4-week delta, human-readable
 * notes, and an is_skipped flag for fail-soft when data is missing.
 */

import type { IndicatorReading, RegimeSeverity } from './types';
import { MassiveDataFetcher, type DailyPriceBar } from '../../data/massive-fetcher';
import type { SpyHolding } from '../../data/spy-holdings-fetcher';
import type { SoxPoint } from '../../data/sox-index-fetcher';
import { fetchFredSeries, latestPoint, pointDaysBack } from '../../data/fred-series-fetcher';
import {
    fetchUsTreasuryCurve as fetchMassiveTreasuryCurve,
    latestY10,
    latestY2,
    y10DaysBack,
    type UsTreasuryPoint
} from '../../data/massive-treasury-yields';
import { fetchFredTreasuryCurve } from '../../data/fred-treasury-fallback';
import { confirmHyOasSeverity, type HyOasConfirmationResult } from './persistence';

type TreasuryCurveResult = {
    curve: UsTreasuryPoint[];
    source: string;  // human-readable vendor label for notes
};

/**
 * Treasury curve fetch with vendor fallback.
 * Massive primary (canonical Polygon Treasury Yields endpoint) → FRED fallback
 * (DGS10 + DGS2, same canonical source, just published via FRED). Both verified
 * 2026-05-18 to return identical values for 5/12-5/14. Notes downstream get
 * data date + vendor label so the UI is transparent about freshness.
 */
async function fetchTreasuryCurveWithSource(daysBack: number): Promise<TreasuryCurveResult> {
    const massive = await fetchMassiveTreasuryCurve(daysBack);
    if (massive.length > 0) {
        return { curve: massive, source: 'Massive Treasury Yields' };
    }
    const fred = await fetchFredTreasuryCurve(daysBack);
    if (fred.length > 0) {
        return { curve: fred, source: 'FRED DGS10 + DGS2 (fallback)' };
    }
    return { curve: [], source: 'unavailable' };
}

function freshnessNote(latestDate: string, source: string): string {
    return `数据截至 ${latestDate} · ${source}`;
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

function escalateSeverityOnce(severity: RegimeSeverity): RegimeSeverity {
    const ladder: RegimeSeverity[] = ['Healthy', 'Neutral', 'Warning', 'Critical'];
    const idx = ladder.indexOf(severity);
    return ladder[Math.min(idx + 1, ladder.length - 1)];
}

function makeSkipped(name: string, reason: string): IndicatorReading {
    return {
        name,
        value: null,
        status: 'Neutral',
        delta_4w: null,
        notes: [reason],
        is_skipped: true
    };
}

function todayUtcDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function ma(values: number[], window: number): number | null {
    if (values.length < window) return null;
    const slice = values.slice(-window);
    const sum = slice.reduce((acc, v) => acc + v, 0);
    return sum / window;
}

function signedPercent(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

// ─────────────────────────────────────────────────────────────────────────
// 1. HY_OAS — High-Yield Credit Spread
// ─────────────────────────────────────────────────────────────────────────

export async function computeHyOas(): Promise<IndicatorReading> {
    const series = await fetchFredSeries('BAMLH0A0HYM2', 60);
    if (!series || series.length === 0) {
        return makeSkipped('HY OAS', 'FRED BAMLH0A0HYM2 unavailable');
    }

    // FRED returns this as a percentage (e.g. 2.76 = 276bp). Convert to bp.
    const latest = latestPoint(series);
    if (!latest) {
        return makeSkipped('HY OAS', 'No recent BAMLH0A0HYM2 observations');
    }
    const valueBp = latest.value * 100;

    const fourWeeksBack = pointDaysBack(series, 28);
    const delta4wBp = fourWeeksBack ? (latest.value - fourWeeksBack.value) * 100 : null;

    let rawStatus = computeHyOasRawSeverity(valueBp);

    const notes: string[] = [];
    if (delta4wBp !== null) {
        notes.push(`Δ4w ${delta4wBp >= 0 ? '+' : ''}${delta4wBp.toFixed(0)}bp`);
    }
    // Rule: if Δ4w > +75bp, force escalate 1 step (acceleration)
    if (delta4wBp !== null && delta4wBp > 75) {
        rawStatus = escalateSeverityOnce(rawStatus);
        notes.push('Δ4w > +75bp triggered acceleration escalation');
    }

    const tightZone = buildHyOasTightZone(valueBp);
    if (tightZone) {
        notes.push('Sub-300bp:信用自满风险,信用-基本面背离监测中');
    }

    const confirmation: HyOasConfirmationResult = await confirmHyOasSeverity(rawStatus, todayUtcDate()).catch((error) => {
        console.warn('[macro-regime] HY OAS confirmation failed:', error instanceof Error ? error.message : error);
        return { confirmedSeverity: rawStatus };
    });
    if (confirmation.pendingUpgrade) {
        notes.push(`${confirmation.pendingUpgrade.target_severity} 升档待确认:已持续 ${confirmation.pendingUpgrade.confirmation_days_elapsed} / ${confirmation.pendingUpgrade.confirmation_days_required} 个交易日`);
    }

    return {
        name: 'HY OAS',
        value: Math.round(valueBp * 10) / 10,
        status: confirmation.confirmedSeverity,
        delta_4w: delta4wBp !== null ? Math.round(delta4wBp * 10) / 10 : null,
        notes,
        is_skipped: false,
        tight_zone: tightZone,
        pending_upgrade: confirmation.pendingUpgrade
    };
}

export function computeHyOasRawSeverity(valueBp: number): RegimeSeverity {
    if (valueBp < 350) return 'Healthy';
    if (valueBp < 450) return 'Neutral';
    if (valueBp < 600) return 'Warning';
    return 'Critical';
}

export function buildHyOasTightZone(valueBp: number): IndicatorReading['tight_zone'] {
    if (valueBp >= 300) return undefined;
    return {
        active: true,
        label: '信用自满区 · 接近周期低点',
        historical_anchor: '2007-06 周期低点 241bp · 2021-10 周期低点 290bp · 3/3 历史 sub-300 期均以重定价收场'
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 2. YIELD_CURVE — 10Y-2Y Spread
// ─────────────────────────────────────────────────────────────────────────

export async function computeYieldCurve(): Promise<IndicatorReading> {
    const { curve, source } = await fetchTreasuryCurveWithSource(120);
    const latest10 = latestY10(curve);
    const latest2 = latestY2(curve);
    if (latest10 === null || latest2 === null || curve.length === 0) {
        return makeSkipped('10Y-2Y', 'Treasury curve unavailable (Massive + FRED both failed)');
    }
    const latestDate = curve[curve.length - 1].date;

    const valueBp = (latest10 - latest2) * 100;
    const notes: string[] = [freshnessNote(latestDate, source)];

    let status: RegimeSeverity;
    if (valueBp > 50) {
        status = 'Healthy';
    } else if (valueBp > 0) {
        status = 'Neutral';
    } else if (valueBp > -50) {
        status = 'Warning';
    } else {
        // Critical only if persistent < -50bp for last ~60 trading days (≈3 months)
        const last60 = curve.filter((point) => point.y10 !== null && point.y2 !== null).slice(-60);
        const allInverted =
            last60.length >= 60 && last60.every((point) => (point.y10! - point.y2!) * 100 < -50);
        if (allInverted) {
            status = 'Critical';
            notes.push('< -50bp persisted ≥ 60 days');
        } else {
            status = 'Warning';
            notes.push('Below -50bp but persistence < 60 days');
        }
    }

    return {
        name: '10Y-2Y',
        value: Math.round(valueBp * 10) / 10,
        status,
        delta_4w: null,
        notes,
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 3. VIX (+ SPY RV20 quality check + persistence requirement)
// ─────────────────────────────────────────────────────────────────────────

export async function computeVix(massiveFetcher: MassiveDataFetcher): Promise<IndicatorReading> {
    const vixSeries = await fetchFredSeries('VIXCLS', 30);
    if (!vixSeries || vixSeries.length === 0) {
        return makeSkipped('VIX + RV20', 'FRED VIXCLS unavailable');
    }
    const latest = latestPoint(vixSeries);
    if (!latest) return makeSkipped('VIX + RV20', 'No VIXCLS observations');
    const vix = latest.value;

    const notes: string[] = [];
    let status: RegimeSeverity;
    if (vix < 18) status = 'Healthy';
    else if (vix < 25) status = 'Neutral';
    else if (vix < 35) status = 'Warning';
    else status = 'Critical';

    // Quality sub-check: compute SPY 20-day realized volatility (annualized %).
    // If VIX is Healthy (< 18) but RV20 > VIX + 3, vol is being suppressed.
    let spyBars: DailyPriceBar[] = [];
    try {
        spyBars = await massiveFetcher.fetchPriceHistory('SPY', 35);
    } catch {
        spyBars = [];
    }

    if (spyBars.length >= 21) {
        const recent = spyBars.slice(-21);
        const logReturns: number[] = [];
        for (let i = 1; i < recent.length; i += 1) {
            const prev = recent[i - 1].close;
            const curr = recent[i].close;
            if (prev > 0 && curr > 0) {
                logReturns.push(Math.log(curr / prev));
            }
        }
        if (logReturns.length >= 15) {
            const mean = logReturns.reduce((s, v) => s + v, 0) / logReturns.length;
            const variance =
                logReturns.reduce((s, v) => s + (v - mean) * (v - mean), 0) / (logReturns.length - 1);
            const rv20 = Math.sqrt(variance) * Math.sqrt(252) * 100;
            notes.push(`RV20 = ${rv20.toFixed(1)}`);
            if (status === 'Healthy' && rv20 > vix + 3) {
                status = 'Neutral';
                notes.push('Vol suppression (RV20 > VIX + 3) → downgraded Healthy → Neutral');
            }
        }
    } else {
        notes.push('SPY 20d bars unavailable for RV20 quality check');
    }

    // Persistence requirement for Warning+: VIX ≥ 25 must hold ≥ 3 sessions.
    // Single-day spikes don't escalate.
    if (status === 'Warning' || status === 'Critical') {
        const lastThree = vixSeries.slice(-3);
        const persistent = lastThree.length >= 3 && lastThree.every((point) => point.value >= 25);
        if (!persistent) {
            status = 'Neutral';
            notes.push('VIX spike not persistent (< 3 sessions ≥ 25) → reverted to Neutral');
        }
    }

    return {
        name: 'VIX + RV20',
        value: Math.round(vix * 10) / 10,
        status,
        delta_4w: null,
        notes,
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 4. 10Y Yield 4-Week Shock
// ─────────────────────────────────────────────────────────────────────────

export async function computeDgs10FourWeekShock(): Promise<IndicatorReading> {
    const { curve, source } = await fetchTreasuryCurveWithSource(40);
    const latest = latestY10(curve);
    const fourWeeksBack = y10DaysBack(curve, 28);
    if (latest === null || fourWeeksBack === null || curve.length === 0) {
        return makeSkipped('10Y yield 4w shock', 'Treasury curve unavailable (Massive + FRED both failed)');
    }
    const latestDate = curve[curve.length - 1].date;

    const deltaBp = (latest - fourWeeksBack) * 100;
    const absDelta = Math.abs(deltaBp);

    let status: RegimeSeverity;
    if (absDelta < 35) status = 'Healthy';
    else if (absDelta < 50) status = 'Neutral';
    else if (absDelta < 75) status = 'Warning';
    else status = 'Critical';

    return {
        name: '10Y yield 4w shock',
        value: Math.round(deltaBp * 10) / 10,
        status,
        delta_4w: Math.round(deltaBp * 10) / 10,
        notes: [
            freshnessNote(latestDate, source),
            `Latest 10Y ${latest.toFixed(2)}%; 4w prior ${fourWeeksBack.toFixed(2)}%`
        ],
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 4b. 10Y Absolute Level — DGS10 waterline
// ─────────────────────────────────────────────────────────────────────────

export async function computeDgs10AbsLevel(): Promise<IndicatorReading> {
    const { curve, source } = await fetchTreasuryCurveWithSource(40);
    const latest = latestY10(curve);
    if (latest === null || curve.length === 0) {
        return makeSkipped('10Y absolute level', 'Treasury curve unavailable (Massive + FRED both failed)');
    }
    const latestDate = curve[curve.length - 1].date;
    const fourWeeksBack = y10DaysBack(curve, 28);
    const delta4w = fourWeeksBack !== null ? (latest - fourWeeksBack) * 100 : null;

    // Anchors:
    // <4.0%: long-run mean neighborhood; 4.0–4.5%: upper valuation compression band;
    // 4.5–5.0%: P/E compression zone observed in multiple risk-asset selloffs;
    // >5.0%: 2023-10 risk-asset break point.
    let status: RegimeSeverity;
    if (latest < 4.0) status = 'Healthy';
    else if (latest < 4.5) status = 'Neutral';
    else if (latest <= 5.0) status = 'Warning';
    else status = 'Critical';

    const zone =
        status === 'Healthy' ? 'Healthy zone' :
            status === 'Neutral' ? 'Neutral zone' :
                status === 'Warning' ? 'Warning zone' : 'Critical zone';

    return {
        name: '10Y absolute level',
        value: Math.round(latest * 100) / 100,
        status,
        delta_4w: delta4w !== null ? Math.round(delta4w * 10) / 10 : null,
        notes: [
            freshnessNote(latestDate, source),
            `absolute ${latest.toFixed(2)}% (${zone})`,
            '4.5%+ marks the P/E compression zone; >5.0% echoes the 2023-10 risk-asset break'
        ],
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 4c. Top-10 Concentration — S&P 500 structural tail risk
// ─────────────────────────────────────────────────────────────────────────

function topTenWeight(holdings: SpyHolding[]): number {
    return [...holdings]
        .filter((holding) => Number.isFinite(holding.weight_pct) && holding.weight_pct > 0)
        .sort((left, right) => right.weight_pct - left.weight_pct)
        .slice(0, 10)
        .reduce((sum, holding) => sum + holding.weight_pct, 0);
}

function hhi(holdings: SpyHolding[]): number {
    return Math.round(
        holdings.reduce((sum, holding) => sum + holding.weight_pct * holding.weight_pct, 0)
    );
}

export function computeConcentration(spyHoldings: SpyHolding[] | null): IndicatorReading {
    if (!spyHoldings || spyHoldings.length === 0) {
        return makeSkipped('Top-10 concentration', 'SPY holdings unavailable');
    }
    const value = topTenWeight(spyHoldings);
    const hhiValue = hhi(spyHoldings);

    // Anchors:
    // <25% = pre-2015 historical zone; 25–32% = 2020–2022 range;
    // 32–38% = warning band; >38% = extreme concentration. This indicator is
    // soft-capped at Warning in v1.6 so it cannot mechanically create Critical
    // by itself; it is a structural tail-risk context, not a timing trigger.
    let status: RegimeSeverity;
    if (value < 25) status = 'Healthy';
    else if (value < 32) status = 'Neutral';
    else status = 'Warning';

    return {
        name: 'Top-10 concentration',
        value: Math.round(value * 10) / 10,
        status,
        delta_4w: null,
        notes: [
            `Top-10 ~${value.toFixed(1)}%`,
            `HHI ~${hhiValue}`,
            value > 38
                ? 'Extreme concentration (>38%) soft-capped to Warning; does not solo-trigger Critical'
                : 'Top-heavy market structure raises mechanical tail risk'
        ],
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 5. AI_BREADTH — 16 mega-cap weighted % above 50DMA
// ─────────────────────────────────────────────────────────────────────────

const AI_BREADTH_UNIVERSE: Array<{ ticker: string; tier: 1 | 2 | 3 }> = [
    { ticker: 'NVDA', tier: 1 },
    { ticker: 'MSFT', tier: 1 },
    { ticker: 'GOOG', tier: 1 },
    { ticker: 'AMZN', tier: 1 },
    { ticker: 'META', tier: 1 },
    { ticker: 'AVGO', tier: 1 },
    { ticker: 'AMD', tier: 2 },
    { ticker: 'TSM', tier: 2 },
    { ticker: 'MU', tier: 2 },
    { ticker: 'ANET', tier: 2 },
    { ticker: 'MRVL', tier: 2 },
    { ticker: 'CRDO', tier: 2 },
    { ticker: 'LITE', tier: 2 },
    { ticker: 'VRT', tier: 3 },
    { ticker: 'GEV', tier: 3 },
    { ticker: 'ALAB', tier: 3 }
];

const TIER_WEIGHT: Record<1 | 2 | 3, number> = { 1: 2.0, 2: 1.5, 3: 1.0 };

export async function computeAiBreadth(
    massiveFetcher: MassiveDataFetcher
): Promise<IndicatorReading> {
    const results = await Promise.allSettled(
        AI_BREADTH_UNIVERSE.map(async ({ ticker, tier }) => {
            // 100 calendar days ≈ 70 trading bars (need 50 for MA50 with safety buffer)
            const bars = await massiveFetcher.fetchPriceHistory(ticker, 100);
            return { ticker, tier, bars };
        })
    );

    const valid: Array<{ ticker: string; tier: 1 | 2 | 3; aboveMa50: boolean }> = [];
    const failed: string[] = [];

    for (const result of results) {
        if (result.status !== 'fulfilled') {
            failed.push('unknown');
            continue;
        }
        const { ticker, tier, bars } = result.value;
        if (bars.length < 50) {
            failed.push(ticker);
            continue;
        }
        const ma50 = ma(
            bars.map((b) => b.close),
            50
        );
        const close = bars[bars.length - 1].close;
        if (ma50 === null) {
            failed.push(ticker);
            continue;
        }
        valid.push({ ticker, tier, aboveMa50: close > ma50 });
    }

    if (valid.length < AI_BREADTH_UNIVERSE.length * 0.7) {
        return makeSkipped(
            'AI Breadth (16 mega cap)',
            `Insufficient data: ${valid.length}/${AI_BREADTH_UNIVERSE.length} resolved`
        );
    }

    const totalWeight = valid.reduce((sum, v) => sum + TIER_WEIGHT[v.tier], 0);
    const aboveWeight = valid
        .filter((v) => v.aboveMa50)
        .reduce((sum, v) => sum + TIER_WEIGHT[v.tier], 0);
    const weightedPct = totalWeight > 0 ? (aboveWeight / totalWeight) * 100 : 0;

    let status: RegimeSeverity;
    if (weightedPct > 75) status = 'Healthy';
    else if (weightedPct > 50) status = 'Neutral';
    else if (weightedPct > 30) status = 'Warning';
    else status = 'Critical';

    const tier1 = valid.filter((v) => v.tier === 1);
    const tier1Below = tier1.filter((v) => !v.aboveMa50);
    const notes: string[] = [];
    notes.push(`${valid.length}/${AI_BREADTH_UNIVERSE.length} active`);
    notes.push(`Tier 1: ${tier1Below.length}/${tier1.length} below 50DMA`);
    if (failed.length > 0) {
        notes.push(`Skipped tickers: ${failed.slice(0, 4).join(',')}${failed.length > 4 ? '…' : ''}`);
    }

    // Special guardrail: if ≥ 4 Tier 1 names below 50DMA, force Critical
    // (but only escalate to actionable if weighted < 30% OR 10-day persistence).
    // For snapshot purposes here we surface Critical status but leave the
    // "action eligible" gating to the aggregation layer.
    if (tier1Below.length >= 4) {
        status = 'Critical';
        notes.push('Tier 1 guardrail: ≥ 4 Tier 1 below 50DMA → Critical');
    }

    return {
        name: 'AI Breadth (16 mega cap)',
        value: Math.round(weightedPct * 10) / 10,
        status,
        delta_4w: null,
        notes,
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 5b. SOX_200DMA_DEVIATION — semiconductor stretch vs 200DMA
// ─────────────────────────────────────────────────────────────────────────

export function computeSox200DmaDeviation(
    history: SoxPoint[],
    sourceLabel = 'Polygon I:SOX'
): IndicatorReading {
    if (history.length < 200) {
        return makeSkipped('SOX 200DMA Deviation', '数据不足 200 交易日');
    }

    const closes = history.map((point) => point.close);
    const ma200 = ma(closes, 200);
    const latest = history[history.length - 1];
    if (ma200 === null || ma200 <= 0 || latest.close <= 0) {
        return makeSkipped('SOX 200DMA Deviation', 'SOX 200DMA unavailable');
    }

    const deviationPct = ((latest.close - ma200) / ma200) * 100;

    let status: RegimeSeverity;
    if (deviationPct < 15) status = 'Healthy';
    else if (deviationPct < 30) status = 'Neutral';
    else if (deviationPct < 50) status = 'Warning';
    else status = 'Critical';

    return {
        name: 'SOX 200DMA Deviation',
        value: Math.round(deviationPct * 10) / 10,
        status,
        delta_4w: null,
        notes: [
            `SOX 当前 ${signedPercent(deviationPct)} vs 200 日均线`,
            '历史泡沫峰值:Mississippi 1720 +73% · Dotcom 2000 +55% · 历史均值 +35%(BofA Hartnett《Flow Show》2026-05-15)',
            '仅作估值过热预警,需 VIX / 龙头动量 / 利率冲击共振才会触发减仓',
            `数据截至 ${latest.date} · ${sourceLabel}`
        ],
        is_skipped: false
    };
}

// ─────────────────────────────────────────────────────────────────────────
// 6. BROAD_BREADTH — Nasdaq-100 % above 200DMA (equal weight)
// ─────────────────────────────────────────────────────────────────────────

// NDX components are large and change occasionally. Hardcoding a snapshot list
// matches the Python project's approach and avoids per-snapshot index lookup.
// If components shift materially, refresh this constant; the indicator
// quietly skips tickers that fail to resolve.
const NDX_COMPONENTS: string[] = [
    'AAPL', 'ABNB', 'ADBE', 'ADI', 'ADP', 'ADSK', 'AEP', 'AMAT', 'AMD', 'AMGN',
    'AMZN', 'ANSS', 'APP', 'ARM', 'ASML', 'AVGO', 'AXON', 'AZN', 'BIIB', 'BKNG',
    'BKR', 'CCEP', 'CDNS', 'CDW', 'CEG', 'CHTR', 'CMCSA', 'COST', 'CPRT', 'CRWD',
    'CSCO', 'CSGP', 'CSX', 'CTAS', 'CTSH', 'DASH', 'DDOG', 'DLTR', 'DXCM', 'EA',
    'EXC', 'FANG', 'FAST', 'FTNT', 'GEHC', 'GFS', 'GILD', 'GOOG', 'GOOGL', 'HON',
    'IDXX', 'INTC', 'INTU', 'ISRG', 'KDP', 'KHC', 'KLAC', 'LIN', 'LRCX', 'LULU',
    'MAR', 'MCHP', 'MDB', 'MDLZ', 'MELI', 'META', 'MNST', 'MRVL', 'MSFT', 'MSTR',
    'MU', 'NFLX', 'NVDA', 'NXPI', 'ODFL', 'ON', 'ORLY', 'PANW', 'PAYX', 'PCAR',
    'PDD', 'PEP', 'PLTR', 'PYPL', 'QCOM', 'REGN', 'ROP', 'ROST', 'SBUX', 'SNPS',
    'TEAM', 'TMUS', 'TSLA', 'TTD', 'TTWO', 'TXN', 'VRSK', 'VRTX', 'WBD', 'WDAY',
    'XEL', 'ZS'
];

export async function computeBroadBreadth(
    massiveFetcher: MassiveDataFetcher
): Promise<IndicatorReading> {
    const results = await Promise.allSettled(
        NDX_COMPONENTS.map(async (ticker) => {
            // 330 calendar days ≈ 230 trading bars (need 200 for MA200 + 20 buffer for Δ4w lookback)
            const bars = await massiveFetcher.fetchPriceHistory(ticker, 330);
            return { ticker, bars };
        })
    );

    const valid: Array<{ ticker: string; aboveMa200: boolean }> = [];
    const failed: string[] = [];

    for (const result of results) {
        if (result.status !== 'fulfilled') {
            failed.push('unknown');
            continue;
        }
        const { ticker, bars } = result.value;
        if (bars.length < 200) {
            failed.push(ticker);
            continue;
        }
        const ma200 = ma(
            bars.map((b) => b.close),
            200
        );
        const close = bars[bars.length - 1].close;
        if (ma200 === null) {
            failed.push(ticker);
            continue;
        }
        valid.push({ ticker, aboveMa200: close > ma200 });
    }

    if (valid.length < NDX_COMPONENTS.length * 0.7) {
        return makeSkipped(
            'NDX-100 above 200DMA',
            `Insufficient data: ${valid.length}/${NDX_COMPONENTS.length} resolved`
        );
    }

    const pct = (valid.filter((v) => v.aboveMa200).length / valid.length) * 100;

    let status: RegimeSeverity;
    if (pct > 65) status = 'Healthy';
    else if (pct > 45) status = 'Neutral';
    else if (pct > 30) status = 'Warning';
    else status = 'Critical';

    const notes: string[] = [`${valid.length}/${NDX_COMPONENTS.length} active`];

    // Special escalation: if 4w breadth Δ < -25pp, force escalate one step.
    // Compute by snapshotting MA200 with bars 4 weeks (≈20 trading days) ago.
    let delta4wPp: number | null = null;
    try {
        const fourWeeksAgoValid: Array<{ aboveMa200: boolean }> = [];
        for (const result of results) {
            if (result.status !== 'fulfilled') continue;
            const { bars } = result.value;
            if (bars.length < 220) continue;
            const barsThen = bars.slice(0, bars.length - 20);
            if (barsThen.length < 200) continue;
            const ma200Then = ma(
                barsThen.map((b) => b.close),
                200
            );
            const closeThen = barsThen[barsThen.length - 1].close;
            if (ma200Then === null) continue;
            fourWeeksAgoValid.push({ aboveMa200: closeThen > ma200Then });
        }
        if (fourWeeksAgoValid.length >= NDX_COMPONENTS.length * 0.6) {
            const pctThen =
                (fourWeeksAgoValid.filter((v) => v.aboveMa200).length / fourWeeksAgoValid.length) * 100;
            delta4wPp = pct - pctThen;
            notes.push(`Δ4w ${delta4wPp >= 0 ? '+' : ''}${delta4wPp.toFixed(1)}pp`);
            if (delta4wPp < -25) {
                status = escalateSeverityOnce(status);
                notes.push('Δ4w < -25pp triggered escalation');
            }
        }
    } catch {
        // Best-effort; delta computation failure does not break primary reading.
    }

    return {
        name: 'NDX-100 above 200DMA',
        value: Math.round(pct * 10) / 10,
        status,
        delta_4w: delta4wPp !== null ? Math.round(delta4wPp * 10) / 10 : null,
        notes,
        is_skipped: false
    };
}
