import {
    fetchIndicatorHistory,
    upsertIndicatorHistory
} from '../../db/queries/macro-regime';
import type {
    IndicatorReading,
    IndicatorVelocity,
    LateCycleContext,
    RegimeSeverity,
    SubBandNextAnchor
} from './types';

type SubBandIndicatorId =
    | 'DGS10_ABS_LEVEL'
    | 'DGS10_4W_SHOCK'
    | 'HY_OAS'
    | 'VIX'
    | 'SOX_200DMA_DEVIATION'
    | 'SENTIMENT_MANUAL';
type SubBandUnit = 'bp' | 'pct' | 'pts' | 'pp';

interface BandConfig {
    id: SubBandIndicatorId;
    unit: SubBandUnit;
    thresholds: Array<{ severity: RegimeSeverity; lower: number; upper: number | null }>;
    useAbsValue?: boolean;
    anchorLabel?: (nextSeverity: RegimeSeverity, value: number) => string;
    distanceLabel?: (distance: number) => string;
}

const SUB_BAND_CONFIGS: Record<Exclude<SubBandIndicatorId, 'SENTIMENT_MANUAL'>, BandConfig> = {
    DGS10_ABS_LEVEL: {
        id: 'DGS10_ABS_LEVEL',
        unit: 'pct',
        thresholds: [
            { severity: 'Healthy', lower: 0, upper: 4.0 },
            { severity: 'Neutral', lower: 4.0, upper: 4.5 },
            { severity: 'Warning', lower: 4.5, upper: 5.0 },
            { severity: 'Critical', lower: 5.0, upper: null }
        ],
        anchorLabel: (next, value) => `${severityLabel(next)} @ ${value.toFixed(2)}%`,
        distanceLabel: (distance) => `${distance >= 0 ? '+' : ''}${Math.round(distance * 100)}bp`
    },
    DGS10_4W_SHOCK: {
        id: 'DGS10_4W_SHOCK',
        unit: 'bp',
        useAbsValue: true,
        thresholds: [
            { severity: 'Healthy', lower: 0, upper: 35 },
            { severity: 'Neutral', lower: 35, upper: 50 },
            { severity: 'Warning', lower: 50, upper: 75 },
            { severity: 'Critical', lower: 75, upper: null }
        ],
        anchorLabel: (next, value) => `${severityLabel(next)} @ ${value.toFixed(0)}bp`,
        distanceLabel: (distance) => `${distance >= 0 ? '+' : ''}${distance.toFixed(0)}bp`
    },
    HY_OAS: {
        id: 'HY_OAS',
        unit: 'bp',
        thresholds: [
            { severity: 'Healthy', lower: 0, upper: 350 },
            { severity: 'Neutral', lower: 350, upper: 450 },
            { severity: 'Warning', lower: 450, upper: 600 },
            { severity: 'Critical', lower: 600, upper: null }
        ],
        anchorLabel: (next, value) => `${severityLabel(next)} @ ${value.toFixed(0)}bp`,
        distanceLabel: (distance) => `${distance >= 0 ? '+' : ''}${distance.toFixed(0)}bp`
    },
    VIX: {
        id: 'VIX',
        unit: 'pts',
        thresholds: [
            { severity: 'Healthy', lower: 0, upper: 18 },
            { severity: 'Neutral', lower: 18, upper: 25 },
            { severity: 'Warning', lower: 25, upper: 35 },
            { severity: 'Critical', lower: 35, upper: null }
        ],
        anchorLabel: (next, value) => `${severityLabel(next)} @ ${value.toFixed(1)}`,
        distanceLabel: (distance) => `${distance >= 0 ? '+' : ''}${distance.toFixed(1)}pts`
    },
    SOX_200DMA_DEVIATION: {
        id: 'SOX_200DMA_DEVIATION',
        unit: 'pp',
        thresholds: [
            { severity: 'Healthy', lower: 0, upper: 15 },
            { severity: 'Neutral', lower: 15, upper: 30 },
            { severity: 'Warning', lower: 30, upper: 50 },
            { severity: 'Critical', lower: 50, upper: null }
        ],
        anchorLabel: (next, value) => `${severityLabel(next)} @ +${value.toFixed(0)}%`,
        distanceLabel: (distance) => `${distance >= 0 ? '+' : ''}${distance.toFixed(1)}pp`
    }
};

function severityLabel(severity: RegimeSeverity): string {
    if (severity === 'Healthy') return 'Healthy';
    if (severity === 'Neutral') return 'Neutral';
    if (severity === 'Warning') return 'Warning';
    return 'Critical';
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function round(value: number, decimals = 1): number {
    const factor = 10 ** decimals;
    return Math.round(value * factor) / factor;
}

function bandFor(config: BandConfig, value: number, severity: RegimeSeverity) {
    return config.thresholds.find((band) => band.severity === severity) ??
        config.thresholds.find((band) => band.upper === null || value < band.upper) ??
        null;
}

function nextBandFor(config: BandConfig, severity: RegimeSeverity) {
    const index = config.thresholds.findIndex((band) => band.severity === severity);
    if (index < 0 || index >= config.thresholds.length - 1) return null;
    return config.thresholds[index + 1];
}

async function velocityFor(
    indicatorId: SubBandIndicatorId,
    asOf: string,
    currentValue: number,
    unit: SubBandUnit
): Promise<IndicatorVelocity | null> {
    const history = await fetchIndicatorHistory(indicatorId, asOf, 10);
    if (history.length < 5) return null;
    const prior = history[history.length - 5];
    const value = round(currentValue - prior.raw_value, unit === 'pct' ? 2 : 1);
    const suffix = unit === 'pct'
        ? `${value >= 0 ? '+' : ''}${Math.round(value * 100)}bp / 5d`
        : unit === 'pp'
            ? `${value >= 0 ? '+' : ''}${value.toFixed(1)}pp / 5d`
        : `${value >= 0 ? '+' : ''}${value.toFixed(unit === 'pts' ? 1 : 0)}${unit} / 5d`;
    return {
        value,
        unit,
        label: suffix
    };
}

async function decorateIndicator(
    indicatorId: Exclude<SubBandIndicatorId, 'SENTIMENT_MANUAL'>,
    reading: IndicatorReading,
    asOf: string
): Promise<void> {
    if (reading.is_skipped || reading.value === null) return;
    const config = SUB_BAND_CONFIGS[indicatorId];
    const rawValue = reading.value;
    const bandValue = config.useAbsValue ? Math.abs(rawValue) : rawValue;
    const band = bandFor(config, bandValue, reading.status);
    if (band?.upper !== null && band?.upper !== undefined) {
        reading.band_position_pct = round(clamp(((bandValue - band.lower) / (band.upper - band.lower)) * 100, 0, 100), 0);
    }

    const next = nextBandFor(config, reading.status);
    if (next) {
        const distance = next.lower - bandValue;
        reading.next_anchor = {
            value: next.lower,
            label: config.anchorLabel?.(next.severity, next.lower) ?? `${severityLabel(next.severity)} @ ${next.lower}`,
            distance: round(distance, config.unit === 'pct' ? 2 : 1),
            distance_label: config.distanceLabel?.(distance)
        };
    }

    reading.velocity_5d = await velocityFor(indicatorId, asOf, rawValue, config.unit);
}

function parseFearGreedScore(summary: string): number | null {
    const match = summary.match(/F&G\s+(\d+(?:\.\d+)?)/);
    if (!match) return null;
    const score = Number(match[1]);
    return Number.isFinite(score) ? score : null;
}

function fearGreedSeverity(score: number): RegimeSeverity {
    if (score >= 75) return 'Critical';
    if (score >= 55) return 'Warning';
    return 'Neutral';
}

async function decorateFearGreed(context: LateCycleContext, asOf: string): Promise<void> {
    const pillar = context.pillars.sentiment_manual;
    const score = parseFearGreedScore(pillar.summary);
    if (score === null) return;

    // PB risk framing is asymmetric: only greed-side readings are treated as
    // add-risk. Fear-side readings do not render a sub-band risk bar.
    if (score >= 55) {
        pillar.band_position_pct = round(clamp(((score - 50) / 50) * 100, 0, 100), 0);
        if (score < 75) {
            const distance = 75 - score;
            pillar.next_anchor = {
                value: 75,
                label: '极度贪婪 @ 75',
                distance: round(distance, 1),
                distance_label: `${distance >= 0 ? '+' : ''}${distance.toFixed(0)}pts`
            };
        }
        pillar.velocity_5d = await velocityFor('SENTIMENT_MANUAL', asOf, score, 'pts');
    }
}

export async function attachSubBandMetadata(
    asOf: string,
    indicators: {
        DGS10_ABS_LEVEL: IndicatorReading;
        DGS10_4W_SHOCK: IndicatorReading;
        HY_OAS: IndicatorReading;
        VIX: IndicatorReading;
        SOX_200DMA_DEVIATION: IndicatorReading;
    },
    lateCycleContext: LateCycleContext
): Promise<void> {
    await Promise.all([
        decorateIndicator('DGS10_ABS_LEVEL', indicators.DGS10_ABS_LEVEL, asOf),
        decorateIndicator('DGS10_4W_SHOCK', indicators.DGS10_4W_SHOCK, asOf),
        decorateIndicator('HY_OAS', indicators.HY_OAS, asOf),
        decorateIndicator('VIX', indicators.VIX, asOf),
        decorateIndicator('SOX_200DMA_DEVIATION', indicators.SOX_200DMA_DEVIATION, asOf),
        decorateFearGreed(lateCycleContext, asOf)
    ]);
}

export async function persistSubBandHistory(
    asOf: string,
    indicators: {
        DGS10_ABS_LEVEL: IndicatorReading;
        DGS10_4W_SHOCK: IndicatorReading;
        HY_OAS: IndicatorReading;
        VIX: IndicatorReading;
        SOX_200DMA_DEVIATION: IndicatorReading;
    },
    lateCycleContext: LateCycleContext
): Promise<void> {
    const writes: Array<Promise<void>> = [];
    for (const [indicatorId, reading] of Object.entries(indicators) as Array<[Exclude<SubBandIndicatorId, 'SENTIMENT_MANUAL'>, IndicatorReading]>) {
        if (!reading.is_skipped && reading.value !== null) {
            writes.push(upsertIndicatorHistory({
                indicator_id: indicatorId,
                snapshot_date: asOf,
                raw_value: reading.value,
                severity: reading.status
            }));
        }
    }

    const fearGreedScore = parseFearGreedScore(lateCycleContext.pillars.sentiment_manual.summary);
    if (fearGreedScore !== null) {
        writes.push(upsertIndicatorHistory({
            indicator_id: 'SENTIMENT_MANUAL',
            snapshot_date: asOf,
            raw_value: fearGreedScore,
            severity: fearGreedSeverity(fearGreedScore)
        }));
    }

    await Promise.all(writes);
}
