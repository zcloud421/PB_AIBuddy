import { pool } from '../db/client';
import {
    getIdeaCandidatesMissingRiskReward,
    getRecentPriceHistoryBySymbol,
    updateIdeaCandidateRiskRewardScore
} from '../db/queries/ideas';

type TailRiskStats = {
    max_drawdown_pct: number | null;
    worst_episode: {
        recovery_days: number | null;
        total_duration_days: number | null;
        recovered: boolean;
    } | null;
} | null;

type StrikeRiskSummary = {
    breachCount: number;
};

type ThresholdDrawdownEvent = {
    peak_date: string;
    trough_date: string;
    max_drawdown_pct: number;
};

async function main() {
    const limitArg = process.argv.find((arg) => arg.startsWith('--limit='));
    const limit = limitArg ? Number(limitArg.split('=')[1]) : 500;
    const dryRun = process.argv.includes('--dry-run');

    const rows = await getIdeaCandidatesMissingRiskReward(Number.isFinite(limit) ? limit : 500);
    console.log(`[backfill-risk-reward] found ${rows.length} rows missing risk_reward_score`);

    let updated = 0;
    let skipped = 0;

    for (const row of rows) {
        const priceHistory = await getRecentPriceHistoryBySymbol(row.symbol, 1500).catch(() => []);
        const tailRisk = buildTailRiskStats(priceHistory);
        const strikeRisk = buildStrikeRiskSummary(priceHistory, row.current_price, row.recommended_strike);
        const riskRewardScore = calculateRiskRewardScore({
            premiumScore: parseRefCouponPctToPremiumScore(toNullableNumber(row.ref_coupon_pct)),
            ivScore: clampScore(toNullableNumber(row.iv_rank_score) ?? 0.45, 0, 1),
            skewScore: clampScore(toNullableNumber(row.skew_score) ?? 0.45, 0, 1),
            tailRisk,
            strikeRisk
        });

        if (riskRewardScore === null) {
            skipped += 1;
            continue;
        }

        if (!dryRun) {
            await updateIdeaCandidateRiskRewardScore(row.run_id, row.symbol, riskRewardScore);
        }

        updated += 1;
        console.log(
            `[backfill-risk-reward] ${dryRun ? 'would update' : 'updated'} ${row.symbol} ${row.run_id} => ${riskRewardScore}`
        );
    }

    console.log(
        `[backfill-risk-reward] complete updated=${updated} skipped=${skipped} dryRun=${dryRun ? 'yes' : 'no'}`
    );

    await pool.end();
}

function calculateRiskRewardScore(input: {
    premiumScore: number | null;
    ivScore: number;
    skewScore: number;
    tailRisk: TailRiskStats;
    strikeRisk: StrikeRiskSummary | null;
}): number | null {
    const maxDrawdownScore =
        input.tailRisk?.max_drawdown_pct === null || input.tailRisk?.max_drawdown_pct === undefined
            ? 0.45
            : input.tailRisk.max_drawdown_pct >= -25
              ? 0.85
              : input.tailRisk.max_drawdown_pct >= -40
                ? 0.65
                : input.tailRisk.max_drawdown_pct >= -55
                  ? 0.45
                  : 0.25;
    const recoveryScore =
        !input.tailRisk?.worst_episode
            ? 0.5
            : !input.tailRisk.worst_episode.recovered
              ? 0.25
              : (input.tailRisk.worst_episode.total_duration_days ?? 0) <= 60
                ? 0.85
                : (input.tailRisk.worst_episode.total_duration_days ?? 0) <= 180
                  ? 0.6
                  : 0.35;
    const thresholdBreachScore =
        !input.strikeRisk
            ? 0.5
            : input.strikeRisk.breachCount === 0
              ? 0.9
              : input.strikeRisk.breachCount <= 2
                ? 0.65
                : input.strikeRisk.breachCount <= 5
                  ? 0.45
                  : 0.25;

    const score =
        ((input.premiumScore ?? 0.5) * 0.25) +
        (input.ivScore * 0.15) +
        (input.skewScore * 0.1) +
        (maxDrawdownScore * 0.2) +
        (recoveryScore * 0.15) +
        (thresholdBreachScore * 0.15);

    return Number(clampScore(score, 0, 1).toFixed(4));
}

function parseRefCouponPctToPremiumScore(refCouponPct: number | null): number | null {
    if (refCouponPct === null || !Number.isFinite(refCouponPct)) {
        return null;
    }

    if (refCouponPct >= 20) {
        return 1;
    }

    if (refCouponPct >= 10) {
        return 0.6;
    }

    return 0.3;
}

function buildStrikeRiskSummary(
    history: Array<{ date: string; close: number }>,
    currentPrice: number | null,
    strike: number | null
): StrikeRiskSummary | null {
    if (history.length < 2 || currentPrice === null || currentPrice <= 0 || strike === null || strike <= 0) {
        return null;
    }

    const thresholdPct = Math.abs(((strike / currentPrice) - 1) * 100);
    const events = buildThresholdDrawdownEvents(history, thresholdPct);

    return {
        breachCount: events.length
    };
}

function buildThresholdDrawdownEvents(
    history: Array<{ date: string; close: number }>,
    thresholdPct: number
): ThresholdDrawdownEvent[] {
    const extrema = buildLocalExtrema(history, 15);
    const events: ThresholdDrawdownEvent[] = [];

    for (let index = 0; index < extrema.length - 1; index += 1) {
        const current = extrema[index];
        const next = extrema[index + 1];
        if (current.type !== 'peak' || next.type !== 'trough') {
            continue;
        }

        const drawdownPct = ((next.price / current.price) - 1) * 100;
        if (Math.abs(drawdownPct) < thresholdPct) {
            continue;
        }

        events.push({
            peak_date: current.date,
            trough_date: next.date,
            max_drawdown_pct: Number(drawdownPct.toFixed(1))
        });
    }

    return dedupeNearbyThresholdEvents(events);
}

function buildLocalExtrema(history: Array<{ date: string; close: number }>, windowSize: number) {
    const extrema: Array<{ type: 'peak' | 'trough'; date: string; price: number }> = [];

    for (let index = 0; index < history.length; index += 1) {
        const start = Math.max(0, index - windowSize);
        const end = Math.min(history.length - 1, index + windowSize);
        const slice = history.slice(start, end + 1).map((point) => point.close);
        const current = history[index].close;
        const localMax = Math.max(...slice);
        const localMin = Math.min(...slice);

        if (current === localMax) {
            extrema.push({ type: 'peak', date: history[index].date, price: current });
            continue;
        }

        if (current === localMin) {
            extrema.push({ type: 'trough', date: history[index].date, price: current });
        }
    }

    const compressed: Array<{ type: 'peak' | 'trough'; date: string; price: number }> = [];
    for (const point of extrema) {
        const previous = compressed[compressed.length - 1];
        if (!previous) {
            compressed.push(point);
            continue;
        }

        if (previous.type !== point.type) {
            compressed.push(point);
            continue;
        }

        const shouldReplace = point.type === 'peak' ? point.price >= previous.price : point.price <= previous.price;
        if (shouldReplace) {
            compressed[compressed.length - 1] = point;
        }
    }

    return compressed;
}

function dedupeNearbyThresholdEvents(events: ThresholdDrawdownEvent[]): ThresholdDrawdownEvent[] {
    if (events.length <= 1) {
        return events;
    }

    const deduped: ThresholdDrawdownEvent[] = [events[0]];
    for (let index = 1; index < events.length; index += 1) {
        const current = events[index];
        const previous = deduped[deduped.length - 1];
        const gapDays = daysBetweenIso(previous.trough_date, current.peak_date);
        if (gapDays !== null && gapDays <= 20) {
            if (current.max_drawdown_pct < previous.max_drawdown_pct) {
                deduped[deduped.length - 1] = current;
            }
            continue;
        }
        deduped.push(current);
    }

    return deduped;
}

function daysBetweenIso(fromDate: string, toDate: string): number | null {
    const from = Date.parse(fromDate);
    const to = Date.parse(toDate);
    if (Number.isNaN(from) || Number.isNaN(to)) {
        return null;
    }

    return Math.round((to - from) / (24 * 60 * 60 * 1000));
}

function buildTailRiskStats(priceHistory: Array<{ date: string; close: number }>): TailRiskStats {
    const normalizedHistory = priceHistory
        .map((point) => ({
            date: point.date,
            close: toFiniteNumber(point.close)
        }))
        .filter((point): point is { date: string; close: number } => point.close !== null);

    if (normalizedHistory.length < 2) {
        return null;
    }

    const episodes: Array<{
        max_drawdown_pct: number;
        recovery_days: number | null;
        total_duration_days: number | null;
        recovered: boolean;
    }> = [];

    let peakIndex = 0;
    let peakPrice = normalizedHistory[0].close;
    let activeEpisode:
        | {
              peakIndex: number;
              troughIndex: number;
              maxDrawdownPct: number;
          }
        | null = null;

    for (let index = 1; index < normalizedHistory.length; index += 1) {
        const point = normalizedHistory[index];

        if (point.close >= peakPrice) {
            if (activeEpisode) {
                episodes.push({
                    max_drawdown_pct: roundPct(activeEpisode.maxDrawdownPct),
                    recovery_days: index - activeEpisode.troughIndex,
                    total_duration_days: index - activeEpisode.peakIndex,
                    recovered: true
                });
                activeEpisode = null;
            }

            peakIndex = index;
            peakPrice = point.close;
            continue;
        }

        const drawdownPct = ((point.close / peakPrice) - 1) * 100;
        if (!activeEpisode) {
            activeEpisode = {
                peakIndex,
                troughIndex: index,
                maxDrawdownPct: drawdownPct
            };
            continue;
        }

        if (drawdownPct < activeEpisode.maxDrawdownPct) {
            activeEpisode.troughIndex = index;
            activeEpisode.maxDrawdownPct = drawdownPct;
        }
    }

    if (activeEpisode) {
        episodes.push({
            max_drawdown_pct: roundPct(activeEpisode.maxDrawdownPct),
            recovery_days: null,
            total_duration_days: null,
            recovered: false
        });
    }

    const worstEpisode =
        episodes.length === 0
            ? null
            : episodes.reduce((worst, episode) =>
                  episode.max_drawdown_pct < worst.max_drawdown_pct ? episode : worst
              );

    return {
        max_drawdown_pct: worstEpisode?.max_drawdown_pct ?? null,
        worst_episode: worstEpisode
            ? {
                  recovery_days: worstEpisode.recovery_days,
                  total_duration_days: worstEpisode.total_duration_days,
                  recovered: worstEpisode.recovered
              }
            : null
    };
}

function roundPct(value: number): number {
    const numeric = toFiniteNumber(value);
    return numeric === null ? 0 : Number(numeric.toFixed(1));
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const numeric = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

function toNullableNumber(value: number | string | null | undefined): number | null {
    return toFiniteNumber(value);
}

function clampScore(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

void main().catch(async (error) => {
    console.error(
        `[backfill-risk-reward] failed: ${error instanceof Error ? error.stack ?? error.message : String(error)}`
    );
    await pool.end().catch(() => undefined);
    process.exitCode = 1;
});
