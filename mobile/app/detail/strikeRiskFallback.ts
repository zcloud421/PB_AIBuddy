import { matchMacroEvent } from '../../constants/macro-events';
import { formatPrice } from '../../constants/display';
import type {
  DrawdownAttribution,
  InteractiveStrikeRiskSummary,
  PriceHistoryPoint,
  TailRiskFooterSummary,
  TailRiskStats,
} from '../../types/api';

export function resolveTailRiskFooterSummary(
  backendSummary: TailRiskFooterSummary | null,
  tailRisk: TailRiskStats | null,
) {
  if (backendSummary) {
    return {
      maxDrawdownText: backendSummary.max_drawdown_text,
      longestRecoveryText: backendSummary.longest_recovery_text,
      hasUnrecoveredDrawdown: backendSummary.has_unrecovered_drawdown,
    };
  }

  return buildTailRiskFooterFallback(tailRisk);
}

function buildTailRiskFooterFallback(
  tailRisk: TailRiskStats | null,
) {
  if (!tailRisk) {
    return null;
  }

  const maxYear = tailRisk.max_drawdown_trough_date?.slice(0, 4) ?? '—';
  const hasUnrecoveredDrawdown = tailRisk.worst_episode?.recovered === false;
  const longestRecoveryDays = hasUnrecoveredDrawdown
    ? null
    : tailRisk.longest_recovery_episode?.recovery_days ?? null;
  const longestRecoveryYear = hasUnrecoveredDrawdown
    ? '—'
    : tailRisk.longest_recovery_episode?.trough_date?.slice(0, 4) ?? '—';

  return {
    maxDrawdownText: `${formatSignedPercent(tailRisk.max_drawdown_pct)} (${maxYear})`,
    longestRecoveryText: `${longestRecoveryDays !== null ? formatDaysLabel(longestRecoveryDays) : '—'} (${longestRecoveryYear})`,
    hasUnrecoveredDrawdown,
  };
}

export function buildInteractiveStrikeRiskSummaryFallback(
  history: PriceHistoryPoint[],
  currentPrice: number | null,
  strikePct: number | null,
  symbol: string,
  drawdownAttributions: DrawdownAttribution[] = [],
): InteractiveStrikeRiskSummary | null {
  if (
    history.length < 2 ||
    currentPrice === null ||
    currentPrice <= 0 ||
    strikePct === null ||
    strikePct <= 0 ||
    strikePct > 100
  ) {
    return null;
  }

  const thresholdPct = Math.abs(strikePct - 100);
  const allEpisodes = buildDrawdownEpisodes(history);
  const meaningfulEpisodes = allEpisodes.filter((event) => Math.abs(event.max_drawdown_pct) >= 10);
  const rawBreachEvents = allEpisodes.filter((event) => Math.abs(event.max_drawdown_pct) >= thresholdPct);

  const resolveEventAttribution = (event: typeof rawBreachEvents[number]) =>
    drawdownAttributions.find(
      (item) => item.peak_date === event.peak_date && item.trough_date === event.trough_date,
    ) ?? null;

  const resolveEventReason = (event: typeof rawBreachEvents[number]) =>
    resolveEventAttribution(event)?.reason_zh ?? matchMacroEvent(event.peak_date, event.trough_date, symbol)?.reason_zh ?? null;

  const resolveEventMacroId = (event: typeof rawBreachEvents[number]): string | null =>
    resolveEventAttribution(event)?.primary_rule_id ?? matchMacroEvent(event.peak_date, event.trough_date, symbol)?.id ?? null;

  const allBreachEventsHaveBackendAttribution = rawBreachEvents.every((event) =>
    resolveEventAttribution(event)
  );

  const events: typeof rawBreachEvents = allBreachEventsHaveBackendAttribution
    ? [...rawBreachEvents]
    : rawBreachEvents.reduce<typeof rawBreachEvents>((acc, event) => {
        const isSecondaryDip = acc.some((prior) => {
          const daysDiff =
            (new Date(event.peak_date).getTime() - new Date(prior.peak_date).getTime()) /
            (1000 * 60 * 60 * 24);
          const eventMacroId = resolveEventMacroId(event);
          const priorMacroId = resolveEventMacroId(prior);
          return daysDiff < 900 && event.peak_price <= prior.peak_price && eventMacroId === priorMacroId;
        });
        if (!isSecondaryDip) {
          acc.push(event);
        }
        return acc;
      }, []);

  const eventsWithBelowStrikeDays = events.map((event) => {
    const episodeBarrier = event.peak_price * (strikePct / 100);
    const peakIdx = history.findIndex((p) => p.date === event.peak_date);
    if (peakIdx === -1) return { ...event, days_below_strike: null as number | null };

    let firstBreachIdx = -1;
    for (let i = peakIdx; i < history.length; i += 1) {
      if (history[i].close < episodeBarrier) {
        firstBreachIdx = i;
        break;
      }
    }
    if (firstBreachIdx === -1) return { ...event, days_below_strike: null as number | null };

    const troughIdx = history.findIndex((p) => p.date === event.trough_date);
    const searchFromIdx = troughIdx !== -1 ? troughIdx : firstBreachIdx;

    let recoveryIdx = -1;
    for (let i = searchFromIdx + 1; i < history.length; i += 1) {
      if (history[i].close >= episodeBarrier) {
        recoveryIdx = i;
        break;
      }
    }

    return {
      ...event,
      days_below_strike: recoveryIdx === -1 ? null : (recoveryIdx - firstBreachIdx) as number | null,
    };
  });
  const annotatedEvents = eventsWithBelowStrikeDays.map((event) => ({
    ...event,
    reason_zh: resolveEventReason(event),
    display_order: resolveEventAttribution(event)?.display_order ?? null,
  }));
  const groupedEvents = groupAnnotatedDrawdownEventsFallback(annotatedEvents);
  const belowStrikeDays = eventsWithBelowStrikeDays
    .map((event) => event.days_below_strike)
    .filter((value): value is number => value !== null);
  const sortedRecoveryDays = [...belowStrikeDays].sort((left, right) => left - right);

  const medianRecoveryDays =
    sortedRecoveryDays.length > 0 ? calculateMedian(sortedRecoveryDays) : null;
  const averageRecoveryDays =
    sortedRecoveryDays.length > 0
      ? Math.round(sortedRecoveryDays.reduce((sum, value) => sum + value, 0) / sortedRecoveryDays.length)
      : null;
  const longestRecoveryDays = sortedRecoveryDays.length > 0 ? sortedRecoveryDays[sortedRecoveryDays.length - 1] : null;
  const recoveredWithin30 = belowStrikeDays.filter((days) => days <= 30).length;
  const recoveredWithin180 = belowStrikeDays.filter((days) => days > 30 && days <= 180).length;
  const recoveredOver180 = belowStrikeDays.filter((days) => days > 180).length;
  const unrecoveredInDistribution = eventsWithBelowStrikeDays.filter(
    (event) => event.days_below_strike === null
  ).length;
  const unrecoveredCount = unrecoveredInDistribution;
  const partiallyRecoveredCount = eventsWithBelowStrikeDays.filter(
    (event) => event.closed_by_partial_recovery && event.recovery_days !== null
  ).length;
  const fullyRecoveredCount = eventsWithBelowStrikeDays.filter((event) => event.recovered).length;
  const totalRecoveredCount = fullyRecoveredCount + partiallyRecoveredCount;
  const breachProbabilityPct =
    meaningfulEpisodes.length > 0 ? Math.round((events.length / meaningfulEpisodes.length) * 100) : null;
  const maxOvershootPct =
    events.length > 0
      ? Math.max(...events.map((event) => Math.abs(event.max_drawdown_pct) - thresholdPct))
      : null;
  const strikePrice = currentPrice * (strikePct / 100);
  const breachFrequencyLabel =
    breachProbabilityPct !== null && breachProbabilityPct >= 50
      ? '较高'
      : breachProbabilityPct !== null && breachProbabilityPct >= 20
        ? '中等'
        : '较低';
  const tailRiskLabel =
    unrecoveredCount > 0 || (maxOvershootPct !== null && maxOvershootPct >= 25) || (longestRecoveryDays !== null && longestRecoveryDays >= 180)
      ? '较高'
      : (maxOvershootPct !== null && maxOvershootPct >= 12) || (longestRecoveryDays !== null && longestRecoveryDays >= 60)
        ? '中等'
        : '较低';
  const conclusionStatsLine =
    events.length === 0
      ? `过去5年出现 ${meaningfulEpisodes.length} 次跌幅≥10%的回撤，均未触及执行价对应的 -${thresholdPct.toFixed(0)}% 跌幅。`
      : `过去5年出现 ${meaningfulEpisodes.length} 次跌幅≥10%的回撤，其中 ${events.length} 次超过执行价对应的跌幅（-${thresholdPct.toFixed(0)}%）。`;
  return {
    breachCount: events.length,
    thresholdPct: Number(thresholdPct.toFixed(1)),
    medianRecoveryDays,
    recoveryDaysSample: sortedRecoveryDays,
    recoveredCount: totalRecoveredCount,
    averageRecoveryDays,
    longestRecoveryDays,
    breachProbabilityPct,
    maxOvershootPct: maxOvershootPct !== null ? Number(maxOvershootPct.toFixed(1)) : null,
    unrecoveredCount,
    groupedEvents,
    conclusion: conclusionStatsLine,
    conclusionStatsLine,
    conclusionRiskLine: events.length === 0 ? null : {
      breachFrequencyLabel,
      tailRiskLabel,
    },
    conclusionQualifierLine: null,
    currentPriceLabel: formatPrice(currentPrice),
    strikePriceLabel: formatPrice(strikePrice),
    metricCards: [
      {
        label: '历史敲入概率',
        value:
          breachProbabilityPct !== null ? `${breachProbabilityPct}% (${events.length}/${meaningfulEpisodes.length}次)` : '—',
        tone: 'default' as const,
      },
      {
        label: '已修复案例中位反弹时间',
        value: medianRecoveryDays !== null ? formatDaysWithMonthsLabel(medianRecoveryDays) : '—',
        tone: 'default' as const,
      },
      {
        label: '敲入后超跌幅度',
        value: maxOvershootPct !== null ? `${maxOvershootPct.toFixed(1)}%` : '—',
        tone: 'default' as const,
      },
      {
        label: '敲入后未反弹回执行价格',
        value: unrecoveredCount > 0 ? `${unrecoveredCount}次` : '0次',
        tone: unrecoveredCount > 0 ? 'warning' as const : 'default' as const,
      },
    ],
    recoveryDistribution: (() => {
      const distributionTotal = belowStrikeDays.length + unrecoveredInDistribution;
      return [
      {
        label: '30天内',
        value: formatPercentShare(recoveredWithin30, distributionTotal),
        count: recoveredWithin30,
        tone: 'fast' as const,
      },
      {
        label: '30–180天',
        value: formatPercentShare(recoveredWithin180, distributionTotal),
        count: recoveredWithin180,
        tone: 'mid' as const,
      },
      {
        label: '180天以上',
        value: formatPercentShare(recoveredOver180, distributionTotal),
        count: recoveredOver180,
        tone: 'slow' as const,
      },
      {
        label: '未修复',
        value: formatPercentShare(unrecoveredInDistribution, distributionTotal),
        count: unrecoveredInDistribution,
        tone: 'unrecovered' as const,
      },
    ];
    })(),
  };
}

function buildDrawdownEpisodes(
  history: PriceHistoryPoint[],
) {
  const episodes: Array<{
    peak_date: string;
    trough_date: string;
    max_drawdown_pct: number;
    recovery_days: number | null;
    total_duration_days: number | null;
    recovered: boolean;
    closed_by_partial_recovery: boolean;
    peak_price: number;
  }> = [];

  const PARTIAL_RECOVERY_THRESHOLD = 0.25;

  let peakIndex = 0;
  let peakPrice = history[0]?.close ?? 0;
  let activeEpisode:
    | {
        peakIndex: number;
        troughIndex: number;
        maxDrawdownPct: number;
      }
    | null = null;

  for (let index = 1; index < history.length; index += 1) {
    const point = history[index];

    if (point.close >= peakPrice) {
      if (activeEpisode) {
        episodes.push({
          peak_date: history[activeEpisode.peakIndex].date,
          trough_date: history[activeEpisode.troughIndex].date,
          max_drawdown_pct: Number(activeEpisode.maxDrawdownPct.toFixed(1)),
          recovery_days: index - activeEpisode.troughIndex,
          total_duration_days: index - activeEpisode.peakIndex,
          recovered: true,
          closed_by_partial_recovery: false,
          peak_price: peakPrice,
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
        maxDrawdownPct: drawdownPct,
      };
      continue;
    }

    if (drawdownPct < activeEpisode.maxDrawdownPct) {
      activeEpisode.troughIndex = index;
      activeEpisode.maxDrawdownPct = drawdownPct;
    } else {
      const troughPrice = history[activeEpisode.troughIndex].close;
      const rallyFromTrough = (point.close / troughPrice) - 1;

      if (rallyFromTrough >= PARTIAL_RECOVERY_THRESHOLD) {
        episodes.push({
          peak_date: history[activeEpisode.peakIndex].date,
          trough_date: history[activeEpisode.troughIndex].date,
          max_drawdown_pct: Number(activeEpisode.maxDrawdownPct.toFixed(1)),
          recovery_days: index - activeEpisode.troughIndex,
          total_duration_days: index - activeEpisode.peakIndex,
          recovered: false,
          closed_by_partial_recovery: true,
          peak_price: peakPrice,
        });
        activeEpisode = null;
        peakIndex = index;
        peakPrice = point.close;
      }
    }
  }

  if (activeEpisode) {
    episodes.push({
      peak_date: history[activeEpisode.peakIndex].date,
      trough_date: history[activeEpisode.troughIndex].date,
      max_drawdown_pct: Number(activeEpisode.maxDrawdownPct.toFixed(1)),
      recovery_days: null,
      total_duration_days: null,
      recovered: false,
      closed_by_partial_recovery: false,
      peak_price: peakPrice,
    });
  }

  return episodes;
}

function groupAnnotatedDrawdownEventsFallback(
  events: Array<{
    peak_date: string;
    trough_date: string;
    max_drawdown_pct: number;
    recovery_days: number | null;
    total_duration_days: number | null;
    recovered: boolean;
    days_below_strike: number | null;
    reason_zh: string | null;
    display_order?: number | null;
  }>,
) {
  const grouped = new Map<string, {
    reason_zh: string;
    count: number;
    max_drawdown_pct: number;
    hasUnrecovered: boolean;
    years: string[];
    peakLabel: string;
    displayOrder: number | null;
    latestPeakYear: number;
    latestPeakTimestamp: number;
  }>();

  for (const event of events) {
    const peakDate = new Date(event.peak_date);
    const peakLabel = `${peakDate.getFullYear()}年${peakDate.getMonth() + 1}月`;
    const year = event.trough_date.slice(0, 4);
    const reason = event.reason_zh ?? `${peakLabel} 暂无明确宏观归因，或为个股／板块特定因素驱动`;
    const groupKey = `${reason}::${event.peak_date.slice(0, 7)}`;
    const existing = grouped.get(groupKey);

    if (!existing) {
      grouped.set(groupKey, {
        reason_zh: reason,
        count: 1,
        max_drawdown_pct: event.max_drawdown_pct,
        hasUnrecovered: event.days_below_strike === null,
        years: [year],
        peakLabel,
        displayOrder: event.display_order ?? null,
        latestPeakYear: peakDate.getFullYear(),
        latestPeakTimestamp: peakDate.getTime(),
      });
      continue;
    }

    existing.count += 1;
    existing.max_drawdown_pct = Math.min(existing.max_drawdown_pct, event.max_drawdown_pct);
    existing.hasUnrecovered = existing.hasUnrecovered || event.days_below_strike === null;
    if (event.display_order !== null && event.display_order !== undefined) {
      existing.displayOrder =
        existing.displayOrder === null ? event.display_order : Math.min(existing.displayOrder, event.display_order);
    }
    if (!existing.years.includes(year)) {
      existing.years.push(year);
    }
  }

  return Array.from(grouped.values())
    .map((group) => ({
      yearLabel: group.peakLabel,
      displayOrder: group.displayOrder,
      latestYear: group.latestPeakYear,
      latestPeakTimestamp: group.latestPeakTimestamp,
      max_drawdown_pct: group.max_drawdown_pct,
      hasUnrecovered: group.hasUnrecovered,
      reason_zh: group.reason_zh,
      displayReason: (() => {
        const prose = group.reason_zh.includes(' → ')
          ? group.reason_zh.split(' → ').join('，')
          : group.reason_zh;
        return group.count > 1 ? `${prose}（共${group.count}段回撤）` : prose;
      })(),
    }))
    .sort((left, right) => {
      if (left.displayOrder !== null && left.displayOrder !== undefined && right.displayOrder !== null && right.displayOrder !== undefined && left.displayOrder !== right.displayOrder) {
        return left.displayOrder - right.displayOrder;
      }

      if (right.latestPeakTimestamp !== left.latestPeakTimestamp) {
        return right.latestPeakTimestamp - left.latestPeakTimestamp;
      }

      return left.max_drawdown_pct - right.max_drawdown_pct;
    });
}

function calculateMedian(values: number[]) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }

  return Math.round((sorted[middle - 1] + sorted[middle]) / 2);
}

function formatPercentShare(part: number, total: number) {
  if (total <= 0) {
    return '—';
  }

  return `${Math.round((part / total) * 100)}%`;
}

function formatSignedPercent(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '—';
  }

  return `${value > 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function formatDaysLabel(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '未修复';
  }

  return `${Math.round(value)}天`;
}

function formatDaysWithMonthsLabel(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return '—';
  }

  const roundedDays = Math.round(value);
  if (roundedDays < 21) {
    return `${roundedDays}天 (<1个月)`;
  }
  const months = Math.round(roundedDays / 21);
  return `${roundedDays}天 (${months}个月)`;
}
