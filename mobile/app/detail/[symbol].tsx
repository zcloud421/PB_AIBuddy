import { useEffect, useState } from 'react';
import { useLocalSearchParams, useRouter } from 'expo-router';
import {
  ActivityIndicator,
  FlatList,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import Svg, { Defs, Line, LinearGradient, Path, Stop } from 'react-native-svg';
import { SafeAreaView } from 'react-native-safe-area-context';

import { colors, gradeColor } from '../../constants/colors';
import { typography } from '../../constants/typography';
import {
  formatExpiryDate,
  formatMonths,
  formatPrice,
  formatStrikeParts,
} from '../../constants/display';
import { NarrativeBlock } from '../../components/NarrativeBlock';
import { useFavorites } from '../../hooks/useFavorites';
import { useSymbolPriceHistory } from '../../hooks/useSymbolPriceHistory';
import { useSymbolIdea } from '../../hooks/useSymbolIdea';
import { useSymbolNarrative } from '../../hooks/useSymbolNarrative';
import {
  buildInteractiveStrikeRiskSummaryFallback,
  resolveTailRiskFooterSummary,
} from './strikeRiskFallback';
import type { PriceHistoryPoint } from '../../types/api';
const loadingStages = ['正在获取市场数据...', '正在分析FCN结构...', '正在生成Pitch话术...'];

export default function DetailScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ symbol?: string }>();
  const symbol = String(params.symbol ?? '').toUpperCase();

  return (
    <SafeAreaView style={styles.safeArea}>
      <View style={styles.nav}>
        <Pressable onPress={() => router.back()}>
          <Text style={styles.back}>← 返回</Text>
        </Pressable>
      </View>
      <DetailPage symbol={symbol} />
    </SafeAreaView>
  );
}

function formatDisplayCompanyName(symbol: string, companyName: string | null) {
  const upper = symbol.toUpperCase();
  const aliasMap: Partial<Record<string, string>> = {
    GOOG: 'Google (Class C)',
    GOOGL: 'Google (Class A)',
    META: 'Meta',
    TSLA: 'Tesla',
    MSFT: 'Microsoft',
    AAPL: 'Apple',
    AMZN: 'Amazon',
    TSM: 'TSMC',
    PDD: '拼多多',
    BIDU: '百度',
  };

  const alias = aliasMap[upper];
  if (alias) {
    return alias;
  }

  const cleaned = (companyName?.trim() || '')
    .replace(
      /\b(American Depositary Shares?(?:,\s*each\s*represent(?:s)?\s*[^,]+)?|Class\s+[A-Z]\s+Capital\s+Stock|Class\s+[A-Z]\s+Common\s+Stock|Common\s+Stock)\b/gi,
      '',
    )
    .replace(/\b(Inc\.?|Corporation|Corp\.?|Holdings?|Group|Ltd\.?|Limited|PLC|S\.A\.)\b/gi, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+,/g, ',')
    .trim()
    .replace(/[,\s.]+$/g, '');

  return cleaned || companyName?.trim() || upper;
}

function formatTargetCouponLine(data: {
  target_coupon_pct?: number | null;
  achieved_coupon_pct?: number | null;
  max_achievable_coupon_pct?: number | null;
  target_unreachable?: boolean | null;
  recommended_strike?: number | null;
  moneyness_pct?: number | null;
}) {
  const target = data.target_coupon_pct;
  if (target === null || target === undefined) return null;
  const targetText = Number(target).toFixed(0);
  const max = data.max_achievable_coupon_pct;
  const strike = data.recommended_strike !== null && data.recommended_strike !== undefined
    ? `$${Number(data.recommended_strike).toFixed(2)}`
    : '—';
  const buffer = data.moneyness_pct !== null && data.moneyness_pct !== undefined
    ? `${Math.max(0, 100 - Number(data.moneyness_pct)).toFixed(1)}%`
    : '—';

  if (data.target_unreachable) {
    const maxText = max !== null && max !== undefined ? `${Number(max).toFixed(1)}%` : '—';
    return `本期标准 ${targetText}% 不可达，最高约 ${maxText}（执行价 ${strike} / buffer ${buffer}）`;
  }

  return `票息 ${targetText}% 为本期标准参考，当前执行价 ${strike}（buffer ${buffer}）`;
}

function DetailPage({ symbol }: { symbol: string }) {
  const { data, isLoading, isError, refetch } = useSymbolIdea(symbol);
  const narrativeReady = !!(data?.narrative?.why_now);
  const { data: narrativeData } = useSymbolNarrative(symbol, {
    enabled: !!data && !narrativeReady,
  });
  const { isFavorite, toggleFavorite } = useFavorites();
  const [expandedDimension, setExpandedDimension] = useState<string | null>(null);
  const [riskMethodologyExpanded, setRiskMethodologyExpanded] = useState(false);
  const [drawdownInfoExpanded, setDrawdownInfoExpanded] = useState(false);
  const [loadingStageIndex, setLoadingStageIndex] = useState(0);
  const [customStrikeInput, setCustomStrikeInput] = useState('');
  const customStrikePct = parsePositiveNumber(customStrikeInput);
  const priceHistoryQuery = useSymbolPriceHistory(symbol, Boolean(symbol), customStrikePct);

  useEffect(() => {
    if (!isLoading) {
      setLoadingStageIndex(0);
      return;
    }

    const timer = setInterval(() => {
      setLoadingStageIndex((current) => (current + 1) % loadingStages.length);
    }, 5000);

    return () => clearInterval(timer);
  }, [isLoading]);

  useEffect(() => {
    if (data?.moneyness_pct !== null && data?.moneyness_pct !== undefined) {
      setCustomStrikeInput(String(Number(data.moneyness_pct.toFixed(1))));
    }
  }, [data?.moneyness_pct, symbol]);

  if (isLoading && !data) {
    return <DetailSkeleton symbol={symbol} loadingText={loadingStages[loadingStageIndex]} />;
  }

  if (isError || !data) {
    return <CenteredState text="数据加载失败，请重试" onPress={() => refetch()} />;
  }

  const gradePresentation = buildGradePresentation(
    data.grade,
    data.wait_reason ?? null,
    data.price_context.days_to_earnings,
    data.flags ?? [],
  );
  const currentGradeColor = gradePresentation.color;
  const isEarningsWait = gradePresentation.badgeText === 'WAIT';
  const isNotRecommendable = data.grade === 'NOT_RECOMMENDABLE';
  const scoreLabel = `综合评分 ${Math.round((data.composite_score ?? 0) * 100)}/100`;
  const strikeParts = formatStrikeParts(data.recommended_strike, data.moneyness_pct);
  const targetCouponLine = formatTargetCouponLine(data);
  const effectiveNarrative = data.narrative?.why_now
    ? data.narrative
    : narrativeData?.narrative ?? null;
  const flags = Array.isArray(data.flags) ? data.flags : [];
  const priceHistory = Array.isArray(priceHistoryQuery.data?.price_history)
    ? priceHistoryQuery.data.price_history
    : [];
  const latestClose = priceHistory.length > 0 ? priceHistory[priceHistory.length - 1].close : null;
  const tailRisk = priceHistoryQuery.data?.tail_risk ?? null;
  const displayDataAsOfDate = resolveUsDataAsOfDate(
    data.data_as_of_date ?? data.price_context.data_date ?? data.run_date ?? null,
  );
  const favorite = isFavorite(data.symbol);
  const trendReference = buildTrendReference(
    latestClose ?? data.price_context.current_price,
    data.price_context.ma50,
    data.price_context.ma200,
  );
  const ivReference = buildIvReference(data.signals, data.price_context.implied_volatility);
  const ivRiskNote = buildIvRiskNote(data.signals, data.price_context.implied_volatility);
  const earningsRisk = buildEarningsRiskReference(
    data.price_context.earnings_date,
    data.price_context.days_to_earnings,
    data.price_context.days_since_earnings,
    data.price_context.extended_move_pct,
  );
  const high52w = priceHistory.length > 0 ? Math.max(...priceHistory.map((p) => p.close)) : null;
  const liveHigh52wPct =
    latestClose !== null && high52w !== null ? ((latestClose - high52w) / high52w) * 100 : null;
  const high52wStatus = format52WeekHighStatus(liveHigh52wPct ?? data.price_context.pct_from_52w_high);
  const backendStrikeRiskSummary = priceHistoryQuery.data?.interactive_strike_risk_summary ?? null;
  const strikeRiskSummary =
    backendStrikeRiskSummary ??
    buildInteractiveStrikeRiskSummaryFallback(
      priceHistory,
      data.price_context.current_price ?? latestClose,
      customStrikePct,
      symbol,
      priceHistoryQuery.data?.drawdown_attributions ?? [],
    );
  const displayDrawdownEvents =
    priceHistoryQuery.data?.display_drawdown_events ?? strikeRiskSummary?.groupedEvents ?? [];
  const dimensionRows = buildScoreDimensions(data);
  const tailRiskFooter = resolveTailRiskFooterSummary(
    priceHistoryQuery.data?.tail_risk_footer_summary ?? null,
    tailRisk,
  );
  const displayCompanyName = formatDisplayCompanyName(data.symbol, data.company_name);

  return (
    <FlatList
      data={[{ key: symbol }]}
      keyExtractor={(item) => item.key}
      renderItem={() => (
        <View style={styles.content}>
      <View style={styles.verdict}>
        <View style={styles.verdictRow}>
          <View style={styles.verdictLeft}>
            <Text style={styles.headline}>{data.symbol}</Text>
            {displayCompanyName ? <Text style={styles.companyName}>{displayCompanyName}</Text> : null}
          </View>
          <View style={styles.verdictRight}>
            {!isEarningsWait ? (
              <Text style={[styles.scoreBadge, { color: currentGradeColor }]}>{scoreLabel}</Text>
            ) : (
              <Text style={[styles.scoreBadge, { color: colors.warning }]}>
                {gradePresentation.waitContext ?? '建议等待事件落地后再评估'}
              </Text>
            )}
            <View style={styles.badgeRow}>
              <View style={[styles.gradeBadge, { borderColor: currentGradeColor }]}>
                <Text style={[styles.gradeText, { color: currentGradeColor }]}>
                  {gradePresentation.badgeText}
                </Text>
              </View>
              <Pressable
                style={styles.favoriteButton}
                onPress={() => toggleFavorite(data.symbol)}
                hitSlop={8}
              >
                <Text style={[styles.favoriteIcon, favorite && styles.favoriteIconActive]}>
                  {favorite ? '★' : '☆'}
                </Text>
              </Pressable>
            </View>
            {gradePresentation.waitContext ? (
              <Text style={styles.waitContextText}>{gradePresentation.waitContext}</Text>
            ) : null}
          </View>
        </View>
      </View>

      {isNotRecommendable ? (
        <View style={styles.eligibilityBanner}>
          <Text style={styles.eligibilityTitle}>标的暂不可推介</Text>
          <Text style={styles.eligibilityBody}>
            {data.eligibility?.message ?? '该标的当前不在私行 FCN 推荐池中，需 IC 审批后再评估。'}
          </Text>
        </View>
      ) : null}

      {data.house_override ? (
        <View style={styles.overrideBanner}>
          <Text style={styles.overrideTitle}>House Override: {data.house_override.action}</Text>
          <Text style={styles.overrideBody}>
            设置: {data.house_override.set_by} · {formatCalendarDate(data.house_override.set_at)} · 原因: {data.house_override.reason}
          </Text>
        </View>
      ) : null}

      {!isNotRecommendable && isEarningsWait ? (
        <View style={styles.earningsWaitCard}>
          <Text style={styles.earningsWaitTitle}>财报窗口期</Text>
          <Text style={styles.earningsWaitBody}>
            {gradePresentation.waitContext ?? '建议等待事件落地后再重新评估参考报价。'}
          </Text>
        </View>
      ) : !isNotRecommendable ? (
        <View style={styles.comboCard}>
          <Text style={styles.sectionLabel}>参考询价参数</Text>
          <View style={styles.comboPrimaryMetric}>
            <Text style={styles.metricTitle}>期限</Text>
            <Text style={styles.comboPrimaryValue}>{formatMonths(data.recommended_tenor_days)}</Text>
          </View>
          <View style={styles.comboSecondaryRow}>
            <View style={styles.comboSecondaryMetric}>
              <Text style={styles.metricTitle}>执行价</Text>
              <Text style={styles.metricValue}>{strikeParts.primary}</Text>
              {strikeParts.secondary ? (
                <Text style={styles.metricSecondary}>{strikeParts.secondary}</Text>
              ) : null}
            </View>
            <View style={styles.comboSecondaryMetric}>
              <Text style={styles.metricTitle}>参考票息（无敲出条款）</Text>
              <Text style={styles.couponValue}>
                {data.estimated_coupon_range ?? '—'}
              </Text>
            </View>
          </View>
          <View style={styles.comboNotes}>
            <Text style={styles.note}>
              参考期权链到期日：{formatExpiryDate(data.recommended_expiry_date)}
            </Text>
            <Text style={styles.note}>参考票息基于无敲出条款估算，实际票息请向交易台询价</Text>
            {targetCouponLine ? <Text style={styles.note}>{targetCouponLine}</Text> : null}
            <Text style={styles.note}>数据截至 {displayDataAsOfDate ?? '—'} 收盘</Text>
          </View>
          {flags.some((flag) => flag.type === 'HIGH_VOL_LOW_STRIKE') ? (
            <Text style={styles.comboWarning}>⚠️ 高波动标的，执行价已保守下调，建议充分评估客户适合性</Text>
          ) : null}
        </View>
      ) : null}

      {!isNotRecommendable && effectiveNarrative?.why_now ? (
        <NarrativeBlock
          narrative={effectiveNarrative}
          grade={gradePresentation.narrativeGrade}
          hideRiskNote={isEarningsWait}
          isEarningsWait={isEarningsWait}
        />
      ) : !isNotRecommendable ? (
        <NarrativeSkeleton grade={gradePresentation.narrativeGrade} />
      ) : null}

      {tailRisk || priceHistoryQuery.isLoading ? (
        <Section
          title="FCN敲入风险评估"
          titleAccessory={
            <Pressable
              onPress={() => setRiskMethodologyExpanded((current) => !current)}
              style={styles.sectionInfoButton}
              hitSlop={8}
            >
              <Text style={styles.sectionInfoButtonText}>?</Text>
            </Pressable>
          }
        >
              <View style={styles.riskAnalysisBlock}>
                <View style={styles.riskInputBlock}>
                  <Text style={styles.riskControlHeader}>输入执行价 (%) 进行分析</Text>
                  <View style={styles.riskControlStrip}>
                    <View style={[styles.riskControlCell, styles.riskControlCellInput]}>
                      <View style={styles.riskControlInputWrap}>
                        <TextInput
                          value={customStrikeInput}
                          onChangeText={(text) => {
                            const digits = text.replace(/[^0-9]/g, '').slice(0, 2);
                            if (digits === '' || parseInt(digits, 10) <= 99) {
                              setCustomStrikeInput(digits);
                            }
                          }}
                          placeholder="81"
                          placeholderTextColor={colors.textDisabled}
                          keyboardType="number-pad"
                          style={styles.riskInput}
                        />
                        <Text style={styles.riskControlSuffix}>%</Text>
                      </View>
                    </View>
                    <View style={[styles.riskControlCell, styles.riskControlCellHint]}>
                      {strikeRiskSummary ? (
                        <View style={styles.riskControlHintBlock}>
                          <Text style={styles.riskControlHintText}>
                            {`以 -${strikeRiskSummary.thresholdPct.toFixed(1)}% 跌幅阈值回看历史回撤`}
                          </Text>
                          <Text style={styles.riskControlHintText}>
                            {`按当前价格折算，${customStrikePct?.toFixed(1) ?? '—'}% 约为 ${strikeRiskSummary.strikePriceLabel}`}
                          </Text>
                        </View>
                      ) : (
                        <View style={styles.riskControlHintBlock}>
                          <Text style={styles.riskControlHintText}>以 — 跌幅阈值回看历史回撤</Text>
                          <Text style={styles.riskControlHintText}>按当前价格折算，执行价约为 —</Text>
                        </View>
                      )}
                    </View>
                  </View>
                  {strikeRiskSummary ? (
                    <View style={styles.riskInteractiveSummary}>
                      <View style={styles.riskSummaryBand}>
                        <Text style={styles.riskConclusionText}>{strikeRiskSummary.conclusionStatsLine}</Text>
                        {ivRiskNote ? (
                          <Text style={[styles.riskSummarySubline, { color: ivRiskNote.color }]}>
                            {ivRiskNote.text}
                          </Text>
                        ) : null}
                      </View>
                      <View style={styles.riskSummaryMetricGrid}>
                        {strikeRiskSummary.metricCards.map((metric) => {
                          const statusBadgeLabel =
                            metric.label === '历史敲入概率'
                              ? strikeRiskSummary.conclusionRiskLine?.breachFrequencyLabel ?? null
                              : metric.label === '敲入后超跌幅度'
                                ? strikeRiskSummary.conclusionRiskLine?.tailRiskLabel ?? null
                                : null;
                          const statusBadgeTone = statusBadgeLabel ? getRiskLevelTone(statusBadgeLabel) : null;

                          return (
                          <View key={metric.label} style={styles.riskSummaryMetricCard}>
                            {statusBadgeLabel ? (
                              <View
                                style={[
                                  styles.riskMetricStatusBadge,
                                  statusBadgeTone === 'high'
                                    ? styles.riskMetricStatusBadgeHigh
                                    : statusBadgeTone === 'medium'
                                      ? styles.riskMetricStatusBadgeMedium
                                      : styles.riskMetricStatusBadgeLow,
                                ]}
                              >
                                <Text
                                  style={[
                                    styles.riskMetricStatusBadgeText,
                                    statusBadgeTone === 'high'
                                      ? styles.riskMetricStatusBadgeTextHigh
                                      : statusBadgeTone === 'medium'
                                        ? styles.riskMetricStatusBadgeTextMedium
                                        : styles.riskMetricStatusBadgeTextLow,
                                  ]}
                                >
                                  {statusBadgeLabel}
                                </Text>
                              </View>
                            ) : null}
                            <Text style={styles.riskSummaryMetricValue}>{metric.value}</Text>
                            <Text style={styles.riskSummaryMetricLabel}>{metric.label}</Text>
                          </View>
                        )})}
                      </View>
                      {strikeRiskSummary.breachCount > 0 ? (
                        <View style={styles.riskDistributionBlock}>
                          <View style={styles.riskSectionHeaderRow}>
                            <Text style={styles.riskSectionEyebrow}>敲入后回到执行价用时</Text>
                          </View>
                          <View style={styles.riskDistributionBar}>
                            {strikeRiskSummary.recoveryDistribution.map((item) => (
                              item.count > 0 ? (
                                <View
                                  key={item.label}
                                  style={[
                                    styles.riskDistributionSegment,
                                    item.tone === 'fast'
                                      ? styles.riskDistributionSegmentFast
                                      : item.tone === 'mid'
                                        ? styles.riskDistributionSegmentMid
                                        : item.tone === 'unrecovered'
                                          ? styles.riskDistributionSegmentUnrecovered
                                        : styles.riskDistributionSegmentSlow,
                                    { flex: item.count },
                                  ]}
                                />
                              ) : null
                            ))}
                          </View>
                          {strikeRiskSummary.recoveryDistribution.map((item) => (
                            <View key={item.label} style={styles.riskDistributionRow}>
                              <View style={styles.riskDistributionLabelRow}>
                                <View
                                  style={[
                                    styles.riskDistributionDot,
                                    item.tone === 'fast'
                                      ? styles.riskDistributionSegmentFast
                                      : item.tone === 'mid'
                                        ? styles.riskDistributionSegmentMid
                                        : item.tone === 'unrecovered'
                                          ? styles.riskDistributionSegmentUnrecovered
                                          : styles.riskDistributionSegmentSlow,
                                  ]}
                                />
                                <Text style={styles.riskDistributionLabel}>{item.label}</Text>
                              </View>
                              <Text style={styles.riskDistributionValue}>{item.value}</Text>
                            </View>
                          ))}
                        </View>
                      ) : (
                        <View style={styles.riskDistributionEmpty}>
                          <Text style={styles.riskDistributionEmptyText}>无历史反弹记录</Text>
                        </View>
                      )}
                    </View>
                  ) : priceHistoryQuery.isLoading ? (
                    <View style={styles.riskLoadingBlock}>
                      <View style={styles.riskSummaryBand}>
                        <View style={styles.riskLoadingLineLong} />
                        <View style={styles.riskLoadingLineShort} />
                      </View>
                      <View style={styles.riskSummaryMetricGrid}>
                        {Array.from({ length: 4 }).map((_, index) => (
                          <View key={index} style={styles.riskSummaryMetricCard}>
                            <View style={styles.riskLoadingMetricValue} />
                            <View style={styles.riskLoadingMetricLabel} />
                          </View>
                        ))}
                      </View>
                      <View style={styles.riskDistributionBlock}>
                        <View style={styles.riskLoadingEyebrow} />
                        <View style={styles.riskDistributionBar}>
                          <View style={[styles.riskDistributionSegment, styles.riskLoadingSegment, { flex: 3 }]} />
                          <View style={[styles.riskDistributionSegment, styles.riskLoadingSegment, { flex: 2 }]} />
                          <View style={[styles.riskDistributionSegment, styles.riskLoadingSegment, { flex: 1 }]} />
                        </View>
                        {Array.from({ length: 3 }).map((_, index) => (
                          <View key={index} style={styles.riskDistributionRow}>
                            <View style={styles.riskDistributionLabelRow}>
                              <View style={styles.riskLoadingDot} />
                              <View style={styles.riskLoadingDistributionLabel} />
                            </View>
                            <View style={styles.riskLoadingDistributionValue} />
                          </View>
                        ))}
                      </View>
                    </View>
                  ) : null}
                </View>
                {tailRiskFooter ? (
                  <View style={styles.riskStaticFooter}>
                    <View style={styles.riskStaticFooterDivider} />
                    <View style={styles.riskStaticFooterStrip}>
                      <Text style={styles.riskStaticFooterLabel}>历史极值（全段）</Text>
                      <View style={styles.riskStaticFooterMetrics}>
                        <View style={styles.riskStaticFooterMetric}>
                          <Text style={styles.riskStaticFooterMetricLabel}>最大回撤</Text>
                          <Text style={styles.riskStaticFooterMetricValue}>{tailRiskFooter.maxDrawdownText}</Text>
                        </View>
                        <View style={styles.riskStaticFooterMetricDivider} />
                        <View style={styles.riskStaticFooterMetric}>
                          <Text style={styles.riskStaticFooterMetricLabel}>最长修复</Text>
                          <Text style={styles.riskStaticFooterMetricValue}>{tailRiskFooter.longestRecoveryText}</Text>
                        </View>
                      </View>
                    </View>
                    {tailRiskFooter.hasUnrecoveredDrawdown ? (
                      <Text style={styles.riskStaticFooterNote}>当前有未修复回撤</Text>
                    ) : null}
                    {riskMethodologyExpanded ? (
                      <View style={styles.riskMethodologyInline}>
                        <Text style={styles.riskMethodologySummary}>
                          基于过去5年日线收盘价，回撤按独立周期统计。修复时间：跌破至回到执行价。
                        </Text>
                        <View style={styles.riskMethodologyExpanded}>
                          <Text style={styles.riskMethodologyExpandedText}>
                            回撤按独立周期识别。若股价自低点反弹≥25%，或距上一轮已超过2.5年且驱动因素明显不同，则视为新周期；同一市场环境下的二次下探并入主周期。页面中的敲入概率与修复分布按输入的跌幅阈值统计；中位时间仅基于已回到该跌幅阈值以内的历史案例计算，未修复案例不计入中位数。不构成未来预测。
                          </Text>
                        </View>
                      </View>
                    ) : null}
                  </View>
                ) : null}
              </View>
        </Section>
      ) : null}

      {strikeRiskSummary ? (
        <Section
          title="历史回撤归因分析"
          titleAccessory={
            <Pressable
              onPress={() => setDrawdownInfoExpanded((current) => !current)}
              style={styles.sectionInfoButton}
              hitSlop={8}
            >
              <Text style={styles.sectionInfoButtonText}>?</Text>
            </Pressable>
          }
        >
          <View style={styles.drawdownEventSection}>
            {drawdownInfoExpanded ? (
              <View style={styles.drawdownEventIntroBlock}>
                <Text style={styles.drawdownEventSectionIntro}>
                  围绕各阶段高点，解释历史大跌发生在什么背景下。
                </Text>
                <Text style={styles.drawdownEventIntroMeta}>
                  以下跌幅按各自阶段高点测算；“未修复”表示该轮回撤跌破执行价后，截至当前仍未回到执行价。
                </Text>
              </View>
            ) : null}
            {displayDrawdownEvents.length > 0 ? (
              <>
                <View style={styles.drawdownEventList}>
                  {displayDrawdownEvents.map((event, i) => (
                    <View
                      key={`${event.displayReason}-${event.yearLabel}-${i}`}
                      style={[
                        styles.drawdownEventRow,
                        i === displayDrawdownEvents.length - 1
                          ? styles.drawdownEventRowLast
                          : null,
                      ]}
                    >
                      <View style={styles.drawdownEventLeft}>
                        <Text style={styles.drawdownEventYear} numberOfLines={1}>
                          {event.yearLabel}
                        </Text>
                        <Text style={styles.drawdownEventPct}>{event.max_drawdown_pct.toFixed(0)}%</Text>
                        {event.hasUnrecovered ? (
                          <View style={styles.drawdownEventBadge}>
                            <Text style={styles.drawdownEventBadgeText}>未修复</Text>
                          </View>
                        ) : null}
                      </View>
                      <Text style={styles.drawdownEventReason}>
                        {event.displayReason}
                      </Text>
                    </View>
                  ))}
                </View>
              </>
            ) : (
              <Text style={styles.drawdownEventEmpty}>无历史触及案例</Text>
            )}
          </View>
        </Section>
      ) : null}

      <Section title="价格走势">
        {priceHistory.length > 1 ? (
          <TrendChart
            history={priceHistory}
            currentPrice={latestClose ?? data.price_context.current_price}
            dataDate={resolveUsDataAsOfDate(
              priceHistoryQuery.data?.data_as_of_date ?? data.price_context.data_date ?? data.run_date ?? null,
            )}
          />
        ) : priceHistoryQuery.isLoading ? (
          <View style={styles.trendEmptyCard}>
            <Text style={styles.trendEmptyTitle}>走势图加载中</Text>
            <Text style={styles.trendEmptyText}>
              详情主内容已优先展示，历史价格数据正在单独加载。
            </Text>
          </View>
        ) : (
          <View style={styles.trendEmptyCard}>
            <Text style={styles.trendEmptyTitle}>暂无可展示的历史价格数据</Text>
            <Text style={styles.trendEmptyText}>
              当前未拿到足够的历史价格点位，稍后可再刷新查看。
            </Text>
          </View>
        )}
      </Section>

      <Section title="行情参考">
        <PriceRow label="趋势强度" value={trendReference.text} valueColor={trendReference.color} />
        <PriceRow label="期权隐含波动率" value={ivReference.text} valueColor={ivReference.color} />
        <PriceRow label="财报风险" value={earningsRisk.text} valueColor={earningsRisk.color} />
        <PriceRow
          label="距52周高点"
          value={high52wStatus.text}
          valueColor={high52wStatus.color}
        />
        <PriceRow label="MA50" value={formatPrice(data.price_context.ma50)} />
        <PriceRow label="MA200" value={formatPrice(data.price_context.ma200)} />
      </Section>

      <Section title="评分维度">
        {dimensionRows.map((row, index) => {
          const expanded = expandedDimension === row.label;
          return (
            <Pressable
              key={row.label}
              style={[styles.dimensionRow, index > 0 && styles.dimensionRowWithDivider]}
              onPress={() =>
                setExpandedDimension((current) => (current === row.label ? null : row.label))
              }
            >
              <View style={styles.dimensionHeader}>
                <Text style={styles.dimensionLabel}>{row.label}</Text>
                <View style={styles.dimensionHeaderRight}>
                  <Text style={[styles.dimensionStatus, { color: progressColor(row.percent) }]}>
                    {row.status}
                  </Text>
                  <Text style={styles.dimensionChevron}>{expanded ? '⌃' : '⌄'}</Text>
                </View>
              </View>
              <View style={styles.progressTrack}>
                <View
                  style={[
                    styles.progressFill,
                    { width: `${row.percent}%`, backgroundColor: progressColor(row.percent) },
                  ]}
                />
              </View>
              <Text style={styles.dimensionSummary}>{row.summary}</Text>
              {expanded ? <Text style={styles.dimensionDetail}>{getInfoCopy(row.label)}</Text> : null}
            </Pressable>
          );
        })}
      </Section>
        </View>
      )}
      style={styles.page}
      contentContainerStyle={styles.detailListContent}
      showsVerticalScrollIndicator={false}
    />
  );
}

function NarrativeSkeleton({ grade }: { grade: 'GO' | 'CAUTION' | 'AVOID' }) {
  const title =
    grade === 'AVOID' ? '当前主要顾虑' : grade === 'CAUTION' ? '需留意风险' : '客户沟通参考';

  return (
    <View style={styles.narrativeSkeletonWrap}>
      <View style={styles.narrativeSkeletonHeader}>
        <Text style={styles.narrativeSkeletonTitle}>{title}</Text>
      </View>
      <View style={styles.narrativeSkeletonLines}>
        {[100, 85, 92].map((width, i) => (
          <View
            key={i}
            style={[
              styles.narrativeSkeletonLine,
              { width: `${width}%` },
            ]}
          />
        ))}
      </View>
    </View>
  );
}

function Section({
  title,
  label,
  titleAccessory,
  children,
}: {
  title: string;
  label?: string;
  titleAccessory?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <View style={styles.section}>
      {label ? (
        <View style={styles.sectionHeading}>
          <Text style={styles.sectionEyebrow}>{label}</Text>
          <Text style={styles.sectionTitle}>{title}</Text>
        </View>
      ) : (
        <View style={styles.sectionTitleRow}>
          <Text style={styles.sectionTitle}>{title}</Text>
          {titleAccessory}
        </View>
      )}
      {children}
    </View>
  );
}

function PriceRow({
  label,
  value,
  valueColor,
}: {
  label: string;
  value: string;
  valueColor?: string;
}) {
  return (
    <View style={styles.priceRow}>
      <Text style={styles.priceLabel}>{label}</Text>
      <Text style={[styles.priceValue, valueColor ? { color: valueColor } : null]}>{value}</Text>
    </View>
  );
}

function TrendChart({
  history,
  currentPrice,
  dataDate,
}: {
  history: PriceHistoryPoint[];
  currentPrice: number | null;
  dataDate: string | null;
}) {
  const [activeRange, setActiveRange] = useState<'1M' | '3M' | 'YTD' | '1Y'>('3M');
  const [chartWidth, setChartWidth] = useState(286);
  const chartHeight = 128;
  const filteredHistory = filterPriceHistory(history, activeRange);
  const points = buildChartPoints(filteredHistory, chartWidth, chartHeight);
  const performance = calculateRangePerformance(filteredHistory);
  const axisLabels = buildAxisLabels(filteredHistory);
  const linePath = buildSmoothLinePath(points);
  const areaPath = buildAreaFillPath(points, chartHeight);

  if (filteredHistory.length < 2 || points.length < 2 || !linePath || !areaPath) {
    return null;
  }

  return (
    <View style={styles.trendCard}>
      <View style={styles.trendHeader}>
        <View style={styles.trendHeaderLeft}>
          <Text style={styles.trendPrice}>
            {currentPrice !== null ? formatPrice(currentPrice) : '—'}
          </Text>
          <Text
            style={[
              styles.trendChange,
              performance >= 0 ? styles.trendChangePositive : styles.trendChangeNegative,
            ]}
          >
            {`${performance >= 0 ? '+' : ''}${performance.toFixed(1)}%`}
          </Text>
        </View>
        <Text style={styles.trendDate}>截至 {dataDate ?? filteredHistory[filteredHistory.length - 1]?.date ?? '—'}</Text>
      </View>

      <View style={styles.chartFrame}>
        {axisLabels.map((label, index) => (
          <Text key={`${label}-${index}`} style={[styles.chartAxisLabel, { top: index * 52 }]}>
            {label}
          </Text>
        ))}
        <View
          style={styles.chartCanvas}
          onLayout={(e) => {
            const w = e.nativeEvent.layout.width;
            if (w > 0) setChartWidth(w);
          }}
        >
          <Svg width={chartWidth} height={chartHeight}>
            <Defs>
              <LinearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
                <Stop offset="0%" stopColor={colors.chartLine} stopOpacity="0.15" />
                <Stop offset="100%" stopColor={colors.chartLine} stopOpacity="0" />
              </LinearGradient>
            </Defs>
            {[0, chartHeight / 2, chartHeight].map((y) => (
              <Line
                key={`grid-${y}`}
                x1="0"
                y1={y}
                x2={chartWidth}
                y2={y}
                stroke={colors.chartGrid}
                strokeWidth="1"
                opacity="0.75"
              />
            ))}
            <Path d={areaPath} fill="url(#trendFill)" />
            <Path
              d={linePath}
              fill="none"
              stroke={colors.chartLine}
              strokeWidth="1.5"
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          </Svg>
        </View>
      </View>

      <View style={styles.rangeSwitcher}>
        {(['1M', '3M', 'YTD', '1Y'] as const).map((range) => (
          <Pressable
            key={range}
            style={[styles.rangeChip, activeRange === range && styles.rangeChipActive]}
            onPress={() => setActiveRange(range)}
          >
            <Text style={[styles.rangeChipText, activeRange === range && styles.rangeChipTextActive]}>
              {range}
            </Text>
          </Pressable>
        ))}
      </View>
    </View>
  );
}

function CenteredState({ text, onPress }: { text: string; onPress?: () => void }) {
  return (
    <View style={styles.centeredPage}>
      <ActivityIndicator color={colors.success} />
      <Text style={styles.centeredText}>{text}</Text>
      {onPress ? (
        <Pressable onPress={onPress}>
          <Text style={styles.retry}>重试</Text>
        </Pressable>
      ) : null}
    </View>
  );
}

function DetailSkeleton({ symbol, loadingText }: { symbol: string; loadingText: string }) {
  return (
    <View style={styles.content}>
      <View style={styles.verdict}>
        <View style={styles.verdictRow}>
          <View style={styles.verdictLeft}>
            <Text style={styles.headline}>{symbol}</Text>
            <View style={[styles.skeletonBase, styles.companySkeleton]} />
          </View>
          <View style={styles.verdictRight}>
            <View style={[styles.skeletonBase, styles.scoreSkeleton]} />
            <View style={styles.badgeRow}>
              <View style={[styles.skeletonBase, styles.gradeSkeleton]} />
              <View style={[styles.skeletonBase, styles.favoriteSkeleton]} />
            </View>
          </View>
        </View>
      </View>

      <View style={styles.comboCard}>
        <Text style={styles.sectionLabel}>参考询价参数</Text>
        <View style={styles.comboPrimaryMetric}>
          <Text style={styles.metricTitle}>期限</Text>
          <View style={[styles.skeletonBase, styles.comboPrimaryValueSkeleton]} />
        </View>
        <View style={styles.comboSecondaryRow}>
          <View style={styles.comboSecondaryMetric}>
            <Text style={styles.metricTitle}>执行价</Text>
            <View style={[styles.skeletonBase, styles.metricValueSkeleton]} />
            <View style={[styles.skeletonBase, styles.metricSecondarySkeleton]} />
          </View>
          <View style={styles.comboSecondaryMetric}>
            <Text style={styles.metricTitle}>参考票息（无敲出条款）</Text>
            <View style={[styles.skeletonBase, styles.metricValueSkeleton]} />
          </View>
        </View>
        <View style={styles.comboNotes}>
          <Text style={styles.note}>{loadingText}</Text>
        </View>
      </View>

      <Section title="客户沟通参考">
        <View style={styles.skeletonCard}>
          <View style={[styles.skeletonBase, styles.textLineLong]} />
          <View style={[styles.skeletonBase, styles.textLineFull]} />
          <View style={[styles.skeletonBase, styles.textLineFull]} />
        </View>
      </Section>

      <Section title="评分维度">
        {[1, 2, 3].map((row) => (
          <View key={row} style={[styles.dimensionRow, row > 1 && styles.dimensionRowWithDivider]}>
              <View style={styles.dimensionHeader}>
                <View style={[styles.skeletonBase, styles.dimensionLabelSkeleton]} />
                <View style={styles.dimensionHeaderRight}>
                  <View style={[styles.skeletonBase, styles.dimensionStatusSkeleton]} />
                  <View style={[styles.skeletonBase, styles.dimensionChevronSkeleton]} />
                </View>
              </View>
              <View style={styles.progressTrack}>
                <View style={[styles.progressFill, styles.progressFillSkeleton]} />
              </View>
              <View style={[styles.skeletonBase, styles.dimensionSummarySkeleton]} />
          </View>
        ))}
      </Section>
    </View>
  );
}

function buildScoreDimensions(
  data: NonNullable<ReturnType<typeof useSymbolIdea>['data']>,
) {
  const technical = scorePercent(data.trend_score);
  const eventRisk = scorePercent(data.event_risk_score);
  const ivPremium = scorePercent(data.iv_premium_score);

  return [
    {
      label: '技术面',
      percent: technical,
      status: technical >= 75 ? '偏强' : technical >= 50 ? '中性' : '偏弱',
      summary: '均线、MACD 与 RSI 共同判断趋势健康度',
    },
    {
      label: '事件风险',
      percent: eventRisk,
      status: eventRisk >= 75 ? '较低' : eventRisk >= 50 ? '中性' : '偏高',
      summary: '财报窗口、事件密度与近期冲击共同决定事件风险评分',
    },
    {
      label: 'IV/票息',
      percent: ivPremium,
      status: ivPremium >= 75 ? '较强' : ivPremium >= 50 ? '一般' : '偏弱',
      summary: '期权隐含波动率、参考票息与put skew共同决定结构补偿',
    },
  ];
}

function progressColor(percent: number) {
  if (percent >= 75) {
    return colors.success;
  }
  if (percent >= 50) {
    return colors.warning;
  }
  return colors.danger;
}

function getInfoCopy(label: string | null) {
  if (label === '技术面') {
    return '后端评分组件，基于均线结构、MACD动量与RSI强弱综合判断趋势健康度。当前版本权重：40%';
  }
  if (label === '事件风险') {
    return '后端评分组件，综合财报窗口、tenor内事件密度和近期财报后冲击。分数越高代表事件风险越低。当前版本权重：25%';
  }
  if (label === 'IV/票息') {
    return '后端评分组件，综合隐含波动率、参考票息与put skew。该项反映结构补偿强弱，不单独代表适合度。当前版本权重：35%';
  }
  return '';
}

function scorePercent(score: number | null | undefined) {
  if (score === null || score === undefined || !Number.isFinite(score)) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round(score * 100)));
}

function format52WeekHighStatus(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return { text: '—', color: undefined };
  }
  if (value > 0.1) {
    return { text: '创新高', color: colors.success };
  }
  if (value >= -0.5) {
    return { text: '处于52周高位', color: colors.success };
  }
  return { text: `${value.toFixed(1)}%`, color: colors.danger };
}

function buildTrendReference(
  currentPrice: number | null,
  ma50: number | null,
  ma200: number | null,
) {
  if (currentPrice === null || ma50 === null || ma200 === null) {
    return { text: '—', color: undefined as string | undefined };
  }

  if (currentPrice > ma50 && ma50 > ma200) {
    return { text: '强势上行', color: colors.success };
  }

  if (currentPrice > ma200 && currentPrice < ma50) {
    return { text: '温和上行', color: colors.success };
  }

  return { text: '趋势偏弱', color: colors.danger };
}

function buildIvReference(
  signals: NonNullable<ReturnType<typeof useSymbolIdea>['data']>['signals'],
  impliedVolatility: number | null,
) {
  const ivSignal = signals.find((signal) => signal.name === 'IV rank');
  if (ivSignal) {
    const isHigh = ivSignal.color === 'green' || ivSignal.color === 'red';
    return { text: ivSignal.value, color: isHigh ? colors.warning : signalColorToHex(ivSignal.color) };
  }

  if (impliedVolatility === null) {
    return { text: '—', color: undefined as string | undefined };
  }

  const ivPct = Math.round(impliedVolatility * 100);
  if (impliedVolatility >= 0.6) {
    return { text: `波动率偏高（${ivPct}%）`, color: colors.warning };
  }
  if (impliedVolatility >= 0.35) {
    return { text: `波动率中等（${ivPct}%）`, color: colors.warning };
  }
  return { text: `波动率偏低（${ivPct}%）`, color: colors.neutral };
}

function buildIvRiskNote(
  signals: NonNullable<ReturnType<typeof useSymbolIdea>['data']>['signals'],
  impliedVolatility: number | null,
): { text: string; color: string } | null {
  const ivSignal = signals.find((signal) => signal.name === 'IV rank');

  if (ivSignal && (ivSignal.color === 'green' || ivSignal.color === 'red')) {
    const ivPct = impliedVolatility !== null ? `${Math.round(impliedVolatility * 100)}%` : ivSignal.value;
    return {
      text: `注：当前期权波动率处于高位（${ivPct}），前瞻敲入风险可能高于历史统计。`,
      color: colors.warning,
    };
  }

  if (impliedVolatility === null || impliedVolatility < 0.6) return null;
  return {
    text: `注：当前隐含波动率偏高（${Math.round(impliedVolatility * 100)}%），前瞻敲入风险可能高于历史统计。`,
    color: colors.warning,
  };
}

function buildEarningsRiskReference(
  earningsDate: string | null,
  daysToEarnings: number | null,
  daysSinceEarnings?: number | null,
  extendedMovePct?: number | null,
) {
  if (earningsDate && daysSinceEarnings !== null && daysSinceEarnings !== undefined && daysSinceEarnings <= 3) {
    const moveText =
      extendedMovePct !== null && extendedMovePct !== undefined
        ? `，盘后${extendedMovePct >= 0 ? '+' : ''}${extendedMovePct.toFixed(1)}%`
        : '';
    return {
      text: `⚠️ 财报已发布：${formatCalendarDate(earningsDate)}${moveText}`,
      color: extendedMovePct !== null && extendedMovePct !== undefined && extendedMovePct <= -5
        ? colors.danger
        : colors.warning,
    };
  }

  if (earningsDate && daysSinceEarnings !== null && daysSinceEarnings !== undefined && daysSinceEarnings <= 14) {
    return {
      text: `财报已发布：${formatCalendarDate(earningsDate)}`,
      color: colors.warning,
    };
  }

  if (earningsDate && daysToEarnings !== null && daysToEarnings <= 14) {
    return {
      text: `⚠️ 财报日：${formatCalendarDate(earningsDate)}`,
      color: colors.warning,
    };
  }

  return {
    text: '近期无财报风险',
    color: colors.neutral,
  };
}

function buildGradePresentation(
  grade: 'GO' | 'CAUTION' | 'AVOID' | 'NOT_RECOMMENDABLE',
  waitReason: 'WAIT_EARNINGS_RISK' | 'WAIT_POST_EARNINGS_SHOCK' | 'WAIT_SETUP_RESET' | null,
  daysToEarnings: number | null,
  flags: Array<{ type: string }>,
): {
  badgeText: 'GO' | 'CAUTION' | 'AVOID' | 'WAIT' | 'NOT RECOMMENDABLE';
  color: string;
  narrativeGrade: 'GO' | 'CAUTION' | 'AVOID';
  waitContext?: string;
} {
  if (grade === 'NOT_RECOMMENDABLE') {
    return {
      badgeText: 'NOT RECOMMENDABLE',
      color: colors.neutral,
      narrativeGrade: 'AVOID',
    };
  }

  if (waitReason) {
    return {
      badgeText: 'WAIT',
      color: colors.warning,
      narrativeGrade: grade,
      waitContext: getWaitContextMessage(waitReason, flags),
    };
  }

  if (grade === 'AVOID' && daysToEarnings !== null && daysToEarnings >= 0 && daysToEarnings <= 14) {
    return {
      badgeText: 'WAIT',
      color: colors.warning,
      narrativeGrade: 'AVOID',
      waitContext: '财报待落地',
    };
  }

  return {
    badgeText: grade,
    color: gradeColor(grade),
    narrativeGrade: grade,
  };
}

function getWaitContextMessage(
  waitReason: 'WAIT_EARNINGS_RISK' | 'WAIT_POST_EARNINGS_SHOCK' | 'WAIT_SETUP_RESET',
  flags: Array<{ type: string }>,
) {
  if (waitReason === 'WAIT_EARNINGS_RISK') {
    return '财报待落地';
  }
  if (waitReason === 'WAIT_POST_EARNINGS_SHOCK') {
    return '波动待消化';
  }
  if (flags.some((flag) => flag.type === 'BEARISH_STRUCTURE' || flag.type === 'BROKEN_TREND')) {
    return '回调待企稳';
  }
  if (flags.some((flag) => flag.type === 'MATERIAL_NEWS_SHOCK' || flag.type === 'MATERIAL_NEWS_OVERHANG')) {
    return '事件待明朗';
  }
  return '趋势待企稳';
}

function getRiskLevelTone(label: string): 'high' | 'medium' | 'low' {
  if (label === '较高') {
    return 'high';
  }
  if (label === '中等') {
    return 'medium';
  }
  return 'low';
}



function formatCalendarDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, '0');
  const day = String(parsed.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function resolveUsDataAsOfDate(value: string | null | undefined) {
  const latestClosedTradingDate = latestCompletedUsTradingDate();
  if (!value) {
    return latestClosedTradingDate;
  }

  const normalized = normalizeIsoDate(value);
  if (!normalized) {
    return latestClosedTradingDate;
  }

  return normalized > latestClosedTradingDate ? latestClosedTradingDate : normalized;
}

function latestCompletedUsTradingDate() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    hour12: false,
  }).formatToParts(now);

  const getPart = (type: string) => parts.find((part) => part.type === type)?.value ?? '';
  const year = Number(getPart('year'));
  const month = Number(getPart('month'));
  const day = Number(getPart('day'));
  const hour = Number(getPart('hour'));
  const etDate = new Date(Date.UTC(year, month - 1, day));

  if (hour < 16) {
    etDate.setUTCDate(etDate.getUTCDate() - 1);
  }

  return normalizeToPreviousWeekday(etDate);
}

function normalizeToPreviousWeekday(date: Date) {
  const normalized = new Date(date);
  while (normalized.getUTCDay() === 0 || normalized.getUTCDay() === 6) {
    normalized.setUTCDate(normalized.getUTCDate() - 1);
  }
  return normalized.toISOString().slice(0, 10);
}

function normalizeIsoDate(value: string) {
  const match = value.match(/^(\d{4}-\d{2}-\d{2})/);
  if (match) {
    return match[1];
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString().slice(0, 10);
}

function filterPriceHistory(
  history: PriceHistoryPoint[],
  range: '1M' | '3M' | 'YTD' | '1Y',
) {
  const latest = history[history.length - 1];
  if (!latest) {
    return [];
  }

  const latestDate = new Date(latest.date);
  if (Number.isNaN(latestDate.getTime())) {
    return history;
  }

  let startDate = new Date(latestDate);

  if (range === '1M') {
    startDate.setMonth(startDate.getMonth() - 1);
  } else if (range === '3M') {
    startDate.setMonth(startDate.getMonth() - 3);
  } else if (range === 'YTD') {
    startDate = new Date(latestDate.getFullYear(), 0, 1);
  } else {
    startDate.setFullYear(startDate.getFullYear() - 1);
  }

  const filtered = history.filter((point) => {
    const pointDate = new Date(point.date);
    return !Number.isNaN(pointDate.getTime()) && pointDate >= startDate;
  });

  return filtered.length >= 2 ? filtered : history.slice(-30);
}

function buildChartPoints(
  history: PriceHistoryPoint[],
  width: number,
  height: number,
) {
  if (history.length < 2) {
    return [];
  }

  const closes = history.map((point) => point.close);
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const range = max - min || 1;

  return history.map((point, index) => ({
    x: (index / (history.length - 1)) * width,
    y: height - (((point.close - min) / range) * height),
  }));
}

function buildSmoothLinePath(points: Array<{ x: number; y: number }>) {
  if (points.length < 2) {
    return '';
  }

  let path = `M ${points[0].x} ${points[0].y}`;

  for (let index = 1; index < points.length - 1; index += 1) {
    const current = points[index];
    const next = points[index + 1];
    const midX = (current.x + next.x) / 2;
    const midY = (current.y + next.y) / 2;
    path += ` Q ${current.x} ${current.y} ${midX} ${midY}`;
  }

  const penultimate = points[points.length - 2];
  const last = points[points.length - 1];
  path += ` Q ${penultimate.x} ${penultimate.y} ${last.x} ${last.y}`;

  return path;
}

function buildAreaFillPath(points: Array<{ x: number; y: number }>, height: number) {
  const linePath = buildSmoothLinePath(points);
  if (!linePath || points.length < 2) {
    return '';
  }

  const first = points[0];
  const last = points[points.length - 1];
  return `${linePath} L ${last.x} ${height} L ${first.x} ${height} Z`;
}

function calculateRangePerformance(
  history: PriceHistoryPoint[],
) {
  const first = history[0]?.close;
  const last = history[history.length - 1]?.close;
  if (!first || !last) {
    return 0;
  }

  return ((last - first) / first) * 100;
}

function buildAxisLabels(
  history: PriceHistoryPoint[],
) {
  const closes = history.map((point) => point.close);
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const mid = (min + max) / 2;
  return [max, mid, min].map((value) => value.toFixed(0));
}

function signalColorToHex(color: 'green' | 'amber' | 'red' | 'gray') {
  if (color === 'green') {
    return colors.success;
  }
  if (color === 'amber') {
    return colors.warning;
  }
  if (color === 'red') {
    return colors.danger;
  }
  return colors.neutral;
}

function parsePositiveNumber(value: string): number | null {
  const normalized = value.replace(/[^0-9.]/g, '');
  if (!normalized) {
    return null;
  }

  const numeric = Number(normalized);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

const styles = StyleSheet.create({
  safeArea: { flex: 1, backgroundColor: colors.background },
  narrativeSkeletonWrap: { gap: 10 },
  narrativeSkeletonHeader: { gap: 4 },
  narrativeSkeletonTitle: { color: colors.textPrimary, fontSize: 18, fontWeight: '700' },
  narrativeSkeletonLines: { gap: 8 },
  narrativeSkeletonLine: {
    height: 14,
    backgroundColor: '#2A2A2A',
    borderRadius: 4,
  },
  nav: {
    flexDirection: 'row',
    justifyContent: 'flex-start',
    alignItems: 'center',
    paddingHorizontal: 20,
    paddingTop: 8,
    paddingBottom: 8,
  },
  back: { color: colors.link, fontSize: 15, fontWeight: '700' },
  page: {
    flex: 1,
    backgroundColor: colors.background,
  },
  detailListContent: {
    paddingBottom: 40,
  },
  content: { padding: 20, gap: 22 },
  verdict: { gap: 10 },
  verdictRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: 16,
  },
  verdictLeft: {
    flex: 1,
    gap: 6,
  },
  verdictRight: {
    alignItems: 'flex-end',
    gap: 10,
    minWidth: 122,
  },
  badgeRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  headline: {
    color: colors.textPrimary,
    fontSize: 34,
    fontWeight: '800',
    letterSpacing: -0.6,
    fontFamily: typography.uiBold,
  },
  companyName: { color: colors.textMuted, fontSize: 13, lineHeight: 18, maxWidth: '92%' },
  scoreBadge: {
    fontSize: 15,
    fontWeight: '800',
    textAlign: 'right',
    lineHeight: 20,
    fontFamily: typography.monoBold,
  },
  waitContextText: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
    textAlign: 'right',
    maxWidth: 180,
  },
  gradeBadge: {
    borderWidth: 1,
    borderRadius: 3,
    minWidth: 78,
    alignItems: 'center',
    paddingHorizontal: 12,
    paddingVertical: 7,
  },
  gradeText: { fontSize: 12, fontWeight: '800', fontFamily: typography.monoBold },
  eligibilityBanner: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.borderAccent,
    padding: 16,
    gap: 8,
  },
  eligibilityTitle: {
    color: colors.warning,
    fontSize: 14,
    fontWeight: '700',
    fontFamily: typography.uiSemiBold,
  },
  eligibilityBody: {
    color: colors.textPrimary,
    fontSize: 13,
    lineHeight: 19,
    fontFamily: typography.uiRegular,
  },
  overrideBanner: {
    backgroundColor: colors.warningSurface,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.warningBorder,
    padding: 14,
    gap: 6,
  },
  overrideTitle: {
    color: colors.warning,
    fontSize: 12,
    fontWeight: '800',
    fontFamily: typography.monoBold,
  },
  overrideBody: {
    color: colors.textSecondary,
    fontSize: 12,
    lineHeight: 17,
    fontFamily: typography.uiRegular,
  },
  favoriteButton: {
    alignItems: 'center',
    justifyContent: 'center',
    minWidth: 24,
    minHeight: 24,
  },
  favoriteIcon: {
    color: colors.textSecondary,
    fontSize: 19,
    lineHeight: 22,
  },
  favoriteIconActive: {
    color: colors.gold,
  },
  comboCard: {
    backgroundColor: colors.surfaceElevated,
    borderRadius: 4,
    padding: 20,
    gap: 16,
    borderWidth: 1,
    borderColor: colors.borderStrong,
  },
  sectionLabel: {
    color: colors.label,
    fontSize: 12,
    fontWeight: '700',
    letterSpacing: 0.3,
    fontFamily: typography.uiSemiBold,
  },
  comboPrimaryMetric: { gap: 6 },
  comboSecondaryRow: { flexDirection: 'row', gap: 18 },
  comboSecondaryMetric: { flex: 1, gap: 6 },
  metricTitle: { color: colors.label, fontSize: 12, fontFamily: typography.uiRegular },
  metricValue: {
    color: colors.textPrimary,
    fontSize: 21,
    fontWeight: '700',
    lineHeight: 26,
    fontFamily: typography.monoBold,
  },
  comboPrimaryValue: {
    color: colors.textPrimary,
    fontSize: 23,
    fontWeight: '700',
    lineHeight: 29,
    fontFamily: typography.monoBold,
  },
  metricValueAccent: { color: colors.textPrimary, fontSize: 19, fontFamily: typography.monoSemiBold },
  couponValue: {
    color: colors.textPrimary,
    fontWeight: '700',
    fontSize: 15,
    fontFamily: typography.monoSemiBold,
  },
  metricSecondary: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '700',
    lineHeight: 20,
    fontFamily: typography.monoSemiBold,
  },
  comboNotes: { gap: 6 },
  note: { color: colors.textMuted, fontSize: 12, lineHeight: 18 },
  comboWarning: {
    color: colors.warning,
    fontSize: 12,
    lineHeight: 18,
    fontWeight: '600',
  },
  section: { gap: 10 },
  riskAnalysisIntro: {
    gap: 8,
  },
  riskAnalysisBlock: {
    gap: 18,
  },
  riskAnalysisHeadline: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '400',
    lineHeight: 26,
  },
  riskAnalysisInlineStrong: {
    color: colors.textPrimary,
    fontWeight: '700',
  },
  riskAnalysisGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    rowGap: 14,
  },
  riskAnalysisMetric: {
    width: '50%',
    paddingRight: 12,
    gap: 5,
  },
  riskAnalysisLabel: {
    color: colors.textSecondary,
    fontSize: 12,
    lineHeight: 16,
    letterSpacing: 0.2,
  },
  riskAnalysisValue: {
    color: colors.textPrimary,
    fontSize: 18,
    fontWeight: '700',
    lineHeight: 23,
  },
  riskAnalysisValueDanger: {
    color: colors.danger,
  },
  riskAnalysisValueWarning: {
    color: colors.warning,
  },
  riskAnalysisBody: {
    color: colors.textMuted,
    fontSize: 13,
    lineHeight: 20,
  },
  riskAnalysisCase: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
    paddingTop: 14,
    gap: 6,
  },
  riskAnalysisCaseEyebrow: {
    color: colors.textDisabled,
    fontSize: 10,
    fontWeight: '700',
    letterSpacing: 2,
    textTransform: 'uppercase',
    fontFamily: typography.monoBold,
  },
  riskAnalysisCaseText: {
    color: colors.textSecondary,
    fontSize: 13,
    lineHeight: 20,
  },
  riskInputBlock: {
    gap: 8,
  },
  riskControlHeader: {
    color: colors.textSecondary,
    fontSize: 10,
    lineHeight: 13,
  },
  riskInputLabel: {
    color: colors.textSecondary,
    fontSize: 11,
    fontWeight: '600',
    letterSpacing: 0.3,
  },
  riskControlStrip: {
    flexDirection: 'row',
    gap: 6,
    alignItems: 'flex-start',
  },
  riskControlCell: {
    minHeight: 34,
    justifyContent: 'center',
  },
  riskControlCellInput: {
    width: 84,
    flexShrink: 0,
  },
  riskControlCellHint: {
    flex: 1,
    minWidth: 0,
  },
  riskControlInputWrap: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  riskControlHintBlock: {
    gap: 2,
    justifyContent: 'center',
  },
  riskInput: {
    minWidth: 64,
    paddingHorizontal: 10,
    paddingVertical: 7,
    borderRadius: 3,
    borderWidth: 1,
    borderColor: colors.divider,
    backgroundColor: colors.background,
    color: colors.textPrimary,
    fontSize: 13,
    fontWeight: '500',
    fontFamily: typography.monoBold,
  },
  riskControlSuffix: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 16,
  },
  riskControlValue: {
    color: colors.textSecondary,
    fontSize: 11,
    fontWeight: '500',
    lineHeight: 16,
    fontFamily: typography.monoRegular,
  },
  riskControlHintText: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
  },
  riskInteractiveSummary: {
    gap: 10,
  },
  riskLoadingBlock: {
    gap: 10,
  },
  riskSummaryBand: {
    gap: 4,
    paddingTop: 2,
    paddingBottom: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
  },
  riskConclusionText: {
    color: colors.textPrimary,
    fontSize: 12,
    fontWeight: '500',
    lineHeight: 18,
  },
  riskConclusionStatusRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingTop: 2,
  },
  riskConclusionStatusItem: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 6,
  },
  riskConclusionStatusLabel: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 16,
  },
  riskConclusionStatusPill: {
    minWidth: 34,
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 3,
    alignItems: 'center',
  },
  riskConclusionStatusPillHigh: {
    backgroundColor: colors.warningSurface,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.warningBorder,
  },
  riskConclusionStatusPillMedium: {
    backgroundColor: 'rgba(138,155,176,0.14)',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(138,155,176,0.28)',
  },
  riskConclusionStatusPillLow: {
    backgroundColor: 'rgba(123,135,148,0.10)',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(123,135,148,0.22)',
  },
  riskConclusionStatusPillText: {
    fontSize: 10,
    lineHeight: 14,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  riskConclusionStatusPillTextHigh: {
    color: colors.warning,
  },
  riskConclusionStatusPillTextMedium: {
    color: '#AEB8C4',
  },
  riskConclusionStatusPillTextLow: {
    color: '#8A9BB0',
  },
  riskConclusionStatusDivider: {
    width: 1,
    alignSelf: 'stretch',
    backgroundColor: colors.divider,
  },
  riskConclusionHighlight: {
    color: colors.warning,
    fontWeight: '700',
  },
  riskConclusionNote: {
    color: colors.textPrimary,
    fontSize: 11,
    lineHeight: 17,
  },
  riskSummarySubline: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
  },
  riskSummaryMetaText: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
  },
  riskLoadingLineLong: {
    width: '92%',
    height: 12,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskLoadingLineShort: {
    width: '58%',
    height: 12,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskSummaryMetricGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
  },
  riskSummaryMetricCard: {
    flex: 1,
    flexBasis: '45%',
    gap: 3,
    position: 'relative',
    paddingVertical: 7,
    paddingHorizontal: 10,
    borderRadius: 3,
    borderWidth: 1,
    borderColor: colors.divider,
  },
  riskMetricStatusBadge: {
    position: 'absolute',
    top: 8,
    right: 8,
    minWidth: 28,
    paddingHorizontal: 6,
    paddingVertical: 1,
    borderRadius: 3,
    alignItems: 'center',
  },
  riskMetricStatusBadgeHigh: {
    backgroundColor: colors.warningSurface,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.warningBorder,
  },
  riskMetricStatusBadgeMedium: {
    backgroundColor: 'rgba(138,155,176,0.14)',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(138,155,176,0.28)',
  },
  riskMetricStatusBadgeLow: {
    backgroundColor: 'rgba(123,135,148,0.10)',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(123,135,148,0.22)',
  },
  riskMetricStatusBadgeText: {
    fontSize: 9,
    lineHeight: 12,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  riskMetricStatusBadgeTextHigh: {
    color: colors.warning,
  },
  riskMetricStatusBadgeTextMedium: {
    color: '#AEB8C4',
  },
  riskMetricStatusBadgeTextLow: {
    color: '#8A9BB0',
  },
  riskSummaryMetricValue: {
    color: colors.textPrimary,
    fontSize: 13,
    fontWeight: '500',
    lineHeight: 17,
    fontFamily: typography.monoSemiBold,
  },
  riskSummaryMetricLabel: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 13,
  },
  riskLoadingMetricValue: {
    width: '52%',
    height: 14,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskLoadingMetricLabel: {
    width: '42%',
    height: 10,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskDistributionBlock: {
    gap: 6,
    paddingTop: 2,
  },
  riskSectionBlock: {
    gap: 5,
    paddingTop: 2,
  },
  riskSectionHeaderRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  riskSectionMarker: {
    width: 2,
    height: 14,
    borderRadius: 3,
    backgroundColor: '#2A2F36',
  },
  riskDistributionBar: {
    flexDirection: 'row',
    alignItems: 'stretch',
    gap: 4,
    height: 5,
  },
  riskDistributionSegment: {
    borderRadius: 2,
  },
  riskDistributionSegmentFast: {
    backgroundColor: '#C9A06A',
  },
  riskDistributionSegmentMid: {
    backgroundColor: '#A87944',
  },
  riskDistributionSegmentSlow: {
    backgroundColor: '#785637',
  },
  riskDistributionSegmentUnrecovered: {
    backgroundColor: '#8A8A8A',
  },
  riskLoadingSegment: {
    backgroundColor: colors.surfaceElevated,
  },
  riskSectionEyebrow: {
    color: '#9FA8B2',
    fontSize: 11,
    fontWeight: '600',
    letterSpacing: 0.2,
    fontFamily: typography.monoBold,
  },
  riskSectionIntro: {
    color: '#A7B0BA',
    fontSize: 11,
    lineHeight: 17,
  },
  riskDistributionRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
  },
  riskDistributionLabelRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  riskDistributionDot: {
    width: 8,
    height: 8,
    borderRadius: 1,
  },
  riskLoadingDot: {
    width: 8,
    height: 8,
    borderRadius: 1,
    backgroundColor: colors.surfaceElevated,
  },
  riskDistributionLabel: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 16,
  },
  riskDistributionValue: {
    color: colors.textPrimary,
    fontSize: 11,
    fontWeight: '600',
    lineHeight: 16,
    fontFamily: typography.monoSemiBold,
  },
  riskLoadingDistributionLabel: {
    width: 72,
    height: 11,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskLoadingDistributionValue: {
    width: 34,
    height: 11,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  riskDistributionEmpty: {
    paddingVertical: 2,
  },
  riskDistributionEmptyText: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 17,
  },
  riskLoadingEyebrow: {
    width: 94,
    height: 11,
    borderRadius: 3,
    backgroundColor: colors.surfaceElevated,
  },
  drawdownEventSection: {
    gap: 9,
    paddingTop: 2,
  },
  drawdownEventIntroBlock: {
    gap: 4,
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 4,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(123, 135, 148, 0.22)',
    backgroundColor: 'rgba(255, 255, 255, 0.025)',
  },
  drawdownEventSectionIntro: {
    color: '#97A2AE',
    fontSize: 11,
    lineHeight: 17,
  },
  drawdownEventIntroMeta: {
    color: '#97A2AE',
    fontSize: 11,
    lineHeight: 17,
  },
  drawdownEventList: {
    gap: 0,
  },
  drawdownEventRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 10,
    paddingVertical: 6,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
  },
  drawdownEventRowLast: {
    borderBottomWidth: 0,
  },
  drawdownEventLeft: {
    width: 74,
    alignItems: 'flex-end',
    gap: 1,
    flexShrink: 0,
  },
  drawdownEventYear: {
    color: '#7E8792',
    fontSize: 10,
    lineHeight: 13,
    fontFamily: typography.monoRegular,
    width: '100%',
    textAlign: 'right',
  },
  drawdownEventPct: {
    color: colors.danger,
    fontSize: 13,
    fontWeight: '700',
    lineHeight: 17,
    fontFamily: typography.monoBold,
  },
  drawdownEventBadge: {
    marginTop: 1,
    paddingHorizontal: 5,
    paddingVertical: 1,
    borderRadius: 3,
    backgroundColor: 'rgba(201, 160, 106, 0.10)',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(201, 160, 106, 0.28)',
  },
  drawdownEventBadgeText: {
    color: '#C8A66D',
    fontSize: 8,
    lineHeight: 10,
    fontWeight: '500',
    letterSpacing: 0.12,
    fontFamily: typography.monoMedium,
  },
  drawdownEventReason: {
    flex: 1,
    color: '#A7B0BA',
    fontSize: 12,
    lineHeight: 19,
  },
  drawdownEventEmpty: {
    color: colors.textSecondary,
    fontSize: 12,
    lineHeight: 18,
  },
  riskStaticFooter: {
    paddingTop: 0,
    gap: 4,
  },
  riskStaticFooterStrip: {
    gap: 6,
  },
  riskStaticFooterLabel: {
    color: '#B8C0CA',
    fontSize: 10,
    lineHeight: 14,
    fontWeight: '700',
  },
  riskStaticFooterMetrics: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 10,
  },
  riskStaticFooterMetric: {
    flex: 1,
    gap: 2,
  },
  riskStaticFooterMetricDivider: {
    width: StyleSheet.hairlineWidth,
    alignSelf: 'stretch',
    backgroundColor: colors.divider,
  },
  riskStaticFooterMetricLabel: {
    color: '#7E8792',
    fontSize: 10,
    lineHeight: 14,
  },
  riskStaticFooterMetricValue: {
    color: '#AEB7C2',
    fontSize: 11,
    lineHeight: 16,
    fontWeight: '600',
    fontFamily: typography.monoSemiBold,
  },
  riskStaticFooterNote: {
    color: '#9CA3AF',
    fontSize: 10,
    lineHeight: 14,
  },
  riskStaticFooterDivider: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
  },
  riskMethodologyInline: {
    gap: 6,
    paddingTop: 8,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
  },
  riskMethodologySummary: {
    color: '#9CA3AF',
    fontSize: 11,
    lineHeight: 17,
  },
  riskMethodologyExpanded: {
    gap: 4,
    paddingTop: 2,
  },
  riskMethodologyExpandedText: {
    color: 'rgba(107, 114, 128, 0.78)',
    fontSize: 10,
    lineHeight: 15,
  },
  sectionHeading: {
    gap: 5,
  },
  sectionEyebrow: {
    color: colors.label,
    fontSize: 10,
    fontWeight: '700',
    letterSpacing: 2.1,
    textTransform: 'uppercase',
    fontFamily: typography.monoBold,
  },
  sectionTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 8,
  },
  sectionTitle: { color: colors.textPrimary, fontSize: 20, fontWeight: '700' },
  sectionInfoButton: {
    width: 18,
    height: 18,
    borderRadius: 3,
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: 'rgba(123, 135, 148, 0.34)',
    backgroundColor: 'rgba(255, 255, 255, 0.03)',
  },
  sectionInfoButtonText: {
    color: '#A7B0BA',
    fontSize: 11,
    lineHeight: 13,
    fontWeight: '700',
  },
  dimensionRow: {
    gap: 8,
    paddingVertical: 10,
  },
  dimensionRowWithDivider: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
    paddingTop: 16,
  },
  dimensionHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
  },
  dimensionHeaderRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  dimensionLabel: {
    color: colors.textPrimary,
    fontSize: 15,
    fontWeight: '600',
  },
  dimensionStatus: {
    fontSize: 13,
    fontWeight: '700',
    letterSpacing: 0.2,
  },
  dimensionChevron: {
    color: colors.textMuted,
    fontSize: 14,
    lineHeight: 16,
  },
  dimensionSummary: {
    color: colors.textSecondary,
    fontSize: 13,
    lineHeight: 19,
  },
  dimensionDetail: {
    color: colors.textSecondary,
    fontSize: 13,
    lineHeight: 20,
    paddingTop: 2,
  },
  progressTrack: {
    height: 8,
    borderRadius: 3,
    backgroundColor: colors.track,
    overflow: 'hidden',
  },
  progressFill: {
    height: '100%',
    borderRadius: 3,
  },
  trendCard: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    padding: 16,
    gap: 14,
  },
  trendEmptyCard: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    padding: 16,
    gap: 8,
  },
  trendEmptyTitle: {
    color: colors.textPrimary,
    fontSize: 15,
    fontWeight: '700',
  },
  trendEmptyText: {
    color: colors.textSecondary,
    fontSize: 13,
    lineHeight: 19,
  },
  trendHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: 12,
  },
  trendHeaderLeft: {
    gap: 4,
  },
  trendPrice: {
    color: colors.textPrimary,
    fontSize: 24,
    fontWeight: '800',
    lineHeight: 28,
    fontFamily: typography.monoBold,
  },
  trendChange: {
    fontSize: 14,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  trendChangePositive: {
    color: colors.success,
  },
  trendChangeNegative: {
    color: colors.danger,
  },
  trendDate: {
    color: colors.textMuted,
    fontSize: 12,
    lineHeight: 17,
    textAlign: 'right',
  },
  chartFrame: {
    height: 128,
    justifyContent: 'center',
  },
  chartAxisLabel: {
    position: 'absolute',
    right: 0,
    color: colors.textDisabled,
    fontSize: 11,
  },
  chartCanvas: {
    flex: 1,
    height: 128,
    marginRight: 34,
    position: 'relative',
  },
  rangeSwitcher: {
    flexDirection: 'row',
    gap: 8,
  },
  rangeChip: {
    borderRadius: 3,
    paddingHorizontal: 12,
    paddingVertical: 7,
    backgroundColor: colors.surface,
  },
  rangeChipActive: {
    backgroundColor: 'transparent',
  },
  rangeChipText: {
    color: colors.textMuted,
    fontSize: 12,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  rangeChipTextActive: {
    color: colors.textPrimary,
  },
  priceRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
    paddingVertical: 12,
  },
  priceLabel: { color: colors.textSecondary, fontSize: 15 },
  priceValue: { color: colors.textPrimary, fontSize: 15, fontWeight: '600', fontFamily: typography.monoSemiBold },
  newsList: { gap: 10 },
  centeredPage: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    gap: 10,
    paddingHorizontal: 24,
  },
  centeredText: { color: colors.textSecondary },
  retry: { color: colors.success, fontWeight: '700' },
  skeletonBase: {
    backgroundColor: colors.surface,
    borderRadius: 3,
  },
  companySkeleton: {
    width: 188,
    height: 18,
    marginTop: 6,
  },
  scoreSkeleton: {
    width: 124,
    height: 18,
  },
  gradeSkeleton: {
    width: 88,
    height: 38,
    borderRadius: 3,
  },
  favoriteSkeleton: {
    width: 24,
    height: 24,
  },
  comboPrimaryValueSkeleton: {
    width: 102,
    height: 28,
    borderRadius: 4,
    marginTop: 6,
  },
  metricValueSkeleton: {
    width: 76,
    height: 28,
    borderRadius: 4,
    marginTop: 6,
  },
  metricSecondarySkeleton: {
    width: 94,
    height: 16,
    borderRadius: 4,
    marginTop: 8,
  },
  noteSkeleton: {
    width: '68%',
    height: 14,
    borderRadius: 4,
  },
  skeletonCard: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    padding: 18,
    gap: 10,
    borderWidth: 1,
    borderColor: colors.border,
  },
  textLineLong: {
    width: '70%',
    height: 16,
    borderRadius: 4,
  },
  textLineFull: {
    width: '100%',
    height: 16,
    borderRadius: 4,
  },
  earningsWaitCard: {
    backgroundColor: colors.surfaceElevated,
    borderRadius: 4,
    padding: 16,
    gap: 8,
    borderLeftWidth: 3,
    borderLeftColor: colors.warning,
  },
  earningsWaitTitle: {
    color: colors.textSecondary,
    fontSize: 12,
    fontWeight: '700',
    letterSpacing: 0.8,
  },
  earningsWaitBody: {
    color: colors.textSecondary,
    fontSize: 14,
    lineHeight: 22,
  },
  dimensionLabelSkeleton: {
    width: 62,
    height: 16,
  },
  dimensionStatusSkeleton: {
    width: 44,
    height: 14,
  },
  dimensionChevronSkeleton: {
    width: 12,
    height: 12,
    borderRadius: 6,
  },
  dimensionSummarySkeleton: {
    width: '74%',
    height: 14,
    borderRadius: 8,
  },
  progressFillSkeleton: {
    width: '55%',
    backgroundColor: colors.surface,
  },
});
