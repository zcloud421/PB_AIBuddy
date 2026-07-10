import { useLocalSearchParams, useRouter } from 'expo-router';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { Pressable, SafeAreaView, ScrollView, StyleSheet, Text, View } from 'react-native';

import { API_BASE } from '../../../constants/api';
import { colors } from '../../../constants/colors';
import { typography } from '../../../constants/typography';
import type { PairAnalysisResponse } from '../../../types/api';

type ApiErrorBody = {
  message?: string;
};

type FieldTone = 'pass' | 'neutral' | 'caveat' | 'fail' | 'muted';

type SuitabilityMetrics = {
  downSyncTone: FieldTone;
  bearTone: FieldTone;
  dailyTone: FieldTone;
  met: number;
};

type Criterion = {
  id: string;
  prefix: '✓' | '!' | '✗' | '~' | '·';
  label: string;
  sublabel?: string;
  value: string;
  tone: FieldTone;
};

type CorrelationDisplayRow = {
  label: string;
  value: number | null | undefined;
  tone: FieldTone;
};

async function fetchPairAnalysis(symbolA: string, symbolB: string) {
  const response = await axios.post<PairAnalysisResponse>(`${API_BASE}/api/pair-analysis`, {
    symbolA,
    symbolB,
  });

  return response.data;
}

export default function PairAnalysisScreen() {
  const router = useRouter();
  const params = useLocalSearchParams<{ symbolA?: string; symbolB?: string }>();
  const symbolA = String(params.symbolA ?? '').toUpperCase();
  const symbolB = String(params.symbolB ?? '').toUpperCase();

  const query = useQuery({
    queryKey: ['pair-analysis', symbolA, symbolB],
    queryFn: () => fetchPairAnalysis(symbolA, symbolB),
    enabled: Boolean(symbolA && symbolB),
  });

  const errorMessage = getErrorMessage(query.error);

  return (
    <SafeAreaView style={styles.safeArea}>
      <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
        <Pressable onPress={() => router.back()}>
          <Text style={styles.back}>← 返回</Text>
        </Pressable>

        {query.isLoading ? (
          <PairAnalysisSkeleton symbolA={symbolA} symbolB={symbolB} />
        ) : query.isError || !query.data ? (
          <View style={styles.errorWrap}>
            <Text style={styles.errorText}>{errorMessage}</Text>
            <Pressable onPress={() => router.back()}>
              <Text style={styles.back}>← 返回</Text>
            </Pressable>
          </View>
        ) : (
          <PairAnalysisContent data={query.data} />
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

function PairAnalysisContent({ data }: { data: PairAnalysisResponse }) {
  const verdictTone = getVerdictTone(data.suitability);
  const metrics = buildSuitabilityMetrics(data);
  const criteria = buildCriteria(data, metrics);
  const structured = data.suitability_note_structured;

  return (
    <>
      <View style={styles.headerRow}>
        <Text style={styles.tickerPair}>{data.symbolA} × {data.symbolB}</Text>
        <View style={styles.metaCol}>
          <Text style={styles.metaLine}>{data.trading_days_overlap} TRD DAYS</Text>
          <Text style={styles.metaLine}>AS-OF {formatAsOf(data.data_as_of)}</Text>
        </View>
      </View>

      <View style={styles.sectionDivider} />

      <View style={styles.verdictBlock}>
        <View style={styles.verdictRow}>
          <View style={styles.verdictLeftCol}>
            <Text style={styles.sectionSubtitle}>挂钩双标的适合度</Text>
          </View>
          <View style={styles.verdictRightCol}>
            <View
              style={[
                styles.verdictPill,
                { borderColor: verdictTone.color, backgroundColor: verdictTone.tint },
              ]}
            >
              <Text style={[styles.verdictPillText, { color: verdictTone.color }]}>
                {getVerdictText(data.suitability)}
              </Text>
            </View>
          </View>
        </View>

        {structured ? (
          <View style={styles.criteriaSection}>
            <Text style={styles.sectionLabel}>评估维度</Text>
            <View style={styles.criteriaList}>
              {criteria.map((criterion) => (
                <View key={criterion.id}>
                  <CriterionRow criterion={criterion} />
                </View>
              ))}
            </View>
            {structured.next_step ? (
              <Text style={styles.recommendationInline}>建议:{structured.next_step}</Text>
            ) : null}
          </View>
        ) : null}
      </View>

      <View style={styles.sectionDivider} />

      <CorrelationDetail data={data} />

      <Text style={styles.disclaimer}>
        本页为挂钩双标的相关性参考工具，基于历史价格序列计算；
        {'\n'}历史相关性不代表未来表现，具体产品执行以机构 house view、适当性要求和产品政策为准。
      </Text>
    </>
  );
}

function CriterionRow({ criterion }: { criterion: Criterion }) {
  const color = toneColor(criterion.tone);
  return (
    <View style={styles.criteriaRow}>
      <Text style={[styles.criteriaPrefix, { color }]}>{criterion.prefix}</Text>
      <View style={styles.criteriaTextCol}>
        <Text style={styles.criteriaName}>{criterion.label}</Text>
        {criterion.sublabel ? <Text style={styles.criteriaSubName}>{criterion.sublabel}</Text> : null}
      </View>
      <Text style={[styles.criteriaValue, { color }]}>{criterion.value}</Text>
    </View>
  );
}

function CorrelationDetail({ data }: { data: PairAnalysisResponse }) {
  const rows: CorrelationDisplayRow[] = [];
  rows.push(
    { label: '3M', value: data.correlation.d90, tone: correlationTone(data.correlation.d90, 'daily') },
    { label: '4M', value: data.correlation.d120, tone: correlationTone(data.correlation.d120, 'daily') },
    { label: '6M', value: data.correlation.d180, tone: correlationTone(data.correlation.d180, 'daily') },
    { label: '1Y', value: data.correlation.d252, tone: correlationTone(data.correlation.d252, 'daily') },
    {
      label: '2022\nBEAR',
      value: data.correlation.bear_2022,
      tone: data.correlation.bear_2022 === null
        ? 'muted'
        : correlationTone(data.correlation.bear_2022, 'bear'),
    },
  );

  return (
    <View style={styles.correlationSection}>
      <View style={styles.correlationDetailHeader}>
        <Text style={styles.correlationTitle}>历史相关性</Text>
        <StabilityChip stability={data.correlation_stability} />
      </View>
      <ScaleAxis />
      <View style={styles.correlationRowsWrap}>
        {rows.map((row) => (
          <CorrelationMarkerRow key={row.label} label={row.label} value={row.value} tone={row.tone} />
        ))}
      </View>
    </View>
  );
}

function ScaleAxis() {
  return (
    <View style={styles.scaleAxisRow}>
      <View style={styles.corrLabelCol} />
      <View style={styles.scaleTrack}>
        {[0, 0.3, 0.6, 0.9].map((tick) => (
          <Text key={tick} style={[styles.scaleTick, { left: `${correlationAxisPosition(tick)}%` }]}>
            {tick.toFixed(1)}
          </Text>
        ))}
      </View>
      <View style={styles.corrValueCol} />
    </View>
  );
}

function CorrelationMarkerRow({
  label,
  value,
  tone,
}: {
  label: string;
  value: number | null | undefined;
  tone: FieldTone;
}) {
  const numeric = typeof value === 'number' && Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : null;
  const color = toneColor(tone);
  const markerLeft = numeric !== null ? correlationAxisPosition(numeric) : null;

  return (
    <View style={styles.correlationMarkerRow}>
      <Text style={styles.corrRowLabel}>{label}</Text>
      <View style={styles.corrBarWrap}>
        <View style={styles.corrBarLine} />
        {markerLeft !== null ? (
          <View style={[styles.corrMarker, { left: `${markerLeft}%`, backgroundColor: color }]} />
        ) : null}
      </View>
      <Text style={[styles.corrRowValue, { color }]}>
        {numeric !== null ? numeric.toFixed(2) : '数据不足'}
      </Text>
    </View>
  );
}

function StabilityChip({ stability }: { stability: PairAnalysisResponse['correlation_stability'] }) {
  const color = getStabilityColor(stability);
  return (
    <View style={styles.stabilityChipRow}>
      <Text style={[styles.stabilityDot, { color }]}>●</Text>
      <Text style={styles.stabilityChipText}>{getStabilityText(stability)}</Text>
    </View>
  );
}

function PairAnalysisSkeleton({ symbolA, symbolB }: { symbolA: string; symbolB: string }) {
  return (
    <>
      <View style={styles.headerRow}>
        <Text style={styles.tickerPair}>{symbolA} × {symbolB}</Text>
        <View style={styles.metaCol}>
          <View style={[styles.skeletonBase, styles.metaSkeleton]} />
          <View style={[styles.skeletonBase, styles.metaSkeletonShort]} />
        </View>
      </View>

      <View style={styles.sectionDivider} />

      <View style={styles.skeletonVerdictBlock}>
        <View style={[styles.skeletonBase, styles.labelSkeleton]} />
        <View style={[styles.skeletonBase, styles.verdictSkeleton]} />
        <View style={[styles.skeletonBase, styles.noteSkeletonLong]} />
      </View>

      <View style={styles.sectionDivider} />

      <View style={styles.correlationSection}>
        <View style={[styles.skeletonBase, styles.labelSkeleton]} />
        {[1, 2, 3, 4].map((row) => (
          <View key={row} style={styles.correlationMarkerRow}>
            <View style={[styles.skeletonBase, styles.rowLabelSkeleton]} />
            <View style={[styles.skeletonBase, styles.rowTrackSkeleton]} />
            <View style={[styles.skeletonBase, styles.rowValueSkeleton]} />
          </View>
        ))}
      </View>
    </>
  );
}

function buildSuitabilityMetrics(data: PairAnalysisResponse): SuitabilityMetrics {
  const maxDailyCorr = Math.max(data.correlation.d90, data.correlation.d180, data.correlation.d252);
  const downSyncHigh = data.downside_sync >= 0.70;
  const hasBear2022 = data.correlation.bear_2022 !== null;
  const bearHigh = hasBear2022 && data.correlation.bear_2022! >= 0.60;
  const dailyHigh = data.correlation.d90 >= 0.50;

  return {
    downSyncTone: data.downside_sync < 0.55 ? 'fail' : downSyncHigh ? 'pass' : 'neutral',
    bearTone: !hasBear2022 ? 'muted' : data.correlation.bear_2022! < 0.40 ? 'fail' : bearHigh ? 'pass' : 'neutral',
    dailyTone: maxDailyCorr < 0.30 ? 'fail' : dailyHigh ? 'pass' : 'neutral',
    met: [downSyncHigh, hasBear2022 ? bearHigh : false, dailyHigh].filter(Boolean).length,
  };
}

function buildCriteria(data: PairAnalysisResponse, metrics: SuitabilityMetrics): Criterion[] {
  const rows: Criterion[] = [
    criterion('downside-sync', '下跌同步率', `${Math.round(data.downside_sync * 100)}%`, metrics.downSyncTone),
    data.correlation.bear_2022 === null
      ? { id: 'bear-corr', prefix: '·', label: '2022 熊市相关性', value: '数据不足', tone: 'muted' }
      : criterion('bear-corr', '2022 熊市相关性', data.correlation.bear_2022.toFixed(2), metrics.bearTone),
    criterion('daily-corr', '3M 相关性', data.correlation.d90.toFixed(2), metrics.dailyTone, data.correlation.d90 < 0.30),
  ];

  if (data.volatility?.gap_flag) {
    const leg = data.volatility.gap_leg ?? '高波动标的';
    const peer =
      leg === data.symbolA
        ? data.symbolB
        : leg === data.symbolB
          ? data.symbolA
          : '对手';
    const ratio = data.volatility.ratio ?? 1;
    rows.push({
      id: 'vol-gap',
      prefix: '!',
      label: `主要风险腿: ${leg}`,
      sublabel: `波动率较 ${peer} 高`,
      value: `${Math.round(Math.max(0, ratio - 1) * 100)}%`,
      tone: 'caveat',
    });
  }

  return rows;
}

function criterion(id: string, label: string, value: string, tone: FieldTone, forceFail = false): Criterion {
  if (tone === 'fail' || forceFail) return { id, label, value, prefix: '✗', tone: 'fail' };
  if (tone === 'pass') return { id, label, value, prefix: '✓', tone: 'pass' };
  return { id, label, value, prefix: '~', tone: 'muted' };
}

function getErrorMessage(error: unknown) {
  if (!axios.isAxiosError(error)) {
    return '请求失败';
  }

  const apiMessage = (error.response?.data as ApiErrorBody | undefined)?.message;
  return apiMessage ?? error.message ?? '请求失败';
}

function getVerdictTone(suitability: PairAnalysisResponse['suitability']) {
  if (suitability === 'HIGH') return { color: colors.success, tint: colors.statusSuccessTint };
  if (suitability === 'LOW') return { color: colors.danger, tint: colors.statusDangerTint };
  return { color: colors.warning, tint: colors.statusWarningTint };
}

function getVerdictText(suitability: PairAnalysisResponse['suitability']) {
  if (suitability === 'HIGH') return '高';
  if (suitability === 'LOW') return '低';
  return '中';
}

function getStabilityText(stability: PairAnalysisResponse['correlation_stability']) {
  if (stability === 'STABLE') return '稳定';
  if (stability === 'UNSTABLE') return '不稳定';
  return '波动中';
}

function getStabilityColor(stability: PairAnalysisResponse['correlation_stability']) {
  if (stability === 'STABLE') return colors.success;
  if (stability === 'UNSTABLE') return colors.danger;
  return colors.warning;
}

function toneColor(tone: FieldTone) {
  if (tone === 'pass') return colors.success;
  if (tone === 'fail') return colors.danger;
  if (tone === 'caveat' || tone === 'neutral') return colors.warning;
  return colors.textMuted;
}

function correlationTone(value: number | null | undefined, kind: 'daily' | 'bear' = 'daily'): FieldTone {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 'muted';
  const passThreshold = kind === 'bear' ? 0.60 : 0.50;
  const failThreshold = kind === 'bear' ? 0.40 : 0.30;
  if (value >= passThreshold) return 'pass';
  if (value >= failThreshold) return 'caveat';
  return 'fail';
}

function correlationAxisPosition(value: number) {
  const clamped = Math.max(0, Math.min(1, value));
  return clamped * 100;
}

function formatAsOf(date: string) {
  return date.replace(/-/g, '.');
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: colors.background,
  },
  content: {
    padding: 20,
    paddingBottom: 48,
  },
  back: {
    color: colors.link,
    fontSize: 15,
    fontWeight: '700',
  },
  headerRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginTop: 36,
    marginBottom: 14,
    gap: 16,
  },
  sectionDivider: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: colors.divider,
    opacity: 0.5,
    marginVertical: 0,
  },
  tickerPair: {
    fontFamily: typography.monoBold,
    fontSize: 23,
    fontWeight: '700',
    color: colors.textPrimary,
    letterSpacing: 0.5,
  },
  metaCol: {
    alignItems: 'flex-end',
    gap: 3,
  },
  metaLine: {
    fontFamily: typography.monoRegular,
    fontSize: 11,
    color: colors.textMuted,
    letterSpacing: 0.5,
    textTransform: 'uppercase',
  },
  verdictBlock: {
    marginTop: 16,
    marginBottom: 16,
  },
  verdictRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 8,
    gap: 12,
  },
  verdictLeftCol: {
    flex: 1,
  },
  verdictRightCol: {
    alignItems: 'flex-end',
  },
  sectionLabel: {
    fontFamily: typography.uiRegular,
    fontSize: 12,
    color: colors.textMuted,
    fontWeight: '500',
    letterSpacing: 0.5,
    marginBottom: 10,
  },
  sectionSubtitle: {
    fontSize: 16,
    color: colors.textPrimary,
    fontFamily: typography.uiSemiBold,
    fontWeight: '700',
  },
  verdictPill: {
    borderWidth: 1,
    borderRadius: 4,
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  verdictPillText: {
    fontFamily: typography.monoBold,
    fontSize: 14,
    lineHeight: 18,
    fontWeight: '700',
    letterSpacing: 0.5,
  },
  criteriaSection: {
    marginTop: 8,
    marginBottom: 10,
  },
  criteriaList: {
    marginTop: 4,
  },
  criteriaRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
  },
  criteriaPrefix: {
    fontFamily: typography.monoBold,
    fontSize: 13,
    width: 24,
  },
  criteriaTextCol: {
    flex: 1,
    paddingRight: 10,
  },
  criteriaName: {
    fontSize: 14,
    color: colors.textPrimary,
    fontFamily: typography.uiRegular,
  },
  criteriaSubName: {
    marginTop: 3,
    fontSize: 12,
    lineHeight: 16,
    color: colors.textMuted,
    fontFamily: typography.uiRegular,
  },
  criteriaValue: {
    fontFamily: typography.monoBold,
    fontSize: 14,
    minWidth: 48,
    textAlign: 'right',
  },
  recommendationInline: {
    fontSize: 13,
    lineHeight: 18,
    color: colors.textMuted,
    fontFamily: typography.uiRegular,
    fontStyle: 'italic',
    marginTop: 12,
  },
  correlationSection: {
    gap: 10,
    marginTop: 16,
    marginBottom: 18,
  },
  correlationDetailHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'baseline',
    gap: 10,
  },
  correlationTitle: {
    fontSize: 12,
    color: colors.textMuted,
    fontFamily: typography.uiRegular,
    fontWeight: '500',
    letterSpacing: 0.5,
  },
  stabilityChipRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 5,
  },
  stabilityDot: {
    fontSize: 9,
    lineHeight: 12,
  },
  stabilityChipText: {
    fontFamily: typography.monoRegular,
    fontSize: 11,
    color: colors.textMuted,
    letterSpacing: 0.6,
    textTransform: 'uppercase',
  },
  scaleAxisRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: 4,
    marginBottom: -2,
  },
  corrLabelCol: {
    width: 58,
  },
  corrValueCol: {
    width: 60,
  },
  scaleTrack: {
    flex: 1,
    height: 16,
    position: 'relative',
  },
  scaleTick: {
    position: 'absolute',
    top: 0,
    marginLeft: -9,
    fontFamily: typography.monoRegular,
    fontSize: 10,
    color: colors.textMuted,
  },
  correlationRowsWrap: {
    gap: 12,
  },
  correlationMarkerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    minHeight: 24,
  },
  corrRowLabel: {
    width: 58,
    fontFamily: typography.monoRegular,
    fontSize: 11,
    lineHeight: 13,
    color: colors.textMuted,
    letterSpacing: 0.4,
  },
  corrBarWrap: {
    flex: 1,
    height: 16,
    justifyContent: 'center',
    position: 'relative',
  },
  corrBarLine: {
    height: StyleSheet.hairlineWidth,
    backgroundColor: colors.divider,
  },
  corrMarker: {
    position: 'absolute',
    width: 8,
    height: 8,
    borderRadius: 4,
    marginLeft: -4,
  },
  corrRowValue: {
    width: 60,
    textAlign: 'right',
    fontFamily: typography.monoBold,
    fontSize: 13,
    color: colors.textMuted,
  },
  disclaimer: {
    fontSize: 11,
    lineHeight: 16,
    color: colors.textMuted,
    textAlign: 'center',
    marginTop: 32,
    marginBottom: 24,
    paddingHorizontal: 12,
    letterSpacing: 0.3,
  },
  errorWrap: {
    flex: 1,
    minHeight: 420,
    alignItems: 'center',
    justifyContent: 'center',
    gap: 16,
  },
  errorText: {
    color: colors.textSecondary,
    fontSize: 15,
    lineHeight: 24,
    textAlign: 'center',
  },
  skeletonBase: {
    backgroundColor: colors.surface,
    borderRadius: 4,
  },
  skeletonVerdictBlock: {
    marginTop: 20,
    marginBottom: 20,
    gap: 12,
  },
  labelSkeleton: {
    height: 12,
    width: 128,
  },
  verdictSkeleton: {
    height: 30,
    width: 94,
  },
  noteSkeletonLong: {
    height: 16,
    width: '82%',
  },
  metaSkeleton: {
    height: 12,
    width: 92,
  },
  metaSkeletonShort: {
    height: 12,
    width: 112,
  },
  rowLabelSkeleton: {
    height: 14,
    width: 46,
  },
  rowTrackSkeleton: {
    flex: 1,
    height: 8,
  },
  rowValueSkeleton: {
    height: 14,
    width: 42,
  },
});
