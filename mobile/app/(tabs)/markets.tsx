import { Feather } from '@expo/vector-icons';
import { useState } from 'react';
import {
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from 'react-native';

import { AppPageFrame, CenteredState, sharedStyles as shared } from '../../components/tab-shared';
import { colors } from '../../constants/colors';
import { typography } from '../../constants/typography';
import { useMacroRegime } from '../../hooks/useMacroRegime';
import type {
  ExposureTimingAsset,
  ExposureTimingStatus,
  MacroRegimeSnapshot,
  RegimeVerdictState,
} from '../../types/macro-regime';

const STATUS_META: Record<
  ExposureTimingStatus,
  { label: string; color: string; icon: keyof typeof Feather.glyphMap }
> = {
  BUILD_WINDOW: { label: '下行压力缓和', color: colors.success, icon: 'check-circle' },
  WATCH_SUPPORT: { label: '下行风险待确认', color: colors.warning, icon: 'eye' },
  EXTENDED: { label: '下行风险待确认', color: colors.warning, icon: 'arrow-down-right' },
  WAIT: { label: '下行风险仍高', color: colors.danger, icon: 'alert-circle' },
};

const RISK_BUCKETS = [
  { key: 'easing', label: '压力缓和', color: colors.success, statuses: ['BUILD_WINDOW'] },
  { key: 'pending', label: '风险待确认', color: colors.warning, statuses: ['WATCH_SUPPORT', 'EXTENDED'] },
  { key: 'high', label: '风险仍高', color: colors.danger, statuses: ['WAIT'] },
] as const;

const MACRO_META: Record<
  RegimeVerdictState,
  { label: string; color: string; scope: string }
> = {
  STABLE: { label: '宏观风险未阻断', color: colors.success, scope: '技术支撑状态可独立判断' },
  NOISE: { label: '宏观风险未阻断', color: colors.link, scope: '当前波动仍偏技术性' },
  BREAK_FORMING: { label: '宏观风险形成', color: colors.warning, scope: '技术支撑需要更高确认度' },
  CONFIRMED_BREAK: { label: '宏观风险确认', color: colors.danger, scope: '系统性机制确认，技术支撑可靠性下降' },
};

export default function MarketsTab() {
  const { data, isLoading, isError, refetch, isRefreshing, error } = useMacroRegime();
  const [expandedSymbol, setExpandedSymbol] = useState<string | null | undefined>(undefined);

  if (isLoading) {
    return (
      <AppPageFrame>
        <CenteredState text="加载下行风险 …" />
      </AppPageFrame>
    );
  }

  if (isError || !data) {
    const message = error instanceof Error && error.message ? `加载失败：${error.message}` : '加载失败';
    return (
      <AppPageFrame>
        <CenteredState text={message} onPress={() => refetch()} />
      </AppPageFrame>
    );
  }

  const assets = [...(data.exposure_timing?.assets ?? [])].sort(
    (a, b) => b.readiness_rank - a.readiness_rank || a.symbol.localeCompare(b.symbol)
  );
  const activeSymbol = expandedSymbol === undefined ? (assets[0]?.symbol ?? null) : expandedSymbol;

  return (
    <AppPageFrame>
      <ScrollView
        contentContainerStyle={shared.scrollContent}
        showsVerticalScrollIndicator={false}
        refreshControl={
          <RefreshControl
            refreshing={isRefreshing}
            onRefresh={() => refetch()}
            tintColor={colors.textMuted}
          />
        }
      >
        <PageHeading asOf={data.exposure_timing?.as_of ?? data.as_of} />
        <MacroGate snapshot={data} />

        {assets.length > 0 ? (
          <>
            <ReadinessSummary assets={assets} />
            <View style={styles.assetList}>
              {assets.map((asset) => (
                <TimingRow
                  key={asset.symbol}
                  asset={asset}
                  expanded={activeSymbol === asset.symbol}
                  onToggle={() => {
                    setExpandedSymbol((current) => {
                      const resolved = current === undefined ? (assets[0]?.symbol ?? null) : current;
                      return resolved === asset.symbol ? null : asset.symbol;
                    });
                  }}
                />
              ))}
            </View>
          </>
        ) : (
          <View style={styles.pendingBlock}>
            <Text style={styles.pendingTitle}>等待下一次市场快照</Text>
            <Text style={styles.pendingText}>
              下行风险数据将在美股收盘后的下一次刷新生成。
            </Text>
          </View>
        )}

        <View style={styles.footerBlock}>
          <Text style={styles.footer}>美股收盘后每日刷新</Text>
          <Text style={styles.complianceNote}>
            本页提供客观市场状态、价格支撑与风险转换条件，供 RM / IC 内部研究参考；不构成针对任何客户或产品的投资建议。
          </Text>
        </View>
      </ScrollView>
    </AppPageFrame>
  );
}

function PageHeading({ asOf }: { asOf: string }) {
  return (
    <View style={styles.pageHeading}>
      <View>
        <Text style={styles.eyebrow}>DOWNSIDE RISK</Text>
        <Text style={styles.pageTitle}>下行风险</Text>
      </View>
      <Text style={styles.asOf}>{asOf}</Text>
    </View>
  );
}

function MacroGate({ snapshot }: { snapshot: MacroRegimeSnapshot }) {
  const verdict = snapshot.regime_verdict;
  if (!verdict) {
    return (
      <View style={styles.macroGate}>
        <View style={[styles.statusDot, { backgroundColor: colors.neutral }]} />
        <View style={styles.macroCopy}>
          <Text style={styles.macroTitle}>宏观状态待更新</Text>
          <Text style={styles.macroScope}>技术状态保留，但不形成高置信度窗口。</Text>
        </View>
      </View>
    );
  }

  const meta = MACRO_META[verdict.state];
  return (
    <View style={styles.macroGate}>
      <View style={[styles.statusDot, { backgroundColor: meta.color }]} />
      <View style={styles.macroCopy}>
        <View style={styles.macroTitleLine}>
          <Text style={[styles.macroTitle, { color: meta.color }]}>{meta.label}</Text>
          <Text style={styles.macroState}>{verdict.state}</Text>
        </View>
        <Text style={styles.macroScope}>{meta.scope}</Text>
        <Text style={styles.macroOneLine}>{verdict.one_line}</Text>
      </View>
    </View>
  );
}

function ReadinessSummary({ assets }: { assets: ExposureTimingAsset[] }) {
  return (
    <View style={styles.summaryBlock}>
      <View style={styles.sectionTitleLine}>
        <Text style={styles.sectionTitle}>风险状态</Text>
        <Text style={styles.sectionMeta}>按下行压力排序</Text>
      </View>
      <View style={styles.countLine}>
        {RISK_BUCKETS.map((bucket) => (
          <View key={bucket.key} style={styles.countItem}>
            <View style={[styles.countBar, { backgroundColor: bucket.color }]} />
            <Text style={styles.countLabel}>{bucket.label}</Text>
            <Text style={styles.countValue}>
              {assets.filter((asset) => bucket.statuses.some((status) => status === asset.status)).length}
            </Text>
          </View>
        ))}
      </View>
    </View>
  );
}

function TimingRow({
  asset,
  expanded,
  onToggle,
}: {
  asset: ExposureTimingAsset;
  expanded: boolean;
  onToggle: () => void;
}) {
  const meta = STATUS_META[asset.status];
  const changeColor =
    asset.change_5d_pct === null
      ? colors.textMuted
      : asset.change_5d_pct >= 0
        ? colors.success
        : colors.danger;

  return (
    <View style={styles.assetRow}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`${asset.symbol} ${meta.label}`}
        onPress={onToggle}
        style={({ pressed }) => [styles.assetToggle, pressed && styles.assetTogglePressed]}
      >
        <View style={styles.assetTopLine}>
          <View style={styles.assetIdentity}>
            <Text style={styles.assetSymbol}>{asset.symbol}</Text>
            <Text style={styles.assetLabel}>{asset.label}</Text>
          </View>
          <View style={styles.assetMarketData}>
            <Text style={styles.assetPrice}>{formatPrice(asset.current_price)}</Text>
            <Text style={[styles.assetChange, { color: changeColor }]}>
              {formatChange(asset.change_5d_pct)}
            </Text>
          </View>
        </View>

        <View style={styles.statusLine}>
          <Feather name={meta.icon} size={14} color={meta.color} />
          <Text style={[styles.statusText, { color: meta.color }]}>{meta.label}</Text>
          {asset.support ? (
            <Text style={styles.supportText}>
              {asset.support.label} {formatDistance(asset.support.distance_pct)}
            </Text>
          ) : null}
          <Feather
            name={expanded ? 'chevron-up' : 'chevron-down'}
            size={16}
            color={colors.textMuted}
            style={styles.expandIcon}
          />
        </View>
      </Pressable>

      {expanded ? (
        <View style={styles.assetDetails}>
          <Text style={styles.assetSummary}>{asset.summary}</Text>

          <View style={styles.evidenceGrid}>
            {asset.evidence.map((item) => (
              <View key={`${asset.symbol}-${item.label}`} style={styles.evidenceItem}>
                <Text style={styles.evidenceLabel}>{item.label}</Text>
                <Text style={styles.evidenceValue}>{item.value}</Text>
              </View>
            ))}
          </View>

          <View style={styles.conditionBlock}>
            <Text style={styles.conditionLine}>
              <Text style={styles.conditionLabel}>风险缓和条件 </Text>
              {asset.next_trigger}
            </Text>
            <Text style={styles.conditionLine}>
              <Text style={styles.invalidationLabel}>风险升高条件 </Text>
              {asset.invalidation}
            </Text>
          </View>
        </View>
      ) : null}
    </View>
  );
}

function formatPrice(value: number | null): string {
  if (value === null) return '—';
  return `$${value >= 100 ? value.toFixed(1) : value.toFixed(2)}`;
}

function formatChange(value: number | null): string {
  if (value === null) return '5D —';
  return `5D ${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function formatDistance(value: number): string {
  return `${value >= 0 ? '上方' : '下方'} ${Math.abs(value).toFixed(1)}%`;
}

const styles = StyleSheet.create({
  pageHeading: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    justifyContent: 'space-between',
    gap: 16,
    paddingTop: 2,
    paddingBottom: 4,
  },
  eyebrow: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.monoSemiBold,
    letterSpacing: 1.2,
  },
  pageTitle: {
    color: colors.textPrimary,
    fontSize: 24,
    lineHeight: 31,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  asOf: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoRegular,
  },
  macroGate: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 10,
    paddingVertical: 13,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderColor: colors.divider,
  },
  statusDot: {
    width: 7,
    height: 7,
    borderRadius: 3.5,
    marginTop: 5,
  },
  macroCopy: {
    flex: 1,
    gap: 4,
  },
  macroTitleLine: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 8,
  },
  macroTitle: {
    color: colors.textPrimary,
    fontSize: 14,
    lineHeight: 18,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  macroState: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.monoSemiBold,
  },
  macroScope: {
    color: colors.textSecondary,
    fontSize: 12,
    lineHeight: 17,
    fontFamily: typography.uiSemiBold,
    fontWeight: '600',
  },
  macroOneLine: {
    color: colors.textMuted,
    fontSize: 12,
    lineHeight: 18,
    fontFamily: typography.uiRegular,
  },
  summaryBlock: {
    gap: 10,
  },
  sectionTitleLine: {
    flexDirection: 'row',
    alignItems: 'baseline',
    justifyContent: 'space-between',
    gap: 12,
    paddingBottom: 7,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
  },
  sectionTitle: {
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.uiBold,
    fontWeight: '800',
    letterSpacing: 0.6,
  },
  sectionMeta: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.uiRegular,
  },
  countLine: {
    flexDirection: 'row',
    gap: 14,
  },
  countItem: {
    flex: 1,
    minWidth: 0,
    gap: 4,
  },
  countBar: {
    height: 2,
    width: '100%',
  },
  countLabel: {
    color: colors.textMuted,
    fontSize: 9,
    lineHeight: 13,
    fontFamily: typography.uiRegular,
  },
  countValue: {
    color: colors.textPrimary,
    fontSize: 16,
    lineHeight: 20,
    fontFamily: typography.monoSemiBold,
    fontWeight: '700',
  },
  assetList: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
  },
  assetRow: {
    paddingVertical: 17,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
  },
  assetToggle: {
    gap: 10,
  },
  assetTogglePressed: {
    opacity: 0.72,
  },
  assetDetails: {
    gap: 10,
    paddingTop: 12,
  },
  assetTopLine: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    gap: 12,
  },
  assetIdentity: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 8,
    flexShrink: 1,
  },
  assetSymbol: {
    color: colors.textPrimary,
    fontSize: 19,
    lineHeight: 24,
    fontFamily: typography.monoBold,
    fontWeight: '800',
  },
  assetLabel: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.uiRegular,
  },
  assetMarketData: {
    alignItems: 'flex-end',
    gap: 2,
  },
  assetPrice: {
    color: colors.textPrimary,
    fontSize: 14,
    lineHeight: 18,
    fontFamily: typography.monoSemiBold,
  },
  assetChange: {
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.monoRegular,
  },
  statusLine: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  statusText: {
    fontSize: 13,
    lineHeight: 17,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  supportText: {
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoRegular,
  },
  expandIcon: {
    marginLeft: 'auto',
  },
  assetSummary: {
    color: colors.textPrimary,
    fontSize: 13,
    lineHeight: 20,
    fontFamily: typography.uiRegular,
  },
  evidenceGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 6,
  },
  evidenceItem: {
    minWidth: '30%',
    flexGrow: 1,
    gap: 2,
    paddingTop: 7,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.borderSoft,
  },
  evidenceLabel: {
    color: colors.textMuted,
    fontSize: 9,
    lineHeight: 13,
    fontFamily: typography.uiRegular,
  },
  evidenceValue: {
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoSemiBold,
    fontWeight: '700',
  },
  conditionBlock: {
    gap: 5,
    paddingLeft: 10,
    borderLeftWidth: 2,
    borderLeftColor: colors.borderStrong,
  },
  conditionLine: {
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 17,
    fontFamily: typography.uiRegular,
  },
  conditionLabel: {
    color: colors.link,
    fontFamily: typography.uiSemiBold,
    fontWeight: '700',
  },
  invalidationLabel: {
    color: colors.textMuted,
    fontFamily: typography.uiSemiBold,
    fontWeight: '700',
  },
  pendingBlock: {
    gap: 5,
    paddingVertical: 22,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderColor: colors.divider,
  },
  pendingTitle: {
    color: colors.textPrimary,
    fontSize: 14,
    lineHeight: 19,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  pendingText: {
    color: colors.textMuted,
    fontSize: 12,
    lineHeight: 18,
    fontFamily: typography.uiRegular,
  },
  footerBlock: {
    gap: 6,
    paddingTop: 2,
  },
  footer: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    textAlign: 'center',
    fontFamily: typography.monoRegular,
  },
  complianceNote: {
    color: colors.textDisabled,
    fontSize: 9,
    lineHeight: 14,
    textAlign: 'center',
    fontFamily: typography.uiRegular,
  },
});
