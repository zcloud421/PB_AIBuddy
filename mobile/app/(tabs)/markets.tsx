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
} from '../../types/macro-regime';

const STATUS_META: Record<
  ExposureTimingStatus,
  { label: string; color: string }
> = {
  BUILD_WINDOW: { label: '下行压力缓和', color: colors.success },
  WATCH_SUPPORT: { label: '支撑确认中', color: colors.warning },
  EXTENDED: { label: '追高风险偏高', color: colors.warning },
  WAIT: { label: '下行风险仍高', color: colors.danger },
};

const ASSET_ORDER = [
  'SPY',
  'QQQ',
  'SOXX',
  'DRAM',
  'XLV',
  'XLF',
  'XLE',
  'MCHI',
  'GLD',
  'GDX',
] as const;
const STATUS_ORDER: ExposureTimingStatus[] = [
  'BUILD_WINDOW',
  'WATCH_SUPPORT',
  'EXTENDED',
  'WAIT',
];

const PRODUCT_SCOPE: Record<string, string> = {
  SPY: '美国大型股票及大盘基金',
  QQQ: '科技、创新及大型成长基金',
  SOXX: '半导体、AI 硬件占比较高的股票及基金',
  DRAM: '存储芯片产业链个股及主题策略',
  XLV: '医疗、制药及生命科学相关基金',
  XLF: '银行、保险及金融服务相关基金',
  XLE: '综合能源、油气及天然资源相关基金',
  MCHI: '中国及大中华股票基金',
  GLD: '实物黄金 ETF、黄金挂钩基金及黄金配置',
  GDX: '黄金矿业股票及贵金属股票基金',
};

export default function MarketsTab() {
  const { data, isLoading, isError, refetch, isRefreshing, error } = useMacroRegime();
  const [expandedSymbol, setExpandedSymbol] = useState<string | null | undefined>(undefined);

  if (isLoading) {
    return (
      <AppPageFrame>
        <CenteredState text="加载择时参考 …" />
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

  const assets = [...(data.exposure_timing?.assets ?? [])].sort((a, b) => {
    const aIndex = ASSET_ORDER.indexOf(a.symbol as (typeof ASSET_ORDER)[number]);
    const bIndex = ASSET_ORDER.indexOf(b.symbol as (typeof ASSET_ORDER)[number]);
    return (aIndex === -1 ? ASSET_ORDER.length : aIndex) -
      (bIndex === -1 ? ASSET_ORDER.length : bIndex);
  });
  const statusGroups = STATUS_ORDER.map((status) => ({
    status,
    assets: assets.filter((asset) => asset.status === status),
  })).filter((group) => group.assets.length > 0);
  const firstVisibleSymbol = statusGroups[0]?.assets[0]?.symbol ?? null;
  const activeSymbol = expandedSymbol === undefined ? firstVisibleSymbol : expandedSymbol;
  const marketDataAsOf = assets.find((asset) => asset.data_as_of)?.data_as_of ??
    data.exposure_timing?.as_of ?? data.as_of;

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
        <PageHeading asOf={marketDataAsOf} />

        {assets.length > 0 ? (
          <View style={styles.assetList}>
            {statusGroups.map((group) => {
              const meta = STATUS_META[group.status];
              return (
                <View key={group.status} style={styles.statusGroup}>
                  <View style={styles.groupHeader}>
                    <Text style={[styles.groupLabel, { color: meta.color }]}>{meta.label}</Text>
                    <Text style={styles.groupCount}>{group.assets.length}</Text>
                  </View>
                  {group.assets.map((asset) => (
                    <TimingRow
                      key={asset.symbol}
                      asset={asset}
                      expanded={activeSymbol === asset.symbol}
                      onToggle={() => {
                        setExpandedSymbol((current) => {
                          const resolved = current === undefined ? firstVisibleSymbol : current;
                          return resolved === asset.symbol ? null : asset.symbol;
                        });
                      }}
                    />
                  ))}
                </View>
              );
            })}
          </View>
        ) : (
          <View style={styles.pendingBlock}>
            <Text style={styles.pendingTitle}>等待下一次市场快照</Text>
            <Text style={styles.pendingText}>
              择时数据将在美股收盘后的下一次刷新生成。
            </Text>
          </View>
        )}

        <View style={styles.footerBlock}>
          <Text style={styles.footer}>美股收盘后每日刷新</Text>
          <Text style={styles.complianceNote}>
            本页通过主要市场代理观察价格支撑、下行空间与止跌条件，供 RM / IC 评估多头敞口时点；不构成针对任何客户或产品的投资建议。
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
        <Text style={styles.eyebrow}>ENTRY TIMING</Text>
        <Text style={styles.pageTitle}>择时参考</Text>
      </View>
      <View style={styles.asOfBlock}>
        <Text style={styles.asOfLabel}>数据截至</Text>
        <Text style={styles.asOf}>{formatMarketDate(asOf)} 美股收盘</Text>
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
  const evidence = asset.evidence.filter((item) =>
    item.label !== '现价' &&
    item.label !== '5日' &&
    item.label !== asset.support?.label
  );

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
            <Text style={styles.assetCategory}>{asset.label}</Text>
            <Text style={styles.assetProxy}>{asset.symbol}</Text>
          </View>
          <View style={styles.assetMarketData}>
            <Text style={styles.assetPrice}>{formatPrice(asset.current_price)}</Text>
            <Text style={styles.assetChange}>{formatChange(asset.change_5d_pct)}</Text>
          </View>
        </View>

        <View style={styles.statusLine}>
          {asset.support ? (
            <Text style={styles.supportText}>
              {asset.support.label} {formatPrice(asset.support.level)} · {formatDistance(asset.support.distance_pct)}
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
          <Text style={styles.productScope}>
            <Text style={styles.productScopeLabel}>参考产品 · </Text>
            <Text style={styles.productScopeValue}>
              {PRODUCT_SCOPE[asset.symbol] ?? '相关股票及基金'}
            </Text>
          </Text>
          <Text style={styles.assetSummary}>{asset.summary}</Text>

          <View style={styles.evidenceGrid}>
            {evidence.map((item) => (
              <View key={`${asset.symbol}-${item.label}`} style={styles.evidenceItem}>
                <Text style={styles.evidenceLabel}>{item.label}</Text>
                <Text style={styles.evidenceValue}>{item.value}</Text>
              </View>
            ))}
          </View>

          <View style={styles.conditionBlock}>
            <Text style={styles.conditionLine}>
              <Text style={styles.conditionLabel}>缓和 </Text>
              {asset.next_trigger}
            </Text>
            <Text style={styles.conditionLine}>
              <Text style={styles.invalidationLabel}>失效 </Text>
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

function formatMarketDate(value: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  return match ? `${match[2]}/${match[3]}` : value;
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
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoRegular,
  },
  asOfBlock: {
    alignItems: 'flex-end',
    gap: 1,
  },
  asOfLabel: {
    color: colors.textMuted,
    fontSize: 9,
    lineHeight: 12,
    fontFamily: typography.uiRegular,
  },
  assetList: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.divider,
  },
  statusGroup: {
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
  },
  groupHeader: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 7,
    paddingTop: 14,
    paddingBottom: 7,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.borderSoft,
  },
  groupLabel: {
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  groupCount: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.monoRegular,
  },
  assetRow: {
    paddingVertical: 14,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.borderSoft,
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
  // RM 的产品语言优先:基金类别是主标题,ETF ticker 是代理标签。
  assetCategory: {
    color: colors.textPrimary,
    fontSize: 17,
    lineHeight: 22,
    fontFamily: typography.uiBold,
    fontWeight: '800',
  },
  assetProxy: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoRegular,
  },
  assetMarketData: {
    alignItems: 'flex-end',
    gap: 2,
  },
  assetPrice: {
    color: colors.textPrimary,
    fontSize: 16,
    lineHeight: 20,
    fontFamily: typography.monoSemiBold,
  },
  assetChange: {
    color: colors.textMuted,
    fontSize: 10,
    lineHeight: 14,
    fontFamily: typography.monoRegular,
  },
  statusLine: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
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
  productScope: {
    fontSize: 10,
    lineHeight: 15,
    fontFamily: typography.uiRegular,
  },
  productScopeLabel: {
    color: colors.textSecondary,
    fontFamily: typography.uiSemiBold,
    fontWeight: '700',
  },
  productScopeValue: {
    color: colors.textMuted,
    fontFamily: typography.uiRegular,
  },
  evidenceGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 6,
  },
  evidenceItem: {
    width: '31%',
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
    color: colors.textPrimary,
    fontSize: 11,
    lineHeight: 15,
    fontFamily: typography.monoSemiBold,
    fontWeight: '700',
  },
  conditionBlock: {
    gap: 5,
    paddingLeft: 10,
    borderLeftWidth: StyleSheet.hairlineWidth,
    borderLeftColor: colors.divider,
  },
  conditionLine: {
    color: colors.textSecondary,
    fontSize: 11,
    lineHeight: 17,
    fontFamily: typography.uiRegular,
  },
  conditionLabel: {
    color: colors.textSecondary,
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
    color: colors.textMuted,
    fontSize: 9,
    lineHeight: 14,
    textAlign: 'center',
    fontFamily: typography.uiRegular,
  },
});
