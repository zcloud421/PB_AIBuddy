import type { ReactNode } from 'react';
import { router } from 'expo-router';
import { Feather } from '@expo/vector-icons';
import { Pressable, StyleSheet, Text, View } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import { colors } from '../constants/colors';
import { typography } from '../constants/typography';

export const WATCHLIST_THEME_FALLBACKS: Record<string, string> = {
  AAPL: 'Mag7',
  AMD: 'AI Infrastructure',
  AMZN: 'Mag7',
  APP: 'Ad Tech',
  AVGO: 'AI Infrastructure',
  BABA: 'China Tech',
  BE: 'Clean Energy',
  BIDU: 'China Tech',
  COIN: 'Crypto',
  CRCL: 'Crypto',
  CRWV: 'AI Infrastructure',
  GOOG: 'Mag7',
  GDX: 'Gold',
  HOOD: 'Crypto',
  INTC: 'Semiconductors',
  LITE: 'AI Infrastructure',
  META: 'Mag7',
  MSTR: 'Crypto',
  MSFT: 'Mag7',
  MU: 'AI Infrastructure',
  MP: 'Rare Earths',
  NVDA: 'AI Infrastructure',
  OKLO: 'Nuclear',
  ORCL: 'AI Infrastructure',
  PDD: 'China Tech',
  PLTR: 'AI Infrastructure',
  TSLA: 'Mag7',
  TSM: 'AI Infrastructure',
  UBER: 'Mobility',
  UNH: 'Healthcare',
  USO: 'Energy',
  VRT: 'AI Infrastructure',
  XOM: 'Energy',
};

export const REFRESH_BANNER_MIN_VISIBLE_MS = 1000;

export function AppPageFrame({ children }: { children: ReactNode }) {
  return (
    <SafeAreaView style={styles.page} edges={['top']}>
      <View style={styles.container}>
        <View style={styles.header}>
          <Text style={styles.brand}>Josan</Text>
          <Pressable style={styles.searchButton} onPress={() => router.push('/pull')}>
            <Feather name="search" size={28} color={colors.textPrimary} />
          </Pressable>
        </View>
        {children}
      </View>
    </SafeAreaView>
  );
}

export function CenteredState({ text, onPress }: { text: string; onPress?: () => void }) {
  return (
    <SafeAreaView style={styles.stateWrap}>
      <Text style={styles.stateText}>{text}</Text>
      {onPress ? (
        <Pressable onPress={onPress}>
          <Text style={styles.retry}>重试</Text>
        </Pressable>
      ) : null}
    </SafeAreaView>
  );
}

export function DailyBestSkeleton() {
  return (
    <View style={styles.bestSkeleton}>
      <View style={styles.bestSkeletonHeader}>
        <View style={styles.bestSkeletonTitleBlock}>
          <View style={[styles.skeletonBase, styles.bestSymbolSkeleton]} />
          <View style={[styles.skeletonBase, styles.bestThemeSkeleton]} />
        </View>
        <View style={[styles.skeletonBase, styles.bestBadgeSkeleton]} />
      </View>
      <View style={styles.bestSkeletonBody}>
        <View style={[styles.skeletonBase, styles.bestLineLong]} />
        <View style={[styles.skeletonBase, styles.bestLineShort]} />
        <View style={[styles.skeletonBase, styles.bestParagraphLine]} />
        <View style={[styles.skeletonBase, styles.bestParagraphLine]} />
      </View>
    </View>
  );
}

export function DailyBestEmptyCard() {
  return (
    <View style={styles.bestEmptyCard}>
      <View style={styles.bestEmptyHeader}>
        <Text style={styles.bestEmptyTitle}>今日暂无GO标的</Text>
        <Text style={styles.bestEmptyBadge}>WAIT</Text>
      </View>
      <Text style={styles.bestEmptyText}>
        今日筛选结果未出现满足新建FCN条件的标的，优先查看主题篮子或关注列表里的等待原因。
      </Text>
    </View>
  );
}

export function IdeaCardSkeleton() {
  return (
    <View style={styles.ideaSkeleton}>
      <View style={styles.ideaSkeletonTop}>
        <View style={[styles.skeletonBase, styles.ideaSymbolSkeleton]} />
        <View style={[styles.skeletonBase, styles.ideaBadgeSkeleton]} />
      </View>
      <View style={[styles.skeletonBase, styles.ideaThemeSkeleton]} />
      <View style={[styles.skeletonBase, styles.ideaComboSkeleton]} />
    </View>
  );
}

export function isHighVolatilityIdea(idea: { symbol: string; themes: string[] }) {
  const themes = Array.isArray(idea.themes) ? idea.themes : [];

  if (idea.symbol === 'CRCL' || idea.symbol === 'GLD') {
    return true;
  }

  return themes.some((theme) => theme === 'High Volatility' || theme === 'Crypto');
}

export const sharedStyles = StyleSheet.create({
  skeletonHeadingBar: {
    height: 20,
    width: 120,
    backgroundColor: '#242424',
    borderRadius: 6,
  },
  skeletonSummaryBar: {
    height: 13,
    width: '90%',
    backgroundColor: '#1E1E1E',
    borderRadius: 4,
    marginTop: 4,
  },
  skeletonSummaryBarShort: {
    height: 13,
    width: '60%',
    backgroundColor: '#1E1E1E',
    borderRadius: 4,
  },
  skeletonGroupTitle: {
    height: 10,
    width: 40,
    backgroundColor: '#1E1E1E',
    borderRadius: 4,
  },
  skeletonCard: {
    backgroundColor: '#242424',
    opacity: 0.5,
  },
  page: {
    flex: 1,
    backgroundColor: colors.background,
  },
  container: {
    flex: 1,
    backgroundColor: colors.background,
    paddingHorizontal: 20,
    paddingTop: 0,
    gap: 14,
  },
  scrollContent: {
    paddingBottom: 28,
    gap: 16,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingBottom: 10,
    borderBottomWidth: 1,
    borderBottomColor: colors.borderSoft,
  },
  brand: {
    color: colors.textPrimary,
    fontSize: 34,
    fontWeight: '800',
    letterSpacing: -0.8,
  },
  searchButton: {
    width: 32,
    height: 32,
    alignItems: 'center',
    justifyContent: 'center',
  },
  refreshBannerWrap: {
    position: 'absolute',
    top: 8,
    left: 0,
    right: 0,
    alignItems: 'center',
    zIndex: 10,
  },
  refreshBanner: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 3,
    backgroundColor: '#111111',
    borderWidth: 1,
    borderColor: '#1C1C1C',
    shadowColor: '#000',
    shadowOpacity: 0.4,
    shadowRadius: 10,
    shadowOffset: { width: 0, height: 6 },
    elevation: 8,
  },
  refreshDot: {
    width: 6,
    height: 6,
    borderRadius: 999,
    backgroundColor: colors.success,
  },
  refreshBannerText: {
    color: colors.textSecondary,
    fontSize: 12,
    fontWeight: '600',
    letterSpacing: 0.5,
  },
  listGap: {
    gap: 12,
  },
  themeRail: {
    gap: 12,
    paddingRight: 20,
  },
  pairThemeCard: {
    width: 176,
    minHeight: 88,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#214C8F',
    backgroundColor: colors.surfaceElevated,
    paddingHorizontal: 16,
    paddingVertical: 12,
    justifyContent: 'space-between',
    gap: 12,
  },
  pairThemeCardBody: {
    gap: 6,
  },
  pairThemeCardTitle: {
    color: colors.textPrimary,
    fontSize: 20,
    fontWeight: '800',
    lineHeight: 24,
    fontFamily: typography.uiBold,
  },
  pairThemeCardSubtitle: {
    color: colors.label,
    fontSize: 13,
    fontWeight: '600',
    lineHeight: 18,
  },
  pairThemeCardFooter: {
    paddingTop: 6,
  },
  pairThemeCardMeta: {
    color: colors.textSecondary,
    fontSize: 12,
    fontWeight: '700',
    lineHeight: 16,
    fontFamily: typography.monoBold,
  },
  focusSection: {
    gap: 0,
  },
  marketStateSection: {
    gap: 10,
    paddingBottom: 28,
  },
  marketStateHeader: {
    gap: 4,
  },
  marketStatePanel: {
    marginTop: 4,
    gap: 14,
  },
  marketStateHeadline: {
    color: '#B8B8B8',
    fontSize: 13,
    fontWeight: '500',
    lineHeight: 19,
  },
  marketStateGroup: {
    gap: 8,
  },
  marketStateGroupTitle: {
    color: '#848484',
    fontSize: 10,
    fontWeight: '700',
    letterSpacing: 2.2,
    textTransform: 'uppercase',
    fontFamily: typography.uiSemiBold,
  },
  marketStateRail: {
    gap: 8,
    paddingRight: 24,
  },
  marketStateRailCard: {
    width: 102,
    minHeight: 78,
    borderRadius: 3,
    backgroundColor: '#242424',
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: '#2D2D2D',
    paddingHorizontal: 10,
    paddingVertical: 9,
    gap: 2,
  },
  marketStateRailCardAlert: {
    borderTopWidth: 2,
    borderTopColor: colors.warning,
    paddingTop: 7,
  },
  marketStateRailStatus: {
    color: '#7D7D7D',
    fontSize: 9,
    fontWeight: '700',
    letterSpacing: 1.2,
    textTransform: 'uppercase',
    fontFamily: typography.uiSemiBold,
  },
  marketStateRailTitle: {
    color: colors.textPrimary,
    fontSize: 11,
    fontWeight: '700',
    lineHeight: 14,
    minHeight: 28,
  },
  marketStateRailLatest: {
    color: colors.textPrimary,
    fontSize: 14,
    fontWeight: '600',
    lineHeight: 17,
    fontFamily: typography.monoSemiBold,
  },
  marketStateRailChange: {
    fontSize: 11,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  marketStateChangePositive: {
    color: colors.success,
  },
  marketStateChangeNegative: {
    color: colors.danger,
  },
  featuredFocusCard: {
    marginTop: 0,
    paddingTop: 20,
    paddingBottom: 18,
    gap: 12,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#2D2D2D',
    backgroundColor: 'transparent',
  },
  featuredFocusHeader: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    gap: 12,
  },
  featuredFocusHeaderRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  featuredFocusTitleBlock: {
    flex: 1,
    gap: 3,
  },
  featuredFocusTitle: {
    color: '#FFFFFF',
    fontSize: 20,
    fontWeight: '600',
    letterSpacing: -0.2,
    lineHeight: 25,
  },
  featuredFocusBadge: {
    borderRadius: 3,
    paddingHorizontal: 8,
    paddingVertical: 4,
    backgroundColor: 'rgba(244, 165, 36, 0.14)',
    borderWidth: 1,
    borderColor: 'rgba(244, 165, 36, 0.5)',
  },
  featuredFocusBadgeText: {
    color: colors.warning,
    fontSize: 11,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  featuredFocusQuestionBlock: {
    gap: 5,
  },
  featuredFocusQuestionText: {
    color: '#777777',
    fontSize: 14,
    lineHeight: 20,
  },
  focusCardList: {
    gap: 0,
  },
  focusCard: {
    position: 'relative',
    paddingVertical: 18,
    gap: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: 'rgba(255,255,255,0.12)',
    backgroundColor: 'transparent',
  },
  focusCardHeader: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 10,
  },
  focusCardTitle: {
    flex: 1,
    color: '#FFFFFF',
    fontSize: 18,
    fontWeight: '600',
    letterSpacing: -0.1,
    lineHeight: 23,
  },
  focusCardChevron: {
    color: '#6F6F6F',
    fontSize: 20,
    lineHeight: 24,
  },
  focusCardBadge: {
    borderRadius: 3,
    paddingHorizontal: 8,
    paddingVertical: 3,
  },
  focusCardBadgeText: {
    fontSize: 11,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  focusCardQuestionBlock: {
    gap: 5,
  },
  focusCardQuestionList: {
    gap: 6,
  },
  focusCardQuestionText: {
    color: '#777777',
    fontSize: 14,
    lineHeight: 20,
  },
  focusCardSummary: {
    color: '#777777',
    fontSize: 14,
    lineHeight: 20,
  },
  focusCardTimestamp: {
    fontSize: 11,
    color: '#777777',
    marginTop: 6,
    fontFamily: typography.monoRegular,
  },
  focusFooterText: {
    color: colors.textDisabled,
    fontSize: 12,
    lineHeight: 18,
    textAlign: 'center',
    paddingTop: 2,
  },
  watchlistEmpty: {
    minHeight: 260,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    backgroundColor: colors.surface,
    padding: 20,
    justifyContent: 'center',
    gap: 8,
  },
  watchlistEmptyTitle: {
    color: colors.textPrimary,
    fontSize: 18,
    fontWeight: '700',
  },
  watchlistEmptyBody: {
    color: colors.textSecondary,
    fontSize: 14,
    lineHeight: 21,
  },
  watchlistList: {
    gap: 12,
  },
  watchlistRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    backgroundColor: colors.surface,
    paddingHorizontal: 16,
    paddingVertical: 16,
  },
  watchlistRowLeft: {
    flex: 1,
    gap: 4,
    paddingRight: 12,
  },
  watchlistSymbol: {
    color: colors.textPrimary,
    fontSize: 20,
    fontWeight: '800',
    fontFamily: typography.uiBold,
  },
  watchlistSubtitle: {
    color: colors.textSecondary,
    fontSize: 13,
    fontFamily: typography.uiRegular,
  },
  watchlistRowRight: {
    alignItems: 'flex-end',
  },
  watchlistGrade: {
    fontSize: 12,
    fontWeight: '800',
    fontFamily: typography.monoBold,
  },
  watchlistGradeGo: {
    color: colors.success,
  },
  watchlistGradeCaution: {
    color: colors.warning,
  },
  watchlistGradeAvoid: {
    color: colors.danger,
  },
  watchlistGradePending: {
    color: colors.neutral,
    fontSize: 12,
    fontWeight: '700',
    fontFamily: typography.monoBold,
  },
  stateWrap: {
    flex: 1,
    backgroundColor: colors.background,
    alignItems: 'center',
    justifyContent: 'center',
    gap: 10,
  },
  stateText: {
    color: colors.textSecondary,
    fontSize: 15,
  },
  retry: {
    color: colors.success,
    fontSize: 14,
    fontWeight: '700',
  },
  skeletonBase: {
    backgroundColor: colors.surface,
    borderRadius: 3,
  },
  bestSkeleton: {
    backgroundColor: colors.surfaceElevated,
    borderRadius: 6,
    padding: 20,
    gap: 12,
    borderWidth: 1,
    borderColor: colors.borderStrong,
  },
  bestSkeletonHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  bestSkeletonTitleBlock: {
    gap: 8,
  },
  bestSymbolSkeleton: {
    width: 96,
    height: 34,
  },
  bestThemeSkeleton: {
    width: 132,
    height: 16,
  },
  bestBadgeSkeleton: {
    width: 64,
    height: 30,
  },
  bestSkeletonBody: {
    gap: 10,
  },
  bestLineLong: {
    width: '78%',
    height: 18,
  },
  bestLineShort: {
    width: '56%',
    height: 18,
  },
  bestParagraphLine: {
    width: '100%',
    height: 16,
    borderRadius: 4,
  },
  bestEmptyCard: {
    backgroundColor: colors.surfaceElevated,
    borderRadius: 6,
    padding: 20,
    gap: 12,
    borderWidth: 1,
    borderColor: colors.borderStrong,
  },
  bestEmptyHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 12,
  },
  bestEmptyTitle: {
    color: colors.textPrimary,
    fontSize: 24,
    fontWeight: '800',
  },
  bestEmptyBadge: {
    color: colors.warning,
    borderColor: colors.warning,
    borderWidth: 1,
    borderRadius: 4,
    paddingHorizontal: 12,
    paddingVertical: 7,
    fontFamily: typography.monoBold,
    fontSize: 13,
    letterSpacing: 0,
  },
  bestEmptyText: {
    color: colors.textSecondary,
    fontSize: 15,
    lineHeight: 23,
  },
  ideaSkeleton: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    paddingHorizontal: 16,
    paddingVertical: 15,
    gap: 7,
    borderWidth: 1,
    borderColor: colors.border,
  },
  ideaSkeletonTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  ideaSymbolSkeleton: {
    width: 68,
    height: 24,
  },
  ideaBadgeSkeleton: {
    width: 58,
    height: 24,
  },
  ideaThemeSkeleton: {
    width: '48%',
    height: 14,
    borderRadius: 4,
  },
  ideaComboSkeleton: {
    width: '88%',
    height: 16,
    borderRadius: 4,
  },
});

const styles = sharedStyles;
