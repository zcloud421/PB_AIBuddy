import { router } from 'expo-router';
import { Pressable, StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import { formatMonths, formatStrikeWithMoneyness } from '../constants/display';
import { typography } from '../constants/typography';
import type { IdeaCardData } from '../types/api';

export function IdeaCard({ idea, onPress }: { idea: IdeaCardData; onPress?: (symbol: string) => void }) {
  const badgeStyles =
    idea.grade === 'GO'
      ? { container: styles.goBadge, text: styles.goBadgeText }
      : { container: styles.cautionBadge, text: styles.cautionBadgeText };

  return (
    <Pressable onPress={() => (onPress ? onPress(idea.symbol) : router.push(`/detail/${idea.symbol}`))} style={styles.card}>
      <View style={styles.topRow}>
        <Text style={styles.symbol}>{idea.symbol}</Text>
        <View style={[styles.gradeBadge, badgeStyles.container]}>
          <Text style={[styles.gradeBadgeText, badgeStyles.text]}>{idea.grade}</Text>
        </View>
      </View>
      <Text style={styles.theme} numberOfLines={1}>
        {formatThemes(idea.themes)}
      </Text>
      <Text style={styles.combo} numberOfLines={1}>
        {formatMonths(idea.recommended_tenor_days)} · {formatStrikeWithMoneyness(idea.recommended_strike, idea.moneyness_pct)} · 参考票息
        <Text style={styles.couponValue}>{idea.estimated_coupon_range ?? '—'}</Text>
      </Text>
      {idea.grade === 'CAUTION' && idea.wait_reason === 'WAIT_SETUP_RESET' ? (
        <Text style={styles.waitHint}>等回调后评估 · 需更深执行价</Text>
      ) : idea.grade === 'CAUTION' && idea.actionable_caution ? (
        <Text style={styles.actionableHint}>可讨论 · 需更严执行价纪律</Text>
      ) : null}
    </Pressable>
  );
}

function formatThemes(themes: string[] | null | undefined) {
  if (!Array.isArray(themes) || themes.length === 0) {
    return '—';
  }

  return themes.join(' · ');
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surface,
    borderRadius: 8,
    paddingHorizontal: 16,
    paddingVertical: 15,
    gap: 7,
    borderWidth: 1,
    borderColor: colors.border,
  },
  topRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  symbol: {
    color: colors.textPrimary,
    fontSize: 22,
    fontWeight: '700',
    fontFamily: typography.uiBold,
  },
  gradeBadge: {
    borderRadius: 3,
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderWidth: 1,
  },
  gradeBadgeText: {
    fontSize: 11,
    fontWeight: '800',
    fontFamily: typography.monoBold,
  },
  goBadge: {
    backgroundColor: 'rgba(0, 168, 118, 0.08)',
    borderColor: colors.success,
  },
  goBadgeText: {
    color: colors.success,
  },
  cautionBadge: {
    backgroundColor: 'rgba(212, 130, 10, 0.08)',
    borderColor: colors.warning,
  },
  cautionBadgeText: {
    color: colors.warning,
  },
  theme: {
    color: colors.textMuted,
    fontSize: 12,
    fontFamily: typography.uiRegular,
  },
  combo: {
    color: colors.textSecondary,
    fontSize: 13,
    fontWeight: '600',
    fontFamily: typography.monoSemiBold,
  },
  couponValue: {
    color: colors.textPrimary,
    fontWeight: '700',
    fontSize: 14,
    fontFamily: typography.monoSemiBold,
  },
  waitHint: {
    fontSize: 11,
    color: colors.textDisabled,
    fontWeight: '500',
    fontFamily: typography.uiRegular,
  },
  actionableHint: {
    fontSize: 11,
    color: colors.warning,
    fontWeight: '500',
    fontFamily: typography.uiRegular,
  },
});
