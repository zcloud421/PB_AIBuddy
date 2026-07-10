import { router } from 'expo-router';
import { Pressable, StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import { formatMonths, formatStrikeWithMoneyness, truncateWhyNow } from '../constants/display';
import { typography } from '../constants/typography';
import type { DailyBestData } from '../types/api';

export function DailyBestCard({ idea, onPress }: { idea: DailyBestData; onPress?: (symbol: string) => void }) {
  const comboPrimary = `${formatMonths(idea.recommended_tenor_days)} · ${formatStrikeWithMoneyness(idea.recommended_strike, idea.moneyness_pct)}`;
  const comboCoupon = `参考票息 ${idea.estimated_coupon_range}`;

  return (
    <Pressable onPress={() => (onPress ? onPress(idea.symbol) : router.push(`/detail/${idea.symbol}`))} style={styles.card}>
      <View style={styles.header}>
        <View>
          <Text style={styles.symbol}>{idea.symbol}</Text>
          <Text style={styles.theme}>{idea.theme}</Text>
        </View>
        <View style={styles.gradeBadge}>
          <Text style={styles.gradeText}>{idea.grade}</Text>
        </View>
      </View>

      <View style={styles.comboBlock}>
        <Text style={styles.comboPrimary}>{comboPrimary}</Text>
        <Text style={styles.comboSecondary}>{comboCoupon}</Text>
      </View>
      <Text style={styles.body} numberOfLines={2}>
        {truncateWhyNow(idea.narrative?.why_now ?? idea.theme_narrative)}
      </Text>
      <Text style={styles.cta}>查看详情 →</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surfaceElevated,
    borderRadius: 14,
    padding: 20,
    gap: 12,
    borderWidth: 1,
    borderColor: '#214C8F',
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  symbol: {
    color: colors.textPrimary,
    fontSize: 32,
    fontWeight: '800',
    fontFamily: typography.uiBold,
  },
  theme: {
    color: colors.textMuted,
    fontSize: 14,
    marginTop: 4,
    fontFamily: typography.uiRegular,
  },
  gradeBadge: {
    borderRadius: 3,
    paddingHorizontal: 12,
    paddingVertical: 7,
    backgroundColor: 'rgba(0, 168, 118, 0.08)',
    borderWidth: 1,
    borderColor: colors.success,
  },
  gradeText: {
    color: colors.success,
    fontSize: 12,
    fontWeight: '800',
    fontFamily: typography.monoBold,
  },
  comboBlock: {
    gap: 2,
  },
  comboPrimary: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '700',
    fontFamily: typography.monoSemiBold,
  },
  comboSecondary: {
    color: colors.textPrimary,
    fontSize: 15,
    fontWeight: '700',
    fontFamily: typography.monoSemiBold,
  },
  body: {
    color: colors.textMuted,
    fontSize: 15,
    lineHeight: 22,
    fontFamily: typography.uiRegular,
  },
  cta: {
    color: colors.link,
    fontSize: 14,
    fontWeight: '700',
    fontFamily: typography.uiSemiBold,
  },
});
