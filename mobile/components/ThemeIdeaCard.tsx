import { Pressable, StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import { typography } from '../constants/typography';

export type ThemeIdeaGrade = 'GO' | 'CAUTION' | 'AVOID' | 'CHECK';

export interface ThemeIdeaCardItem {
  symbol: string;
  role: string;
  grade: ThemeIdeaGrade;
  comboText: string | null;
  isEarningsWait?: boolean;
}

export function ThemeIdeaCard({
  item,
  onPress,
}: {
  item: ThemeIdeaCardItem;
  onPress: (symbol: string) => void;
}) {
  const effectiveGrade = item.isEarningsWait ? 'WAIT' : item.grade;
  const badgeStyle =
    effectiveGrade === 'GO'
      ? styles.goBadge
      : effectiveGrade === 'CAUTION'
        ? styles.cautionBadge
        : effectiveGrade === 'WAIT'
          ? styles.waitBadge
        : effectiveGrade === 'AVOID'
          ? styles.avoidBadge
          : styles.checkBadge;

  const badgeTextStyle =
    effectiveGrade === 'GO'
      ? styles.goBadgeText
      : effectiveGrade === 'CAUTION'
        ? styles.cautionBadgeText
        : effectiveGrade === 'WAIT'
          ? styles.waitBadgeText
        : effectiveGrade === 'AVOID'
          ? styles.avoidBadgeText
          : styles.checkBadgeText;

  return (
    <Pressable style={styles.card} onPress={() => onPress(item.symbol)}>
      <View style={styles.topRow}>
        <Text style={styles.symbol}>{item.symbol}</Text>
        <View style={[styles.badge, badgeStyle]}>
          <Text style={[styles.badgeText, badgeTextStyle]}>{effectiveGrade}</Text>
        </View>
      </View>
      <Text style={styles.role} numberOfLines={1}>
        {item.role}
      </Text>
      {item.comboText ? (
        <Text style={styles.combo} numberOfLines={1}>
          {item.comboText}
        </Text>
      ) : null}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surface,
    borderRadius: 4,
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
  role: {
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
  badge: {
    borderRadius: 3,
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderWidth: 1,
  },
  badgeText: {
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
  waitBadge: {
    backgroundColor: 'rgba(212, 130, 10, 0.08)',
    borderColor: colors.warning,
  },
  waitBadgeText: {
    color: colors.warning,
  },
  avoidBadge: {
    backgroundColor: 'rgba(204, 43, 40, 0.08)',
    borderColor: colors.danger,
  },
  avoidBadgeText: {
    color: colors.danger,
  },
  checkBadge: {
    backgroundColor: 'rgba(129, 136, 146, 0.08)',
    borderColor: colors.neutral,
  },
  checkBadgeText: {
    color: colors.neutral,
  },
});
