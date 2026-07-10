import { Pressable, StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import { typography } from '../constants/typography';
import type { ThemeBasket } from '../constants/theme-baskets';

export function ThemeCard({ basket, onPress }: { basket: ThemeBasket; onPress: (slug: string) => void }) {
  return (
    <Pressable style={styles.card} onPress={() => onPress(basket.slug)}>
      <Text style={styles.title}>{basket.title}</Text>
      <View style={styles.tickerRow}>
        <Text style={styles.tickers}>{basket.tickers.join(' · ')}</Text>
      </View>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    width: 176,
    minHeight: 88,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    paddingHorizontal: 16,
    paddingVertical: 12,
    justifyContent: 'space-between',
    gap: 12,
  },
  title: {
    color: colors.textPrimary,
    fontSize: 20,
    fontWeight: '800',
    lineHeight: 24,
    fontFamily: typography.uiBold,
  },
  tickerRow: {
    paddingTop: 6,
  },
  tickers: {
    color: colors.textSecondary,
    fontSize: 12,
    fontWeight: '700',
    lineHeight: 16,
    fontFamily: typography.monoBold,
  },
});
