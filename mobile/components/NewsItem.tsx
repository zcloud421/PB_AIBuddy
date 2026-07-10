import { Linking, Pressable, StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import type { NewsItem as NewsItemData } from '../types/api';

export function NewsItem({ item }: { item: NewsItemData }) {
  return (
    <Pressable onPress={() => Linking.openURL(item.url)} style={styles.card}>
      <Text style={styles.title} numberOfLines={2}>
        {item.title}
      </Text>
      <View style={styles.meta}>
        <Text style={styles.metaText}>{item.source || 'Unknown source'}</Text>
        <Text style={styles.metaText}>{formatDate(item.published_at)}</Text>
      </View>
    </Pressable>
  );
}

function formatDate(value: string) {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');

  return `${month}-${day}`;
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    padding: 14,
    gap: 8,
  },
  title: {
    color: colors.textPrimary,
    fontSize: 15,
    lineHeight: 22,
    fontWeight: '600',
  },
  meta: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    gap: 12,
  },
  metaText: {
    color: colors.textSecondary,
    fontSize: 12,
  },
});
