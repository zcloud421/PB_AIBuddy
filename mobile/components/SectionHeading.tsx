import { StyleSheet, Text, View } from 'react-native';

import { typography } from '../constants/typography';

export function SectionHeading({ title, label }: { title: string; label?: string }) {
  return (
    <View style={styles.wrap}>
      <Text style={styles.title}>{title}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
  },
  title: {
    color: '#E4E4E4',
    fontSize: 16,
    fontWeight: '600',
    fontFamily: typography.uiSemiBold,
  },
});
