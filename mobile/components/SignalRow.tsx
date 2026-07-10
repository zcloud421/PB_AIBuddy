import { StyleSheet, Text, View } from 'react-native';

import { colors } from '../constants/colors';
import { translateSignalName, translateSignalValue } from '../constants/display';
import { typography } from '../constants/typography';
import type { SignalRowData } from '../types/api';

const signalColors: Record<string, string> = {
  green: colors.success,
  amber: colors.warning,
  red: colors.danger,
  gray: colors.neutral,
};

export function SignalRow({ signal }: { signal: SignalRowData }) {
  return (
    <View style={styles.row}>
      <Text style={styles.name}>{translateSignalName(signal.name)}</Text>
      <Text style={[styles.value, { color: signalColors[signal.color] ?? colors.neutral }]}>
        {translateSignalValue(signal.value)}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    paddingVertical: 12,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.divider,
    gap: 16,
  },
  name: {
    fontFamily: typography.monoBold,
    color: colors.textDisabled,
    fontSize: 10,
    fontWeight: '700',
    letterSpacing: 1,
    textTransform: 'uppercase',
    flex: 1,
  },
  value: {
    fontFamily: typography.monoBold,
    fontSize: 13,
    fontWeight: '700',
    textAlign: 'right',
    flex: 1,
    lineHeight: 20,
  },
});
