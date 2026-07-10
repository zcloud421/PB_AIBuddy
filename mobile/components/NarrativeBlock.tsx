import { useState } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';
import * as Clipboard from 'expo-clipboard';

import { colors } from '../constants/colors';
import type { NarrativeOutput } from '../types/api';

export function NarrativeBlock({
  narrative,
  grade,
  hideRiskNote = false,
  isEarningsWait = false,
}: {
  narrative: NarrativeOutput | null;
  grade: 'GO' | 'CAUTION' | 'AVOID';
  hideRiskNote?: boolean;
  isEarningsWait?: boolean;
}) {
  const [copied, setCopied] = useState(false);
  const title = isEarningsWait
    ? '标的背景参考'
    : grade === 'AVOID'
      ? '当前主要顾虑'
      : grade === 'CAUTION'
        ? '需留意风险'
        : '客户沟通参考';
  const showInternalNotice = grade === 'GO';

  if (!narrative) {
    return null;
  }

  const onCopy = async () => {
    await Clipboard.setStringAsync(narrative.why_now);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <View style={styles.wrap}>
      <View style={styles.header}>
        <View style={styles.titleBlock}>
          <Text style={styles.title}>{title}</Text>
          {showInternalNotice ? (
            <Text style={styles.notice}>仅供讨论参考，最终建议需结合贵行观点及客户适当性。</Text>
          ) : null}
        </View>
        <Pressable onPress={onCopy} style={styles.copyButton}>
          <Text style={styles.copyText}>{copied ? 'Copied' : 'Copy'}</Text>
        </Pressable>
      </View>
      <Text style={styles.body}>{narrative.why_now}</Text>
      {narrative.risk_note && !hideRiskNote ? (
        <View style={styles.riskBox}>
          <Text style={styles.riskLabel}>风险提示</Text>
          <Text style={styles.riskText}>{narrative.risk_note}</Text>
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    gap: 10,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    gap: 12,
  },
  titleBlock: {
    flex: 1,
    gap: 4,
  },
  title: {
    color: colors.textPrimary,
    fontSize: 18,
    fontWeight: '700',
  },
  notice: {
    color: colors.textMuted,
    fontSize: 11,
    lineHeight: 16,
    fontWeight: '400',
  },
  copyButton: {
    borderRadius: 4,
    borderWidth: 1,
    borderColor: colors.copyBorder,
    backgroundColor: colors.copyBg,
    paddingHorizontal: 12,
    paddingVertical: 5,
  },
  copyText: {
    color: colors.copyText,
    fontSize: 12,
    fontWeight: '700',
  },
  body: {
    color: colors.textSecondary,
    fontSize: 15,
    lineHeight: 26,
  },
  riskBox: {
    backgroundColor: colors.warningSurface,
    borderColor: colors.warningBorder,
    borderWidth: 1,
    borderRadius: 4,
    padding: 13,
    gap: 6,
  },
  riskLabel: {
    color: colors.warning,
    fontSize: 12,
    fontWeight: '700',
    letterSpacing: 0.6,
    textTransform: 'uppercase',
  },
  riskText: {
    color: colors.riskTint,
    fontSize: 14,
    lineHeight: 22,
  },
});
