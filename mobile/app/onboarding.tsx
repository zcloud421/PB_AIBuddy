import { useState } from 'react';
import { Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { router } from 'expo-router';

import { colors } from '../constants/colors';

export const ONBOARDING_KEY = 'onboarding_accepted';

const DECLARATIONS = [
  '本人为香港持牌法团或注册机构下的持牌／注册证券从业人员',
  '本人将以内部工作辅助工具的用途使用本应用，不作任何商业或对外服务用途',
  '本人不会将系统生成内容原文直接转发或引用予终端客户，所有客户沟通内容须经由本人专业判断后重新组织',
  '本人理解系统输出不构成正式投资建议，最终建议须由本人结合所属机构 house view 及 suitability 评估后自行作出',
];

export default function OnboardingScreen() {
  const [checked, setChecked] = useState<boolean[]>(DECLARATIONS.map(() => false));

  const allChecked = checked.every(Boolean);

  const toggle = (index: number) => {
    setChecked((prev) => prev.map((v, i) => (i === index ? !v : v)));
  };

  const onConfirm = async () => {
    await AsyncStorage.setItem(ONBOARDING_KEY, '1').catch(() => {
      // Allow onboarding to proceed even when legacy storage is unavailable.
    });
    router.replace('/');
  };

  return (
    <SafeAreaView style={styles.page}>
      <ScrollView contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
        <View style={styles.header}>
          <Text style={styles.label}>INTERNAL USE ONLY</Text>
          <Text style={styles.title}>使用声明</Text>
        </View>

        <Text style={styles.intro}>
          本应用（Josan）为内部工作辅助工具，仅供香港持牌证券从业人员使用。{'\n\n'}
          使用本应用前，请确认以下事项：
        </Text>

        <View style={styles.declarationList}>
          {DECLARATIONS.map((text, index) => (
            <Pressable
              key={index}
              style={styles.declarationRow}
              onPress={() => toggle(index)}
              accessibilityRole="checkbox"
              accessibilityState={{ checked: checked[index] }}
            >
              <View style={[styles.checkbox, checked[index] && styles.checkboxChecked]}>
                {checked[index] ? <Text style={styles.checkmark}>✓</Text> : null}
              </View>
              <Text style={styles.declarationText}>{text}</Text>
            </Pressable>
          ))}
        </View>

        <Pressable
          style={[styles.button, !allChecked && styles.buttonDisabled]}
          onPress={allChecked ? onConfirm : undefined}
          disabled={!allChecked}
        >
          <Text style={[styles.buttonText, !allChecked && styles.buttonTextDisabled]}>
            我已阅读并同意，进入应用
          </Text>
        </Pressable>

        <Text style={styles.footnote}>
          如您不符合上述条件，请勿使用本应用。
        </Text>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  page: {
    flex: 1,
    backgroundColor: colors.background,
  },
  content: {
    padding: 24,
    paddingTop: 40,
    paddingBottom: 48,
    gap: 28,
  },
  header: {
    gap: 10,
  },
  label: {
    color: colors.label,
    fontSize: 10,
    fontWeight: '700',
    letterSpacing: 2.2,
  },
  title: {
    color: colors.textPrimary,
    fontSize: 30,
    fontWeight: '800',
    letterSpacing: -0.5,
  },
  intro: {
    color: colors.textSecondary,
    fontSize: 15,
    lineHeight: 24,
  },
  declarationList: {
    gap: 16,
  },
  declarationRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 14,
  },
  checkbox: {
    width: 22,
    height: 22,
    borderRadius: 6,
    borderWidth: 1.5,
    borderColor: colors.borderStrong,
    backgroundColor: colors.surface,
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: 1,
    flexShrink: 0,
  },
  checkboxChecked: {
    backgroundColor: colors.link,
    borderColor: colors.link,
  },
  checkmark: {
    color: '#fff',
    fontSize: 13,
    fontWeight: '800',
    lineHeight: 16,
  },
  declarationText: {
    flex: 1,
    color: colors.textSecondary,
    fontSize: 15,
    lineHeight: 24,
  },
  button: {
    backgroundColor: colors.link,
    borderRadius: 4,
    paddingVertical: 17,
    alignItems: 'center',
  },
  buttonDisabled: {
    backgroundColor: colors.surface,
    borderWidth: 1,
    borderColor: colors.border,
  },
  buttonText: {
    color: '#fff',
    fontSize: 16,
    fontWeight: '700',
  },
  buttonTextDisabled: {
    color: colors.textDisabled,
  },
  footnote: {
    color: colors.textMuted,
    fontSize: 12,
    lineHeight: 18,
    textAlign: 'center',
  },
});
