import { useEffect, useMemo, useState } from 'react';
import { router, useLocalSearchParams } from 'expo-router';
import { useQueryClient } from '@tanstack/react-query';
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import { colors } from '../constants/colors';
import { typography } from '../constants/typography';
import { fetchSymbolIdea } from '../hooks/useSymbolIdea';
import { useTodayIdeas } from '../hooks/useTodayIdeas';

export default function PullScreen() {
  const params = useLocalSearchParams<{ mode?: string }>();
  const initialMode = params.mode === 'pair' ? 'pair' : 'single';
  const [mode, setMode] = useState<'single' | 'pair'>(initialMode);
  const [symbolA, setSymbolA] = useState('');
  const [symbol, setSymbol] = useState('');
  const [symbolB, setSymbolB] = useState('');
  const [loading, setLoading] = useState(false);
  const { data } = useTodayIdeas();
  const queryClient = useQueryClient();

  const quickSymbols = useMemo(
    () => (data?.recommended ?? []).slice(0, 5).map((idea) => idea.symbol),
    [data],
  );

  useEffect(() => {
    if (params.mode === 'pair') {
      setMode('pair');
      return;
    }

    if (params.mode === 'single') {
      setMode('single');
    }
  }, [params.mode]);

  const normalizeSymbol = (value: string) => value.trim().toUpperCase();

  const goToSymbol = async (nextSymbol: string) => {
    const target = nextSymbol.trim().toUpperCase();
    if (!target) {
      return;
    }

    setLoading(true);
    const isTodayUniverseSymbol =
      [...(data?.recommended ?? []), ...(data?.caution ?? [])].some((idea) => idea.symbol === target) ||
      data?.daily_best?.symbol === target;
    if (isTodayUniverseSymbol) {
      void queryClient.prefetchQuery({
        queryKey: ['ideas', target],
        queryFn: () => fetchSymbolIdea(target),
        staleTime: 5 * 60 * 1000,
      });
    }
    router.push(`/detail/${target}`);
    setTimeout(() => setLoading(false), 300);
  };

  const lockPrimarySymbol = (nextSymbol: string) => {
    const target = normalizeSymbol(nextSymbol);
    if (!target) {
      return;
    }

    setSymbolA(target);
    setSymbol('');
    setSymbolB('');
  };

  const onAnalyze = () => {
    if (mode === 'single') {
      goToSymbol(symbol);
      return;
    }

    lockPrimarySymbol(symbol);
  };

  const onAnalyzePair = () => {
    const targetB = normalizeSymbol(symbolB);
    if (!symbolA || !targetB) {
      return;
    }

    setLoading(true);
    router.push(`/pair/${symbolA}/${targetB}`);
    setTimeout(() => setLoading(false), 300);
  };

  const onClearSymbolA = () => {
    setSymbolA('');
    setSymbol('');
    setSymbolB('');
  };

  const onSwitchMode = (nextMode: 'single' | 'pair') => {
    setMode(nextMode);
    setSymbolA('');
    setSymbol('');
    setSymbolB('');
  };

  const onQuickSelect = (nextSymbol: string) => {
    if (mode === 'single') {
      goToSymbol(nextSymbol);
      return;
    }

    if (!symbolA) {
      lockPrimarySymbol(nextSymbol);
      return;
    }

    setSymbolB(normalizeSymbol(nextSymbol));
  };

  return (
    <SafeAreaView style={styles.page}>
      <ScrollView contentContainerStyle={styles.scrollContent} showsVerticalScrollIndicator={false}>
        <View style={styles.nav}>
          <Pressable onPress={() => router.back()}>
            <Text style={styles.back}>← 返回</Text>
          </Pressable>
        </View>
        <View style={styles.pullContent}>
          <View style={styles.modeSwitcher}>
            <Pressable
              onPress={() => onSwitchMode('single')}
              style={[styles.modeChip, mode === 'single' && styles.modeChipActive]}
            >
              <Text style={[styles.modeChipText, mode === 'single' && styles.modeChipTextActive]}>单标的</Text>
            </Pressable>
            <Pressable
              onPress={() => onSwitchMode('pair')}
              style={[styles.modeChip, mode === 'pair' && styles.modeChipActive]}
            >
              <Text style={[styles.modeChipText, mode === 'pair' && styles.modeChipTextActive]}>双标的</Text>
            </Pressable>
          </View>
          <Text style={styles.pullTitle}>我想查询FCN挂钩</Text>
          {mode === 'pair' && symbolA ? (
            <View style={styles.lockedChipRow}>
              <View style={styles.lockedChip}>
                <Text style={styles.lockedChipText}>{symbolA}</Text>
                <Pressable onPress={onClearSymbolA}>
                  <Text style={styles.lockedChipRemove}>✕</Text>
                </Pressable>
              </View>
            </View>
          ) : null}
          {mode === 'single' ? (
            <>
              <TextInput
                value={symbol}
                onChangeText={(value) => setSymbol(value.toUpperCase())}
                placeholder="输入标的，如 NVDA"
                placeholderTextColor={colors.textMuted}
                autoCapitalize="characters"
                autoCorrect={false}
                style={styles.input}
              />
              <View style={styles.chips}>
                {quickSymbols.map((item) => (
                  <Pressable key={item} onPress={() => onQuickSelect(item)} style={styles.chip}>
                    <Text style={styles.chipText}>{item}</Text>
                  </Pressable>
                ))}
              </View>
              <Pressable onPress={onAnalyze} style={styles.button}>
                {loading ? <ActivityIndicator color={colors.background} /> : <Text style={styles.buttonText}>分析 →</Text>}
              </Pressable>
            </>
          ) : (
            <View style={styles.followupActions}>
              {!symbolA ? (
                <>
                  <TextInput
                    value={symbol}
                    onChangeText={(value) => setSymbol(value.toUpperCase())}
                    placeholder="输入第一个标的，如 NVDA"
                    placeholderTextColor={colors.textMuted}
                    autoCapitalize="characters"
                    autoCorrect={false}
                    style={styles.input}
                  />
                  <View style={styles.chips}>
                    {quickSymbols.map((item) => (
                      <Pressable key={item} onPress={() => onQuickSelect(item)} style={styles.chip}>
                        <Text style={styles.chipText}>{item}</Text>
                      </Pressable>
                    ))}
                  </View>
                  <Pressable onPress={onAnalyze} style={styles.button}>
                    {loading ? <ActivityIndicator color={colors.background} /> : <Text style={styles.buttonText}>确认第一个标的 →</Text>}
                  </Pressable>
                </>
              ) : (
                <View style={styles.pairInputBlock}>
                  <TextInput
                    value={symbolB}
                    onChangeText={(value) => setSymbolB(value.toUpperCase())}
                    placeholder="输入第二个标的，如 TSM"
                    placeholderTextColor={colors.textMuted}
                    autoCapitalize="characters"
                    autoCorrect={false}
                    style={styles.input}
                  />
                  <View style={styles.chips}>
                    {quickSymbols
                      .filter((item) => item !== symbolA)
                      .map((item) => (
                        <Pressable key={item} onPress={() => onQuickSelect(item)} style={styles.chip}>
                          <Text style={styles.chipText}>{item}</Text>
                        </Pressable>
                      ))}
                  </View>
                  <Pressable onPress={onAnalyzePair} style={styles.button}>
                    {loading ? (
                      <ActivityIndicator color={colors.background} />
                    ) : (
                      <Text style={styles.buttonText}>开始相关性分析 →</Text>
                    )}
                  </Pressable>
                </View>
              )}
            </View>
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  page: {
    flex: 1,
    backgroundColor: colors.background,
  },
  scrollContent: {
    flexGrow: 1,
    padding: 20,
  },
  nav: {
    paddingTop: 2,
  },
  back: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '600',
  },
  pullContent: {
    flex: 1,
    justifyContent: 'center',
    gap: 18,
    paddingBottom: 72,
  },
  modeSwitcher: {
    flexDirection: 'row',
    alignSelf: 'flex-start',
    backgroundColor: colors.surface,
    borderRadius: 4,
    padding: 3,
    gap: 3,
    borderWidth: 1,
    borderColor: colors.divider,
  },
  pullTitle: {
    color: colors.textPrimary,
    fontSize: 32,
    fontWeight: '800',
    lineHeight: 38,
  },
  modeChip: {
    borderRadius: 4,
    paddingHorizontal: 14,
    paddingVertical: 7,
  },
  modeChipActive: {
    backgroundColor: '#34343A',
    borderWidth: 1,
    borderColor: '#45454D',
  },
  modeChipText: {
    color: colors.textSecondary,
    fontSize: 13,
    fontWeight: '700',
    fontFamily: typography.uiSemiBold,
  },
  modeChipTextActive: {
    color: colors.textPrimary,
  },
  lockedChipRow: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  lockedChip: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 3,
    backgroundColor: 'transparent',
    borderWidth: 1,
    borderColor: colors.textSecondary,
  },
  lockedChipText: {
    color: colors.textPrimary,
    fontSize: 16,
    fontWeight: '800',
    fontFamily: typography.monoBold,
  },
  lockedChipRemove: {
    color: colors.textMuted,
    fontSize: 13,
    fontWeight: '700',
  },
  input: {
    backgroundColor: colors.surface,
    borderRadius: 4,
    paddingHorizontal: 18,
    paddingVertical: 18,
    color: colors.textPrimary,
    fontSize: 28,
    fontWeight: '700',
    fontFamily: typography.monoBold,
    borderWidth: 1,
    borderColor: colors.divider,
  },
  chips: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 10,
  },
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 3,
    backgroundColor: colors.surface,
    borderWidth: 1,
    borderColor: colors.divider,
  },
  chipText: {
    color: colors.textSecondary,
    fontSize: 13,
    fontWeight: '700',
    fontFamily: typography.uiSemiBold,
  },
  button: {
    marginTop: 10,
    backgroundColor: colors.success,
    borderRadius: 4,
    paddingVertical: 16,
    alignItems: 'center',
  },
  buttonText: {
    color: colors.background,
    fontSize: 18,
    fontWeight: '800',
  },
  followupActions: {
    gap: 14,
  },
  pairInputBlock: {
    gap: 14,
  },
  secondaryActionText: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '500',
  },
});
