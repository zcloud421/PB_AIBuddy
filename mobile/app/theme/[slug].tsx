import { router, useLocalSearchParams } from 'expo-router';
import { useQueries } from '@tanstack/react-query';
import { useMemo } from 'react';
import { ScrollView, StyleSheet, Text, View, Pressable } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import { SectionHeading } from '../../components/SectionHeading';
import { ThemeIdeaCard, type ThemeIdeaCardItem } from '../../components/ThemeIdeaCard';
import { colors } from '../../constants/colors';
import { formatMonths, formatStrikeWithMoneyness } from '../../constants/display';
import { getThemeBasket } from '../../constants/theme-baskets';
import { fetchSymbolIdea } from '../../hooks/useSymbolIdea';
import { useTodayIdeas } from '../../hooks/useTodayIdeas';
import type { Grade, WaitReason } from '../../types/api';

function isWaitReason(waitReason?: WaitReason | null) {
  return waitReason === 'WAIT_EARNINGS_RISK' || waitReason === 'WAIT_POST_EARNINGS_SHOCK';
}

function isNearEarnings(daysToEarnings?: number | null) {
  return daysToEarnings !== null && daysToEarnings !== undefined && daysToEarnings >= 0 && daysToEarnings <= 3;
}

export default function ThemeDetailScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const { data } = useTodayIdeas();

  const basket = getThemeBasket(Array.isArray(slug) ? slug[0] : slug ?? '');
  const detailQueries = useQueries({
    queries: (basket?.members ?? []).map((member) => ({
      queryKey: ['symbol-idea', member.symbol],
      queryFn: () => fetchSymbolIdea(member.symbol),
      staleTime: 60_000,
    })),
  });

  const detailMap = useMemo(() => {
    const entries = detailQueries
      .map((query) => query.data)
      .filter((idea): idea is NonNullable<typeof idea> => Boolean(idea))
      .map((idea) => [idea.symbol, idea] as const);

    return new Map(entries);
  }, [detailQueries]);

  const todayIdeaMap = useMemo(() => {
    const entries = [
      ...((data?.recommended ?? []).map((idea) => ({
        ...idea,
        isEarningsWait: isWaitReason(idea.wait_reason),
      }))),
      ...((data?.caution ?? []).map((idea) => ({
        ...idea,
        isEarningsWait: isWaitReason(idea.wait_reason),
      }))),
      ...(data?.not_recommended ?? []).map((idea) => ({
        symbol: idea.symbol,
        grade: 'AVOID' as const,
        isEarningsWait: isWaitReason(idea.wait_reason),
        recommended_tenor_days: null,
        recommended_expiry_date: null,
        recommended_strike: null,
        moneyness_pct: null,
        estimated_coupon_range: null,
      })),
    ];

    return new Map(entries.map((entry) => [entry.symbol, entry]));
  }, [data]);

  const items: ThemeIdeaCardItem[] = useMemo(() => {
    if (!basket) {
      return [];
    }

    return basket.members.map((member) => {
      const idea = todayIdeaMap.get(member.symbol);
      const detail = detailMap.get(member.symbol);
      const detailWait = isWaitReason(detail?.wait_reason) || isNearEarnings(detail?.price_context.days_to_earnings);
      const isEarningsWait = detailWait || idea?.isEarningsWait || false;
      const recommendedTenorDays = detail?.recommended_tenor_days ?? idea?.recommended_tenor_days ?? null;
      const recommendedStrike = detail?.recommended_strike ?? idea?.recommended_strike ?? null;
      const moneynessPct = detail?.moneyness_pct ?? idea?.moneyness_pct ?? null;
      const couponRange = detail?.estimated_coupon_range ?? idea?.estimated_coupon_range ?? null;
      const comboText =
        !isEarningsWait && recommendedTenorDays && recommendedStrike && moneynessPct
          ? `${formatMonths(recommendedTenorDays)} · ${formatStrikeWithMoneyness(recommendedStrike, moneynessPct)} · 参考票息${couponRange ?? '—'}`
          : null;

      return {
        symbol: member.symbol,
        role: member.role,
        grade: normalizeThemeIdeaGrade(detail?.grade ?? idea?.grade ?? 'CHECK'),
        isEarningsWait,
        comboText,
      };
    });
  }, [basket, detailMap, todayIdeaMap]);

  if (!basket) {
    return (
      <SafeAreaView style={styles.page}>
        <View style={styles.emptyWrap}>
          <Text style={styles.emptyText}>主题暂不可用</Text>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={styles.page}>
      <ScrollView style={styles.container} contentContainerStyle={styles.content} showsVerticalScrollIndicator={false}>
        <Pressable onPress={() => router.back()}>
          <Text style={styles.back}>← 返回</Text>
        </Pressable>

        <SectionHeading title={basket.title} label="Thematic Basket" />

        <Text style={styles.description}>{basket.description}</Text>

        <View style={styles.listGap}>
          {items.map((item) => (
            <ThemeIdeaCard key={item.symbol} item={item} onPress={(symbol) => router.push(`/detail/${symbol}`)} />
          ))}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function normalizeThemeIdeaGrade(grade: Grade | 'CHECK'): 'GO' | 'CAUTION' | 'AVOID' | 'CHECK' {
  return grade === 'NOT_RECOMMENDABLE' ? 'CHECK' : grade;
}

const styles = StyleSheet.create({
  page: {
    flex: 1,
    backgroundColor: colors.background,
  },
  container: {
    flex: 1,
    backgroundColor: colors.background,
  },
  content: {
    padding: 20,
    gap: 18,
    paddingBottom: 28,
  },
  back: {
    color: colors.textSecondary,
    fontSize: 15,
    fontWeight: '600',
  },
  description: {
    color: colors.textSecondary,
    fontSize: 13,
    lineHeight: 20,
  },
  listGap: {
    gap: 12,
  },
  emptyWrap: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
  },
  emptyText: {
    color: colors.textSecondary,
    fontSize: 15,
  },
});
