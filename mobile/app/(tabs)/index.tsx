import { useEffect, useMemo, useRef, useState } from 'react';
import { router } from 'expo-router';
import { useQueryClient } from '@tanstack/react-query';
import { Alert, Animated, Easing, Pressable, ScrollView, Text, View } from 'react-native';

import { DailyBestCard } from '../../components/DailyBestCard';
import { IdeaCard } from '../../components/IdeaCard';
import { SectionHeading } from '../../components/SectionHeading';
import { ThemeCard } from '../../components/ThemeCard';
import { THEME_BASKETS } from '../../constants/theme-baskets';
import { PITCH_ENGINE_VERSION } from '../../constants/pitchEngine';
import { useFavorites } from '../../hooks/useFavorites';
import { fetchSymbolIdea } from '../../hooks/useSymbolIdea';
import { useTodayIdeas } from '../../hooks/useTodayIdeas';
import {
  AppPageFrame,
  CenteredState,
  DailyBestEmptyCard,
  DailyBestSkeleton,
  IdeaCardSkeleton,
  REFRESH_BANNER_MIN_VISIBLE_MS,
  WATCHLIST_THEME_FALLBACKS,
  isHighVolatilityIdea,
  sharedStyles as styles,
} from '../../components/tab-shared';

export default function TodayIdeasTab() {
  const [showRefreshBanner, setShowRefreshBanner] = useState(false);
  const { data, isError, isRefreshing, refetch } = useTodayIdeas();
  const { favorites, removeFavorite } = useFavorites();
  const queryClient = useQueryClient();
  const refreshBannerOpacity = useRef(new Animated.Value(0)).current;
  const refreshBannerTranslateY = useRef(new Animated.Value(-18)).current;
  const refreshStartedAtRef = useRef<number | null>(null);
  const refreshHideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (isRefreshing) {
      refreshStartedAtRef.current = Date.now();
      if (refreshHideTimerRef.current) {
        clearTimeout(refreshHideTimerRef.current);
        refreshHideTimerRef.current = null;
      }
      setShowRefreshBanner(true);
      return;
    }

    const visibleFor =
      refreshStartedAtRef.current === null ? REFRESH_BANNER_MIN_VISIBLE_MS : Date.now() - refreshStartedAtRef.current;
    const remainingMs = Math.max(0, REFRESH_BANNER_MIN_VISIBLE_MS - visibleFor);

    refreshHideTimerRef.current = setTimeout(() => {
      setShowRefreshBanner(false);
      refreshStartedAtRef.current = null;
      refreshHideTimerRef.current = null;
    }, remainingMs);

    return () => {
      if (refreshHideTimerRef.current) {
        clearTimeout(refreshHideTimerRef.current);
        refreshHideTimerRef.current = null;
      }
    };
  }, [isRefreshing]);

  useEffect(() => {
    Animated.parallel([
      Animated.timing(refreshBannerOpacity, {
        toValue: showRefreshBanner ? 1 : 0,
        duration: showRefreshBanner ? 180 : 140,
        easing: showRefreshBanner ? Easing.out(Easing.cubic) : Easing.in(Easing.cubic),
        useNativeDriver: true,
      }),
      Animated.timing(refreshBannerTranslateY, {
        toValue: showRefreshBanner ? 0 : -18,
        duration: showRefreshBanner ? 220 : 140,
        easing: showRefreshBanner ? Easing.out(Easing.cubic) : Easing.in(Easing.cubic),
        useNativeDriver: true,
      }),
    ]).start();
  }, [showRefreshBanner, refreshBannerOpacity, refreshBannerTranslateY]);

  const todayIdeaMap = useMemo(() => {
    const entries = [
      data?.daily_best
        ? {
            symbol: data.daily_best.symbol,
            grade: data.daily_best.grade,
            subtitle: data.daily_best.theme,
          }
        : null,
      ...(data?.recommended ?? []).map((idea) => ({
        symbol: idea.symbol,
        grade: idea.grade,
        subtitle: idea.themes[0] ?? null,
      })),
      ...(data?.caution ?? []).map((idea) => ({
        symbol: idea.symbol,
        grade: idea.grade,
        subtitle: idea.themes[0] ?? null,
      })),
      ...(data?.not_recommended ?? []).map((idea) => ({
        symbol: idea.symbol,
        grade: 'AVOID' as const,
        subtitle: WATCHLIST_THEME_FALLBACKS[idea.symbol] ?? null,
      })),
    ].filter(Boolean) as Array<{
      symbol: string;
      grade: 'GO' | 'CAUTION' | 'AVOID';
      subtitle: string | null;
    }>;

    return new Map(entries.map((entry) => [entry.symbol, entry]));
  }, [data]);

  const watchlistItems = useMemo(
    () =>
      favorites.map((favorite) => {
        const todayIdea = todayIdeaMap.get(favorite);

        return {
          symbol: favorite,
          grade: todayIdea?.grade ?? null,
          subtitle: todayIdea?.subtitle ?? WATCHLIST_THEME_FALLBACKS[favorite] ?? '已加入关注列表',
        };
      }),
    [favorites, todayIdeaMap],
  );

  const goToSymbol = async (nextSymbol: string) => {
    const target = nextSymbol.trim().toUpperCase();
    if (!target) {
      return;
    }

    const isTodayUniverseSymbol =
      [...(data?.recommended ?? []), ...(data?.caution ?? [])].some((idea) => idea.symbol === target) ||
      data?.daily_best?.symbol === target;
    if (isTodayUniverseSymbol) {
      void queryClient.prefetchQuery({
        queryKey: ['ideas', target, PITCH_ENGINE_VERSION],
        queryFn: () => fetchSymbolIdea(target),
        staleTime: 5 * 60 * 1000,
      });
    }
    router.push(`/detail/${target}`);
  };

  const confirmRemoveFavorite = (symbolToRemove: string) => {
    Alert.alert('删除自选', `将 ${symbolToRemove} 从我的关注中移除？`, [
      { text: '取消', style: 'cancel' },
      {
        text: '删除',
        style: 'destructive',
        onPress: () => removeFavorite(symbolToRemove),
      },
    ]);
  };

  if (isError && !data) {
    return <CenteredState text="数据加载失败，请重试" onPress={() => refetch()} />;
  }

  const otherIdeas = [
    ...(data?.recommended ?? []),
    ...(data?.caution ?? []),
  ]
    .filter((idea) => !isHighVolatilityIdea(idea))
    .filter((idea) => idea.symbol !== data?.daily_best?.symbol)
    .slice(0, 3);

  return (
    <AppPageFrame>
      <Animated.View
        pointerEvents="none"
        style={[
          styles.refreshBannerWrap,
          {
            opacity: refreshBannerOpacity,
            transform: [{ translateY: refreshBannerTranslateY }],
          },
        ]}
      >
        <View style={styles.refreshBanner}>
          <View style={styles.refreshDot} />
          <Text style={styles.refreshBannerText}>正在刷新推荐</Text>
        </View>
      </Animated.View>

      <ScrollView contentContainerStyle={styles.scrollContent} showsVerticalScrollIndicator={false}>
        {data ? (
          data.daily_best ? <DailyBestCard idea={data.daily_best} onPress={goToSymbol} /> : <DailyBestEmptyCard />
        ) : (
          <DailyBestSkeleton />
        )}

        <SectionHeading title="今日其他可关注" label="Watchlist" />
        {data ? (
          <View style={styles.listGap}>
            {otherIdeas.map((idea) => (
              <IdeaCard key={idea.symbol} idea={idea} onPress={goToSymbol} />
            ))}
          </View>
        ) : (
          <View style={styles.listGap}>
            <IdeaCardSkeleton />
            <IdeaCardSkeleton />
            <IdeaCardSkeleton />
          </View>
        )}

        <SectionHeading title="热门交易主题" label="Thematic Basket" />
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={styles.themeRail}
        >
          {THEME_BASKETS.map((basket) => (
            <ThemeCard key={basket.slug} basket={basket} onPress={(slug) => router.push(`/theme/${slug}`)} />
          ))}
          <Pressable style={styles.pairThemeCard} onPress={() => router.push('/pull?mode=pair')}>
            <View style={styles.pairThemeCardBody}>
              <Text style={styles.pairThemeCardTitle}>双标的FCN</Text>
            </View>
            <View style={styles.pairThemeCardFooter}>
              <Text style={styles.pairThemeCardMeta}>3M/6M相关性 · 下跌同步</Text>
            </View>
          </Pressable>
        </ScrollView>

        {watchlistItems.length > 0 ? (
          <>
            <SectionHeading title="我的关注快览" label="Favorites" />
            <View style={styles.watchlistList}>
              {watchlistItems.slice(0, 3).map((item) => (
                <Pressable
                  key={item.symbol}
                  style={styles.watchlistRow}
                  onPress={() => goToSymbol(item.symbol)}
                  onLongPress={() => confirmRemoveFavorite(item.symbol)}
                  delayLongPress={350}
                >
                  <View style={styles.watchlistRowLeft}>
                    <Text style={styles.watchlistSymbol}>{item.symbol}</Text>
                    <Text style={styles.watchlistSubtitle} numberOfLines={1}>
                      {item.subtitle}
                    </Text>
                  </View>
                  <View style={styles.watchlistRowRight}>
                    {item.grade ? (
                      <Text
                        style={[
                          styles.watchlistGrade,
                          item.grade === 'GO'
                            ? styles.watchlistGradeGo
                            : item.grade === 'CAUTION'
                              ? styles.watchlistGradeCaution
                              : styles.watchlistGradeAvoid,
                        ]}
                      >
                        {item.grade}
                      </Text>
                    ) : (
                      <Text style={styles.watchlistGradePending}>CHECK</Text>
                    )}
                  </View>
                </Pressable>
              ))}
            </View>
          </>
        ) : null}
      </ScrollView>
    </AppPageFrame>
  );
}
