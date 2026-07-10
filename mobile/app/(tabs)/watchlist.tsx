import { router } from 'expo-router';
import { Alert, Pressable, ScrollView, Text, View } from 'react-native';
import { useMemo } from 'react';

import { useFavorites } from '../../hooks/useFavorites';
import { useTodayIdeas } from '../../hooks/useTodayIdeas';
import {
  AppPageFrame,
  WATCHLIST_THEME_FALLBACKS,
  sharedStyles as styles,
} from '../../components/tab-shared';

export default function WatchlistTab() {
  const { data } = useTodayIdeas();
  const { favorites, loaded: favoritesLoaded, removeFavorite } = useFavorites();

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

  const goToSymbol = (nextSymbol: string) => {
    const target = nextSymbol.trim().toUpperCase();
    if (!target) {
      return;
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

  return (
    <AppPageFrame>
      <ScrollView contentContainerStyle={styles.scrollContent} showsVerticalScrollIndicator={false}>
        {favoritesLoaded && watchlistItems.length > 0 ? (
          <View style={styles.watchlistList}>
            {watchlistItems.map((item) => (
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
        ) : (
          <View style={styles.watchlistEmpty}>
            <Text style={styles.watchlistEmptyTitle}>暂无关注标的</Text>
            <Text style={styles.watchlistEmptyBody}>在详情页点击星标后，这里会显示你的关注列表。</Text>
          </View>
        )}
      </ScrollView>
    </AppPageFrame>
  );
}
