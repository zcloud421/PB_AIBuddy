import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import AsyncStorage from '@react-native-async-storage/async-storage';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import { PITCH_ENGINE_VERSION } from '../constants/pitchEngine';
import type { SymbolIdeaResponse } from '../types/api';

function symbolCacheKey(symbol: string) {
  return `josan:v5:${PITCH_ENGINE_VERSION}:ideas:symbol:${symbol.toUpperCase()}`;
}

function isUnavailableIdea(data: SymbolIdeaResponse | null | undefined) {
  if (!data) {
    return false;
  }

  return (
    data.composite_score === 0 &&
    data.reasoning_text === 'Data unavailable for this symbol at the moment.' &&
    data.price_context.current_price == null &&
    data.recommended_strike == null
  );
}

function hasIncompleteKeyEvents(data: SymbolIdeaResponse | null | undefined) {
  if (!data?.narrative?.key_events?.length) {
    return false;
  }

  const danglingSuffixes = [
    '引发股价',
    '导致股价',
    '拖累股价',
    '提振股价',
    '令股价',
    '使股价',
    '引发',
    '导致',
    '拖累',
    '提振',
    '但',
    '并',
  ];

  return data.narrative.key_events.some((event) => {
    const normalized = event.trim();
    return danglingSuffixes.some((suffix) => normalized.endsWith(suffix));
  });
}

export async function fetchSymbolIdea(symbol: string) {
  const response = await axios.get<SymbolIdeaResponse>(`${API_BASE}/ideas/${symbol}`);
  return response.data;
}

export function useSymbolIdea(symbol: string) {
  const normalizedSymbol = symbol.toUpperCase();
  const [cachedData, setCachedData] = useState<SymbolIdeaResponse | null>(null);

  useEffect(() => {
    if (!normalizedSymbol) {
      setCachedData(null);
      return;
    }

    let active = true;

    AsyncStorage.getItem(symbolCacheKey(normalizedSymbol))
      .then((raw) => {
        if (!active || !raw) {
          return;
        }

        try {
          const parsed = JSON.parse(raw) as SymbolIdeaResponse;
          if (!isUnavailableIdea(parsed) && !hasIncompleteKeyEvents(parsed)) {
            setCachedData(parsed);
          }
        } catch {
          // Ignore malformed cache and fetch fresh data.
        }
      })
      .catch(() => {
        // Ignore cache read errors and fetch fresh data.
      });

    return () => {
      active = false;
    };
  }, [normalizedSymbol]);

  const query = useQuery({
    queryKey: ['ideas', symbol, PITCH_ENGINE_VERSION],
    queryFn: async () => fetchSymbolIdea(normalizedSymbol),
    enabled: symbol.length > 0,
    staleTime: 10 * 60 * 1000,
  });

  useEffect(() => {
    if (!normalizedSymbol || !query.data) {
      return;
    }

    if (isUnavailableIdea(query.data) || hasIncompleteKeyEvents(query.data)) {
      setCachedData((previous) => (isUnavailableIdea(previous) ? null : previous));
      void AsyncStorage.removeItem(symbolCacheKey(normalizedSymbol)).catch(() => {
        // Ignore cache cleanup errors to avoid blocking UI.
      });
      return;
    }

    setCachedData(query.data);
    void AsyncStorage.setItem(symbolCacheKey(normalizedSymbol), JSON.stringify(query.data)).catch(() => {
      // Ignore cache write errors to avoid blocking UI.
    });
  }, [normalizedSymbol, query.data]);

  return {
    ...query,
    data: query.data ?? cachedData,
    isLoading: query.isLoading && !cachedData,
  };
}
