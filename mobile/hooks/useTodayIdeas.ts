import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import AsyncStorage from '@react-native-async-storage/async-storage';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import { PITCH_ENGINE_VERSION } from '../constants/pitchEngine';
import type { TodayIdeasResponse } from '../types/api';

const TODAY_CACHE_KEY = `josan:v2:${PITCH_ENGINE_VERSION}:ideas:today`;

export async function fetchTodayIdeas() {
  const response = await axios.get<TodayIdeasResponse>(`${API_BASE}/ideas/today`);
  return response.data;
}

export function useTodayIdeas() {
  const [cachedData, setCachedData] = useState<TodayIdeasResponse | null>(null);

  useEffect(() => {
    let active = true;

    AsyncStorage.getItem(TODAY_CACHE_KEY)
      .then((raw) => {
        if (!active || !raw) {
          return;
        }

        try {
          const parsed = JSON.parse(raw) as TodayIdeasResponse;
          setCachedData(parsed);
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
  }, []);

  const query = useQuery({
    queryKey: ['ideas', 'today', PITCH_ENGINE_VERSION],
    queryFn: fetchTodayIdeas,
    staleTime: 5 * 60 * 1000,
  });

  useEffect(() => {
    if (!query.data) {
      return;
    }

    setCachedData(query.data);
    void AsyncStorage.setItem(TODAY_CACHE_KEY, JSON.stringify(query.data)).catch(() => {
      // Ignore cache write errors to avoid blocking UI.
    });
  }, [query.data]);

  return {
    ...query,
    data: query.data ?? cachedData,
    isLoading: query.isLoading && !cachedData,
    isRefreshing: query.isFetching && !!cachedData,
  };
}
