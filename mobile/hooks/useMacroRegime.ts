import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import AsyncStorage from '@react-native-async-storage/async-storage';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import type { MacroRegimeSnapshot } from '../types/macro-regime';

const CACHE_KEY = 'josan:macro-regime:latest';

export async function fetchMacroRegimeLatest() {
  const response = await axios.get<MacroRegimeSnapshot>(`${API_BASE}/macro-regime/latest`);
  return response.data;
}

export function useMacroRegime() {
  const [cachedData, setCachedData] = useState<MacroRegimeSnapshot | null>(null);

  useEffect(() => {
    let active = true;
    AsyncStorage.getItem(CACHE_KEY)
      .then((raw) => {
        if (!active || !raw) return;
        try {
          setCachedData(JSON.parse(raw) as MacroRegimeSnapshot);
        } catch {
          // Ignore malformed cache.
        }
      })
      .catch(() => {
        // Ignore cache read errors.
      });
    return () => {
      active = false;
    };
  }, []);

  const query = useQuery({
    queryKey: ['macro-regime', 'latest'],
    queryFn: fetchMacroRegimeLatest,
    // Snapshot cron runs once per day (post-US-close); fairly long staleness OK.
    staleTime: 15 * 60 * 1000,
  });

  useEffect(() => {
    if (!query.data) return;
    setCachedData(query.data);
    void AsyncStorage.setItem(CACHE_KEY, JSON.stringify(query.data)).catch(() => {
      // Ignore cache write errors.
    });
  }, [query.data]);

  return {
    ...query,
    data: query.data ?? cachedData,
    isLoading: query.isLoading && !cachedData,
    isRefreshing: query.isFetching && !!cachedData,
  };
}
