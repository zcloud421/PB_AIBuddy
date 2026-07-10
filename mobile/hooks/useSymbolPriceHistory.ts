import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import type { SymbolPriceHistoryResponse } from '../types/api';

export async function fetchSymbolPriceHistory(symbol: string, strikePct?: number | null) {
  const response = await axios.get<SymbolPriceHistoryResponse>(`${API_BASE}/ideas/${symbol}/price-history`, {
    params:
      strikePct !== null && strikePct !== undefined && strikePct > 0 && strikePct <= 100
        ? { strike_pct: strikePct }
        : undefined,
  });
  return response.data;
}

export function useSymbolPriceHistory(symbol: string, enabled = true, strikePct?: number | null) {
  const normalizedSymbol = symbol.toUpperCase();
  const normalizedStrikePct =
    strikePct !== null && strikePct !== undefined && strikePct > 0 && strikePct <= 100
      ? Number(strikePct.toFixed(1))
      : null;

  return useQuery({
    queryKey: ['ideas', normalizedSymbol, 'price-history', normalizedStrikePct],
    queryFn: () => fetchSymbolPriceHistory(normalizedSymbol, normalizedStrikePct),
    enabled: enabled && normalizedSymbol.length > 0,
    staleTime: 60 * 1000,
    refetchOnMount: 'always',
    refetchOnReconnect: 'always',
  });
}
