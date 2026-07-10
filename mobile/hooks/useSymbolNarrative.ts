import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import type { SymbolNarrativeResponse } from '../types/api';

export function useSymbolNarrative(symbol: string, options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: ['ideas', symbol, 'narrative'],
    queryFn: async () => {
      const response = await axios.get<SymbolNarrativeResponse>(`${API_BASE}/ideas/${symbol}/narrative`);
      return response.data;
    },
    enabled: (options?.enabled ?? true) && symbol.length > 0,
    refetchInterval: (query) => {
      const data = query.state.data as SymbolNarrativeResponse | undefined;
      if (data?.ready === false && query.state.dataUpdateCount < 6) {
        return 3000;
      }
      return false;
    },
    refetchIntervalInBackground: false,
    staleTime: 60 * 1000,
  });
}
