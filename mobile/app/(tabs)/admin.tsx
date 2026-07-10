import { useState } from 'react';
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, TextInput, View } from 'react-native';
import axios from 'axios';
import { useQuery, useQueryClient } from '@tanstack/react-query';

import { API_BASE } from '../../constants/api';
import { colors } from '../../constants/colors';
import { typography } from '../../constants/typography';

type OverrideRow = {
  symbol: string;
  override_type: 'FORCE_AVOID' | 'FORCE_CAUTION' | 'WHITELIST_ONLY';
  reason: string;
  created_by: string;
  created_at: string;
};

type UniverseRow = {
  symbol: string;
  company_name: string | null;
  status: 'active' | 'suspended' | 'under_review' | 'deprecated';
  status_reason: string | null;
  reviewed_by: string | null;
};

export default function AdminScreen() {
  const [token, setToken] = useState('');
  const [unlocked, setUnlocked] = useState(false);
  const queryClient = useQueryClient();

  const headers = { 'x-setup-token': token };
  const overridesQuery = useQuery({
    queryKey: ['admin', 'overrides', unlocked],
    enabled: unlocked,
    queryFn: async () => (await axios.get<{ overrides: OverrideRow[] }>(`${API_BASE}/admin/overrides`, { headers })).data.overrides,
  });
  const universeQuery = useQuery({
    queryKey: ['admin', 'universe', unlocked],
    enabled: unlocked,
    queryFn: async () => (await axios.get<{ underlyings: UniverseRow[] }>(`${API_BASE}/admin/universe`, { headers })).data.underlyings,
  });

  if (!unlocked) {
    return (
      <View style={styles.lockScreen}>
        <Text style={styles.title}>IC Admin</Text>
        <Text style={styles.copy}>输入 setup token 后查看 house override 与 universe status。</Text>
        <TextInput
          value={token}
          onChangeText={setToken}
          placeholder="Setup token"
          placeholderTextColor={colors.textMuted}
          secureTextEntry
          style={styles.input}
        />
        <Pressable style={styles.primaryButton} onPress={() => setUnlocked(token.trim().length > 0)}>
          <Text style={styles.primaryButtonText}>进入</Text>
        </Pressable>
      </View>
    );
  }

  const revoke = async (symbol: string) => {
    await axios.delete(`${API_BASE}/admin/overrides/${symbol}`, { headers });
    await queryClient.invalidateQueries({ queryKey: ['admin'] });
  };

  const setStatus = async (symbol: string, current: UniverseRow['status']) => {
    const next: UniverseRow['status'] = current === 'active' ? 'under_review' : 'active';
    await axios.patch(
      `${API_BASE}/admin/universe/${symbol}/status`,
      { new_status: next, reason: 'Mobile IC admin update', changed_by: 'IC-Mobile' },
      { headers },
    );
    await queryClient.invalidateQueries({ queryKey: ['admin'] });
  };

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Text style={styles.title}>IC Admin</Text>

      <Text style={styles.sectionTitle}>Active Overrides</Text>
      {overridesQuery.isLoading ? <ActivityIndicator color={colors.textSecondary} /> : null}
      {(overridesQuery.data ?? []).map((row) => (
        <View key={`${row.symbol}-${row.created_at}`} style={styles.row}>
          <View style={styles.rowMain}>
            <Text style={styles.symbol}>{row.symbol}</Text>
            <Text style={styles.meta}>{row.override_type} · {row.created_by}</Text>
            <Text style={styles.body}>{row.reason}</Text>
          </View>
          <Pressable style={styles.smallButton} onPress={() => revoke(row.symbol)}>
            <Text style={styles.smallButtonText}>撤销</Text>
          </Pressable>
        </View>
      ))}

      <Text style={styles.sectionTitle}>Universe Status</Text>
      {universeQuery.isLoading ? <ActivityIndicator color={colors.textSecondary} /> : null}
      {(universeQuery.data ?? []).map((row) => (
        <View key={row.symbol} style={styles.row}>
          <View style={styles.rowMain}>
            <Text style={styles.symbol}>{row.symbol}</Text>
            <Text style={styles.meta}>{row.company_name ?? '—'} · {row.status}</Text>
            {row.status_reason ? <Text style={styles.body}>{row.status_reason}</Text> : null}
          </View>
          <Pressable style={styles.smallButton} onPress={() => setStatus(row.symbol, row.status)}>
            <Text style={styles.smallButtonText}>{row.status === 'active' ? '审阅' : '激活'}</Text>
          </Pressable>
        </View>
      ))}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: colors.background },
  content: { padding: 20, paddingBottom: 48, gap: 14 },
  lockScreen: { flex: 1, backgroundColor: colors.background, padding: 24, justifyContent: 'center', gap: 16 },
  title: { color: colors.textPrimary, fontSize: 24, fontWeight: '800', fontFamily: typography.uiSemiBold },
  copy: { color: colors.textSecondary, fontSize: 13, lineHeight: 19 },
  input: {
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 4,
    color: colors.textPrimary,
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  primaryButton: { backgroundColor: colors.textPrimary, borderRadius: 4, paddingVertical: 12, alignItems: 'center' },
  primaryButtonText: { color: colors.background, fontWeight: '800' },
  sectionTitle: { color: colors.textMuted, fontSize: 12, fontWeight: '700', marginTop: 12, textTransform: 'uppercase' },
  row: { borderWidth: 1, borderColor: colors.border, borderRadius: 4, padding: 12, flexDirection: 'row', gap: 12 },
  rowMain: { flex: 1, gap: 4 },
  symbol: { color: colors.textPrimary, fontSize: 15, fontWeight: '800', fontFamily: typography.monoBold },
  meta: { color: colors.textSecondary, fontSize: 12 },
  body: { color: colors.textMuted, fontSize: 12, lineHeight: 17 },
  smallButton: { alignSelf: 'center', borderWidth: 1, borderColor: colors.borderAccent, paddingHorizontal: 10, paddingVertical: 7, borderRadius: 4 },
  smallButtonText: { color: colors.warning, fontSize: 12, fontWeight: '700' },
});
