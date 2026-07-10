import { Component, type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Stack } from 'expo-router';
import {
  JetBrainsMono_400Regular,
  JetBrainsMono_500Medium,
  JetBrainsMono_600SemiBold,
  JetBrainsMono_700Bold,
  JetBrainsMono_800ExtraBold,
  useFonts,
} from '@expo-google-fonts/jetbrains-mono';
import { StyleSheet, Text, View } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { colors } from '../constants/colors';

const queryClient = new QueryClient();

export default function RootLayout() {
  const [fontsLoaded] = useFonts({
    JetBrainsMono_400Regular,
    JetBrainsMono_500Medium,
    JetBrainsMono_600SemiBold,
    JetBrainsMono_700Bold,
    JetBrainsMono_800ExtraBold,
  });

  if (!fontsLoaded) {
    return null;
  }

  return (
    <RootErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <SafeAreaProvider>
          <Stack
            screenOptions={{
              headerShown: false,
              contentStyle: { backgroundColor: colors.background },
            }}
          />
        </SafeAreaProvider>
      </QueryClientProvider>
    </RootErrorBoundary>
  );
}

type RootErrorBoundaryProps = {
  children: ReactNode;
};

type RootErrorBoundaryState = {
  hasError: boolean;
  message: string;
};

class RootErrorBoundary extends Component<RootErrorBoundaryProps, RootErrorBoundaryState> {
  state: RootErrorBoundaryState = {
    hasError: false,
    message: '',
  };

  static getDerivedStateFromError(error: unknown): RootErrorBoundaryState {
    return {
      hasError: true,
      message: error instanceof Error ? error.message : 'Unknown render error',
    };
  }

  componentDidCatch(error: unknown) {
    console.error('Root render crash', error);
  }

  render() {
    if (!this.state.hasError) {
      return this.props.children;
    }

    return (
      <View style={styles.errorWrap}>
        <Text style={styles.errorTitle}>应用启动失败</Text>
        <Text style={styles.errorBody}>{this.state.message || '请稍后重试'}</Text>
      </View>
    );
  }
}

const styles = StyleSheet.create({
  errorWrap: {
    flex: 1,
    backgroundColor: colors.background,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 24,
    gap: 10,
  },
  errorTitle: {
    color: colors.textPrimary,
    fontSize: 22,
    fontWeight: '800',
  },
  errorBody: {
    color: colors.textSecondary,
    fontSize: 14,
    lineHeight: 20,
    textAlign: 'center',
  },
});
