import { useEffect } from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Tabs, router } from 'expo-router';
import { Feather } from '@expo/vector-icons';
import { StyleSheet } from 'react-native';

import { ONBOARDING_KEY } from '../onboarding';
import { colors } from '../../constants/colors';
import { typography } from '../../constants/typography';

export default function TabLayout() {
  useEffect(() => {
    AsyncStorage.getItem(ONBOARDING_KEY)
      .then((value) => {
        if (!value) {
          router.replace('/onboarding');
        }
      })
      .catch(() => {
        // Some runtimes/new-architecture setups may not expose legacy storage.
        // Failing open avoids crashing the whole app on boot.
      });
  }, []);

  return (
    <Tabs
      screenOptions={{
        headerShown: false,
        sceneStyle: {
          backgroundColor: colors.background,
        },
        tabBarStyle: {
          backgroundColor: '#0B0B0B',
          borderTopColor: colors.divider,
          borderTopWidth: StyleSheet.hairlineWidth,
          height: 84,
          paddingBottom: 20,
          paddingTop: 10,
        },
        tabBarActiveTintColor: '#E8E8E8',
        tabBarInactiveTintColor: '#4A4A4A',
        tabBarLabelStyle: {
          fontSize: 9,
          fontWeight: '700',
          letterSpacing: 1.2,
          textTransform: 'uppercase',
          fontFamily: typography.uiSemiBold,
        },
        tabBarIconStyle: {
          marginBottom: 2,
        },
      }}
    >
      <Tabs.Screen
        name="index"
        options={{
          title: 'FCN',
          tabBarIcon: ({ color }) => <Feather name="trending-up" size={22} color={color} />,
        }}
      />
      <Tabs.Screen
        name="markets"
        options={{
          title: 'TIMING',
          tabBarIcon: ({ color }) => <Feather name="crosshair" size={22} color={color} />,
        }}
      />
      <Tabs.Screen
        name="watchlist"
        options={{
          title: 'WATCH',
          tabBarIcon: ({ color }) => <Feather name="bookmark" size={22} color={color} />,
        }}
      />
      {/* IC admin screen — hidden from the user-facing tab bar (href: null).
          Route still exists for gated/direct access; not a public tab. */}
      <Tabs.Screen
        name="admin"
        options={{
          href: null,
        }}
      />
    </Tabs>
  );
}
