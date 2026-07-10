import { useCallback, useEffect, useState } from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';
import axios from 'axios';

import { API_BASE } from '../constants/api';
import { getOrCreateDeviceId } from './useDeviceId';
import { registerForPushNotifications } from './usePushNotifications';

const FAVORITES_CACHE_KEY = 'josan:favorites:v2';
const MIGRATION_KEY = 'josan:favorites:migrated';
const LEGACY_KEY = 'josan:favorites';

type FavoritesListener = (favorites: string[], loaded: boolean) => void;

let favoritesStore: string[] = [];
let favoritesLoaded = false;
let loadPromise: Promise<void> | null = null;
const listeners = new Set<FavoritesListener>();

function normalize(symbol: string) {
  return symbol.trim().toUpperCase();
}

function emitFavorites() {
  for (const listener of listeners) {
    listener([...favoritesStore], favoritesLoaded);
  }
}

function setStore(symbols: string[], loaded: boolean) {
  favoritesStore = symbols;
  favoritesLoaded = loaded;
  emitFavorites();
}

async function migrateLegacyFavorites(deviceId: string): Promise<string[]> {
  const alreadyMigrated = await AsyncStorage.getItem(MIGRATION_KEY);
  if (alreadyMigrated) {
    return [];
  }

  const raw = await AsyncStorage.getItem(LEGACY_KEY);
  if (!raw) {
    await AsyncStorage.setItem(MIGRATION_KEY, '1');
    return [];
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    const symbols = Array.isArray(parsed)
      ? parsed.filter((s): s is string => typeof s === 'string').map(normalize).filter(Boolean)
      : [];

    if (symbols.length > 0) {
      await axios.post(`${API_BASE}/device/favorites/sync`, { device_id: deviceId, symbols });
    }

    await AsyncStorage.setItem(MIGRATION_KEY, '1');
    return symbols;
  } catch {
    return [];
  }
}

async function loadFavorites(): Promise<void> {
  const deviceId = await getOrCreateDeviceId();

  // Show cached data immediately while fetching from backend
  const cached = await AsyncStorage.getItem(FAVORITES_CACHE_KEY).catch(() => null);
  if (cached) {
    try {
      const parsed = JSON.parse(cached) as unknown;
      if (Array.isArray(parsed)) {
        setStore(parsed as string[], false);
      }
    } catch {
      // ignore
    }
  }

  // Migrate legacy favorites on first run
  await migrateLegacyFavorites(deviceId);

  // Fetch authoritative list from backend
  const response = await axios.get<{ symbols: string[] }>(`${API_BASE}/device/favorites`, {
    params: { device_id: deviceId },
  });
  const symbols = response.data.symbols.map(normalize);
  await AsyncStorage.setItem(FAVORITES_CACHE_KEY, JSON.stringify(symbols));
  setStore(symbols, true);
}

async function ensureFavoritesLoaded() {
  if (favoritesLoaded) {
    return;
  }
  if (loadPromise) {
    return loadPromise;
  }

  loadPromise = loadFavorites()
    .catch(() => {
      // Fall back to whatever cache we loaded
      favoritesLoaded = true;
      emitFavorites();
    })
    .finally(() => {
      loadPromise = null;
    });

  return loadPromise;
}

export function useFavorites() {
  const [favorites, setFavorites] = useState<string[]>(favoritesStore);
  const [loaded, setLoaded] = useState(favoritesLoaded);

  useEffect(() => {
    const listener: FavoritesListener = (nextFavorites, nextLoaded) => {
      setFavorites(nextFavorites);
      setLoaded(nextLoaded);
    };

    listeners.add(listener);
    void ensureFavoritesLoaded();

    return () => {
      listeners.delete(listener);
    };
  }, []);

  const isFavorite = useCallback(
    (symbol: string) => favorites.includes(normalize(symbol)),
    [favorites],
  );

  const toggleFavorite = useCallback((symbol: string) => {
    const s = normalize(symbol);
    if (!s) return;

    const isCurrentlyFavorite = favoritesStore.includes(s);
    const isFirstFavorite = !isCurrentlyFavorite && favoritesStore.length === 0;
    const nextFavorites = isCurrentlyFavorite
      ? favoritesStore.filter((item) => item !== s)
      : [s, ...favoritesStore];

    setStore(nextFavorites, true);
    void AsyncStorage.setItem(FAVORITES_CACHE_KEY, JSON.stringify(nextFavorites));

    if (isFirstFavorite) {
      registerForPushNotifications()
        .then(async (token) => {
          if (!token) return;
          const deviceId = await getOrCreateDeviceId();
          await axios.post(`${API_BASE}/device/token`, { device_id: deviceId, push_token: token });
        })
        .catch(() => {});
    }

    getOrCreateDeviceId().then((deviceId) => {
      const url = `${API_BASE}/device/favorites`;
      const payload = { device_id: deviceId, symbol: s };
      return isCurrentlyFavorite
        ? axios.delete(url, { data: payload })
        : axios.post(url, payload);
    }).catch(() => {
      // Revert on failure
      setStore(favoritesStore.filter((item) => item !== s).concat(isCurrentlyFavorite ? [s] : []), true);
    });
  }, []);

  const removeFavorite = useCallback((symbol: string) => {
    const s = normalize(symbol);
    if (!s || !favoritesStore.includes(s)) return;

    const nextFavorites = favoritesStore.filter((item) => item !== s);
    setStore(nextFavorites, true);
    void AsyncStorage.setItem(FAVORITES_CACHE_KEY, JSON.stringify(nextFavorites));

    getOrCreateDeviceId().then((deviceId) =>
      axios.delete(`${API_BASE}/device/favorites`, { data: { device_id: deviceId, symbol: s } }),
    ).catch(() => {});
  }, []);

  return { favorites, loaded, isFavorite, toggleFavorite, removeFavorite };
}
