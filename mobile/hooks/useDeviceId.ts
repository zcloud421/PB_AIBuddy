import { useEffect, useState } from 'react';
import AsyncStorage from '@react-native-async-storage/async-storage';

const DEVICE_ID_KEY = 'josan:device_id';

let cachedDeviceId: string | null = null;

function generateId(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export async function getOrCreateDeviceId(): Promise<string> {
  if (cachedDeviceId) {
    return cachedDeviceId;
  }

  const stored = await AsyncStorage.getItem(DEVICE_ID_KEY);
  if (stored) {
    cachedDeviceId = stored;
    return stored;
  }

  const newId = generateId();
  await AsyncStorage.setItem(DEVICE_ID_KEY, newId);
  cachedDeviceId = newId;
  return newId;
}

export function useDeviceId() {
  const [deviceId, setDeviceId] = useState<string | null>(cachedDeviceId);

  useEffect(() => {
    if (cachedDeviceId) {
      return;
    }
    void getOrCreateDeviceId().then(setDeviceId);
  }, []);

  return deviceId;
}
