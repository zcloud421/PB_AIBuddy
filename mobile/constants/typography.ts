import { Platform } from 'react-native';

const uiFontFamily = Platform.select({
  ios: 'System',
  android: 'sans-serif',
  default: 'System',
});

export const typography = {
  uiRegular: uiFontFamily,
  uiMedium: uiFontFamily,
  uiSemiBold: uiFontFamily,
  uiBold: uiFontFamily,
  monoRegular: 'JetBrainsMono_400Regular',
  monoMedium: 'JetBrainsMono_500Medium',
  monoSemiBold: 'JetBrainsMono_600SemiBold',
  monoBold: 'JetBrainsMono_700Bold',
  monoExtraBold: 'JetBrainsMono_800ExtraBold',
} as const;
