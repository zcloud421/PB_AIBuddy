export const colors = {
  // Surfaces
  background: '#080808',
  surface: '#161616',
  surfaceElevated: '#0E1E35',

  // Borders
  border: '#252525',
  borderSoft: '#1E1E1E',
  borderStrong: '#27486B',
  borderAccent: '#E8821C',
  divider: '#2A2A2A',
  track: '#262626',

  // Text
  textPrimary: '#E8E8E8',
  textSecondary: '#888888',
  textMuted: '#606060',
  textDisabled: '#282828',

  // Semantic
  label: '#4A6070',
  link: '#2D7BC4',

  // Status
  success: '#00C087',
  warning: '#E8821C',
  danger: '#E04040',
  neutral: '#666666',

  // Accent
  gold: '#E8821C',
  warningSurface: 'rgba(232,130,28,0.08)',
  warningBorder: 'rgba(232,130,28,0.25)',

  // Copy block
  copyBg: '#121212',
  copyBorder: '#1E2A36',
  copyText: '#6A8098',

  // Risk
  riskTint: '#C8A870',

  // Chart (SVG primitives)
  chartLine: '#6B7B8D',
  chartGrid: '#222222',

  // Market risk monitor
  marketPanel: '#101010',
  marketPanelElevated: '#0E1E35',
  marketPanelManual: '#111111',
  marketGaugeTrack: '#242424',
  marketGaugeNeedle: '#D8D8D8',
  marketLadderMuted: '#777777',
  statusSuccessTint: 'rgba(0,192,135,0.10)',
  statusNeutralTint: 'rgba(160,160,160,0.10)',
  statusWarningTint: 'rgba(232,130,28,0.10)',
  statusDangerTint: 'rgba(224,64,64,0.10)',
  amberMuted: '#C69A55',
  amberStrong: '#D7A24C',
} as const;

export function gradeColor(grade: 'GO' | 'CAUTION' | 'AVOID' | 'CHECK' | 'NOT_RECOMMENDABLE') {
  if (grade === 'GO') return colors.success;
  if (grade === 'CAUTION') return colors.warning;
  if (grade === 'AVOID') return colors.danger;
  return colors.neutral;
}
