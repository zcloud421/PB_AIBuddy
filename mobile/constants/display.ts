export function formatMonths(days: number | null | undefined): string {
  if (!days) {
    return '—';
  }

  return `${Math.round(days / 30)}个月`;
}

export function formatExpiryDate(value: string | null | undefined): string {
  if (!value) {
    return '—';
  }
  return value;
}

export function formatTenorLabel(
  days: number | null | undefined,
  expiryDate: string | null | undefined,
): string {
  if (expiryDate) {
    const monthsLabel = formatMonths(days);
    return monthsLabel === '—' ? formatExpiryDate(expiryDate) : `${monthsLabel} · ${formatExpiryDate(expiryDate)}`;
  }

  return formatMonths(days);
}

export function formatStrikeWithMoneyness(
  strike: number | null | undefined,
  moneynessPct: number | null | undefined,
): string {
  const numericStrike = toFiniteNumber(strike);
  if (numericStrike === null || numericStrike <= 0) {
    return '—';
  }

  const strikeLabel = `$${stripTrailingZeros(numericStrike)}`;
  const numericMoneyness = toFiniteNumber(moneynessPct);
  if (numericMoneyness === null) {
    return strikeLabel;
  }

  return `${strikeLabel}（约${Math.round(numericMoneyness)}%）`;
}

export function formatStrikeParts(
  strike: number | null | undefined,
  moneynessPct: number | null | undefined,
): { primary: string; secondary: string | null } {
  const numericStrike = toFiniteNumber(strike);
  if (numericStrike === null || numericStrike <= 0) {
    return { primary: '—', secondary: null };
  }

  const strikeLabel = `$${stripTrailingZeros(numericStrike)}`;
  const numericMoneyness = toFiniteNumber(moneynessPct);
  return {
    primary:
      numericMoneyness === null ? strikeLabel : `${Math.round(numericMoneyness)}%`,
    secondary:
      numericMoneyness === null ? null : `${strikeLabel} 参考价`,
  };
}

export function formatPrice(value: number | null | undefined): string {
  const numericValue = toFiniteNumber(value);
  if (numericValue === null) {
    return '—';
  }

  return `$${stripTrailingZeros(numericValue)}`;
}

export function formatPercent(value: number | null | undefined): string {
  const numericValue = toFiniteNumber(value);
  if (numericValue === null) {
    return '—';
  }

  return `${stripTrailingZeros(numericValue)}%`;
}

export function translateSignalName(name: string): string {
  const mapping: Record<string, string> = {
    'Trend structure': '趋势结构',
    '52-week position': '52周位置',
    'IV rank': 'IV水平',
    'Put skew': '波动偏斜',
    'Earnings risk': '财报风险',
  };

  return mapping[name] ?? name;
}

export function translateSignalValue(value: string): string {
  const mapping: Record<string, string> = {
    Unavailable: '数据不可用',
    'Above 200-day moving average': '高于200日均线上方',
    'Below 200-day moving average': '价格在200日均线下方',
    'Mixed trend structure': '趋势结构分化',
    'Below long-term trend': '低于长期趋势',
    'Seller-friendly skew': '偏向卖方',
    'Derived from stored snapshot': '基于历史快照',
    'Manually maintained outside market-data feed': '手动维护',
  };

  return mapping[value] ?? value;
}

export function translateAvoidFlag(flagType: string): string {
  const mapping: Record<string, string> = {
    BROKEN_TREND: '趋势破坏',
    BEARISH_STRUCTURE: '技术面偏弱',
    LOW_LIQUIDITY: '流动性不足',
    EARNINGS_PROXIMITY: '财报临近',
    POST_EARNINGS_SHOCK: '财报后重估',
    NO_APPROVED_STRIKE: '无合适执行价',
  };

  return mapping[flagType] ?? flagType;
}

export function translateVerdictHeadline(headline: string): string {
  const mapping: Record<string, string> = {
    Recommended: '推荐询价',
    'Proceed with caution': '谨慎考虑',
    'Not recommended': '暂不推荐',
  };

  return mapping[headline] ?? headline;
}

export function translateVerdictSub(sub: string): string {
  const mapping: Record<string, string> = {
    'Balanced trend, volatility, and strike setup for PB discussion': '综合评分良好，适合询价',
    'Balanced trend, volatility, and strike setup for PB discussion.': '综合评分良好，适合询价',
    'Setup is usable, but requires tighter strike discipline and client suitability screening':
      '可考虑，注意执行价纪律',
    'Setup is usable, but requires tighter strike discipline and client suitability screening.':
      '可考虑，注意执行价纪律',
    'Current setup does not meet PB FCN risk standards': '当前不符合FCN风险标准',
    'Current setup does not meet PB FCN risk standards.': '当前不符合FCN风险标准',
    'Blocked by current risk controls.': '当前受风险控制限制',
  };

  return mapping[sub] ?? sub;
}

export function truncateWhyNow(text: string | null | undefined, limit = 50): string {
  if (!text) {
    return '—';
  }

  if (text.length <= limit) {
    return text;
  }

  return `${text.slice(0, limit)}...`;
}

export function formatRunDateLabel(runDate: string | null | undefined): string {
  if (!runDate) {
    return '数据截至最近一次美股收盘';
  }

  const match = runDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return `数据截至 ${runDate} 美股收盘`;
  }

  return `数据截至 ${match[2]}-${match[3]} 美股收盘`;
}

function stripTrailingZeros(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2).replace(/\.?0+$/, '');
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
