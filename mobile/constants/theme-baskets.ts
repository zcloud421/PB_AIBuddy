export interface ThemeBasketMember {
  symbol: string;
  role: string;
}

export interface ThemeBasket {
  slug: string;
  title: string;
  description: string;
  tickers: string[];
  members: ThemeBasketMember[];
}

export const THEME_BASKETS: ThemeBasket[] = [
  {
    slug: 'ai-infrastructure',
    title: 'AI基建',
    description: '围绕算力、制造、光互连与数据中心基础设施的股票篮子，契合 HALO 交易主题',
    tickers: ['NVDA', 'TSM', 'MU'],
    members: [
      { symbol: 'NVDA', role: '算力核心' },
      { symbol: 'TSM', role: '先进制造 / 先进封装' },
      { symbol: 'MU', role: 'HBM / AI存储' },
      { symbol: 'AVGO', role: 'ASIC / 网络互连' },
      { symbol: 'LITE', role: '光通信 / 光互连' },
      { symbol: 'VRT', role: '电力与热管理' },
    ],
  },
  {
    slug: 'mag7-pullback',
    title: 'Mag 7回撤',
    description: '大市值科技自高点回调，提供 FCN 逢低布局机会，适合看好长期但能承受短期波动的客户',
    tickers: ['MSFT', 'META', 'AMZN'],
    members: [
      { symbol: 'MSFT', role: '云与企业AI' },
      { symbol: 'META', role: 'AI广告平台' },
      { symbol: 'AMZN', role: '云与消费平台' },
      { symbol: 'GOOG', role: '搜索与云AI' },
    ],
  },
  {
    slug: 'geopolitical-hedge',
    title: '地缘冲突交易',
    description: '中东局势推升油金波动，围绕能源与贵金属的事件驱动型 FCN 篮子',
    tickers: ['GDX', 'XOM', 'USO'],
    members: [
      { symbol: 'GDX', role: '黄金矿业敞口' },
      { symbol: 'XOM', role: '油气龙头' },
      { symbol: 'USO', role: '原油价格敞口' },
    ],
  },
  {
    slug: 'crypto-linked',
    title: '加密货币相关',
    description: '高波动驱动高票息，但与加密市场高度相关，需更谨慎评估客户适合性及公司 House View',
    tickers: ['COIN', 'MSTR', 'CRCL'],
    members: [
      { symbol: 'COIN', role: '加密货币交易平台' },
      { symbol: 'MSTR', role: '比特币资产敞口' },
      { symbol: 'CRCL', role: '稳定币基础设施' },
      { symbol: 'HOOD', role: '零售交易平台 / 加密敞口' },
    ],
  },
];

export function getThemeBasket(slug: string) {
  return THEME_BASKETS.find((basket) => basket.slug === slug);
}
