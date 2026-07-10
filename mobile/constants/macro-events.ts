export interface MacroEvent {
  id: string;
  start: string;
  end: string;
  reason_zh: string;
  applies_to: 'all' | 'us_tech' | 'china_tech' | 'symbols_only';
  symbols?: string[];
}

export const MACRO_EVENTS: MacroEvent[] = [
  {
    id: 'covid-2020',
    start: '2020-02-20',
    end: '2020-03-23',
    reason_zh: 'COVID-19疫情冲击，全球市场恐慌性抛售',
    applies_to: 'all',
  },
  {
    id: 'saas-growth-crash-2021',
    start: '2021-02-01',
    end: '2022-12-31',
    reason_zh: '疫情受益股估值回归：居家经济泡沫消退，高增长SaaS/消费科技股大幅重估',
    applies_to: 'symbols_only',
    symbols: ['ZM', 'DOCU', 'PTON', 'ROKU', 'SNAP', 'RBLX'],
  },
  {
    id: 'china-reg-2021',
    start: '2021-07-01',
    end: '2022-03-15',
    reason_zh: '中国互联网监管整顿：反垄断调查、数据安全立法、教培行业政策清零',
    applies_to: 'china_tech',
    symbols: [
      'BABA', 'JD', 'PDD', 'BIDU', 'NTES', 'TME', 'BILI', 'IQ', 'VIPS',
      '9988', '9618', '9999', '700', '1810', '3690', '9626',
    ],
  },
  {
    id: 'crypto-collapse-2022',
    start: '2022-05-01',
    end: '2023-01-31',
    reason_zh: '加密货币市场崩溃：LUNA归零、FTX暴雷，加密相关金融股遭重创',
    applies_to: 'symbols_only',
    symbols: ['COIN', 'HOOD'],
  },
  {
    id: 'semiconductor-downcycle-2022',
    start: '2022-01-01',
    end: '2023-12-31',
    reason_zh: '半导体存储行业周期性下行：DRAM/NAND供过于求，存储芯片价格大幅下滑',
    applies_to: 'symbols_only',
    symbols: ['MU', 'WDC', 'STX', 'INTC'],
  },
  {
    id: 'broad-semiconductor-downcycle-2022',
    start: '2021-11-01',
    end: '2023-01-31',
    reason_zh: '半导体景气回落：PC需求转弱与渠道库存修正压制行业估值，叠加加息环境放大回撤',
    applies_to: 'symbols_only',
    symbols: ['AMD', 'INTC'],
  },
  {
    id: 'fed-hike-2022',
    start: '2022-01-01',
    end: '2022-10-14',
    reason_zh: '美联储激进加息周期（全年累计+425bp），高估值成长股大幅重估',
    applies_to: 'us_tech',
  },
  {
    id: 'ev-slowdown-2023',
    start: '2023-01-01',
    end: '2024-06-30',
    reason_zh: '电动车需求增速放缓，行业价格战加剧，整车利润率持续承压',
    applies_to: 'symbols_only',
    symbols: ['TSLA', 'RIVN', 'LCID', 'NIO', 'XPEV', 'LI'],
  },
  {
    id: 'svb-crisis-2023',
    start: '2023-03-08',
    end: '2023-03-31',
    reason_zh: '硅谷银行挤兑倒闭，区域银行系统性风险担忧短暂蔓延至科技板块',
    applies_to: 'all',
  },
  {
    id: 'smci-accounting-2024',
    start: '2024-08-01',
    end: '2024-11-30',
    reason_zh: 'Super Micro审计师辞职及财务透明度问题引发合规质疑，股价大幅下挫',
    applies_to: 'symbols_only',
    symbols: ['SMCI'],
  },
  {
    id: 'carry-unwind-2024',
    start: '2024-07-31',
    end: '2024-08-08',
    reason_zh: '日元套利交易集中平仓叠加美国衰退担忧，全球风险资产短暂闪崩',
    applies_to: 'all',
  },
  {
    id: 'china-macro-slowdown-2022',
    start: '2022-03-15',
    end: '2022-11-30',
    reason_zh: '中国经济下行：上海封控重创消费，中概退市风险加剧，科技板块监管压力延续',
    applies_to: 'china_tech',
  },
  {
    id: 'china-recovery-miss-2023',
    start: '2023-01-01',
    end: '2023-12-31',
    reason_zh: '中国经济复苏不及预期：消费与出口双弱，房地产危机冲击市场信心，中概估值持续承压',
    applies_to: 'china_tech',
  },
  {
    id: 'china-stimulus-reversal-2024',
    start: '2024-10-01',
    end: '2024-12-31',
    reason_zh: '中国刺激政策落地效果不及预期，叠加特朗普关税预期升温，中国科技股刺激行情反转',
    applies_to: 'china_tech',
  },
  {
    id: 'us-tech-bear-2022-late',
    start: '2022-10-14',
    end: '2023-02-28',
    reason_zh: '美联储持续紧缩预期叠加经济衰退担忧，美国科技股延续估值压缩至周期低点',
    applies_to: 'us_tech',
  },
  {
    id: 'meta-platform-reset-2022',
    start: '2021-09-01',
    end: '2022-12-31',
    reason_zh: 'Meta平台战略转型承压：苹果ATT隐私新政冲击广告收入、元宇宙高投入拖累利润、用户增长见顶',
    applies_to: 'symbols_only',
    symbols: ['META'],
  },
  {
    id: 'nflx-subscriber-loss-2022',
    start: '2022-01-01',
    end: '2022-09-30',
    reason_zh: 'Netflix首现用户净流失，流媒体饱和度上升叠加竞争加剧，订阅制商业模式受市场质疑',
    applies_to: 'symbols_only',
    symbols: ['NFLX'],
  },
  {
    id: 'meta-ai-capex-reset-2025',
    start: '2025-01-20',
    end: '2025-03-31',
    reason_zh: 'Meta资本开支与AI投入回报再定价：广告主线稳健，但大规模AI投入与回报节奏引发估值回调',
    applies_to: 'symbols_only',
    symbols: ['META'],
  },
  {
    id: 'ai-deepseek-shock-2025',
    start: '2025-01-20',
    end: '2025-03-31',
    reason_zh: 'DeepSeek低成本模型冲击市场预期，AI算力需求及科技巨头资本支出回报受到重估',
    applies_to: 'symbols_only',
    symbols: ['NVDA', 'AMD', 'AVGO', 'MRVL', 'ANET', 'MSFT', 'GOOGL', 'GOOG', 'AMZN'],
  },
  {
    id: 'unh-cost-guidance-reset-2025',
    start: '2025-04-01',
    end: '2026-12-31',
    reason_zh: 'UnitedHealth经营承压：Medicare Advantage医疗成本超预期、全年指引撤回及监管调查拖累估值',
    applies_to: 'symbols_only',
    symbols: ['UNH'],
  },
  {
    id: 'tariff-shock-2025',
    start: '2025-04-01',
    end: '2025-12-31',
    reason_zh: '美国对等关税冲击，全球股市急剧下跌',
    applies_to: 'us_tech',
  },
  {
    id: 'china-macro-2025-early',
    start: '2025-01-01',
    end: '2025-03-31',
    reason_zh: '中美贸易摩擦预期持续升温，外需收缩担忧加剧，中国科技股在关税落地前已提前承压',
    applies_to: 'china_tech',
  },
  {
    id: 'china-macro-2025',
    start: '2025-04-01',
    end: '2025-12-31',
    reason_zh: '中美关税博弈升级，中国科技股受外部需求收缩、汇率及地缘压力拖累',
    applies_to: 'china_tech',
  },
  {
    id: 'mag7-pullback-2026',
    start: '2026-01-01',
    end: '2026-02-27',
    reason_zh: '科技巨头高位回撤：美股创新高后，Mag7估值与AI投入回报预期阶段性回落',
    applies_to: 'symbols_only',
    symbols: ['META', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'AAPL', 'NVDA', 'TSLA'],
  },
  {
    id: 'us-iran-war-2026',
    start: '2026-02-28',
    end: '2026-12-31',
    reason_zh: '美伊战争爆发，中东局势急剧恶化，全球能源与风险资产大幅波动',
    applies_to: 'all',
  },
];

const CHINA_TECH_SYMBOLS = new Set([
  'BABA', 'JD', 'PDD', 'BIDU', 'NTES', 'TME', 'BILI', 'IQ', 'VIPS',
  '9988', '9618', '9999', '700', '1810', '3690', '9626', 'TCEHY', 'BEKE',
]);

const US_TECH_SYMBOLS = new Set([
  'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'META', 'AMZN', 'NVDA', 'AMD', 'TSLA',
  'NFLX', 'SNAP', 'UBER', 'COIN', 'ROKU', 'SHOP', 'SQ', 'PYPL', 'PLTR',
  'RBLX', 'HOOD', 'RIVN', 'LCID', 'PTON', 'ZM', 'DOCU', 'CRWD', 'NET',
  'AVGO', 'MRVL', 'ANET', 'VRT', 'MU', 'SMCI', 'DELL', 'LITE', 'COHR',
  'CIEN', 'ORCL', 'CRM', 'SNOW', 'WDAY', 'ADBE', 'NOW', 'EQIX', 'DLR',
  'ACN', 'IBM', 'TSM',
]);

export function matchMacroEvent(
  peakDate: string,
  troughDate: string,
  symbol: string,
): MacroEvent | null {
  const upperSymbol = symbol.toUpperCase();
  const peak = new Date(peakDate);
  const trough = new Date(troughDate);

  const overlapping = MACRO_EVENTS.filter((event) => {
    const eventStart = new Date(event.start);
    const eventEnd = new Date(event.end);
    return !(peak > eventEnd || trough < eventStart);
  });

  for (const event of overlapping) {
    if (event.symbols?.some((s) => upperSymbol === s)) {
      return event;
    }
  }

  for (const event of overlapping) {
    if (event.applies_to === 'symbols_only') continue;
    if (event.applies_to === 'china_tech' && CHINA_TECH_SYMBOLS.has(upperSymbol)) return event;
    if (event.applies_to === 'us_tech' && US_TECH_SYMBOLS.has(upperSymbol)) return event;
  }

  for (const event of overlapping) {
    if (event.applies_to === 'all') return event;
  }

  return null;
}
