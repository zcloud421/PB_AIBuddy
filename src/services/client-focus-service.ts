import type {
    ClientFocusDailyVerdict,
    ClientFocusDriverItem,
    ClientFocusDetailResponse,
    ClientFocusHibor,
    ClientFocusListItem,
    ClientFocusMarketStateResponse,
    ClientFocusMarketChart,
    ClientFocusMarketSnapshot,
    ClientFocusPolymarketMarket,
    ClientFocusPolymarketResponse,
    ClientFocusPriceHistoryPoint,
    ClientFocusPriceSnapshot,
    ClientFocusQuestion,
    ClientFocusSectorRotation,
    ClientFocusTransmissionItem,
    ClientFocusUpdate,
    NewsItem,
    WhatChangedGroup
} from '../types/api';
import { fetchNewsItemsByQuery, fetchNewsItemsFromNewsData } from '../data/news-fetcher';
import { MassiveDataFetcher } from '../data/massive-fetcher';
import { MassiveClient } from '../data/massive-client';
import axios from 'axios';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';
const DEFAULT_DISCLAIMER = '本页内容仅供市场讨论准备，客户沟通请结合所属机构的策略观点与合规要求。';
const FOCUS_CACHE_TTL_MS = 60 * 60 * 1000;
const FOCUS_LONG_CACHE_TTL_MS = 12 * 60 * 60 * 1000; // 12 hours
const FOCUS_LIVE_MARKET_CACHE_TTL_MS = 5 * 60 * 1000;
const FOCUS_CHAIN_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const POLYMARKET_CACHE_TTL_MS = 5 * 60 * 1000;
const KNOWN_HK_INDEX_SECIDS: Record<string, string> = {
    HSI: '100.HSI',
    HSTECH: '100.HSTECH',
};
const MARKET_STATE_CACHE_KEY = 'focus-market-state';
const POLYMARKET_HISTORY_WINDOW_DAYS = 30;
const POLYMARKET_HISTORY_CHUNK_DAYS = 14;
const WHAT_CHANGED_WINDOW_HOURS = 72;
const PRIVATE_CREDIT_WHAT_CHANGED_WINDOW_DAYS = 7;
const FOCUS_QUESTION_CATEGORIES: Record<string, string[]> = {
    'middle-east-tensions': ['原油', '股票/FCN', '黄金', '债券', '汇率'],
    'gold-repricing': ['黄金', '美元', '股票/FCN'],
    'hk-market-sentiment': ['港股', '科技股', '南向资金'],
    'usd-strength': ['美元', '债券', '新兴市场'],
    'private-credit-stress': ['债券/信贷', '股票/FCN', '房地产'],
};

function getMarketLocalParts(timeZone: string) {
    const formatter = new Intl.DateTimeFormat('en-US', {
        timeZone,
        weekday: 'short',
        hour: 'numeric',
        minute: 'numeric',
        hour12: false,
    });
    const parts = formatter.formatToParts(new Date());
    const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    const weekday = lookup.weekday ?? '';
    const hour = Number(lookup.hour ?? '0');
    const minute = Number(lookup.minute ?? '0');

    return {
        weekday,
        minutes: hour * 60 + minute,
    };
}

interface FocusTopicConfig {
    slug: string;
    title: string;
    accent: string;
    query: string;
    newsQueries?: string[];
    fallbackStatus?: string;
    clientQuestions: ClientFocusQuestion[];
    previewQuestions?: string[];
    relatedAssets: string[];
    fallbackSummary: string;
    fallbackLatestUpdates?: ClientFocusUpdate[];
}

interface FocusTopicModelOutput {
    status?: string;
    summary?: string;
    latest_updates?: ClientFocusUpdate[];
}

interface FocusWeeklyProgressModelOutput {
    topic?: string;
    section_title?: string;
    updated_at?: string;
    items?: Array<{
        date?: string;
        tag?: string;
        summary?: string;
        source?: string;
    }>;
    editor_note?: string;
}

interface FocusTransmissionModelOutput {
    updated_at?: string;
    topic?: string;
    first_order?: {
        title?: string;
        pricing_status?: string;
        description?: string;
        latest_evidence?: string | null;
    };
    second_order?: {
        title?: string;
        pricing_status?: string;
        description?: string;
        latest_evidence?: string | null;
    };
}

const ALLOWED_FOCUS_STATUS = ['关注升温', '持续发酵', '压力上升'] as const;
const ALLOWED_UPDATE_IMPACTS = ['风险抬升', '信用事件', '政策变化', '持续发酵'] as const;
const PREFERRED_WEEKLY_SOURCES = ['Bloomberg', 'Reuters', 'FT', 'CNBC', 'WSJ', 'Financial Times'] as const;
const BLOCKED_WEEKLY_SOURCE_PATTERNS = [
    'financialcontent',
    'benzinga',
    'seeking alpha',
    'yahoo',
    'blog',
    'substack',
    'medium'
] as const;

const FOCUS_TOPICS: FocusTopicConfig[] = [
    {
        slug: 'middle-east-tensions',
        title: '中东冲突',
        accent: '#C9A45C',
        query: 'Iran Israel Pentagon ground operations retaliation infrastructure JD Vance Trump White House strike military latest war',
        newsQueries: [
            'Iran Israel war military strike',
            'Iran Hormuz oil shipping',
            'Iran ceasefire talks diplomacy',
            'Trump Iran deal war'
        ],
        fallbackStatus: '持续发酵',
        clientQuestions: [
            {
                question: '高油价会造成什么影响？',
                answer: '高油价会先推升通胀预期并压缩降息空间，随后传导到股市估值、企业成本与风险偏好。短期最直接受影响的方向是原油、能源股和利率敏感资产。市场目前预期全年最多降息一次，且存在加息的尾部风险。'
            },
            {
                question: '黄金这次没有发挥避险作用，是不是市场逻辑变了？',
                answer: '黄金虽然受益于避险情绪，但实际利率和美元同步走高时，持有黄金的机会成本会上升，金价不一定和地缘风险同方向上行。当前油价冲击推高通胀预期，反而强化了央行鹰派立场，这是金价承压的核心原因。'
            },
            {
                question: '今年还能降息吗，还是反而会加息？',
                answer: 'Fed 3 月点阵图维持全年仅一次降息，但市场定价更悲观，债券市场已将年内完全不降息的概率计入约 48%，且出现小概率加息预期。核心变量是油价走势：若局势缓和、油价回落，年底前一次降息仍是基准情景；若油价维持高位，加息讨论可能重新浮出水面。'
            }
        ],
        previewQuestions: [
            '高油价会造成什么影响？',
            '今年还能降息吗，还是反而会加息？'
        ],
        relatedAssets: ['股票', '债券', '贵金属', '原油', '美元', '港股'],
        fallbackSummary: '地缘冲突推升油价与避险需求，客户近期高频追问黄金、降息路径与风险资产波动。',
        fallbackLatestUpdates: [
            {
                time: '近期',
                title: 'Bushehr 核电站再传遇袭，战局升级。',
                impact: '风险抬升'
            },
            {
                time: '近期',
                title: '伊朗官员要求明确追责，并保证战争损失赔偿。',
                impact: '政策变化'
            },
            {
                time: '近期',
                title: '巴方称美伊潜在会谈时间地点未定，伊朗尚未同意谈判。',
                impact: '政策变化'
            }
        ]
    },
    {
        slug: 'private-credit-stress',
        title: '私募信贷风险',
        accent: '#C9A45C',
        query: 'private credit fund redemptions withdrawal limits investors loss record Apollo Ares Blackstone BDC JPMorgan default',
        newsQueries: [
            'private credit insurance regulators treasury',
            'Fed Powell private credit signs of trouble',
            'Apollo private credit fund limits investor withdrawals',
            'Ares private credit fund withdrawals redemptions surge',
            'private credit investors wait to pull out 5 billion',
            'private credit fund redemptions withdrawal limits Apollo Ares'
        ],
        clientQuestions: [
            {
                question: '这会不会演变成新的信用风险事件？',
                answer: '赎回压力和资产估值下修是流动性问题，不等于信用崩溃。Morgan Stanley 表示，即使违约率升至 8% 也是显著但非系统性的冲击，私募信贷基金杠杆率远低于 2008 年投行体系。真正风险在于个别管理人底层资产恶化，以及赎回潮扩大后对融资环境的二阶收紧。'
            },
            {
                question: '这次和2008年金融危机有什么本质区别？',
                answer: '2008 年的核心是银行资产负债表上的系统性杠杆崩塌，并通过衍生品和存款体系传染至整个金融系统。私募信贷不在银行存款体系内，赎回门槛和季度上限本来就是防止强制抛售的结构性设计。J.P. Morgan 的判断是，近期更像把局部压力与整体基本面恶化混淆，接近 2000 年科技泡沫式的局部估值重估，而非系统性流动性危机。'
            },
            {
                question: '本次问题主要出在哪些板块？',
                answer: '风险最集中在软件和 SaaS，私募信贷市场约 20% 到 25% 的敞口在软件公司，AI 对商业模式的冲击直接影响还款能力，软件敞口较高的 BDC 如 Blue Owl 股价已较 NAV 折价 20% 以上。其次是 2020 到 2024 年低利率时代完成的高杠杆收购标的，以及医疗保健并购 roll-up。相对安全的是资产支持类私募信贷和现金流稳健的中型企业贷款。'
            }
        ],
        relatedAssets: ['信用市场', '金融股', '私募信贷基金', 'BDC', '另类资管股'],
        fallbackSummary: '私募信贷赎回限制与风险事件增多，持有相关产品的客户近期更关注流动性与估值压力。'
    },
    {
        slug: 'hk-market-sentiment',
        title: '港股资金与情绪',
        accent: '#5E88D9',
        query: 'Hong Kong stocks Hang Seng Hang Seng Tech southbound inflow China equities market sentiment',
        newsQueries: [
            'Hong Kong stocks Hang Seng Tech selloff rebound',
            'southbound inflow Hong Kong stocks',
            'Hang Seng index China equities sentiment',
            'Hong Kong market China tech valuation'
        ],
        clientQuestions: [
            {
                question: '港股今年为什么不如去年？',
                answer: '去年港股上涨更多是估值修复和风险偏好回升推动，今年市场开始从“先看故事”转向“看盈利能否兑现”，所以节奏自然没有去年那么顺。再加上 3 月底中东冲突推高油价、扰动全球风险偏好，外部资金对高波动资产更谨慎，港股短期就更容易进入震荡。整体来看，这更像反弹后的正常消化和结构分化，而不是行情已经彻底结束。'
            },
            {
                question: '港股现在是资金行情吗？',
                answer: '短期看，港股仍然很受资金和情绪影响，特别是科技板块，对风险偏好变化最敏感；这一点从 3 月香港科技 ETF 仍有创纪录资金流入就能看出来。中期看，市场最终还是要回到盈利、政策和经济数据，例如中国 3 月官方制造业 PMI 回到 50 以上，说明基本面并不是没有改善。换句话说，当前是“资金先定价，基本面后验证”，所以短线波动会比较快，但中线能否延续，还是要看盈利兑现。'
            },
            {
                question: '恒生科技回调意味着什么？',
                answer: '我觉得不能这样理解。近期回调更像是上涨后的分化，而不是科技主线本身被证伪；一方面，中东冲突确实压制全球科技股风险偏好，另一方面，市场对中国科技股的要求也明显提高了，不再接受“只讲主题、不看利润”。但从一级市场和产业趋势看，香港今年 IPO 募资明显回升，AI 和新经济公司仍在积极推进赴港融资，说明科技热度还在，只是市场开始更重视谁能把主题真正转化成收入和盈利。'
            }
        ],
        relatedAssets: ['恒生指数', '恒生科技指数', '港股', '南下资金', '中国科技股'],
        fallbackSummary: '2025年港股强势后2026年转弱，客户近期更关注南下资金、科技股波动与情绪变化。'
    },
    {
        slug: 'gold-repricing',
        title: '黄金逻辑重估',
        accent: '#C9A45C',
        query: 'gold price real yields dollar central bank buying gold miners latest',
        clientQuestions: [
            {
                question: '2025年以来黄金牛市驱动力是什么？',
                answer: '2025年以来黄金上涨主要由四个因素共同推动：一是市场预期美联储进入降息周期，实际利率回落降低了持有黄金的机会成本；二是美元阶段性走弱，提高了黄金对非美元投资者的吸引力；三是各国央行持续增持黄金，形成结构性需求；四是地缘政治风险上升强化避险配置需求。整体来看，这轮黄金牛市是流动性预期、弱美元与避险情绪共同作用的结果，而非单一因素驱动。'
            },
            {
                question: '中东冲突那么严重，黄金为什么反而下跌？',
                answer: '这是当前市场最反直觉的现象，核心原因是油价冲击悖论。中东冲突推高油价，反而迫使央行维持鹰派立场，实际利率上升和美元走强的力量压过了避险需求。同时，过去一年黄金累积了大量拥挤多头仓位，冲突爆发后机构借机止盈减仓，进一步加剧了下跌。'
            },
            {
                question: '为什么金矿股跑输金价？',
                answer: '金矿股叠加了经营杠杆，矿企开采成本相对固定，金价上涨时利润会放大，金价下跌时亏损也会被放大。当前高通胀环境下，能源和人力成本居高不下，进一步压缩了金矿企业利润空间，所以金矿股不会简单跟随实物黄金同步。'
            }
        ],
        relatedAssets: ['实物黄金', '黄金ETF', '金矿股', '美元'],
        fallbackSummary: '弱美元与央行购金曾支撑黄金牛市，近期逻辑转向后客户更关心金价还能否继续上行。'
    },
    {
        slug: 'usd-strength',
        title: '美元走势重估',
        accent: '#C9A45C',
        query: 'US dollar strength USDCNH Hong Kong stocks China assets rates latest',
        clientQuestions: [
            {
                question: '为什么美元最近出现了反弹？',
                answer: '美元近期阶段性反弹，主要由两个因素驱动：一是地缘冲突触发避险需求，美元成为主要避险目的地；二是油价上涨推高通胀预期，联储降息时间被推迟，美元利率优势得以延续。离岸人民币相比其他亚洲货币更具政策韧性，但若避险情绪缓和，资金存在快速逆转风险。'
            },
            {
                question: '中国央行对人民币汇率的最新表态是什么？',
                answer: '央行明确表示无意通过汇率贬值获取贸易竞争优势，立场是允许双向浮动，但会在必要时使用宏观审慎工具干预过度波动。市场对央行政策信号的解读是：维持人民币相对稳定，同时避免过快升值。'
            },
            {
                question: '强美元对港股意味着什么？',
                answer: '强美元通常压制非美资产风险偏好，对港股有双重影响：一是外资流出压力加大，二是联系汇率机制下 HKMA 被迫收紧本地流动性，进一步压制港股估值。历史上美元强势周期往往对应港股和中国资产承压，但若强美元更多来自短期避险而非利差驱动，持续性会相对有限。'
            }
        ],
        relatedAssets: ['USDCNH', '港股', '中国资产', '美元资产'],
        fallbackSummary: '2025年弱美元后近期因避险与利差反弹，客户重新关注美元方向及对人民币资产的压力。'
    }
];

const focusCache = new Map<string, { expiresAt: number; value: ClientFocusDetailResponse }>();
const focusChainCache = new Map<string, { expiresAt: number; value: ClientFocusTransmissionItem[] }>();
const focusMarketChartCache = new Map<string, { expiresAt: number; value: ClientFocusMarketChart | null }>();
const polymarketCache = new Map<string, { expiresAt: number; value: ClientFocusPolymarketResponse }>();
const focusMarketStateCache = new Map<string, { expiresAt: number; value: ClientFocusMarketStateResponse }>();

interface EastMoneySouthboundRow {
    TRADE_DATE?: string;
    NET_DEAL_AMT?: string | number | null;
}

const MIDDLE_EAST_POLYMARKET_MARKETS = [
    {
        pageUrl: 'https://polymarket.com/event/iran-x-israelus-conflict-ends-by',
        label: '伊朗与以色列/美国冲突何时结束',
        outcomes: [
            { outcomeLabel: 'April 30', displayLabel: '4月底' },
            { outcomeLabel: 'May 15', displayLabel: '5月中' },
            { outcomeLabel: 'June 30', displayLabel: '6月底' }
        ]
    },
    {
        pageUrl: 'https://polymarket.com/event/trump-announces-end-of-military-operations-against-iran-by',
        label: '特朗普何时宣布结束军事行动',
        outcomes: [
            { outcomeLabel: 'April 30', displayLabel: '4月底' },
            { outcomeLabel: 'May 31', displayLabel: '5月底' },
            { outcomeLabel: 'June 30', displayLabel: '6月底' }
        ]
    },
    {
        pageUrl: 'https://polymarket.com/event/what-price-will-wti-hit-in-april-2026',
        label: '原油4月触及什么价位',
        outcomes: [
            { outcomeLabel: '↑ $100', displayLabel: '$100' },
            { outcomeLabel: '↑ $110', displayLabel: '$110' },
            { outcomeLabel: '↑ $120', displayLabel: '$120' }
        ]
    }
] as const;

function getFocusTopic(slug: string): FocusTopicConfig | null {
    return FOCUS_TOPICS.find((topic) => topic.slug === slug) ?? null;
}

function escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

interface PolymarketGammaMarket {
    id: string;
    conditionId?: string;
    question: string;
    groupItemTitle: string;
    outcomePrices: string[] | string;
    clobTokenIds: string[] | string;
}

function normalizePolymarketStringArray(value: string[] | string | undefined): string[] {
    if (Array.isArray(value)) {
        return value.map((item) => String(item));
    }

    if (typeof value !== 'string' || value.trim().length === 0) {
        return [];
    }

    try {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) {
            return parsed.map((item) => String(item));
        }
    } catch {
        return [];
    }

    return [];
}

function getPolymarketSlug(pageUrl: string): string {
    return pageUrl.replace(/\/+$/, '').split('/').pop() ?? pageUrl;
}

async function fetchPolymarketEventMarkets(pageUrl: string): Promise<PolymarketGammaMarket[]> {
    const slug = getPolymarketSlug(pageUrl);
    const response = await axios.get<PolymarketGammaMarket[]>(
        'https://gamma-api.polymarket.com/events',
        {
            params: { slug },
            headers: {
                'User-Agent': 'Josan/1.0',
                Accept: 'application/json'
            },
            timeout: 15000
        }
    );

    const event = Array.isArray(response.data) ? response.data[0] : null;
    const rawMarkets = event && 'markets' in event ? (event as { markets?: PolymarketGammaMarket[] }).markets : null;

    if (!Array.isArray(rawMarkets) || rawMarkets.length === 0) {
        throw new Error(`No Polymarket event markets found for slug ${slug}`);
    }

    return rawMarkets;
}

async function fetchPolymarketHistoryChunk(
    yesTokenId: string,
    startTs: number,
    endTs: number
): Promise<Array<{ t: number; p: number }>> {
    const response = await axios.get<{ history?: Array<{ t?: number; p?: number }> }>(
        'https://clob.polymarket.com/prices-history',
        {
            params: {
                market: yesTokenId,
                startTs,
                endTs,
                fidelity: 60
            },
            headers: {
                'User-Agent': 'Josan/1.0',
                Accept: 'application/json'
            },
            timeout: 15000
        }
    );

    return (response.data.history ?? [])
        .filter((point): point is { t: number; p: number } =>
            Number.isFinite(point.t) && Number.isFinite(point.p)
        )
        .map((point) => ({
            t: Number(point.t),
            p: Number(point.p)
        }));
}

async function fetchPolymarketHistory(yesTokenId: string): Promise<Array<{ t: number; p: number }>> {
    const endTs = Math.floor(Date.now() / 1000);
    const chunks: Array<Promise<Array<{ t: number; p: number }>>> = [];

    for (let offsetDays = 0; offsetDays < POLYMARKET_HISTORY_WINDOW_DAYS; offsetDays += POLYMARKET_HISTORY_CHUNK_DAYS) {
        const chunkEnd = endTs - offsetDays * 24 * 60 * 60;
        const chunkStart = Math.max(
            endTs - POLYMARKET_HISTORY_WINDOW_DAYS * 24 * 60 * 60,
            chunkEnd - POLYMARKET_HISTORY_CHUNK_DAYS * 24 * 60 * 60
        );
        chunks.push(fetchPolymarketHistoryChunk(yesTokenId, chunkStart, chunkEnd));
    }

    const settled = await Promise.allSettled(chunks);
    const deduped = new Map<number, number>();

    settled.forEach((result) => {
        if (result.status !== 'fulfilled') {
            return;
        }

        result.value.forEach((point) => {
            deduped.set(point.t, point.p);
        });
    });

    return Array.from(deduped.entries())
        .map(([t, p]) => ({ t, p }))
        .sort((left, right) => left.t - right.t);
}

async function fetchPolymarketOutcome(
    pageMarkets: PolymarketGammaMarket[],
    outcome: { outcomeLabel: string; displayLabel: string }
) {
    const targetMarket = pageMarkets.find((pageMarket) => pageMarket.groupItemTitle === outcome.outcomeLabel);
    if (!targetMarket) {
        throw new Error(`Target market not found for ${outcome.outcomeLabel}`);
    }

    const outcomePrices = normalizePolymarketStringArray(targetMarket.outcomePrices);
    const clobTokenIds = normalizePolymarketStringArray(targetMarket.clobTokenIds);
    const yesProbability = Number(outcomePrices[0]);
    const yesTokenId = clobTokenIds[0];
    if (!Number.isFinite(yesProbability) || !yesTokenId) {
        throw new Error('Invalid Polymarket market metadata');
    }

    const history = await fetchPolymarketHistory(yesTokenId);
    const normalizedHistory = history.map((point) => ({
        t: point.t,
        p: Math.round(point.p * 1000) / 10
    }));
    const latestProbability = normalizedHistory[normalizedHistory.length - 1]?.p;

    return {
        condition_id: targetMarket.conditionId ?? targetMarket.id,
        display_label: outcome.displayLabel,
        probability: Number.isFinite(latestProbability) ? latestProbability : Math.round(yesProbability * 1000) / 10,
        history: normalizedHistory
    };
}

async function fetchPolymarketMarket(
    market: (typeof MIDDLE_EAST_POLYMARKET_MARKETS)[number]
): Promise<ClientFocusPolymarketMarket> {
    const pageMarkets = await fetchPolymarketEventMarkets(market.pageUrl);
    const outcomes = await Promise.all(
        market.outcomes.map((outcome) => fetchPolymarketOutcome(pageMarkets, outcome))
    );

    return {
        condition_id: getPolymarketSlug(market.pageUrl),
        label: market.label,
        outcomes
    };
}

async function fetchFocusNewsItems(topic: FocusTopicConfig): Promise<NewsItem[]> {
    const queries = topic.newsQueries?.length ? topic.newsQueries : [topic.query];
    const googleResultsPromise = Promise.all(
        queries.map((query) => fetchNewsItemsByQuery(query, { excludeEtfAndFunds: false }))
    );
    const newsDataResultsPromise = topic.slug === 'middle-east-tensions'
        ? Promise.all(
              [
                  {
                      query: 'Iran Israel military attack strike war',
                      categories: ['politics', 'world']
                  },
                  {
                      query: 'Hormuz Iran oil shipping tanker',
                      categories: ['politics', 'world', 'business']
                  },
                  {
                      query: 'Iran ceasefire negotiations deal diplomacy',
                      categories: ['politics', 'world']
                  }
              ].map(({ query, categories }) =>
                  fetchNewsItemsFromNewsData(query, {
                      timeframeHours: 48,
                      language: 'en',
                      categories
                  })
              )
          )
        : topic.slug === 'private-credit-stress'
            ? Promise.all(
                  [
                      {
                          query: 'private credit insurance regulators treasury',
                          categories: ['business']
                      },
                      {
                          query: 'Fed Powell private credit signs of trouble',
                          categories: ['business']
                      },
                      {
                          query: 'Apollo Ares private credit fund withdrawals',
                          categories: ['business']
                      }
                  ].map(({ query, categories }) =>
                      fetchNewsItemsFromNewsData(query, {
                          timeframeHours: 48,
                          language: 'en',
                          categories
                      })
                  )
              )
            : Promise.resolve([]);

    const [googleResults, newsDataResults] = await Promise.all([googleResultsPromise, newsDataResultsPromise]);
    const results = [...googleResults, ...(Array.isArray(newsDataResults) ? newsDataResults : [])];

    const seen = new Set<string>();
    const merged = results
        .flat()
        .filter((item) => (topic.slug === 'middle-east-tensions' ? isUsefulMiddleEastNewsTitle(item.title) : true))
        .filter((item) => {
            const normalizedTitle = item.title.trim().toLowerCase().replace(/\s+/g, ' ');
            const key = `${normalizedTitle}|${item.url}`;
            if (!normalizedTitle || seen.has(key)) {
                return false;
            }

            seen.add(key);
            return true;
        })
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        });

    return merged;
}

function isUsefulMiddleEastNewsTitle(title: string): boolean {
    const normalized = title.toLowerCase().replace(/\s+/g, ' ').trim();
    if (!normalized) {
        return false;
    }

    const blockedPatterns = [
        'iran conflict maritime update',
        'maritime update:',
        'live updates',
        'where things stand',
        'what we know',
        'what to know',
        'analysis:',
        'opinion:',
        'newsletter:',
        'podcast:'
    ];

    if (blockedPatterns.some((pattern) => normalized.includes(pattern))) {
        return false;
    }

    const usefulSignals = [
        'iran',
        'irgc',
        'idf',
        'israel',
        'killed',
        'casualties',
        'escalat',
        'nuclear',
        'threatened',
        'warned',
        'combat',
        'destroyed',
        'bombed',
        'air defense',
        'gulf',
        'forces',
        'war',
        'hormuz',
        'tanker',
        'shipping',
        'fee',
        'pipeline',
        'yanbu',
        'aramco',
        'strike',
        'attack',
        'drone',
        'missile',
        'air base',
        'base',
        'refinery',
        'petrochemical',
        'fifth fleet',
        'special forces',
        'ceasefire',
        'talks',
        'foreign ministers',
        'islamabad',
        'white house',
        'trump',
        'saudi',
        'pakistan',
        'egypt',
        'turkey'
    ];

    return usefulSignals.some((signal) => normalized.includes(signal));
}

function getPreviewQuestions(topic: FocusTopicConfig): Array<Pick<ClientFocusQuestion, 'question'>> {
    if (Array.isArray(topic.previewQuestions) && topic.previewQuestions.length > 0) {
        return topic.previewQuestions.map((question) => ({ question }));
    }

    return topic.clientQuestions.map((entry) => ({ question: entry.question }));
}

function normalizePreviewQuestion(question: string): string {
    let text = question.trim().replace(/[？?。！!]+$/u, '');

    text = text
        .replace(/^停火背景下[，,]\s*/u, '停火后')
        .replace(/^在[^，,]{0,12}背景下[，,]\s*/u, '')
        .replace(/传导路径/u, '')
        .replace(/会有什么变化$/u, '会怎么变化')
        .replace(/还有避险价值吗$/u, '是否仍有价值')
        .replace(/预期的会怎么变化/u, '预期会怎么变化')
        .replace(/路径会怎么变化/u, '会怎么变化');

    const impactMatch = text.match(/^(.+?)[，,]这对(.+?)意味着什么$/u);
    if (impactMatch) {
        const left = impactMatch[1].trim();
        const right = impactMatch[2]
            .replace(/^原油的/u, '原油')
            .replace(/供应中断风险定价/u, '原油定价')
            .replace(/风险定价/u, '定价')
            .trim();
        text = `${left}会如何影响${right}`;
    }

    return text.endsWith('？') ? text : `${text}？`;
}

function buildClientFocusPreviewQuestions(
    topic: FocusTopicConfig,
    summary: string,
    clientQuestions: ClientFocusQuestion[]
): Array<Pick<ClientFocusQuestion, 'question'>> {
    const pushUnique = (buffer: string[], question: string | null | undefined) => {
        if (!question) {
            return;
        }
        const normalized = question.trim();
        if (!normalized || buffer.includes(normalized)) {
            return;
        }
        buffer.push(normalized);
    };

    if (topic.slug === 'middle-east-tensions') {
        const previews: string[] = [];
        const summaryText = summary.toLowerCase();
        const normalizedQuestions = clientQuestions.map((item) => normalizePreviewQuestion(item.question));
        const questionByMatcher = (pattern: RegExp) => normalizedQuestions.find((question) => pattern.test(question));

        if (/停火|ceasefire|谈判|协议/.test(summaryText)) {
            pushUnique(previews, '美伊停火后哪些资产会先被重定价？');
        } else if (/霍尔木兹|航运|油轮|海峡/.test(summaryText)) {
            pushUnique(previews, '霍尔木兹航运变化会如何影响原油定价？');
        }

        pushUnique(previews, questionByMatcher(/降息|美联储|fed|收益率|利率/u));
        pushUnique(previews, questionByMatcher(/油价|原油|黄金|股票|美元|人民币|汇率/u));

        for (const question of normalizedQuestions) {
            pushUnique(previews, question);
            if (previews.length >= 2) {
                break;
            }
        }

        return previews.slice(0, 2).map((question) => ({ question }));
    }

    const previewSource = clientQuestions.length > 0
        ? clientQuestions.map((item) => normalizePreviewQuestion(item.question))
        : getPreviewQuestions(topic).map((item) => normalizePreviewQuestion(item.question));

    return previewSource.slice(0, 2).map((question) => ({ question }));
}

function formatRelativeTime(publishedAt: string | undefined): string {
    if (!publishedAt) {
        return '近期';
    }

    const published = new Date(publishedAt);
    const diffMs = Date.now() - published.getTime();
    if (Number.isNaN(diffMs) || diffMs < 0) {
        return '近期';
    }

    const hours = Math.floor(diffMs / (1000 * 60 * 60));
    if (hours < 1) {
        return '刚刚';
    }
    if (hours < 24) {
        return `${hours}h`;
    }

    const days = Math.floor(hours / 24);
    return `${days}d`;
}

function formatClockTime(publishedAt: string | undefined): string {
    if (!publishedAt) {
        return '近期';
    }

    const date = new Date(publishedAt);
    if (Number.isNaN(date.getTime())) {
        return '近期';
    }

    return date.toLocaleTimeString('zh-HK', {
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
        timeZone: 'Asia/Hong_Kong'
    });
}

function formatDateOnly(publishedAt: string | undefined): string | undefined {
    if (!publishedAt) {
        return undefined;
    }

    const date = new Date(publishedAt);
    if (Number.isNaN(date.getTime())) {
        return undefined;
    }

    const month = String(date.toLocaleDateString('en-US', { timeZone: 'Asia/Hong_Kong', month: 'numeric' }));
    const day = String(date.toLocaleDateString('en-US', { timeZone: 'Asia/Hong_Kong', day: 'numeric' }));
    return `${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

function sanitizeFocusSummary(summary: string | undefined, topic: FocusTopicConfig): string | null {
    if (!summary) {
        return null;
    }

    const trimmed = summary.trim();
    if (!trimmed) {
        return null;
    }

    const blockedPatterns = [
        /RM\s*\/\s*IC/i,
        /RM\b/i,
        /\bIC\b/i,
        /客户经理/,
        /口径无需调整/,
        /无需调整/,
        /本周(暂)?无符合筛选标准/,
        /暂无符合筛选标准/,
        /无符合筛选标准/,
        /暂无.*量化信息/,
        /暂无.*关键信息/,
        /暂无.*有效信息/
    ];

    if (blockedPatterns.some((pattern) => pattern.test(trimmed))) {
        return null;
    }

    if (topic.slug === 'gold-repricing' && /未出现明确变化/.test(trimmed)) {
        return null;
    }

    return trimmed;
}

function buildFocusSummaryGuidance(topic: FocusTopicConfig): string {
    if (topic.slug === 'middle-east-tensions') {
        return '近期中东冲突反复升级，油价、黄金、美元与降息预期一起波动，客户会集中追问地缘风险如何影响资产价格。';
    }

    if (topic.slug === 'private-credit-stress') {
        return '2026年私募信贷集中出现赎回限制、风险事件与监管讨论，不少私人银行客户持有相关产品，因此更关心流动性、估值与是否会继续扩散。';
    }

    if (topic.slug === 'hk-market-sentiment') {
        return '2025年港股曾显著走强，但2026年走势转弱、波动加大，客户近期更关心港股情绪是否转向、南下资金是否持续以及科技股还能否支撑指数。';
    }

    if (topic.slug === 'gold-repricing') {
        return '弱美元、降息预期与央行购金曾推动黄金牛市，但近期驱动开始变化，客户更关心黄金上涨逻辑是否转向，以及金价与金矿股的关系。';
    }

    if (topic.slug === 'usd-strength') {
        return '2025年弱美元是大话题，近期因避险需求和利差变化美元阶段性反弹，客户重新关心美元方向、人民币压力以及对中国资产和港股的影响。';
    }

    return topic.fallbackSummary;
}

function buildFocusSummaryFallback(topic: FocusTopicConfig): string {
    return topic.fallbackSummary;
}

function buildMiddleEastSummaryOverride(newsItems: NewsItem[]): string | null {
    const titles = newsItems
        .slice(0, 8)
        .map((item) => item.title ?? '')
        .join(' ')
        .toLowerCase();

    const ceasefireSignals = ['停火', 'ceasefire', 'truce', 'deal', '谈判', 'negotiation'];
    const escalationSignals = ['空袭', '导弹', 'strike', 'missile', 'escalation', '封锁', '关闭霍尔木兹'];

    const hasCeasefireSignal = ceasefireSignals.some((signal) => titles.includes(signal.toLowerCase()));
    const hasEscalationSignal = escalationSignals.some((signal) => titles.includes(signal.toLowerCase()));

    if (hasCeasefireSignal && !hasEscalationSignal) {
        return '美伊停火信号落地后，客户更关注油价、黄金与降息路径会否重新定价。';
    }

    if (hasCeasefireSignal) {
        return '美伊停火初步落地但局势仍脆弱，客户集中追问油价、黄金与利率路径如何重估。';
    }

    return null;
}

function sanitizeLatestUpdates(
    topic: FocusTopicConfig,
    value: unknown,
    newsItems: NewsItem[]
): ClientFocusUpdate[] {
    if (topic.slug === 'middle-east-tensions') {
        const headlineDriven = buildMiddleEastLatestUpdates(newsItems);
        if (headlineDriven.length > 0) {
            return headlineDriven;
        }
        if (topic.fallbackLatestUpdates?.length) {
            return topic.fallbackLatestUpdates;
        }
    }

    if (!Array.isArray(value)) {
        return buildFallbackLatestUpdates(topic, newsItems);
    }

    const updates = value
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object')
        .map((item, index) => ({
            time: formatClockTime(newsItems[index]?.published_at),
            title: typeof item.title === 'string' ? item.title.trim() : '',
            impact: sanitizeLatestImpact(typeof item.impact === 'string' ? item.impact.trim() : '')
        }))
        .filter((item) => item.title)
        .slice(0, 2);

    return updates.length > 0 ? updates : buildFallbackLatestUpdates(topic, newsItems);
}

function sanitizeWeeklyProgress(value: unknown, newsItems: NewsItem[]): ClientFocusUpdate[] {
    if (!Array.isArray(value)) {
        return buildWeeklyFallbackUpdates(newsItems);
    }

    const updates = value
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object')
        .map((item) => ({
            time: typeof item.date === 'string' && item.date.trim() ? item.date.trim() : '',
            title: typeof item.summary === 'string' ? item.summary.trim() : '',
            impact: sanitizeWeeklyTag(typeof item.tag === 'string' ? item.tag.trim() : ''),
            source: typeof item.source === 'string' && item.source.trim() ? item.source.trim() : undefined
        }))
        .filter((item) => item.time && item.title && (!item.source || isPreferredWeeklySource(item.source)))
        .slice(0, 3);

    return updates.length > 0 ? updates : buildWeeklyFallbackUpdates(newsItems);
}

function sanitizeFocusStatus(value: string | undefined, fallback = '持续发酵') {
    if (!value) {
        return fallback;
    }

    return (ALLOWED_FOCUS_STATUS as readonly string[]).includes(value) ? value : fallback;
}

function sanitizeLatestImpact(value: string): ClientFocusUpdate['impact'] {
    return (ALLOWED_UPDATE_IMPACTS as readonly string[]).includes(value) ? value : '持续发酵';
}

function sanitizeWeeklyTag(value: string): ClientFocusUpdate['impact'] {
    const allowed = new Set(['风险抬升', '持续发酵', '政策变化', '信用事件']);
    return allowed.has(value) ? value : '持续发酵';
}

function sanitizeTransmissionChain(value: unknown): ClientFocusTransmissionItem[] {
    if (!Array.isArray(value)) {
        return [];
    }

    return value
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object')
        .map<ClientFocusTransmissionItem>((item) => ({
            order:
                item.order === '一阶传导' || item.order === '二阶传导'
                    ? item.order
                    : '一阶传导',
            title: typeof item.title === 'string' ? item.title.trim() : '',
            pricing:
                item.pricing === '已定价' || item.pricing === '部分定价' || item.pricing === '未充分定价'
                    ? item.pricing
                    : '部分定价',
            summary: typeof item.summary === 'string' ? item.summary.trim() : '',
            latest_evidence:
                typeof item.latest_evidence === 'string' && item.latest_evidence.trim()
                    ? item.latest_evidence.trim()
                    : null
        }))
        .filter((item) => item.title && item.summary)
        .slice(0, 2);
}

function sanitizePricingStatus(value: string | undefined): ClientFocusTransmissionItem['pricing'] {
    if (value === '已定价' || value === '部分定价' || value === '未充分定价') {
        return value;
    }

    return '部分定价';
}

function buildFallbackLatestUpdates(topic: FocusTopicConfig, newsItems: NewsItem[]): ClientFocusUpdate[] {
    if (topic.fallbackLatestUpdates?.length && newsItems.length === 0) {
        return topic.fallbackLatestUpdates;
    }

    return newsItems.slice(0, 3).map((item) => ({
        time: formatClockTime(item.published_at),
        date: formatDateOnly(item.published_at),
        title: item.title,
        impact: '持续发酵',
        source: item.source
    }));
}

function buildWeeklyFallbackUpdates(newsItems: NewsItem[]): ClientFocusUpdate[] {
    return dedupeRecentNews(newsItems)
        .filter((item) => isPreferredWeeklySource(item.source))
        .slice(0, 3)
        .map((item) => ({
            time: formatMonthDay(item.published_at),
            title: truncateWeeklySummary(item.title),
            impact: inferWeeklyTag(item.title),
            source: item.source
        }));
}

function isPrivateCreditHardNews(item: NewsItem): boolean {
    const normalized = item.title.toLowerCase();
    const source = (item.source ?? '').toLowerCase();

    const actionKeywords = [
        'redemption', 'redemptions',
        'withdrawal', 'withdrawals',
        'limit', 'limits',
        'loss', 'losses',
        'default', 'defaults',
        'files for', 'filing',
        'pulls out', 'pulled out',
        'trapped', 'locked',
        'freeze', 'frozen', 'suspended',
        'write-down', 'writedown',
        'haircut'
    ];
    const institutionKeywords = [
        'apollo', 'ares', 'blackstone', 'blue owl', 'blueowl',
        'jpmorgan', 'j.p. morgan', 'goldman', 'morgan stanley',
        'blackrock', 'carlyle', 'kkr', 'oaktree', 'bdc',
        'brookfield', 'sixth street', 'owl rock',
        'fed', 'federal reserve', 'powell',
        'treasury', 'u.s. treasury', 'us treasury',
        'insurance regulator', 'insurance regulators'
    ];
    const regulatoryKeywords = [
        'private credit',
        'fed watching',
        'signs of trouble',
        'powell says',
        'treasury reportedly',
        'meet with insurance regulators',
        'discuss private credit issues',
        'watching developments',
        'regulators to discuss'
    ];
    const analysisKeywords = [
        'analysis', 'outlook', 'what it means', 'impact on',
        'why private', 'long-term', 'explained', 'guide to'
    ];
    const blockedSources = ['seeking alpha', 'benzinga', 'yahoo', 'substack', 'medium', 'blog'];

    const hasAction = actionKeywords.some((kw) => normalized.includes(kw));
    const hasInstitution = institutionKeywords.some((kw) => normalized.includes(kw));
    const hasRegulatorySignal = regulatoryKeywords.some((kw) => normalized.includes(kw));
    const isAnalysis = analysisKeywords.some((kw) => normalized.includes(kw));
    const isBlocked = blockedSources.some((kw) => source.includes(kw));

    return (hasAction || hasInstitution || hasRegulatorySignal) && !isAnalysis && !isBlocked;
}

function buildPrivateCreditLatestUpdates(newsItems: NewsItem[]): ClientFocusUpdate[] {
    const seen = new Set<string>();

    return newsItems
        .filter((item) => isPrivateCreditHardNews(item))
        .sort((a, b) => new Date(b.published_at ?? 0).getTime() - new Date(a.published_at ?? 0).getTime())
        .filter((item) => {
            const normalized = item.title.toLowerCase().replace(/\s+/g, ' ').trim();
            if (seen.has(normalized)) {
                return false;
            }
            seen.add(normalized);
            return true;
        })
        .slice(0, 2)
        .map((item) => ({
            time: formatMonthDay(item.published_at),
            title: item.title.trim(),
            impact: inferWeeklyTag(item.title)
        }));
}

type MiddleEastActor = 'iran' | 'us_trump' | 'israel' | 'diplomatic' | 'other';

function classifyMiddleEastActor(title: string): MiddleEastActor {
    const n = title.toLowerCase();
    const isDiplomatic =
        n.includes('ceasefire') || n.includes('peace') || n.includes('talks') ||
        n.includes('deal') || n.includes('mediat') || n.includes('backchannel') ||
        n.includes('indirect') || n.includes('negotiat') || n.includes('proposal') ||
        n.includes('postpone') || n.includes('extends deadline') || n.includes('extend deadline') ||
        n.includes('pause') || n.includes('extension');
    if (isDiplomatic) return 'diplomatic';
    const isIran =
        n.includes('iran') && !n.includes('trump') && !n.includes('us ') && !n.includes('u.s.') && !n.includes('america');
    if (isIran) return 'iran';
    const isIsrael =
        n.includes('israel') && !n.includes('trump') && !n.includes('us ') && !n.includes('u.s.');
    if (isIsrael) return 'israel';
    if (n.includes('trump') || n.includes('u.s.') || n.includes('us ') || n.includes('washington') || n.includes('pentagon') || n.includes('white house')) return 'us_trump';
    return 'other';
}

function buildMiddleEastLatestUpdates(newsItems: NewsItem[]): ClientFocusUpdate[] {
    const seen = new Set<string>();
    const actorCount: Record<MiddleEastActor, number> = { iran: 0, us_trump: 0, israel: 0, diplomatic: 0, other: 0 };
    const MAX_PER_ACTOR = 2;
    const result: ClientFocusUpdate[] = [];

    const filtered = newsItems
        .slice()
        .filter((item) => isMiddleEastHardNews(item))
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        });

    for (const item of filtered) {
        if (result.length >= 5) break;
        const normalized = item.title.trim().toLowerCase();
        if (!normalized || seen.has(normalized)) continue;
        const actor = classifyMiddleEastActor(item.title);
        if (actorCount[actor] >= MAX_PER_ACTOR) continue;
        seen.add(normalized);
        actorCount[actor]++;
        result.push({
            time: formatClockTime(item.published_at),
            date: formatDateOnly(item.published_at),
            title: item.title.trim(),
            impact: classifyMiddleEastImpact(item.title)
        });
    }

    return result.filter((item) => Boolean(item.title)) as ClientFocusUpdate[];
}

function dedupeRecentNews(newsItems: NewsItem[]): NewsItem[] {
    const seen = new Set<string>();

    return newsItems
        .filter((item) => isWithinDays(item.published_at, 7))
        .filter((item) => {
            const normalized = item.title.toLowerCase().replace(/\s+/g, ' ').trim();
            if (!normalized || seen.has(normalized)) {
                return false;
            }
            seen.add(normalized);
            return true;
        });
}

function isPreferredWeeklySource(source: string | undefined) {
    if (!source) {
        return false;
    }

    const normalized = source.toLowerCase().trim();
    if (BLOCKED_WEEKLY_SOURCE_PATTERNS.some((item) => normalized.includes(item))) {
        return false;
    }

    return PREFERRED_WEEKLY_SOURCES.some((item) => normalized.includes(item.toLowerCase()))
        || normalized.includes('sec')
        || normalized.includes('federal reserve')
        || normalized.includes('hkma')
        || normalized.includes('people’s bank of china')
        || normalized.includes('people\'s bank of china')
        || normalized.includes('pboc')
        || normalized.includes('official');
}

function isWithinDays(publishedAt: string | undefined, days: number): boolean {
    if (!publishedAt) {
        return false;
    }

    const published = new Date(publishedAt);
    if (Number.isNaN(published.getTime())) {
        return false;
    }

    return (Date.now() - published.getTime()) <= days * 24 * 60 * 60 * 1000;
}

function isWithinHours(publishedAt: string | undefined, hours: number): boolean {
    if (!publishedAt) {
        return false;
    }

    const published = new Date(publishedAt);
    if (Number.isNaN(published.getTime())) {
        return false;
    }

    return (Date.now() - published.getTime()) <= hours * 60 * 60 * 1000;
}

function formatMonthDay(publishedAt: string | undefined): string {
    if (!publishedAt) {
        return '近期';
    }

    const date = new Date(publishedAt);
    if (Number.isNaN(date.getTime())) {
        return '近期';
    }

    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${month}-${day}`;
}

function inferWeeklyTag(title: string): ClientFocusUpdate['impact'] {
    const normalized = title.toLowerCase();
    if (normalized.includes('default') || normalized.includes('loss') || normalized.includes('withdrawal')) {
        return '信用事件';
    }
    if (normalized.includes('limit') || normalized.includes('redemption')) {
        return '风险抬升';
    }
    if (normalized.includes('regulator') || normalized.includes('policy')) {
        return '政策变化';
    }
    return '持续发酵';
}

function truncateWeeklySummary(title: string): string {
    return title.trim().replace(/\s+/g, ' ').slice(0, 80);
}

function isMiddleEastHardNews(item: NewsItem): boolean {
    const normalized = item.title.toLowerCase();
    const source = (item.source ?? '').toLowerCase();

    const eventKeywords = [
        'struck',
        'strike',
        'attacked',
        'attack',
        'missile',
        'drone',
        'official',
        'officials',
        'demands',
        'demand',
        'talks',
        'venue',
        'date',
        'agreed',
        'rejects',
        'reject',
        'bushehr',
        'iran',
        'israel',
        'war damage',
        'payment',
        'hormuz',
        'strait',
        'shipping',
        'ship',
        'tanker',
        'blockade',
        'closure',
        'naval',
        'fleet',
        'maritime',
        'ceasefire',
        'truce',
        'evacuation',
        'humanitarian',
        'negotiate',
        'delegation',
        'mediator',
        'troops',
        'troop',
        'pentagon',
        'deploy',
        'deployment',
        'combat',
        'soldiers',
        'forces',
        'retaliate',
        'retaliation',
        'infrastructure',
        'vice president',
        'jd vance',
        'white house',
        'washington',
        'warship',
        'carrier',
        'military',
        'airstrike',
        'air strike',
        'bombed',
        'bomb'
    ];
    const analysisKeywords = [
        'analysis',
        'where things stand',
        'what we know',
        'what to know',
        'timeline',
        'explained',
        'one month into',
        '1 month into',
        'so far',
        'overview',
        'what means',
        'what it means',
        'impact on',
        'long-term',
        'long term',
        'supply and prices',
        'fuel prices',
        'market focus',
        'why',
        'outlook',
        'oil prices',
        'economy',
        'geoeconomic',
        'firestorm',
        'delivers high oil prices',
        'means for china'
    ];
    const lowSignalPatterns = [
        'reveals iran',
        '\'present\' to us',
        '"present" to us',
        'present to us'
    ];
    const blockedSources = ['bruegel', 'pbs', 'council on foreign relations', 'financial times opinion'];

    return eventKeywords.some((keyword) => normalized.includes(keyword))
        && !analysisKeywords.some((keyword) => normalized.includes(keyword))
        && !lowSignalPatterns.some((keyword) => normalized.includes(keyword))
        && !blockedSources.some((blocked) => source.includes(blocked));
}

function classifyMiddleEastImpact(title: string): ClientFocusUpdate['impact'] {
    const normalized = title.toLowerCase();

    // Escalation signals take priority — check first
    const escalationSignals = [
        'rejects', 'rejection', 'scoffs', 'refuses',
        'vows retaliation', 'vows to intensify', 'vows to expand',
        'warns civilians', 'warns to evacuate',
        'no talks', 'no negotiations', 'no plans for negotiations',
        'denies ceasefire', 'denies talks',
        'maximalist', 'unreasonable',
        'obliterate', 'obliterating',
        'bunker buster',
        'most intense',
    ];
    if (escalationSignals.some((signal) => normalized.includes(signal))) {
        return '风险抬升';
    }

    const deEscalationSignals = [
        'extends deadline',
        'extends pause',
        'extend deadline',
        'extend pause',
        'extension on',
        'another extension',
        'grants iran',
        'grants extension',
        'pauses strikes',
        'pause on',
        'pauses threat',
        'ceasefire',
        'truce',
        'peace proposal',
        'peace plan',
        'peace deal',
        'peace agreement',
        'peace talks',
        'indirect talks',
        'backchannel',
        'mediation',
        'mediator',
        'productive conversations',
        'talks ongoing',
        'talks underway',
        'facilitating',
        'diplomatic',
        'de-escalat',
        'deescalat',
        'withdraw',
        'pullback',
        'pull back',
        'stand down',
        'postpone',
        'delay strike',
        'hold fire',
        'suspend'
    ];
    if (deEscalationSignals.some((signal) => normalized.includes(signal))) {
        return '政策变化';
    }

    if (normalized.includes('talks') || normalized.includes('venue') || normalized.includes('agreed')) {
        return '政策变化';
    }
    if (normalized.includes('official')) {
        return '政策变化';
    }
    return '风险抬升';
}

function classifyMiddleEastChangeImpact(title: string): '风险抬升' | '缓和' | null {
    const normalized = title.toLowerCase();
    const deEscalationSignals = [
        'extends deadline',
        'extends pause',
        'extend deadline',
        'extend pause',
        'extension on',
        'another extension',
        'grants iran',
        'grants extension',
        'pauses strikes',
        'pause on',
        'pauses threat',
        'ceasefire',
        'truce',
        'peace proposal',
        'peace plan',
        'peace deal',
        'peace agreement',
        'peace talks',
        'indirect talks',
        'backchannel',
        'mediation',
        'mediator',
        'productive conversations',
        'talks ongoing',
        'talks underway',
        'facilitating',
        'diplomatic',
        'de-escalat',
        'deescalat',
        'withdraw',
        'pullback',
        'pull back',
        'stand down',
        'postpone',
        'delay strike',
        'hold fire',
        'suspend'
    ];

    if (deEscalationSignals.some((signal) => normalized.includes(signal))) {
        return '缓和';
    }

    return classifyMiddleEastImpact(title) === '风险抬升' ? '风险抬升' : null;
}

async function generateMiddleEastWhatChanged(newsItems: NewsItem[]): Promise<WhatChangedGroup[]> {
    const candidates = newsItems
        .filter((item) => isWithinHours(item.published_at, WHAT_CHANGED_WINDOW_HOURS))
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        })
        .slice(0, 30);

    if (candidates.length === 0) {
        return [];
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return buildFallbackWhatChangedGroups(candidates);
    }

    const systemPrompt = '你是香港私人银行的市场简报助手。请按给定分组选项，把过去72小时的中东冲突新闻整理成可直接给RM使用的中文结构化要点。';
    const newsList = candidates
        .map((item, index) => `${index + 1}. ${formatClockTime(item.published_at)} | ${item.title}`)
        .join('\n');
    const userPrompt = `
你是香港私人银行的市场简报助手。

以下是过去72小时的中东冲突相关新闻标题列表。请将它们整理成以下3个固定分组（每组必须出现，无内容则 items 为空数组）：

分组定义：
1. 霍尔木兹海峡（icon: 🛢️）：海峡通航、航运、油价、封锁
2. 军事动态（icon: 🔴）：空袭、导弹、地面行动、核设施、无人机
3. 外交进展（icon: 🕊️）：停火提议、外交斡旋、谈判、国际调停

每组最多4条，每条：
- time: 从新闻时间取 HH:MM（转换为香港时间 UTC+8）
- headline: 不超过35字。格式：用【名称】标注具体主语（例如【特朗普】【以军】【伊朗】【IRGC】【IAEA】【沙特】），后接具体行动或数据。
  示例：【以军】对伊朗布什尔核设施发动第5轮空袭，投弹120枚
  示例：【特朗普】在内阁会议宣称伊朗已放行10艘油轮作为"礼物"
  示例：【IRGC】发动第86波导弹和无人机攻势，目标为以军南部基地
  禁止：
  - "各方态势评估""局势升级""引发不确定性""主持会议""发表声明"等模糊表述
  - 主语模糊（不允许"某方""双方""各方"，必须说明是哪个国家/机构/人物）
  - 省略关键数字或具体细节（有数字尽量保留：波次、枚数、桶/日、美元价格）

选取标准：
- 军事动态：优先选择具体打击、部署、导弹、无人机、基地受损、核设施相关事件
- 霍尔木兹海峡：优先选择海峡通航、油轮、收费、绕道管线、出口、油价、护航等具体变化
- 外交进展：优先选择停火提议、四国协调、调停、谈判信号等明确外交动作
- 忽略背景分析、观点类、纯评论类文章

输出格式（JSON数组）：
[
  { "group_label": "霍尔木兹海峡", "group_icon": "🛢️", "items": [...] },
  { "group_label": "军事动态", "group_icon": "🔴", "items": [...] },
  { "group_label": "外交进展", "group_icon": "🕊️", "items": [...] }
]

新闻列表：
${newsList}

只输出 JSON，不要解释，不要换行。
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            return buildFallbackWhatChangedGroups(candidates);
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const content = payload.choices?.[0]?.message?.content?.trim() ?? '';
        const parsed = safeParseJson(content) as Array<{
            group_label?: string;
            group_icon?: string;
            items?: Array<{ time?: string; headline?: string }>;
        }> | null;

        if (!Array.isArray(parsed)) {
            return buildFallbackWhatChangedGroups(candidates);
        }

        const fixedGroups = [
            { group_label: '霍尔木兹海峡', group_icon: '🛢️' },
            { group_label: '军事动态', group_icon: '🔴' },
            { group_label: '外交进展', group_icon: '🕊️' }
        ] as const;

        return fixedGroups.map((expectedGroup) => {
            const sourceGroup = parsed.find((group) => group.group_label?.trim() === expectedGroup.group_label);
            const parsedItems = Array.isArray(sourceGroup?.items)
                ? sourceGroup.items
                      .filter((item) => typeof item?.time === 'string' && typeof item?.headline === 'string')
                      .map((item) => ({
                          time: item.time?.trim() ?? '',
                          headline: sanitizeGeneratedMiddleEastHeadline(item.headline?.trim() ?? '')
                      }))
                      .filter((item) => item.headline)
                      .slice(0, 4)
                : [];

            const fallbackItems = buildFallbackWhatChangedItems(candidates, expectedGroup.group_label);
            const items = mergeWhatChangedItems(parsedItems, fallbackItems);

            return {
                group_label: expectedGroup.group_label,
                group_icon: expectedGroup.group_icon,
                items
            };
        });
    } catch {
        return buildFallbackWhatChangedGroups(candidates);
    }
}

async function generatePrivateCreditWhatChanged(newsItems: NewsItem[]): Promise<WhatChangedGroup[]> {
    const candidates = dedupeRecentNews(newsItems)
        .filter((item) => isWithinDays(item.published_at, PRIVATE_CREDIT_WHAT_CHANGED_WINDOW_DAYS))
        .filter((item) => isPrivateCreditHardNews(item))
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        })
        .slice(0, 20);

    if (candidates.length === 0) {
        return [];
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return buildFallbackPrivateCreditWhatChangedGroups(candidates);
    }

    const newsList = candidates
        .map((item, index) => `${index + 1}. ${formatMonthDay(item.published_at)} | ${item.title}`)
        .join('\n');

    const systemPrompt =
        '你是香港私人银行的市场沟通助手。请把过去一周的私募信贷相关新闻整理成可直接给RM使用的中文分组要点。';
    const userPrompt = `
以下是过去7天的私募信贷相关新闻标题。请将它们整理成以下2个固定分组：

1. 流动性与赎回（icon: 💧）：赎回限制、gate、提款、流动性收紧、融资额度变化
2. 监管动向（icon: 🏦）：财政部、美联储、监管机构、保险监管、银行及相关表态

每组最多3条，每条：
- time: 从新闻时间取 MM-DD；无法判断则留空字符串
- headline: 不超过35字，尽量接近原始新闻标题的中文翻译
- headline 中需要高亮的关键词请用 {{关键词}} 标记，例如 {{Apollo}} 限制投资者赎回
- 不要使用【】符号

禁止：
- “市场承压”“风险升温”“持续发酵”等空泛表述
- 只有判断，没有具体机构或动作
- 纯背景分析和观点文章

输出格式（JSON数组）：
[
  { "group_label": "流动性与赎回", "group_icon": "💧", "items": [...] },
  { "group_label": "监管动向", "group_icon": "🏦", "items": [...] }
]

新闻列表：
${newsList}

只输出 JSON，不要解释。
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            return buildFallbackPrivateCreditWhatChangedGroups(candidates);
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const content = payload.choices?.[0]?.message?.content?.trim() ?? '';
        const parsed = safeParseJson(content) as Array<{
            group_label?: string;
            group_icon?: string;
            items?: Array<{ time?: string; headline?: string }>;
        }> | null;

        if (!Array.isArray(parsed)) {
            return buildFallbackPrivateCreditWhatChangedGroups(candidates);
        }

        const fixedGroups = [
            { group_label: '流动性与赎回', group_icon: '💧' },
            { group_label: '监管动向', group_icon: '🏦' }
        ] as const;

        return fixedGroups.map((expectedGroup) => {
            const sourceGroup = parsed.find((group) => group.group_label?.trim() === expectedGroup.group_label);
            const parsedItems = Array.isArray(sourceGroup?.items)
                ? sourceGroup.items
                      .filter((item) => typeof item?.headline === 'string')
                      .map((item) => ({
                          time: item.time?.trim() ?? '',
                          headline: sanitizeGeneratedPrivateCreditHeadline(item.headline?.trim() ?? '')
                      }))
                      .filter((item) => item.headline)
                      .slice(0, 3)
                : [];

            const fallbackItems = buildFallbackPrivateCreditWhatChangedItems(candidates, expectedGroup.group_label);
            return {
                group_label: expectedGroup.group_label,
                group_icon: expectedGroup.group_icon,
                items: mergeWhatChangedItems(parsedItems, fallbackItems).slice(0, 3)
            };
        }).filter((group) => group.items.length > 0);
    } catch {
        return buildFallbackPrivateCreditWhatChangedGroups(candidates);
    }
}

async function generateDynamicClientQuestions(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<Array<{ question: string; answer: string; category?: string }> | null> {
    if (
        topic.slug !== 'middle-east-tensions'
        && topic.slug !== 'private-credit-stress'
        && topic.slug !== 'hk-market-sentiment'
        && topic.slug !== 'gold-repricing'
        && topic.slug !== 'usd-strength'
    ) {
        return null;
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        console.warn(`[focus-client-questions] ${topic.slug}: no_api_key`);
        return null;
    }

    const contextItems = newsItems
        .slice()
        .filter((item) =>
            topic.slug === 'private-credit-stress'
                ? isWithinDays(item.published_at, 7)
                : topic.slug === 'hk-market-sentiment'
                    ? isWithinDays(item.published_at, 7)
                : topic.slug === 'gold-repricing' || topic.slug === 'usd-strength'
                    ? isWithinDays(item.published_at, 14)
                : topic.slug === 'middle-east-tensions'
                    ? true
                    : isWithinDays(item.published_at, 7)
        )
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        })
        .slice(0, 10);

    if (contextItems.length === 0) {
        console.warn(`[focus-client-questions] ${topic.slug}: no_context_items`);
        return null;
    }

    const questionsList = topic.clientQuestions.map((item, index) => `${index + 1}. ${item.question}`).join('\n');
    const baselineAnswers = topic.clientQuestions
        .map(
            (item, index) =>
                `${index + 1}. 问题：${item.question}\n基准回答：${item.answer}`
        )
        .join('\n\n');
    const newsList = contextItems
        .map((item, index) => `${index + 1}. ${formatClockTime(item.published_at)} | ${item.title}`)
        .join('\n');

    const sharedSystemPrompt = `
你是香港私人银行的市场沟通助手，面向 RM/IC 生成客户常问标准回答。

回答目标：
生成可直接发送给高净值客户的沟通口径，而不是研究报告。

写作结构（必须遵守）：
1. 开头先给明确判断（20-30字）
2. 中间补 1 个近期市场、政策、机构、资金流或监管事实（20-30字）
3. 再解释传导机制（20-30字）
4. 最后一句给风险边界、观察点或客户今天该追踪的变量（15-25字）

写作要求：
- 每条回答控制在 90-110 字
- 句子短，像客户沟通短信
- 先讲判断，再补事实，再讲传导，最后给风险边界
- 必须包含至少 1 个具体变量、机构、资金流、监管动作、收益率、指数或政策事实
- 不要复述单条新闻，要提炼成 1-2 周内可复用的客户沟通口径
- 答案必须使用完整自然语句，禁止使用箭头（→）或列表

语气要求：
- 专业、克制、像私行客户沟通口径
- 不像研究报告，不像新闻播报
- 不直接给投资建议

统一禁止：
- 不要使用“市场承压”“风险升温”“引发不确定性”“情绪波动”“逻辑切换”等空泛表述
- 不要堆砌多个数字
- 不要整段复述 headline
`.trim();
    const systemPrompt = topic.slug === 'middle-east-tensions'
        ? `${sharedSystemPrompt}

中东冲突专项要求：
- 只梳理传导链条和解释机制，不输出任何定性方向判断（禁止使用“看好/看空/利好/利空/受益/承压”等词）
- 涉及降息概率等具体市场定价时，引导用户查阅公开来源（如 CME FedWatch），不要硬编码概率数字
- 禁止使用价格数字作为问题描述，要问传导机制和客户影响

示例（脆弱停火情景下油价问题）：
问：停火协议落地后，油价回调的传导路径是什么？
答：停火预期使霍尔木兹断供风险下降，供应中断溢价开始回归。布伦特与能源股通常先反映这一变化，随后通胀与降息路径才会被重新讨论。`.trim()
        : `${sharedSystemPrompt}

${topic.slug === 'private-credit-stress'
            ? '私募信贷专项要求：必须清楚区分流动性压力、信用风险和系统性风险。'
            : topic.slug === 'gold-repricing'
                ? '黄金专项要求：必须优先解释黄金定价驱动，再补充验证这些驱动的最新事实。'
                : topic.slug === 'hk-market-sentiment'
                    ? '港股专项要求：必须优先解释估值修复、盈利验证和资金节奏三者关系。'
                    : '美元专项要求：必须优先解释汇率、利率路径与港股/人民币之间的传导关系。'}`.trim();
    const categoryInstructions = `
请生成 6 到 8 个问答，覆盖以下资产类别分类，每个问题必须包含 category 字段：
${(FOCUS_QUESTION_CATEGORIES[topic.slug] ?? []).join('、')}

每个分类至少生成 1 个问题，最多生成 2 个问题。问题角度要尽量分散，避免同一分类内出现重复视角。

返回 JSON 数组格式：
[
  { "question": "...", "answer": "...", "category": "黄金" }
]
`.trim();

    const rawUserPrompt = topic.slug === 'middle-east-tensions'
        ? `
以下是过去48小时的中东局势相关新闻。请先判断当前情景标签，再生成问答。

情景判断规则：
- 若新闻以"停火/ceasefire/谈判/negotiations/deal"为主 → 情景：脆弱停火
- 若新闻以"空袭/导弹/封锁/escalation"为主 → 情景：冲突升级
- 若新闻以"和平协议/permanent/协议达成"为主 → 情景：和平协议

新闻列表：
${newsList}

生成要求：
- 问题必须与判断出的情景一致，不能使用过时情景的假设
- 优先捕捉情景切换带来的新传导逻辑（如从"美元走强"切换到"避险溢价消退"）
- 覆盖以下维度，每个维度至少1个问题：
  * 汇率（含CNH/USD）：问传导机制和汇率路径，不要问"CNH具体升到多少"
  * 利率/央行路径（美联储降息预期）：问降息预期的触发条件，引导查CME FedWatch
  * 大宗商品（油价、黄金）：问供应中断逻辑和避险溢价机制
  * 股票：优先问美股（标普/纳斯达克）和港股大盘的传导机制，其次问科技股的影响路径，能源股可作为补充，避免问航运股
  * 债券/固收：问美债、信用利差的路径
  * 结构性长期变化（如有，例如petroyuan、结算体系）

问题语气要求（基于情景，体现具体观察点，不包含价格数字）：
✓ "停火协议达成后，霍尔木兹通道风险溢价如何传导到油价？"
✓ "CNH近期走势背后，是避险情绪消退还是结算需求变化？"
✓ "地缘缓和后，美联储降息预期的传导路径会有什么变化？"
✗ "油价跌了，对黄金有什么影响？"（不含传导机制）
✗ "汇率有什么变化？"（太泛）

答案格式要求（必须使用自然完整语句，禁止箭头格式）：
✓ 停火预期落地后，市场对霍尔木兹断供的溢价开始定价回归。布伦特通常领先能源股调整，实际通胀预期数据也会同步松动。
✗ 停火 → 霍尔木兹风险下降 → 油价回调 → 能源股调整（禁止此类箭头列点格式）

禁止：
- 不要以"过去48小时内..."开头
- 不要整段围绕某个headline复述
- 不要使用"局势升级""引发不确定性"等空泛表述
- 不要给具体投资建议或方向性判断
- 答案中禁止出现任何"→"符号

只输出JSON，不要解释。
`.trim()
        : topic.slug === 'private-credit-stress'
            ? `
以下是过去7天的私募信贷相关新闻。请基于这些最新信息，
为以下固定问题与市场框架延伸出 6 到 8 个客户常问问答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 90-110 字
- 必须采用“先判断 → 再补事实 → 再讲传导 → 最后给风险边界”的结构
- 第一两句必须能单独成立，像直接回复客户
- 必须至少包含 1 个具体机构、金额、比例、监管动作或违约/减记事实
- 机构名称、金额、监管动作必须优先来自本周新闻上下文；如果本周新闻没有出现，不要为了显得具体而硬写 Apollo、Ares、Blackstone 等旧案例
- 不要复用基准回答里过时或静态的机构细节，除非它们也出现在本周新闻中
- 尽量用“当前更像…而不是…”“整体来看…”“本周新增变化是…”这类客户沟通句式
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 在保留基准回答逻辑框架的基础上，用本周新事实更新答案
- 若问题是“会不会演变成风险事件”，重点回答是否系统性
- 若问题是“和2008年有何区别”，重点回答风险是否在银行体系内
- 若问题是“主要出在哪些板块”，重点回答哪些板块更脆弱、哪些相对稳健

禁止：
- 不要以“过去一周...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“风险升温”“市场承压”“引发不确定性”“情绪波动”“逻辑切换”等空泛表述
- 不要堆太多数字，不要一段塞入多个括号说明
- 不要写成长段分析，不要出现“这意味着需要持续关注”这类套话
- 不要给具体投资建议

只输出JSON，不要解释。
`.trim()
            : topic.slug === 'gold-repricing'
                ? `
以下是过去7天与黄金、实际利率、美元、央行购金和金矿股相关的新闻。请基于这些最新信息，
为以下固定问题与市场框架延伸出 6 到 8 个客户常问问答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 90-110 字
- 必须采用“先判断 → 再补新事实 → 再讲传导 → 最后给风险边界”的结构
- 第一两句必须能单独成立，像直接回复客户
- 必须至少包含 1 个具体变量或事实，例如美元、实际利率、央行购金、ETF资金流、金矿成本
- 若问题是“2025年以来黄金牛市驱动力是什么”，必须把回答锚定为对 2025 年以来这轮牛市历史驱动的回顾，而不是描述当前金价仍在单边上涨
- 这一题优先围绕 2025 年当时的四个核心驱动来回答：降息预期与实际利率回落、美元阶段性走弱、央行购金、地缘风险与避险需求
- 这一题不要用“当前黄金上涨”“当前这一轮上涨延续”等表述；若要提近期变化，只能放在最后一句，简短说明近期黄金价格也面临美元、实际利率或波动加大的挑战
- 尽量用“当前更关键的是…”“本周新增变化是…”“整体来看…”这类客户沟通句式
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 若问题是“为何黄金反而下跌”，重点回答避险需求为何被美元/实际利率压过
- 若问题是“金矿股为何跑输”，重点回答经营杠杆、成本与利润弹性

禁止：
- 不要以“过去一周...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“风险升温”“市场承压”“引发不确定性”“情绪波动”“逻辑切换”等空泛表述
- 不要写成长段分析，不要堆太多数字
- 不要给具体投资建议

只输出JSON，不要解释。
`.trim()
                : topic.slug === 'hk-market-sentiment'
                    ? `
以下是过去7天与港股、恒生指数、恒生科技和南下资金情绪相关的新闻。请基于这些最新信息，
为以下固定问题与市场框架延伸出 6 到 8 个客户常问问答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 90-110 字
- 必须采用“先判断 → 再补资金或盈利事实 → 再讲传导 → 最后给观察点”的结构
- 第一两句必须能单独成立，像直接回复客户
- 必须至少包含 1 个具体变量或事实，例如恒指、恒生科技、南下资金、ETF资金流、PMI、盈利兑现、风险偏好
- 结构必须清楚解释：估值修复、盈利验证和资金节奏之间是谁在主导
- 尽量用“当前更像…”“整体来看…”“换句话说…”这类客户沟通句式
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 若问题是“今年为什么不如去年”，重点回答估值修复放缓、盈利验证门槛提高，以及外部风险偏好扰动
- 若问题是“现在是资金行情吗”，重点回答短期资金驱动与中期基本面验证的关系
- 若问题是“恒生科技回调意味着什么”，重点回答上涨后分化、风险偏好回落与盈利兑现要求提高

禁止：
- 不要以“过去一周...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“风险升温”“市场承压”“引发不确定性”“情绪波动”“逻辑切换”等空泛表述
- 不要写成长段分析，不要堆太多数字
- 不要给具体投资建议

只输出JSON，不要解释。
`.trim()
                : `
以下是过去7天与美元、人民币、港股和利率路径相关的新闻。请基于这些最新信息，
为以下固定问题与市场框架延伸出 6 到 8 个客户常问问答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 90-110 字
- 必须采用“先判断 → 再补事实 → 再讲传导 → 最后给风险边界”的结构
- 第一两句必须能单独成立，像直接回复客户
- 必须至少包含 1 个具体变量或事实，例如 DXY、USDCNH、联储表态、人民币中间价、港元流动性、10Y美债收益率
- 重点解释避险需求、利率路径、油价通胀渠道，以及美元与港股流动性的关系
- 尽量用“当前更像…”“本周新增变化是…”“整体来看…”这类客户沟通句式
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 若问题是“美元为何反弹”，重点回答避险需求与利率路径
- 若问题是“央行如何表态”，重点回答政策态度与波动容忍度
- 若问题是“强美元对港股意味着什么”，重点回答外资、流动性与估值

禁止：
- 不要以“过去一周...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“风险升温”“市场承压”“引发不确定性”“情绪波动”“逻辑切换”等空泛表述
- 不要写成长段分析，不要堆太多数字
- 不要给具体投资建议

只输出JSON，不要解释。
`.trim();

    const userPrompt = `${rawUserPrompt}\n\n${categoryInstructions}`;

    function inferQuestionCategory(
        slug: string,
        question: string,
        answer: string,
        fallbackCategory: string,
        isLLMAssigned: boolean
    ): string {
        // If LLM already assigned a valid category, trust it.
        if (isLLMAssigned) {
            return fallbackCategory;
        }

        const questionText = question.toLowerCase();
        const fullText = `${question} ${answer}`.toLowerCase();

        if (slug === 'middle-east-tensions') {
            if (
                (questionText.includes('美元') && (questionText.includes('人民币') || questionText.includes('cnh') || questionText.includes('汇率') || questionText.includes('港元')))
                || questionText.includes('cnh')
                || questionText.includes('汇率')
                || questionText.includes('petroyuan')
                || questionText.includes('结算')
            ) {
                return '汇率';
            }
            // Match debt/rate keywords on question only to avoid false positives from answer context
            if (
                questionText.includes('降息')
                || questionText.includes('加息')
                || questionText.includes('债券')
                || questionText.includes('收益率')
                || questionText.includes('利率')
                || questionText.includes('fed')
                || questionText.includes('美联储')
                || questionText.includes('通胀')
            ) {
                return '债券';
            }
            if (
                fullText.includes('原油')
                || fullText.includes('油价')
                || fullText.includes('wti')
                || fullText.includes('能源')
                || fullText.includes('航运')
                || fullText.includes('霍尔木兹')
            ) {
                return '原油';
            }
            if (fullText.includes('黄金') || fullText.includes('金价')) {
                return '黄金';
            }
            if (
                fullText.includes('股票')
                || fullText.includes('fcn')
                || fullText.includes('港股')
                || fullText.includes('风险资产')
                || fullText.includes('估值')
                || fullText.includes('企业成本')
            ) {
                return '股票/FCN';
            }
        }

        return fallbackCategory;
    }

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            console.warn(`[focus-client-questions] ${topic.slug}: http_not_ok status=${response.status}`);
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const content = payload.choices?.[0]?.message?.content?.trim() ?? '';
        const parsed = safeParseJsonArray(content) as Array<{ question?: string; answer?: string; category?: string }> | null;
        console.log(`[focus-client-questions] ${topic.slug}: parsed ${parsed ? parsed.length : 'null'} items, categories=${parsed ? [...new Set(parsed.map((i: any) => i?.category).filter(Boolean))].join(',') : 'n/a'}`);

        if (!parsed) {
            console.warn(`[focus-client-questions] ${topic.slug}: json_parse_failed`);
            return null;
        }

        const categoryPool = FOCUS_QUESTION_CATEGORIES[topic.slug] ?? [];
        const allowedCategories = new Set(categoryPool);
        const sanitized: Array<{ question: string; answer: string; category?: string }> = parsed
            .map((item, index) => {
                const question = item.question?.trim();
                const answer = item.answer?.trim();
                const rawCategory = item.category?.trim();
                const isLLMAssigned = !!(rawCategory && allowedCategories.has(rawCategory));
                const baseCategory =
                    rawCategory && (allowedCategories.size === 0 || allowedCategories.has(rawCategory))
                        ? rawCategory
                        : categoryPool[index % Math.max(categoryPool.length, 1)] ?? '全部';

                if (!question || !answer) {
                    return null;
                }

                const category = inferQuestionCategory(topic.slug, question, answer, baseCategory, isLLMAssigned);

                return {
                    question,
                    answer,
                    category
                };
            })
            .filter((item): item is NonNullable<typeof item> => item !== null);

        const categoryCount = new Map<string, number>();
        const capped = sanitized.filter((item) => {
            const cat = item.category ?? '全部';
            const count = categoryCount.get(cat) ?? 0;
            if (count >= 2) {
                return false;
            }
            categoryCount.set(cat, count + 1);
            return true;
        });

        if (capped.length > 0) {
            return capped;
        }

        return topic.clientQuestions.map((item) => {
            const fallbackCategory = item.category ?? categoryPool[0] ?? '全部';
            return {
                question: item.question,
                answer: item.answer,
                category: inferQuestionCategory(topic.slug, item.question, item.answer, fallbackCategory, false)
            };
        });
    } catch {
        console.warn(`[focus-client-questions] ${topic.slug}: exception`);
        return null;
    }
}

async function generateDailyVerdict(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<ClientFocusDailyVerdict | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const contextItems = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 8);

    if (contextItems.length === 0) {
        return null;
    }

    const newsList = contextItems
        .map((item, index) => `${index + 1}. ${formatClockTime(item.published_at)} | ${item.title}`)
        .join('\n');

    const prompt = `
你是香港私人银行 IC，正在为 RM 准备今天的客户沟通结论。
主题：${topic.title}

今日相关新闻：
${newsList}

请生成一个今日结论，包含三个字段：
1. risk_appetite：今日风险偏好方向，只能是以下三个值之一："偏谨慎"、"中性"、"偏积极"
2. fcn_impact：这个话题对今日 FCN 推荐的影响，一句话，15-25字，不要给投资建议，只描述影响方向
3. key_change：今日最重要的一个新变化，一句话，15-25字，客观描述事实

返回纯 JSON，格式：
{"risk_appetite": "...", "fcn_impact": "...", "key_change": "..."}
`.trim();

    try {
        const response = await fetch('https://api.deepseek.com/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [{ role: 'user', content: prompt }],
                temperature: 0.3,
                response_format: { type: 'json_object' }
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const raw = payload.choices?.[0]?.message?.content;
        if (!raw) {
            return null;
        }

        const parsed = JSON.parse(raw) as Partial<ClientFocusDailyVerdict>;
        const validAppetites = ['偏谨慎', '中性', '偏积极'] as const;
        if (
            !parsed.risk_appetite
            || !validAppetites.includes(parsed.risk_appetite as (typeof validAppetites)[number])
            || !parsed.fcn_impact
            || !parsed.key_change
        ) {
            return null;
        }

        return {
            risk_appetite: parsed.risk_appetite as ClientFocusDailyVerdict['risk_appetite'],
            fcn_impact: parsed.fcn_impact.trim(),
            key_change: parsed.key_change.trim()
        };
    } catch {
        return null;
    }
}

function buildFallbackPrivateCreditWhatChangedGroups(newsItems: NewsItem[]): WhatChangedGroup[] {
    const fixedGroups = [
        { group_label: '流动性与赎回', group_icon: '💧' },
        { group_label: '监管动向', group_icon: '🏦' }
    ] as const;

    return fixedGroups
        .map((group) => ({
            group_label: group.group_label,
            group_icon: group.group_icon,
            items: buildFallbackPrivateCreditWhatChangedItems(newsItems, group.group_label)
        }))
        .filter((group) => group.items.length > 0);
}

function buildFallbackPrivateCreditWhatChangedItems(
    newsItems: NewsItem[],
    groupLabel: '流动性与赎回' | '监管动向'
) {
    return newsItems
        .filter((item) => classifyPrivateCreditWhatChangedGroup(item.title) === groupLabel)
        .map((item) => ({
            time: formatMonthDay(item.published_at),
            headline: buildFallbackPrivateCreditHeadline(item.title)
        }))
        .filter((item) => item.headline)
        .slice(0, 3);
}

function classifyPrivateCreditWhatChangedGroup(
    title: string
): '流动性与赎回' | '监管动向' | null {
    const normalized = title.toLowerCase();

    const liquidityKeywords = [
        'redemption',
        'withdrawal',
        'gate',
        'limit',
        'liquidity',
        'freeze',
        'frozen',
        'suspend',
        'warehouse',
        'credit line',
        'facility',
        'funding'
    ];
    if (liquidityKeywords.some((keyword) => normalized.includes(keyword))) {
        return '流动性与赎回';
    }

    const regulatoryKeywords = [
        'fed',
        'powell',
        'treasury',
        'regulator',
        'regulators',
        'watching',
        'signs of trouble',
        'meet',
        'discussion',
        'insurer',
        'insurance',
        'bank',
        'banks',
        'watch'
    ];
    if (regulatoryKeywords.some((keyword) => normalized.includes(keyword))) {
        return '监管动向';
    }

    return null;
}

function buildFallbackPrivateCreditHeadline(title: string) {
    const actor = extractPrivateCreditActor(title);
    const cleanedTitle = title
        .replace(/\s+-\s+[^-]+$/, '')
        .replace(/^['"]|['"]$/g, '')
        .trim();

    if (!cleanedTitle) {
        return '';
    }

    const translated = translatePrivateCreditHeadline(cleanedTitle, actor);
    if (translated) {
        return translated;
    }

    if (actor) {
        if (containsLongEnglishFragment(cleanedTitle)) {
            return '';
        }
        const actorPrefix = `{{${actor}}}`;
        if (cleanedTitle.startsWith(actorPrefix)) {
            return cleanedTitle.slice(0, 35);
        }
        return `${actorPrefix} ${cleanedTitle}`.slice(0, 35);
    }

    if (containsLongEnglishFragment(cleanedTitle)) {
        return '';
    }

    return cleanedTitle.slice(0, 35);
}

function translatePrivateCreditHeadline(title: string, actor: string | null) {
    const normalized = title.toLowerCase();

    if (
        normalized.includes('treasury')
        && (normalized.includes('insurance regulator') || normalized.includes('insurance regulators'))
        && normalized.includes('private credit')
    ) {
        return '{{美国财政部}}与保险监管机构讨论私募信贷风险';
    }

    if (
        normalized.includes('fed watching')
        && normalized.includes('private credit')
        && normalized.includes('powell')
    ) {
        return '{{美联储}}称正在关注私募信贷潜在压力';
    }

    if (
        normalized.includes('powell says')
        && normalized.includes('private credit')
        && normalized.includes('signs of trouble')
    ) {
        return '{{美联储}}称正在关注私募信贷潜在压力';
    }

    if (
        normalized.includes('apollo')
        && normalized.includes('private credit fund')
        && normalized.includes('withdraw')
        && (normalized.includes('limit') || normalized.includes('limits'))
    ) {
        return '{{Apollo}} 限制投资者赎回';
    }

    if (
        normalized.includes('ares')
        && normalized.includes('private credit fund')
        && normalized.includes('withdraw')
        && (normalized.includes('limit') || normalized.includes('limits'))
    ) {
        return '{{Ares}} 限制赎回请求';
    }

    if (
        normalized.includes('redemptions surge')
        && normalized.includes('ares')
        && normalized.includes('withdraw')
    ) {
        return '{{Ares}} 限制赎回请求';
    }

    if (
        normalized.includes('private credit')
        && normalized.includes('wait to pull out')
        && (normalized.includes('$5 billion') || normalized.includes('5 billion'))
    ) {
        return '{{投资者资金}}被锁约50亿美元';
    }

    if (
        normalized.includes('trapped in private credit')
        && (normalized.includes('$5 billion') || normalized.includes('5 billion'))
    ) {
        return '{{投资者资金}}被锁约50亿美元';
    }

    if (
        normalized.includes('private credit funds halt withdraw')
        || (normalized.includes('private credit funds') && normalized.includes('halt') && normalized.includes('withdraw'))
    ) {
        return '{{私募信贷基金}}多只产品暂停提款或赎回';
    }

    if (
        actor
        && normalized.includes('redemption')
        && normalized.includes('surge')
    ) {
        return `{{${actor}}} 赎回申请激增`;
    }

    return '';
}

function containsLongEnglishFragment(text: string) {
    return /[A-Za-z]{4,}\s+[A-Za-z]{4,}/.test(text);
}

function sanitizeGeneratedPrivateCreditHeadline(headline: string) {
    const normalizedMarkers = headline.replace(/【([^】]+)】/g, '{{$1}}');
    const trimmed = normalizedMarkers.replace(/\s+/g, ' ').trim();
    if (!trimmed) {
        return '';
    }

    if (containsLongEnglishFragment(trimmed)) {
        return '';
    }

    const chineseMatches = trimmed.match(/[\u4e00-\u9fff]/g) ?? [];
    if (chineseMatches.length < 4) {
        return '';
    }

    return trimmed;
}

function extractPrivateCreditActor(title: string) {
    const normalized = title.toLowerCase();
    const actorMap: Array<[string, string]> = [
        ['apollo', 'Apollo'],
        ['ares', 'Ares'],
        ['blackstone', 'Blackstone'],
        ['blue owl', 'Blue Owl'],
        ['jpmorgan', 'JPMorgan'],
        ['morgan stanley', 'Morgan Stanley'],
        ['goldman', 'Goldman'],
        ['blackrock', 'BlackRock'],
        ['bdc', 'BDC'],
        ['insurer', '保险机构'],
        ['pension', '养老金']
    ];

    const found = actorMap.find(([keyword]) => normalized.includes(keyword));
    return found?.[1] ?? null;
}

function buildFallbackWhatChangedGroups(newsItems: NewsItem[]): WhatChangedGroup[] {
    const fixedGroups = [
        { group_label: '霍尔木兹海峡', group_icon: '🛢️' },
        { group_label: '军事动态', group_icon: '🔴' },
        { group_label: '外交进展', group_icon: '🕊️' }
    ] as const;

    return fixedGroups.map((group) => ({
        group_label: group.group_label,
        group_icon: group.group_icon,
        items: buildFallbackWhatChangedItems(newsItems, group.group_label)
    }));
}

function sanitizeGeneratedMiddleEastHeadline(headline: string) {
    const trimmed = headline.replace(/\s+/g, ' ').trim();
    if (!trimmed || isBadMiddleEastHeadline(trimmed)) {
        return '';
    }

    return trimmed;
}

function isBadMiddleEastHeadline(headline: string) {
    const normalized = headline.replace(/\s+/g, ' ').trim();
    if (!normalized) {
        return true;
    }

    const lower = normalized.toLowerCase();
    const chineseMatches = normalized.match(/[\u4e00-\u9fff]/g) ?? [];
    const latinWordMatches = normalized.match(/[A-Za-z]{3,}/g) ?? [];
    const contentAfterActor = normalized.replace(/^【[^】]+】/, '').trim();
    const blockedFragments = [
        'bitcoin',
        'crypto',
        'the latest',
        'ticks up',
        'market update',
        'live updates',
        'breaking news'
    ];

    if (blockedFragments.some((fragment) => lower.includes(fragment))) {
        return true;
    }

    if (chineseMatches.length < 4) {
        return true;
    }

    if (latinWordMatches.length >= 3) {
        return true;
    }

    if (/[A-Za-z]{3,}\s+[A-Za-z]{3,}/.test(contentAfterActor)) {
        return true;
    }

    return false;
}

function buildFallbackWhatChangedItems(
    newsItems: NewsItem[],
    groupLabel: '霍尔木兹海峡' | '军事动态' | '外交进展'
) {
    return newsItems
        .filter((item) => classifyWhatChangedGroup(item.title) === groupLabel)
        .map((item) => ({
            time: formatClockTime(item.published_at),
            headline: buildFallbackWhatChangedHeadline(item.title)
        }))
        .filter((item) => item.headline)
        .slice(0, 4);
}

function mergeWhatChangedItems(
    parsedItems: Array<{ time: string; headline: string }>,
    fallbackItems: Array<{ time: string; headline: string }>
) {
    const merged: Array<{ time: string; headline: string }> = [];
    const seen = new Set<string>();

    for (const item of [...parsedItems, ...fallbackItems]) {
        const normalized = item.headline.replace(/\s+/g, '').toLowerCase();
        if (!normalized || seen.has(normalized)) {
            continue;
        }
        seen.add(normalized);
        merged.push(item);
        if (merged.length >= 4) {
            break;
        }
    }

    return merged;
}

function classifyWhatChangedGroup(title: string): '霍尔木兹海峡' | '军事动态' | '外交进展' | null {
    const normalized = title.toLowerCase();

    const energyKeywords = [
        'hormuz',
        'strait',
        'tanker',
        'shipping',
        'passage',
        'transit',
        'fee',
        'licensing',
        'permit',
        'pipeline',
        'yanbu',
        'aramco',
        'throughput',
        'barrel',
        'oil',
        'wti',
        'brent',
        'fifth fleet'
    ];
    if (energyKeywords.some((keyword) => normalized.includes(keyword))) {
        return '霍尔木兹海峡';
    }

    const diplomacyKeywords = [
        'ceasefire',
        'negotiation',
        'negotiations',
        'talks',
        'dialogue',
        'foreign minister',
        'foreign ministers',
        'islamabad',
        'mediated',
        'committee',
        'agreement',
        'proposal',
        'deal',
        'saudi',
        'turkey',
        'egypt',
        'pakistan',
        'white house'
    ];
    if (diplomacyKeywords.some((keyword) => normalized.includes(keyword))) {
        return '外交进展';
    }

    const militaryKeywords = [
        'strike',
        'attack',
        'airstrike',
        'drone',
        'missile',
        'killed',
        'casualties',
        'escalat',
        'bombed',
        'destroyed',
        'combat',
        'threatened',
        'warned',
        'air defense',
        'gulf',
        'troops',
        'forces',
        'base',
        'facility',
        'refinery',
        'petrochemical',
        'nuclear',
        'tabriz',
        'haifa',
        'azraq',
        'prince sultan',
        'houthis',
        'bab el-mandeb',
        'revolutionary guard',
        'irgc'
    ];
    if (militaryKeywords.some((keyword) => normalized.includes(keyword))) {
        return '军事动态';
    }

    return null;
}

function buildFallbackWhatChangedHeadline(title: string) {
    const actor = extractMiddleEastActor(title);
    const cleanedTitle = title
        .replace(/\s+-\s+[^-]+$/, '')
        .replace(/^['"]|['"]$/g, '')
        .trim();

    if (!cleanedTitle) {
        return '';
    }

    const candidate = actor
        ? (() => {
        const actorPrefix = `【${actor}】`;
        if (cleanedTitle.startsWith(actorPrefix)) {
            return cleanedTitle.slice(0, 35);
        }
        return `${actorPrefix}${cleanedTitle}`.slice(0, 35);
        })()
        : cleanedTitle.slice(0, 35);

    return isBadMiddleEastHeadline(candidate) ? '' : candidate;
}

function summarizeMiddleEastHeadline(title: string, source?: string): string {
    const normalized = title.toLowerCase();
    const actor = extractMiddleEastActor(title);
    const location = extractMiddleEastLocation(title);

    if (normalized.includes('bushehr') && (normalized.includes('struck') || normalized.includes('attack'))) {
        return 'Bushehr 核电站再传遇袭，战局升级。';
    }
    if (
        normalized.includes('hormuz')
        || normalized.includes('strait')
        || normalized.includes('shipping')
        || normalized.includes('tanker')
        || normalized.includes('maritime')
        || normalized.includes('blockade')
        || normalized.includes('closure')
    ) {
        const status = inferHormuzStatus(normalized);
        const impact = inferShippingImpact(normalized);
        if (status && impact) {
            return `霍尔木兹${status}，航运${impact}。`;
        }
        return `${actor ?? '海峡局势'} ${inferFallbackAction(normalized) ?? '再起波动'}，影响待定。`;
    }
    if (normalized.includes('ceasefire') || normalized.includes('truce')) {
        const result = inferCeasefireResult(normalized);
        if (result) {
            return `${actor ?? '相关各方'}停火提议${result}。`;
        }
        return `${actor ?? '相关各方'} ${inferFallbackAction(normalized) ?? '停火博弈加剧'}，影响待定。`;
    }
    if (
        normalized.includes('no confirmed date or venue')
        || (normalized.includes('has not yet agreed to talks') && normalized.includes('pakistani officials'))
    ) {
        return '巴方称美伊潜在会谈时间地点未定，伊朗尚未同意谈判。';
    }
    if (
        normalized.includes('talks')
        || normalized.includes('meeting')
        || normalized.includes('delegation')
        || normalized.includes('mediator')
        || normalized.includes('negotiate')
    ) {
        const result = inferTalkResult(normalized);
        if (result) {
            return `${actor ?? '相关各方'}会谈${result}。`;
        }
        return `${actor ?? '相关各方'} ${inferFallbackAction(normalized) ?? '会谈动向未明'}，影响待定。`;
    }
    if (normalized.includes('war damage') && (normalized.includes('demands') || normalized.includes('payment'))) {
        return '伊朗官员要求明确追责，并保证战争损失赔偿。';
    }
    if (normalized.includes('insurance') || normalized.includes('premium') || normalized.includes('freight') || normalized.includes('shipping risk')) {
        const change = inferInsuranceChange(normalized);
        const reason = inferInsuranceReason(normalized);
        if (change && reason) {
            return `航运保费${change}，${reason}。`;
        }
        return `${actor ?? '航运市场'} ${inferFallbackAction(normalized) ?? '风险抬升'}，影响待定。`;
    }
    if (normalized.includes('naval') || normalized.includes('fleet') || normalized.includes('warship') || normalized.includes('carrier')) {
        const action = inferNavalAction(normalized);
        const purpose = inferNavalPurpose(normalized);
        if (action && purpose) {
            return `${actor ?? '相关国家'}海军${action}，${purpose}。`;
        }
        return `${actor ?? '相关国家'} ${inferFallbackAction(normalized) ?? '海军调动增强'}，影响待定。`;
    }
    if (normalized.includes('humanitarian') || normalized.includes('corridor') || normalized.includes('evacuation')) {
        const status = inferHumanitarianStatus(normalized);
        if (status) {
            return `${location ?? '相关地区'}人道走廊${status}。`;
        }
        return `${location ?? actor ?? '相关地区'} ${inferFallbackAction(normalized) ?? '人道安排变化'}，影响待定。`;
    }
    if (normalized.includes('official') && normalized.includes('iran') && normalized.includes('talks')) {
        return '伊朗方面就潜在谈判释放谨慎表态，停火路径仍未明朗。';
    }
    if (normalized.includes('strike') || normalized.includes('attacked') || normalized.includes('missile')) {
        return '中东局势再现军事冲击，市场继续交易战局升级风险。';
    }
    if (normalized.includes('talks') || normalized.includes('venue') || normalized.includes('date')) {
        return '美伊接触仍停留在试探阶段，正式会谈安排未落实。';
    }
    if (normalized.includes('official') || normalized.includes('demands') || normalized.includes('demand')) {
        return '伊朗官方表态转强，市场继续关注谈判与追责条件。';
    }
    if ((source ?? '').toLowerCase().includes('reuters') && normalized.includes('iran')) {
        return '路透跟进称相关磋商仍未落定，局势缓和缺乏实质确认。';
    }

    return '';
}

function extractMiddleEastActor(title: string) {
    const normalized = title.toLowerCase();
    if (normalized.includes('iran')) {
        return '伊朗';
    }
    if (normalized.includes('israel')) {
        return '以色列';
    }
    if (normalized.includes('u.s.') || normalized.includes('us ') || normalized.includes('united states')) {
        return '美国';
    }
    if (normalized.includes('pakistan')) {
        return '巴基斯坦';
    }
    if (normalized.includes('qatar')) {
        return '卡塔尔';
    }
    return null;
}

function extractMiddleEastLocation(title: string) {
    const normalized = title.toLowerCase();
    if (normalized.includes('gaza')) {
        return '加沙';
    }
    if (normalized.includes('hormuz') || normalized.includes('strait')) {
        return '霍尔木兹';
    }
    if (normalized.includes('lebanon')) {
        return '黎巴嫩';
    }
    return null;
}

function inferHormuzStatus(normalized: string) {
    if (normalized.includes('blockade') || normalized.includes('blocked')) {
        return '封锁风险上升';
    }
    if (normalized.includes('closure') || normalized.includes('close')) {
        return '关闭风险升温';
    }
    if (normalized.includes('disrupt') || normalized.includes('halt') || normalized.includes('divert')) {
        return '运输受扰';
    }
    return null;
}

function inferShippingImpact(normalized: string) {
    if (normalized.includes('disrupt') || normalized.includes('divert') || normalized.includes('delay')) {
        return '受扰';
    }
    if (normalized.includes('risk') || normalized.includes('threat')) {
        return '风险上升';
    }
    if (normalized.includes('premium') || normalized.includes('insurance')) {
        return '成本抬升';
    }
    return null;
}

function inferCeasefireResult(normalized: string) {
    if (normalized.includes('reject') || normalized.includes('refuse')) {
        return '被拒';
    }
    if (normalized.includes('delay') || normalized.includes('postpone')) {
        return '被推迟';
    }
    if (normalized.includes('propose') || normalized.includes('offer')) {
        return '提出';
    }
    return null;
}

function inferTalkResult(normalized: string) {
    if (normalized.includes('cancel')) {
        return '取消';
    }
    if (normalized.includes('delay') || normalized.includes('postpone')) {
        return '推迟';
    }
    if (normalized.includes('no confirmed date or venue') || normalized.includes('not yet agreed')) {
        return '未定';
    }
    if (normalized.includes('resume') || normalized.includes('held') || normalized.includes('meet')) {
        return '重启';
    }
    return null;
}

function inferInsuranceChange(normalized: string) {
    if (normalized.includes('rise') || normalized.includes('higher') || normalized.includes('jump') || normalized.includes('surge')) {
        return '上升';
    }
    if (normalized.includes('fall') || normalized.includes('drop')) {
        return '回落';
    }
    return null;
}

function inferInsuranceReason(normalized: string) {
    if (normalized.includes('hormuz') || normalized.includes('strait')) {
        return '海峡风险升温';
    }
    if (normalized.includes('attack') || normalized.includes('strike')) {
        return '战局升级';
    }
    return null;
}

function inferNavalAction(normalized: string) {
    if (normalized.includes('deploy') || normalized.includes('deployment')) {
        return '部署增强';
    }
    if (normalized.includes('move') || normalized.includes('send')) {
        return '调动加快';
    }
    if (normalized.includes('escort') || normalized.includes('patrol')) {
        return '护航加强';
    }
    return null;
}

function inferNavalPurpose(normalized: string) {
    if (normalized.includes('protect') || normalized.includes('secure')) {
        return '意在保障航运安全';
    }
    if (normalized.includes('deterr') || normalized.includes('warn')) {
        return '意在增强威慑';
    }
    return '意在稳定局势';
}

function inferHumanitarianStatus(normalized: string) {
    if (normalized.includes('open') || normalized.includes('opened')) {
        return '开放';
    }
    if (normalized.includes('close') || normalized.includes('closed')) {
        return '关闭';
    }
    if (normalized.includes('delay') || normalized.includes('stall')) {
        return '受阻';
    }
    if (normalized.includes('evacuation')) {
        return '启动撤离';
    }
    return null;
}

function inferFallbackAction(normalized: string) {
    if (normalized.includes('reject')) {
        return '拒绝相关安排';
    }
    if (normalized.includes('delay') || normalized.includes('postpone')) {
        return '进展推迟';
    }
    if (normalized.includes('deploy') || normalized.includes('naval') || normalized.includes('fleet')) {
        return '军事部署加强';
    }
    if (normalized.includes('shipping') || normalized.includes('tanker')) {
        return '航运风险上行';
    }
    return null;
}

function buildFallbackTransmission(topic: FocusTopicConfig): ClientFocusTransmissionItem[] {
    const defaults: Record<string, ClientFocusTransmissionItem[]> = {
        'middle-east-tensions': [
            {
                order: '一阶传导',
                title: '油价上涨与风险偏好回落',
                pricing: '已定价',
                summary: '市场会先交易能源供应扰动、油价上行和风险偏好降温。'
            },
            {
                order: '二阶传导',
                title: '通胀与利率路径被重新评估',
                pricing: '部分定价',
                summary: '若高油价持续，市场会进一步推迟降息预期，并把压力传导到成长股、黄金和港股。'
            }
        ],
        'private-credit-stress': [
            {
                order: '一阶传导',
                title: 'BDC赎回潮与资产折价',
                pricing: '已定价',
                summary: 'BDC股价折价与赎回封顶、资产减记已充分反映在市场价格中。'
            },
            {
                order: '二阶传导',
                title: '银行收紧BDC信贷额度',
                pricing: '未充分定价',
                summary: '若银行收紧BDC信贷额度，中小企业融资环境收紧的传导尚未充分定价。'
            }
        ],
        'gold-repricing': [
            {
                order: '一阶传导',
                title: '实际利率上行压制金价',
                pricing: '已定价',
                summary: '美元走强、实际利率抬升与ETF净流出已充分反映在金价中。'
            },
            {
                order: '二阶传导',
                title: '央行购金节奏放缓',
                pricing: '部分定价',
                summary: '央行购金节奏边际放缓对黄金结构性买盘的削弱仍属部分定价。'
            }
        ],
        'usd-strength': [
            {
                order: '一阶传导',
                title: '人民币与非美资产承压',
                pricing: '已定价',
                summary: '美元反弹已反映在USDCNH走高、港股估值回落与非美风险偏好降温上。'
            },
            {
                order: '二阶传导',
                title: '港元流动性收紧与估值双压',
                pricing: '部分定价',
                summary: '联系汇率机制下本地流动性收紧对港股估值的压制仍属部分定价。'
            }
        ],
        'hk-market-sentiment': [
            {
                order: '一阶传导',
                title: '资金情绪主导指数波动',
                pricing: '已定价',
                summary: '港股短线表现先反映在恒指与恒生科技的情绪波动和估值收缩上。'
            },
            {
                order: '二阶传导',
                title: '盈利验证决定持续性',
                pricing: '部分定价',
                summary: '若盈利兑现跟不上资金回流节奏，科技与平台股估值弹性会继续受限。'
            }
        ]
    };

    return defaults[topic.slug] ?? [];
}

function buildMiddleEastTransmissionChain(newsItems: NewsItem[]): ClientFocusTransmissionItem[] {
    const titles = newsItems
        .filter((item) => isMiddleEastHardNews(item))
        .slice(0, 6)
        .map((item) => item.title.toLowerCase());

    const easingSignals = titles.filter((title) =>
        title.includes('ceasefire')
        || title.includes('truce')
        || title.includes('talks')
        || title.includes('meeting')
        || title.includes('mediator')
        || title.includes('delegation')
        || title.includes('humanitarian corridor')
        || title.includes('evacuation')
    ).length;

    const escalationSignals = titles.filter((title) =>
        title.includes('strike')
        || title.includes('attack')
        || title.includes('missile')
        || title.includes('drone')
        || title.includes('hormuz')
        || title.includes('strait')
        || title.includes('shipping')
        || title.includes('tanker')
        || title.includes('blockade')
        || title.includes('closure')
        || title.includes('naval')
        || title.includes('fleet')
    ).length;

    const firstOrderPricing: ClientFocusTransmissionItem['pricing'] =
        easingSignals > escalationSignals && easingSignals >= 2 ? '部分定价' : '已定价';
    const secondOrderPricing: ClientFocusTransmissionItem['pricing'] =
        easingSignals >= 2 && easingSignals > escalationSignals ? '未充分定价' : '部分定价';

    const firstOrderSummary =
        firstOrderPricing === '已定价'
            ? '市场已交易原油、能源股与航运风险溢价的同步抬升。'
            : '市场正重估原油、能源股与航运风险溢价的回落空间。';
    const secondOrderSummary =
        secondOrderPricing === '未充分定价'
            ? '加息叙事回摆尚未全面反映，高估值资产的修复路径仍待确认。'
            : '油价冲击正把利率预期从不降息推向可能加息，并压缩高估值资产定价。';
    const firstOrderEvidence =
        firstOrderPricing === '已定价'
            ? '能源链与航运风险溢价已被交易'
            : '缓和预期正触发能源链定价回摆';
    const secondOrderEvidence =
        secondOrderPricing === '未充分定价'
            ? '加息叙事的回摆尚未完全反映'
            : '利率路径定价正向更鹰派切换';

    return [
        {
            order: '一阶传导',
            title: '能源链定价抬升',
            pricing: firstOrderPricing,
            summary: firstOrderSummary,
            latest_evidence: firstOrderEvidence
        },
        {
            order: '二阶传导',
            title: '加息叙事重新抬头',
            pricing: secondOrderPricing,
            summary: secondOrderSummary,
            latest_evidence: secondOrderEvidence
        }
    ];
}

function buildPrivateCreditTransmissionChain(newsItems: NewsItem[]): ClientFocusTransmissionItem[] {
    const titles = newsItems.slice(0, 8).map((item) => item.title.toLowerCase());
    const easingSignals = titles.filter((title) =>
        title.includes('stabilize')
        || title.includes('recovery')
        || title.includes('improve')
        || title.includes('support')
    ).length;
    const bankTighteningSignals = titles.filter((title) =>
        (title.includes('bank') || title.includes('lender') || title.includes('warehouse'))
        && (
            title.includes('tighten')
            || title.includes('cut')
            || title.includes('reduce')
            || title.includes('pull back')
            || title.includes('credit line')
            || title.includes('facility')
            || title.includes('financing')
        )
    ).length;

    const firstOrderPricing: ClientFocusTransmissionItem['pricing'] =
        easingSignals >= 2 ? '部分定价' : '已定价';
    const secondOrderPricing: ClientFocusTransmissionItem['pricing'] =
        bankTighteningSignals >= 2 ? '部分定价' : '未充分定价';

    return [
        {
            order: '一阶传导',
            title: 'BDC赎回潮与资产折价',
            pricing: firstOrderPricing,
            summary:
                firstOrderPricing === '已定价'
                    ? 'BDC股价折价与赎回封顶、资产减记已充分反映在市场价格中。'
                    : 'BDC折价与赎回封顶压力仍在，但市场开始交易其边际缓和空间。',
            latest_evidence:
                firstOrderPricing === '已定价'
                    ? 'BDC折价与减记压力已显性化'
                    : '折价压力边际缓和但未完全修复'
        },
        {
            order: '二阶传导',
            title: '银行收紧BDC信贷额度',
            pricing: secondOrderPricing,
            summary:
                secondOrderPricing === '未充分定价'
                    ? '若银行收紧BDC信贷额度，中小企业融资环境收紧的传导尚未充分定价。'
                    : '银行收紧BDC信贷额度后，新增贷款收缩对中小企业融资已开始被交易。',
            latest_evidence:
                secondOrderPricing === '未充分定价'
                    ? '银行端额度收紧尚未形成主流定价'
                    : '银行额度与融资收缩风险开始被计入'
        }
    ];
}

function buildGoldTransmissionChain(newsItems: NewsItem[]): ClientFocusTransmissionItem[] {
    const titles = newsItems.slice(0, 8).map((item) => item.title.toLowerCase());
    const reliefSignals = titles.filter((title) =>
        title.includes('yield falls')
        || title.includes('dollar weak')
        || title.includes('etf inflow')
        || title.includes('gold rebounds')
    ).length;
    const cbBuyingSlowdownSignals = titles.filter((title) =>
        (title.includes('central bank') || title.includes('央行') || title.includes('jp morgan') || title.includes('j.p. morgan'))
        && (
            title.includes('buying slows')
            || title.includes('slows')
            || title.includes('pause')
            || title.includes('reduced')
            || title.includes('755')
            || title.includes('1000')
            || title.includes('net seller')
        )
    ).length;

    const firstOrderPricing: ClientFocusTransmissionItem['pricing'] =
        reliefSignals >= 2 ? '部分定价' : '已定价';
    const secondOrderPricing: ClientFocusTransmissionItem['pricing'] =
        cbBuyingSlowdownSignals >= 2 ? '已定价' : '部分定价';

    return [
        {
            order: '一阶传导',
            title: '实际利率上行压制金价',
            pricing: firstOrderPricing,
            summary:
                firstOrderPricing === '已定价'
                    ? '美元走强、实际利率抬升与ETF净流出已充分反映在金价中。'
                    : '美元与实际利率压力边际缓和，金价对利率敏感性的回摆正在被重估。',
            latest_evidence:
                firstOrderPricing === '已定价'
                    ? '实际利率与ETF流向已压制金价'
                    : '利率与美元压力边际回落'
        },
        {
            order: '二阶传导',
            title: '央行购金节奏放缓',
            pricing: secondOrderPricing,
            summary:
                secondOrderPricing === '部分定价'
                    ? '央行购金节奏边际放缓对黄金结构性买盘的削弱仍属部分定价。'
                    : '央行购金放缓与买盘边际转弱已更充分反映在黄金中期定价中。',
            latest_evidence:
                secondOrderPricing === '部分定价'
                    ? '央行购金放缓预期仍在扩散'
                    : '央行购金回落已进入主流定价'
        }
    ];
}

function buildUsdReboundTransmissionChain(newsItems: NewsItem[]): ClientFocusTransmissionItem[] {
    const titles = newsItems.slice(0, 8).map((item) => item.title.toLowerCase());
    const dollarEasingSignals = titles.filter((title) =>
        title.includes('dollar falls')
        || title.includes('cnh rebounds')
        || title.includes('risk appetite returns')
        || title.includes('hkd liquidity improves')
    ).length;
    const hkLiquiditySignals = titles.filter((title) =>
        (title.includes('hkma') || title.includes('hong kong') || title.includes('peg'))
        && (
            title.includes('liquidity')
            || title.includes('funding')
            || title.includes('interbank')
            || title.includes('tighten')
            || title.includes('capital outflow')
            || title.includes('outflow')
        )
    ).length;

    const firstOrderPricing: ClientFocusTransmissionItem['pricing'] =
        dollarEasingSignals >= 2 ? '部分定价' : '已定价';
    const secondOrderPricing: ClientFocusTransmissionItem['pricing'] =
        hkLiquiditySignals >= 2 ? '已定价' : '部分定价';

    return [
        {
            order: '一阶传导',
            title: '人民币与非美资产承压',
            pricing: firstOrderPricing,
            summary:
                firstOrderPricing === '已定价'
                    ? '美元反弹已反映在USDCNH走高、港股估值回落与非美风险偏好降温上。'
                    : '美元反弹压力边际缓和，人民币与非美资产的修复空间正在被重估。',
            latest_evidence:
                firstOrderPricing === '已定价'
                    ? 'USDCNH与非美估值压力已反映'
                    : '美元反弹压力边际缓和'
        },
        {
            order: '二阶传导',
            title: '港元流动性收紧与估值双压',
            pricing: secondOrderPricing,
            summary:
                secondOrderPricing === '部分定价'
                    ? '联系汇率机制下本地流动性收紧对港股估值的压制仍属部分定价。'
                    : 'HKMA收紧流动性与外资流出压力对港股估值的双压已被更充分交易。',
            latest_evidence:
                secondOrderPricing === '部分定价'
                    ? '本地流动性收紧传导仍在扩散'
                    : '港元流动性与估值双压已被交易'
        }
    ];
}

function safeParseJson(content: string): FocusTopicModelOutput | null {
    const trimmed = content.trim();
    if (!trimmed) {
        return null;
    }

    try {
        return JSON.parse(trimmed) as FocusTopicModelOutput;
    } catch {
        const start = trimmed.indexOf('{');
        const end = trimmed.lastIndexOf('}');
        if (start === -1 || end === -1 || end <= start) {
            return null;
        }
        try {
            return JSON.parse(trimmed.slice(start, end + 1)) as FocusTopicModelOutput;
        } catch {
            return null;
        }
    }
}

function safeParseJsonArray(content: string): unknown[] | null {
    const trimmed = content.trim();
    if (!trimmed) {
        return null;
    }

    const stripped = trimmed.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();

    try {
        const parsed = JSON.parse(stripped);
        if (Array.isArray(parsed)) {
            return parsed;
        }
        if (parsed && typeof parsed === 'object') {
            const firstArray = Object.values(parsed).find(Array.isArray);
            if (firstArray) {
                return firstArray as unknown[];
            }
        }
        return null;
    } catch {
        const start = stripped.indexOf('[');
        const end = stripped.lastIndexOf(']');
        if (start === -1 || end === -1 || end <= start) {
            return null;
        }
        try {
            const parsed = JSON.parse(stripped.slice(start, end + 1));
            return Array.isArray(parsed) ? parsed : null;
        } catch {
            return null;
        }
    }
}

async function fetchSouthboundFlowChart(): Promise<ClientFocusMarketChart | null> {
    const cached = focusMarketChartCache.get('hk-market-sentiment');
    if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
    }

    try {
        const url = new URL('https://datacenter-web.eastmoney.com/api/data/v1/get');
        url.searchParams.set('sortColumns', 'TRADE_DATE');
        url.searchParams.set('sortTypes', '-1');
        url.searchParams.set('pageSize', '80');
        url.searchParams.set('pageNumber', '1');
        url.searchParams.set('reportName', 'RPT_MUTUAL_DEAL_HISTORY');
        url.searchParams.set('columns', 'ALL');
        url.searchParams.set('source', 'WEB');
        url.searchParams.set('client', 'WEB');
        url.searchParams.set('filter', '(MUTUAL_TYPE="006")');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            result?: {
                data?: EastMoneySouthboundRow[];
            };
        };
        const rows = Array.isArray(payload.result?.data) ? payload.result?.data ?? [] : [];
        const rawPoints = rows
            .map((row) => {
                const tradeDate = typeof row.TRADE_DATE === 'string' ? row.TRADE_DATE.slice(0, 10) : '';
                const rawNetBuy = Number(row.NET_DEAL_AMT);
                return {
                    date: tradeDate,
                    net_buy: Number.isFinite(rawNetBuy) ? rawNetBuy / 100 : null
                };
            })
            .filter((item) => item.date && item.net_buy !== null)
            .sort((left, right) => left.date.localeCompare(right.date))
            .slice(-60);

        if (rawPoints.length === 0) {
            return null;
        }

        const hsiHistory = await fetchHongKongIndexHistory('HSI', 90);
        const hsiMap = new Map(hsiHistory.map((item) => [item.date, item.close]));
        const sortedHsiDates = hsiHistory.map((item) => item.date).sort();
        const getHsiCloseFillForward = (date: string): number | null => {
            if (hsiMap.has(date)) {
                return hsiMap.get(date) ?? null;
            }
            const prev = sortedHsiDates.filter((item) => item <= date).at(-1);
            return prev ? (hsiMap.get(prev) ?? null) : null;
        };
        const points = rawPoints.map((point) => ({
            date: point.date,
            net_buy: point.net_buy,
            hsi_close: getHsiCloseFillForward(point.date)
        }));

        const chart: ClientFocusMarketChart = {
            series_name: '南向资金',
            unit: '亿元',
            latest_trade_date: points[points.length - 1]?.date ?? null,
            points,
            stats: {
                latest_net_buy: points[points.length - 1]?.net_buy ?? null,
                sum_10d: sumRecentNetBuy(points, 10),
                sum_20d: sumRecentNetBuy(points, 20),
                sum_60d: sumRecentNetBuy(points, 60)
            }
        };

        focusMarketChartCache.set('hk-market-sentiment', {
            expiresAt: Date.now() + FOCUS_LIVE_MARKET_CACHE_TTL_MS,
            value: chart
        });

        return chart;
    } catch {
        return null;
    }
}

function sumRecentNetBuy(points: Array<{ net_buy: number | null }>, length: number) {
    const recent = points.slice(-length).map((point) => point.net_buy ?? 0);
    return Number(recent.reduce((sum, value) => sum + value, 0).toFixed(2));
}

const hkIndexSecIdCache = new Map<string, string>();

async function fetchHongKongIndexSecId(code: string) {
    const cached = hkIndexSecIdCache.get(code);
    if (cached) {
        return cached;
    }

    const knownSecId = KNOWN_HK_INDEX_SECIDS[code.toUpperCase()];
    if (knownSecId) {
        hkIndexSecIdCache.set(code, knownSecId);
        return knownSecId;
    }

    try {
        const url = new URL('https://15.push2.eastmoney.com/api/qt/clist/get');
        url.searchParams.set('pn', '1');
        url.searchParams.set('pz', '200');
        url.searchParams.set('po', '1');
        url.searchParams.set('np', '1');
        url.searchParams.set('ut', 'bd1d9ddb04089700cf9c27f6f7426281');
        url.searchParams.set('fltt', '2');
        url.searchParams.set('invt', '2');
        url.searchParams.set('fid', 'f3');
        url.searchParams.set('fs', 'm:124,m:125,m:305');
        url.searchParams.set('fields', 'f12,f13');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            data?: {
                diff?: Array<{ f12?: string; f13?: number | string }>;
            };
        };
        const rows = Array.isArray(payload.data?.diff) ? payload.data?.diff ?? [] : [];
        const match = rows.find((item) => String(item.f12 ?? '').toUpperCase() === code.toUpperCase());
        const marketId = match?.f13;
        if (marketId === undefined || marketId === null || marketId === '') {
            return null;
        }

        const secId = `${marketId}.${code}`;
        hkIndexSecIdCache.set(code, secId);
        return secId;
    } catch {
        return null;
    }
}

async function fetchHongKongIndexHistory(code: string, limit: number) {
    async function fetchHongKongIndexHistoryFromYahoo(indexCode: string, historyLimit: number) {
        try {
            const yahooTicker = indexCode === 'HSI' ? '%5EHSI' : indexCode === 'HSTECH' ? '%5EHSTECH' : null;
            if (!yahooTicker) {
                return [];
            }

            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${yahooTicker}?interval=1d&range=6mo&includePrePost=false`;
            const response = await fetch(url, {
                headers: { 'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)' }
            });
            if (!response.ok) {
                return [];
            }

            const payload = (await response.json()) as {
                chart?: {
                    result?: Array<{
                        timestamp?: number[];
                        indicators?: { quote?: Array<{ close?: Array<number | null> }> };
                    }>;
                };
            };
            const result = payload.chart?.result?.[0];
            const timestamps = Array.isArray(result?.timestamp) ? result?.timestamp ?? [] : [];
            const closes = Array.isArray(result?.indicators?.quote?.[0]?.close)
                ? result?.indicators?.quote?.[0]?.close ?? []
                : [];

            return timestamps
                .map((ts, index) => ({
                    date: new Date(ts * 1000).toISOString().slice(0, 10),
                    close: closes[index]
                }))
                .filter((item): item is { date: string; close: number } =>
                    Boolean(item.date) && typeof item.close === 'number' && Number.isFinite(item.close)
                )
                .slice(-historyLimit);
        } catch {
            return [];
        }
    }

    const requestHistory = async (resetSecId = false) => {
        if (resetSecId) {
            hkIndexSecIdCache.delete(code);
        }

        const secId = await fetchHongKongIndexSecId(code);
        if (!secId) {
            return [];
        }

        const url = new URL('https://push2his.eastmoney.com/api/qt/stock/kline/get');
        url.searchParams.set('secid', secId);
        url.searchParams.set('klt', '101');
        url.searchParams.set('fqt', '1');
        url.searchParams.set('lmt', String(limit));
        url.searchParams.set('end', '20500000');
        url.searchParams.set('iscca', '1');
        url.searchParams.set('fields1', 'f1,f2,f3,f4,f5,f6,f7,f8');
        url.searchParams.set('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64');
        url.searchParams.set('ut', 'f057cbcbce2a86e2866ab8877db1d059');
        url.searchParams.set('forcect', '1');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return [];
        }

        const payload = (await response.json()) as {
            data?: {
                klines?: string[];
            };
        };
        const klines = Array.isArray(payload.data?.klines) ? payload.data?.klines ?? [] : [];
        return klines
            .map((item) => {
                const [date, , close] = item.split(',');
                const closeValue = Number(close);
                return {
                    date,
                    close: Number.isFinite(closeValue) ? closeValue : null
                };
            })
            .filter((item): item is { date: string; close: number } => Boolean(item.date) && item.close !== null);
    };

    try {
        const primary = await requestHistory(false);
        if (primary.length > 0) {
            return primary;
        }
        const retry = await requestHistory(true);
        if (retry.length > 0) {
            return retry;
        }
        return fetchHongKongIndexHistoryFromYahoo(code, limit);
    } catch {
        return [];
    }
}

async function fetchHongKongMarketSnapshot(): Promise<ClientFocusMarketSnapshot | null> {
    try {
        const [indices, southboundChart] = await Promise.all([fetchHongKongSpotIndices(), fetchSouthboundFlowChart()]);

        if (indices.length === 0) {
            return null;
        }

        return {
            summary: buildHongKongSnapshotSummary(indices, southboundChart),
            indices: indices.map((item) => ({
                ...item,
                latest: Number.isFinite(item.latest) ? item.latest : null,
                change_pct: Number.isFinite(item.change_pct) ? item.change_pct : null,
                change_5d_pct: Number.isFinite(item.change_5d_pct) ? item.change_5d_pct : null
            }))
        };
    } catch {
        return null;
    }
}

async function fetchHiborSeries(indicator: '1月' | '3月') {
    try {
        const indicatorId = indicator === '1月' ? '201' : '203';
        const url = new URL('https://datacenter-web.eastmoney.com/api/data/v1/get');
        url.searchParams.set('reportName', 'RPT_IMP_INTRESTRATEN');
        url.searchParams.set('columns', 'REPORT_DATE,IR_RATE');
        url.searchParams.set('filter', `(MARKET_CODE="005")(CURRENCY_CODE="HKD")(INDICATOR_ID="${indicatorId}")`);
        url.searchParams.set('pageNumber', '1');
        url.searchParams.set('pageSize', '60');
        url.searchParams.set('sortTypes', '-1');
        url.searchParams.set('sortColumns', 'REPORT_DATE');
        url.searchParams.set('source', 'WEB');
        url.searchParams.set('client', 'WEB');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return [];
        }

        const payload = (await response.json()) as {
            result?: {
                data?: Array<{ REPORT_DATE?: string; IR_RATE?: number | string }>;
            };
        };
        const rows = Array.isArray(payload.result?.data) ? payload.result?.data ?? [] : [];
        return rows
            .map((row) => {
                const reportDate = typeof row.REPORT_DATE === 'string' ? row.REPORT_DATE.slice(0, 10) : '';
                const rate = Number(row.IR_RATE);
                return {
                    date: reportDate,
                    rate: Number.isFinite(rate) ? rate : null
                };
            })
            .filter((item): item is { date: string; rate: number } => Boolean(item.date) && item.rate !== null)
            .sort((left, right) => left.date.localeCompare(right.date));
    } catch {
        return [];
    }
}

function getBpsChangeFrom7Days(series: Array<{ date: string; rate: number }>) {
    if (series.length === 0) {
        return null;
    }

    const latest = series[series.length - 1];
    const targetTs = new Date(`${latest.date}T00:00:00Z`).getTime() - 7 * 24 * 60 * 60 * 1000;
    const previous = [...series].reverse().find((item) => new Date(`${item.date}T00:00:00Z`).getTime() <= targetTs);
    if (!previous) {
        return null;
    }

    return Number(((latest.rate - previous.rate) * 100).toFixed(0));
}

async function fetchHiborRates(): Promise<ClientFocusHibor | null> {
    const [series1m, series3m] = await Promise.all([
        fetchHiborSeries('1月'),
        fetchHiborSeries('3月')
    ]);

    const latest1m = series1m[series1m.length - 1] ?? null;
    const latest3m = series3m[series3m.length - 1] ?? null;
    const asOf = latest1m?.date ?? latest3m?.date ?? null;
    if (!asOf) {
        return null;
    }

    return {
        rate_1m: latest1m?.rate ?? null,
        rate_3m: latest3m?.rate ?? null,
        change_1m: getBpsChangeFrom7Days(series1m),
        change_3m: getBpsChangeFrom7Days(series3m),
        as_of: asOf
    };
}

async function fetchSectorRotation(): Promise<ClientFocusSectorRotation | null> {
    try {
        const url = new URL('https://push2.eastmoney.com/api/qt/clist/get');
        url.searchParams.set('pn', '1');
        url.searchParams.set('pz', '100');
        url.searchParams.set('po', '1');
        url.searchParams.set('np', '1');
        url.searchParams.set('ut', 'b2884a393a59ad64002292a3e90d46a5');
        url.searchParams.set('fltt', '2');
        url.searchParams.set('invt', '2');
        url.searchParams.set('fid0', 'f62');
        url.searchParams.set('fs', 'm:90 t:2');
        url.searchParams.set('stat', '1');
        url.searchParams.set('fields', 'f14,f3,f124');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            data?: {
                diff?: Array<{ f14?: string; f3?: number | string; f124?: number | string }>;
            };
        };
        const rows = Array.isArray(payload.data?.diff) ? payload.data?.diff ?? [] : [];
        const sectors = rows
            .map((row) => ({
                name: typeof row.f14 === 'string' ? row.f14.trim() : '',
                change_pct: Number(row.f3)
            }))
            .filter((item) => item.name && Number.isFinite(item.change_pct));

        if (sectors.length === 0) {
            return null;
        }

        const sorted = [...sectors].sort((left, right) => right.change_pct - left.change_pct);
        return {
            top: sorted.slice(0, 3),
            bottom: [...sorted].reverse().slice(0, 3),
            as_of: new Date().toISOString().slice(0, 10)
        };
    } catch {
        return null;
    }
}

async function fetchForexSnapshot(symbol: string, name: string): Promise<ClientFocusPriceSnapshot | null> {
    async function fetchYahooForexFallback(ticker: string): Promise<ClientFocusPriceSnapshot | null> {
        const { snapshot } = await fetchYahooChartSeries(ticker, {
            code: symbol,
            name
        });
        return snapshot ?? null;
    }

    const upperSymbol = symbol.toUpperCase();
    const yahooFallbackTicker =
        upperSymbol === 'USDCNH'
            ? 'CNH=X'
            : upperSymbol === 'USDJPY'
                ? 'JPY=X'
                : upperSymbol === 'USDCHF'
                    ? 'CHF=X'
                    : null;

    const yahooPrimary = yahooFallbackTicker
        ? await fetchYahooForexFallback(yahooFallbackTicker)
        : null;

    if (
        yahooPrimary?.latest !== null
        && (Number.isFinite(yahooPrimary?.change_pct) || upperSymbol !== 'USDCNH')
    ) {
        return yahooPrimary;
    }

    try {
        const url = new URL('https://push2.eastmoney.com/api/qt/clist/get');
        url.searchParams.set('pn', '1');
        url.searchParams.set('pz', '500');
        url.searchParams.set('po', '1');
        url.searchParams.set('np', '1');
        url.searchParams.set('ut', 'bd1d9ddb04089700cf9c27f6f7426281');
        url.searchParams.set('fltt', '2');
        url.searchParams.set('invt', '2');
        url.searchParams.set('fid', 'f3');
        url.searchParams.set('fs', 'm:106,m:107,m:108');
        url.searchParams.set('fields', 'f12,f2,f3,f124');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            data?: {
                diff?: Array<{ f12?: string; f2?: number | string; f3?: number | string; f124?: number | string }>;
            };
        };
        const rows = Array.isArray(payload.data?.diff) ? payload.data?.diff ?? [] : [];
        const match = rows.find((item) => String(item.f12 ?? '').toUpperCase() === symbol.toUpperCase());
        if (!match) {
            if (upperSymbol === 'USDCNH') {
                const history = await fetchForexHistory('USDCNH');
                const latestHistory = history.length >= 1 ? history[history.length - 1] : null;
                const previousHistory = history.length >= 2 ? history[history.length - 2] : null;
                const historyChangePct =
                    latestHistory && previousHistory && previousHistory.close !== 0
                        ? ((latestHistory.close - previousHistory.close) / previousHistory.close) * 100
                        : null;

                if (yahooPrimary?.latest !== null || latestHistory) {
                    return {
                        code: symbol,
                        name,
                        latest: yahooPrimary?.latest ?? latestHistory?.close ?? null,
                        change_pct: Number.isFinite(yahooPrimary?.change_pct)
                            ? yahooPrimary?.change_pct ?? null
                            : Number.isFinite(historyChangePct)
                                ? historyChangePct
                                : null,
                        as_of: yahooPrimary?.as_of ?? latestHistory?.date ?? null
                    };
                }

                return yahooFallbackTicker
                    ? fetchYahooForexFallback(yahooFallbackTicker)
                    : null;
            }

            return null;
        }

        const latest = Number(match.f2);
        const changePct = Number(match.f3);
        const ts = Number(match.f124);
        const yahooFallback =
            yahooPrimary
            ?? (
                yahooFallbackTicker && (!Number.isFinite(changePct) || !Number.isFinite(latest))
                    ? await fetchYahooForexFallback(yahooFallbackTicker)
                    : null
            );

        let effectiveChangePct = Number.isFinite(changePct) ? changePct : (yahooFallback?.change_pct ?? null);
        if (!Number.isFinite(effectiveChangePct) && upperSymbol === 'USDCNH') {
            const history = await fetchForexHistory('USDCNH');
            if (history.length >= 2) {
                const latestHistory = history[history.length - 1];
                const previousHistory = history[history.length - 2];
                if (previousHistory.close !== 0) {
                    effectiveChangePct = ((latestHistory.close - previousHistory.close) / previousHistory.close) * 100;
                }
            }
        }

        return {
            code: symbol,
            name,
            latest: Number.isFinite(latest) ? latest : (yahooFallback?.latest ?? null),
            change_pct: Number.isFinite(effectiveChangePct) ? effectiveChangePct : null,
            as_of: Number.isFinite(ts) ? new Date(ts * 1000).toISOString().slice(0, 10) : null
        };
    } catch {
        return yahooFallbackTicker
            ? fetchYahooForexFallback(yahooFallbackTicker)
            : null;
    }
}

async function fetchYahooChartSeries(
    symbol: string,
    snapshotMeta: { code: string; name: string } = { code: 'GC=F', name: 'COMEX黄金期货' }
): Promise<{ snapshot: ClientFocusPriceSnapshot | null; history: ClientFocusPriceHistoryPoint[] | null }> {
    try {
        const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`);
        url.searchParams.set('range', '1y');
        url.searchParams.set('interval', '1d');
        url.searchParams.set('includePrePost', 'false');
        url.searchParams.set('events', 'div,splits');

        const response = await fetch(url.toString(), {
            headers: {
                Accept: 'application/json',
                Referer: 'https://finance.yahoo.com/',
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)'
            }
        });
        if (!response.ok) {
            return { snapshot: null, history: null };
        }

        const payload = (await response.json()) as {
            chart?: {
                result?: Array<{
                    meta?: {
                        regularMarketPrice?: number;
                        previousClose?: number;
                        currency?: string;
                    };
                    timestamp?: number[];
                    indicators?: {
                        quote?: Array<{
                            close?: Array<number | null>;
                        }>;
                    };
                }>;
            };
        };
        const result = payload.chart?.result?.[0];
        const timestamps = Array.isArray(result?.timestamp) ? result?.timestamp ?? [] : [];
        const closes = Array.isArray(result?.indicators?.quote?.[0]?.close)
            ? result?.indicators?.quote?.[0]?.close ?? []
            : [];

        const history = timestamps
            .map((ts, index) => {
                const close = closes[index];
                return {
                    date: new Date(ts * 1000).toISOString().slice(0, 10),
                    close: typeof close === 'number' && Number.isFinite(close) ? close : null
                };
            })
            .filter((item): item is ClientFocusPriceHistoryPoint => Boolean(item.date) && item.close !== null);

        if (history.length === 0) {
            return { snapshot: null, history: null };
        }

        const latestPoint = history[history.length - 1];
        const previousClose = Number(result?.meta?.previousClose);
        const latest = Number(result?.meta?.regularMarketPrice);
        const effectiveLatest = Number.isFinite(latest) ? latest : latestPoint.close;
        const previousHistoryClose = history.length >= 2 ? history[history.length - 2].close : null;
        const resolvedPreviousHistoryClose = Number.isFinite(previousHistoryClose) ? previousHistoryClose : null;
        // Prefer regularMarketPrice vs previousClose (real-time, intraday-aware)
        // Fall back to history[-1] vs history[-2] only if meta fields are missing
        const changePct = Number.isFinite(previousClose) && previousClose !== 0
            ? ((effectiveLatest - previousClose) / previousClose) * 100
            : resolvedPreviousHistoryClose !== null && resolvedPreviousHistoryClose !== 0
                ? ((latestPoint.close - resolvedPreviousHistoryClose) / resolvedPreviousHistoryClose) * 100
                : null;

        return {
            snapshot: {
                code: snapshotMeta.code,
                name: snapshotMeta.name,
                latest: Number.isFinite(effectiveLatest) ? effectiveLatest : null,
                change_pct: Number.isFinite(changePct) ? changePct : null,
                as_of: latestPoint.date
            },
            history
        };
    } catch {
        return { snapshot: null, history: null };
    }
}

async function fetchFocusPriceSnapshot(slug: string): Promise<ClientFocusPriceSnapshot | null> {
    if (slug === 'usd-strength') {
        return fetchForexSnapshot('USDCNH', '美元人民币');
    }
    if (slug === 'gold-repricing') {
        return (await fetchYahooChartSeries('GC=F')).snapshot;
    }
    return null;
}

async function fetchFocusSecondaryPriceSnapshot(slug: string): Promise<ClientFocusPriceSnapshot | null> {
    if (slug === 'usd-strength') {
        const result = await fetchYahooChartSeries('DX-Y.NYB', {
            code: 'DXY',
            name: '美元指数'
        });
        return result.snapshot
            ? result.snapshot
            : null;
    }
    return null;
}

async function fetchForexHistory(symbol: string): Promise<ClientFocusPriceHistoryPoint[]> {
    try {
        const marketCode = symbol.toUpperCase() === 'USDCNH' ? 133 : null;
        if (!marketCode) {
            return [];
        }

        const url = new URL('https://push2his.eastmoney.com/api/qt/stock/kline/get');
        url.searchParams.set('secid', `${marketCode}.${symbol.toUpperCase()}`);
        url.searchParams.set('klt', '101');
        url.searchParams.set('fqt', '1');
        url.searchParams.set('lmt', '500');
        url.searchParams.set('end', '20500000');
        url.searchParams.set('iscca', '1');
        url.searchParams.set('fields1', 'f1,f2,f3,f4,f5,f6,f7,f8');
        url.searchParams.set('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64');
        url.searchParams.set('ut', 'f057cbcbce2a86e2866ab8877db1d059');
        url.searchParams.set('forcect', '1');

        const response = await fetch(url.toString(), {
            headers: {
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)',
                Accept: 'application/json'
            }
        });
        if (!response.ok) {
            return [];
        }

        const payload = (await response.json()) as {
            data?: {
                klines?: string[];
            };
        };
        const klines = Array.isArray(payload.data?.klines) ? payload.data?.klines ?? [] : [];
        return klines
            .map((item) => {
                const [date, , close] = item.split(',');
                const closeValue = Number(close);
                return {
                    date,
                    close: Number.isFinite(closeValue) ? closeValue : null
                };
            })
            .filter((item): item is ClientFocusPriceHistoryPoint => Boolean(item.date) && item.close !== null);
    } catch {
        return [];
    }
}

async function fetchGoldHistory(): Promise<ClientFocusPriceHistoryPoint[]> {
    const result = await fetchYahooChartSeries('GC=F');
    return result.history ?? [];
}

async function fetchFocusPriceHistory(slug: string): Promise<ClientFocusPriceHistoryPoint[] | null> {
    if (slug === 'usd-strength') {
        const history = await fetchForexHistory('USDCNH');
        return history.length > 1 ? history : null;
    }
    if (slug === 'gold-repricing') {
        const history = await fetchGoldHistory();
        return history.length > 1 ? history : null;
    }
    return null;
}

async function fetchFocusSecondaryPriceHistory(slug: string): Promise<ClientFocusPriceHistoryPoint[] | null> {
    if (slug === 'usd-strength') {
        const result = await fetchYahooChartSeries('DX-Y.NYB');
        return result.history && result.history.length > 1 ? result.history : null;
    }
    return null;
}

function buildGoldDrivers(newsItems: NewsItem[]): ClientFocusDriverItem[] {
    const titles = newsItems.map((item) => item.title.toLowerCase()).join(' | ');

    const realYieldStatus =
        /yield rise|yield climbs|yields higher|hawkish|rate hike|higher for longer|yields surge/.test(titles)
            ? '压制'
            : /yield fall|yield drop|yields lower|dovish|rate cut|yields decline/.test(titles)
                ? '中性'
                : '压制';

    const inflationStatus =
        /inflation|cpi|pce|oil price|energy price|inflation expectation|inflation expectations/.test(titles)
            ? '抬升'
            : /inflation cool|disinflation|deflation|inflation fall/.test(titles)
                ? '中性'
                : '中性';

    const dollarStatus =
        /dollar strength|dollar rise|dollar climbs|dxy rise|stronger dollar|dollar index rise/.test(titles)
            ? '偏强'
            : /dollar weak|dollar fall|dollar drop|dxy fall|dollar decline|weaker dollar/.test(titles)
                ? '偏弱'
                : '偏强';

    const centralBankStatus =
        /central bank|official buying|央行购金|gold purchase|sovereign buying/.test(titles)
            ? /slows|放缓|moderate|cool|pause|减少/.test(titles)
                ? '支撑放缓'
                : '支撑'
            : '支撑';

    const havenStatus =
        /iran|israel|war|conflict|geopolitical|hormuz|middle east/.test(titles)
            ? /gold rally|gold surge|haven demand|flight to safety|gold jumps/.test(titles)
                ? '支撑'
                : '有限支撑'
            : '中性';

    const etfStatus =
        /etf outflow|gold etf outflow|fund outflow|gold redemption/.test(titles)
            ? '流出'
            : /etf inflow|gold etf inflow|fund inflow|gold buying/.test(titles)
                ? '流入'
                : '分化';

    return [
        { label: '实际利率', status: realYieldStatus },
        { label: '通胀预期', status: inflationStatus },
        { label: '美元强弱', status: dollarStatus },
        { label: '央行购金', status: centralBankStatus },
        { label: '避险需求', status: havenStatus },
        { label: 'ETF资金流', status: etfStatus }
    ];
}

async function fetchHongKongSpotIndices() {
    const response = await fetch(
        'https://hq.sinajs.cn/rn=mtf2t&list=hkHSI,hkHSTECH',
        {
            headers: {
                Referer: 'https://vip.stock.finance.sina.com.cn/',
                'User-Agent': 'Mozilla/5.0 (compatible; FCNAdvisor/1.0)'
            }
        }
    );
    if (!response.ok) {
        return [];
    }

    const text = await response.text();
    const lines = text.split('\n').filter(Boolean);
    const baseItems = lines
        .map((line) => {
            const codeMatch = line.match(/hq_str_hk([A-Z0-9]+)=/);
            const raw = line.split('"')[1]?.split(',') ?? [];
            if (!codeMatch || raw.length < 8) {
                return null;
            }
            return {
                code: codeMatch[1],
                name:
                    codeMatch[1] === 'HSI'
                        ? '恒生指数'
                        : codeMatch[1] === 'HSTECH'
                            ? '恒生科技指数'
                            : (raw[1] ?? codeMatch[1]),
                latest: Number(raw[6]),
                change_pct: Number(raw[8])
            };
        })
        .filter((item): item is NonNullable<typeof item> => Boolean(item));

    const enriched = await Promise.all(
        baseItems.map(async (item) => ({
            ...item,
            change_5d_pct: await fetchHongKongIndex5dChange(item.code, item.change_pct)
        }))
    );

    return enriched;
}

async function fetchUsMarketStateIndices() {
    const snapshots = await Promise.all([
        fetchMassiveIndexSnapshot('I:SPX', { code: 'SPX', name: '标普500' }),
        fetchMassiveIndexSnapshot('I:NDX', { code: 'NDX', name: '纳斯达克' })
    ]);

    return snapshots
        .filter((item): item is NonNullable<typeof item> => item !== null)
        .map((item) => ({
            code: item.code,
            name: item.name,
            latest: item.latest,
            change_pct: item.change_pct
        }));
}

async function fetchMassiveIndexSnapshot(
    massiveSymbol: string,
    fallbackMeta: { code: string; name: string }
): Promise<ClientFocusPriceSnapshot | null> {
    const yahooSymbol = fallbackMeta.code === 'SPX' ? '^GSPC' : '^IXIC';
    const yahooFallback = await fetchYahooChartSeries(yahooSymbol, fallbackMeta);

    try {
        const fetcher = new MassiveDataFetcher();
        const history = await fetcher.fetchPriceHistory(massiveSymbol, 30);
        if (history.length >= 2) {
            const latest = history[history.length - 1];
            const previous = history[history.length - 2];
            const massiveChangePct = previous.close !== 0
                ? ((latest.close - previous.close) / previous.close) * 100
                : null;
            const changePct = yahooFallback.snapshot?.change_pct ?? massiveChangePct;

            return {
                code: fallbackMeta.code,
                name: fallbackMeta.name,
                latest: latest.close,
                change_pct: changePct,
                as_of: latest.date
            };
        }
    } catch {
        // fall through to Yahoo fallback
    }

    return yahooFallback.snapshot;
}

function buildMarketStateSummary(
    indices: Array<{
        code: string;
        name: string;
        latest: number | null;
        change_pct: number | null;
        change_5d_pct?: number | null;
    }>
) {
    const hkLocal = getMarketLocalParts('Asia/Hong_Kong');
    const isHongKongMidday = hkLocal.minutes >= 12 * 60 && hkLocal.minutes < 16 * 60;
    const isHongKongAfterClose = hkLocal.minutes >= 16 * 60;
    const gold = indices.find((item) => item.code === 'GOLD');
    const dxy = indices.find((item) => item.code === 'DXY');
    const spx = indices.find((item) => item.code === 'SPX');
    const ndx = indices.find((item) => item.code === 'NDX');
    const hsi = indices.find((item) => item.code === 'HSI');
    const hstech = indices.find((item) => item.code === 'HSTECH');
    const oil = indices.find((item) => item.code === 'OIL');
    const tnx = indices.find((item) => item.code === 'TNX');

    const goldUp = (gold?.change_pct ?? 0) >= 0.5;
    const goldDown = (gold?.change_pct ?? 0) <= -0.5;
    const dxyUp = (dxy?.change_pct ?? 0) >= 0.2;
    const dxyDown = (dxy?.change_pct ?? 0) <= -0.2;
    const usRiskOn = (spx?.change_pct ?? 0) >= 0.6 || (ndx?.change_pct ?? 0) >= 0.9;
    const usRiskOff = (spx?.change_pct ?? 0) <= -0.6 || (ndx?.change_pct ?? 0) <= -0.9;
    const hkWeak = (hsi?.change_pct ?? 0) <= -0.6 || (hstech?.change_pct ?? 0) <= -1;
    const hkStrong = (hsi?.change_pct ?? 0) >= 0.6 || (hstech?.change_pct ?? 0) >= 1;
    const oilDown = (oil?.change_pct ?? 0) <= -3;
    const oilUp = (oil?.change_pct ?? 0) >= 3;
    const tnxDown = (tnx?.change_pct ?? 0) <= -5;
    const tnxUp = (tnx?.change_pct ?? 0) >= 5;

    let overnightSummary = '隔夜美股、黄金、原油与美债收益率走势分化，客户会更关注地缘局势之后由谁主导重新定价。';

    if (oilDown && usRiskOn && (goldUp || tnxDown)) {
        overnightSummary = '隔夜中东局势缓和信号带动美股反弹，原油明显回落，黄金与美债收益率的变化也在重定价降息路径。';
    } else if (oilUp && usRiskOff && (goldUp || dxyUp)) {
        overnightSummary = '隔夜中东局势再度主导市场，原油冲高、黄金偏强，美股回落，客户更可能追问通胀与避险资产如何重新定价。';
    } else if (usRiskOn && dxyDown && (goldDown || !goldUp)) {
        overnightSummary = '隔夜美股风险偏好修复，美元回落，市场开始把焦点从避险交易重新切回增长与政策路径。';
    } else if (usRiskOff && (goldUp || dxyUp)) {
        overnightSummary = '隔夜避险交易重新升温，美股回落，黄金或美元偏强，客户会更关注地缘与政策变量谁在主导市场。';
    } else if (goldUp && tnxDown) {
        overnightSummary = '隔夜黄金走强、美债收益率回落，市场更关注避险需求与降息预期是否重新占上风。';
    } else if (tnxUp && dxyUp) {
        overnightSummary = '隔夜美元与美债收益率同步走高，市场开始重新评估通胀路径与降息节奏。';
    }

    if (!isHongKongMidday && !isHongKongAfterClose) {
        return overnightSummary;
    }

    const hkPhaseLabel = isHongKongAfterClose ? '今日港股收盘后' : '上午港股';

    if (hkStrong && hstech && (hstech.change_pct ?? 0) >= 1) {
        return `${overnightSummary} ${hkPhaseLabel}跟随修复，科技板块弹性更强，客户会继续追问这波风险偏好能否延续到亚洲时段。`;
    }

    if (hkWeak && hstech && (hstech.change_pct ?? 0) <= -1) {
        return `${overnightSummary} ${hkPhaseLabel}未能完全承接隔夜情绪，科技板块回吐更明显，客户会更关注中国资产是否仍受压。`;
    }

    if (hkStrong) {
        return `${overnightSummary} ${hkPhaseLabel}同步偏强，说明亚洲时段也在跟进隔夜风险偏好修复。`;
    }

    if (hkWeak) {
        return `${overnightSummary} ${hkPhaseLabel}表现偏弱，说明亚洲时段对隔夜叙事的接力仍然有限。`;
    }

    return `${overnightSummary} ${hkPhaseLabel}延续震荡，客户会更关注亚洲时段是否给出更明确的接力信号。`;
}

function buildHongKongSnapshotSummary(
    indices: Array<{
        code: string;
        name: string;
        latest: number | null;
        change_pct: number | null;
        change_5d_pct?: number | null;
    }>,
    southboundChart: ClientFocusMarketChart | null,
) {
    const hsi = indices.find((item) => item.code === 'HSI');
    const hstech = indices.find((item) => item.code === 'HSTECH');
    const southbound5d = southboundChart ? sumRecentNetBuy(southboundChart.points, 5) : null;

    const leadSentence = (() => {
        const hsiMove = hsi?.change_5d_pct ?? hsi?.change_pct;
        const techMove = hstech?.change_5d_pct ?? hstech?.change_pct;

        if (hsiMove === null || hsiMove === undefined || techMove === null || techMove === undefined) {
            return '恒指与恒生科技近期走势分化';
        }
        if (hsiMove < 0 && techMove < 0 && Math.abs(techMove) > Math.abs(hsiMove) + 0.6) {
            return '恒指回落幅度有限，但恒生科技波动更大';
        }
        if (hsiMove > 0 && techMove > 0 && techMove > hsiMove + 0.6) {
            return '恒指与恒生科技同步反弹，科技板块弹性更强';
        }
        if (hsiMove < 0 && techMove > 0) {
            return '恒指偏弱，但恒生科技相对更有韧性';
        }
        if (hsiMove > 0 && techMove < 0) {
            return '恒指表现尚稳，但恒生科技情绪仍偏谨慎';
        }
        return '恒指与恒生科技走势仍以情绪波动为主';
    })();

    const flowSentence = (() => {
        if (southbound5d === null) {
            return '港股仍偏资金驱动';
        }
        if (southbound5d > 120) {
            return '南向资金持续净流入，港股暂时仍有资金支撑';
        }
        if (southbound5d < -120) {
            return '南向资金明显转弱，港股短线更受情绪影响';
        }
        return '南向资金方向不强，港股仍偏资金驱动';
    })();

    return `${leadSentence}，${flowSentence}。`;
}

async function generateMarketStateSummaryLLM(
    indices: Array<{ code: string; name: string; latest: number | null; change_pct: number | null }>,
    newsHeadlines: string[]
): Promise<string | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) return null;

    const hkLocal = getMarketLocalParts('Asia/Hong_Kong');
    const hkHour = Math.floor(hkLocal.minutes / 60);
    const timeContext =
        hkHour < 12
            ? '现在是港股早盘，RM正在准备开市前的客户沟通。'
            : hkHour < 16
              ? '现在是港股午后交易时段，RM正在跟进盘中走势。'
              : '现在是港股收盘后，RM正在准备下午的客户沟通。';

    const priceLines = indices
        .filter((i) => i.change_pct !== null)
        .map((i) => {
            const change = i.change_pct ?? 0;
            const sign = change >= 0 ? '+' : '';
            // TNX change_pct is stored in bps, not percent
            const formatted = i.code === 'TNX'
                ? `${sign}${change.toFixed(1)}bps`
                : `${sign}${change.toFixed(2)}%`;
            return `${i.name}（${i.code}）: ${formatted}`;
        })
        .join('\n');

    const newsSection =
        newsHeadlines.length > 0
            ? `\n最新地缘/市场事件：\n${newsHeadlines.map((h) => `- ${h}`).join('\n')}`
            : '';

    const userPrompt = `
${timeContext}

当前大类资产涨跌幅：
${priceLines}
${newsSection}

请用一句话（35-50字）写出"今日市场状态"摘要，供香港私行RM/IC在与客户沟通前快速理解今天的市场主线。

要求：
- 直接点出最显著的跨资产信号（哪两个资产走势最重要）
- 如果有具体地缘事件（如霍尔木兹封闭、停火协议、空袭），必须点名，不要用"地缘风险"代替
- 如果没有具体地缘事件信息，直接用价格信号描述原因（例如"美伊停战带动风险资产大涨"、"原油反弹推升通胀预期"），禁止用"地缘风险推升"等空泛归因
- 美债收益率变化用bps描述：收益率下行（bps为负）= 避险/降息预期升温，不是"大跌"；收益率上行（bps为正）= 通胀预期/再通胀交易
- 结尾说明客户最可能追问哪个方向（一句）
- 禁止使用"市场承压""风险升温""不确定性""地缘局势"等空泛表述
- 只输出摘要文字，不要任何解释或标点以外的内容
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: '你是香港私人银行市场助手，用简洁专业的中文为RM/IC生成每日市场状态摘要。' },
                    { role: 'user', content: userPrompt }
                ],
                max_tokens: 120,
                temperature: 0.3
            })
        });
        if (!response.ok) return null;
        const data = (await response.json()) as { choices?: Array<{ message?: { content?: string } }> };
        const text = data.choices?.[0]?.message?.content?.trim() ?? null;
        if (!text || text.length < 10) return null;
        return text;
    } catch {
        return null;
    }
}

async function fetchClientFocusMarketStateSnapshot(): Promise<ClientFocusMarketStateResponse | null> {
    const cached = focusMarketStateCache.get(MARKET_STATE_CACHE_KEY);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
    }

    const [
        hkSnapshot,
        goldSnapshot,
        silverSnapshot,
        usdCnhSnapshot,
        usdJpySnapshot,
        usdChfSnapshot,
        dxySnapshot,
        usIndices,
        wtiSnapshot,
        brentSnapshot,
        naturalGasSnapshot,
        tnxSnapshot
    ] = await Promise.all([
        fetchHongKongMarketSnapshot(),
        fetchYahooChartSeries('GC=F', { code: 'GOLD', name: '黄金' }),
        fetchYahooChartSeries('SI=F', { code: 'SILVER', name: '白银' }),
        fetchForexSnapshot('USDCNH', '美元人民币'),
        fetchForexSnapshot('USDJPY', '美元兑日元'),
        fetchForexSnapshot('USDCHF', '美元兑瑞郎'),
        fetchYahooChartSeries('DX-Y.NYB', { code: 'DXY', name: '美元指数' }),
        fetchUsMarketStateIndices(),
        fetchYahooChartSeries('CL=F', { code: 'OIL', name: 'WTI原油' }),
        fetchYahooChartSeries('BZ=F', { code: 'BRENT', name: 'Brent原油' }),
        fetchYahooChartSeries('NG=F', { code: 'NATGAS', name: '天然气' }),
        fetchYahooChartSeries('^TNX', { code: 'TNX', name: '美债10Y' }),
    ]);

    const tnxLatestClose = tnxSnapshot.history && tnxSnapshot.history.length >= 1
        ? tnxSnapshot.history[tnxSnapshot.history.length - 1].close
        : null;
    const tnxPreviousClose = tnxSnapshot.history && tnxSnapshot.history.length >= 2
        ? tnxSnapshot.history[tnxSnapshot.history.length - 2].close
        : null;
    const tnxBpsChange =
        tnxLatestClose !== null && tnxPreviousClose !== null
            ? (tnxLatestClose - tnxPreviousClose) * 100
            : null;

    const indices = [
        ...usIndices,
        goldSnapshot.snapshot
            ? {
                code: goldSnapshot.snapshot.code,
                name: goldSnapshot.snapshot.name,
                latest: goldSnapshot.snapshot.latest,
                change_pct: goldSnapshot.snapshot.change_pct
            }
            : null,
        silverSnapshot.snapshot
            ? {
                code: silverSnapshot.snapshot.code,
                name: silverSnapshot.snapshot.name,
                latest: silverSnapshot.snapshot.latest,
                change_pct: silverSnapshot.snapshot.change_pct
            }
            : null,
        dxySnapshot.snapshot
            ? {
                code: dxySnapshot.snapshot.code,
                name: dxySnapshot.snapshot.name,
                latest: dxySnapshot.snapshot.latest,
                change_pct: dxySnapshot.snapshot.change_pct
            }
            : null,
        wtiSnapshot.snapshot
            ? {
                code: wtiSnapshot.snapshot.code,
                name: wtiSnapshot.snapshot.name,
                latest: wtiSnapshot.snapshot.latest,
                change_pct: wtiSnapshot.snapshot.change_pct
            }
            : null,
        brentSnapshot.snapshot
            ? {
                code: brentSnapshot.snapshot.code,
                name: brentSnapshot.snapshot.name,
                latest: brentSnapshot.snapshot.latest,
                change_pct: brentSnapshot.snapshot.change_pct
            }
            : null,
        naturalGasSnapshot.snapshot
            ? {
                code: naturalGasSnapshot.snapshot.code,
                name: naturalGasSnapshot.snapshot.name,
                latest: naturalGasSnapshot.snapshot.latest,
                change_pct: naturalGasSnapshot.snapshot.change_pct
            }
            : null,
        tnxSnapshot.snapshot
            ? {
                code: tnxSnapshot.snapshot.code,
                name: tnxSnapshot.snapshot.name,
                latest: tnxSnapshot.snapshot.latest,
                change_pct: Number.isFinite(tnxBpsChange) ? tnxBpsChange : tnxSnapshot.snapshot.change_pct
            }
            : null,
        usdCnhSnapshot
            ? {
                code: usdCnhSnapshot.code,
                name: usdCnhSnapshot.name,
                latest: usdCnhSnapshot.latest,
                change_pct: usdCnhSnapshot.change_pct
            }
            : null,
        usdJpySnapshot
            ? {
                code: usdJpySnapshot.code,
                name: usdJpySnapshot.name,
                latest: usdJpySnapshot.latest,
                change_pct: usdJpySnapshot.change_pct
            }
            : null,
        usdChfSnapshot
            ? {
                code: usdChfSnapshot.code,
                name: usdChfSnapshot.name,
                latest: usdChfSnapshot.latest,
                change_pct: usdChfSnapshot.change_pct
            }
            : null,
        ...(hkSnapshot?.indices ?? [])
    ].filter((item): item is NonNullable<typeof item> => item !== null);

    if (indices.length === 0) {
        return null;
    }

    // Try to get recent news from middle-east focus cache for geopolitical context
    const cachedMiddleEast = focusCache.get('middle-east-tensions');
    const recentNewsHeadlines = cachedMiddleEast?.value?.latest_updates
        ?.slice(0, 3)
        .map((u) => u.title)
        .filter(Boolean) ?? [];

    const llmSummary = await generateMarketStateSummaryLLM(indices, recentNewsHeadlines);

    const payload: ClientFocusMarketStateResponse = {
        summary: llmSummary ?? buildMarketStateSummary(indices),
        indices
    };

    focusMarketStateCache.set(MARKET_STATE_CACHE_KEY, {
        expiresAt: Date.now() + FOCUS_LIVE_MARKET_CACHE_TTL_MS,
        value: payload
    });

    return payload;
}

async function fetchHongKongIndex5dChange(code: string, fallbackChangePct: number | null) {
    try {
        const closes = (await fetchHongKongIndexHistory(code, 10)).map((item) => item.close);

        if (closes.length < 2) {
            return fallbackChangePct;
        }

        const window = closes.slice(-6);
        const start = window[0];
        const end = window[window.length - 1];
        if (!Number.isFinite(start) || !Number.isFinite(end) || start === 0) {
            return fallbackChangePct;
        }

        return ((end - start) / start) * 100;
    } catch {
        return fallbackChangePct;
    }
}

async function generateFocusContent(topic: FocusTopicConfig, newsItems: NewsItem[]): Promise<FocusTopicModelOutput | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const newsSection = newsItems.length > 0
        ? newsItems
              .slice(0, 6)
              .map((item, index) => `${index + 1}. ${item.title} | ${item.source} | ${item.published_at}`)
              .join('\n')
        : '暂无最新新闻。';

    const systemPrompt =
        '你是一位香港私人银行前台助手，负责整理客户高频市场话题。请输出简洁、结构化、专业的中文 JSON。不要输出 markdown。不要写投资建议，不要替代机构 house view。';
    const topicGuidance = buildFocusSummaryGuidance(topic);
    const userPrompt = `
主题：${topic.title}

该主题近期进入客户沟通的背景：
${topicGuidance}

最新新闻：
${newsSection}

请输出 JSON，字段如下：
{
  "status": "只能从以下选一个：关注升温 / 持续发酵 / 压力上升。不可自创。",
  "summary": "28到40字。一句话说明为什么这个话题最近会成为客户高频关注点，必须同时包含：1）近期市场背景或变化；2）客户最关心的核心变量或资产。禁止出现RM、IC、客户经理等内部角色词汇。",
  "latest_updates": [
    {
      "title": "一句话，必须包含具体机构名称或数据，禁止直译英文标题，禁止使用“市场”“投资者”等主语",
      "impact": "只能从以下选一个：风险抬升 / 信用事件 / 政策变化 / 持续发酵"
    }
  ]
}

要求：
- latest_updates 最多2条
- summary、title 都必须完整，不要半句
- summary 必须回答“为什么最近客户会问这个话题”，而不只是复述新闻
- summary 可以提“客户近期关注/高频追问”，但不能写成销售口吻
- summary 优先连接该主题与具体资产、价格变量或持仓暴露
- summary 严禁输出空泛情绪描述、无数据支撑的方向性表达
- latest_updates 应尽量基于给定新闻做翻译、压缩和总结，不要照抄英文标题
- 若新闻不足以支撑具体数据，宁可减少条数，不要编造
- status、summary 与 latest_updates 的方向必须一致
- 严禁输出 JSON 以外任何内容
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        return safeParseJson(payload.choices?.[0]?.message?.content ?? '');
    } catch {
        return null;
    }
}

async function generateWeeklyProgress(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<FocusWeeklyProgressModelOutput | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const recentNews = dedupeRecentNews(newsItems).slice(0, 8);
    if (recentNews.length === 0) {
        return null;
    }

    const newsSection = recentNews
        .map((item, index) => `${index + 1}. ${item.title} | ${item.source} | ${item.published_at}`)
        .join('\n');

    const systemPrompt =
        '你是香港私人银行前台助手，负责整理客户焦点的每周市场摘要。请输出严格 JSON，不要输出 markdown，不要输出任何解释。';
    const userPrompt = `
主题：${topic.title}

我会给你一组通过 Google News RSS 抓取的原始新闻条目。你的任务是从这些新闻中提炼出对客户沟通最有价值的内容，输出结构化 JSON。

输入新闻：
${newsSection}

筛选标准：
1. 有具体机构名称、金额、比例等可量化信息
2. 与客户沟通直接相关（客户持仓、流动性、赎回、估值、信用事件）
3. 发布时间在过去 7 天内
4. 排除重复报道，同一事件只保留信息最完整的一条
5. 排除以下来源：聚合媒体、内容农场、不知名博客。优先保留 Bloomberg、Reuters、FT、CNBC、WSJ、官方机构公告；行业权威源仅在信息显著优于主流媒体时保留

输出格式（严格 JSON）：
{
  "topic": "${topic.title}",
  "section_title": "本周关键进展",
  "updated_at": "YYYY-MM-DD",
  "items": [
    {
      "date": "MM-DD",
      "tag": "风险抬升 / 持续发酵 / 政策变化 / 信用事件",
      "summary": "40字以内。一句话说清楚发生了什么，必须包含机构名称和具体数字。",
      "source": "来源名称"
    }
  ],
  "editor_note": "50字以内。基于本周所有新闻，用一句话概括这周整体风险方向，并提示客户沟通时最该留意的变量。"
}

写作要求：
- 语言：简体中文
- items 最多 3 条，按时间倒序排列
- summary 必须包含具体数字，禁止使用模糊词，禁止把两个独立事件混在一句话里
- tag 只能从以下四个中选一个：风险抬升 / 持续发酵 / 政策变化 / 信用事件
- tag 使用优先级：
  1. 信用事件：有具体违约、停止兑付、资产减记
  2. 风险抬升：有新的机构触发赎回上限、评级下调
  3. 政策变化：央行、监管机构有明确表态或政策调整
  4. 持续发酵：以上均不满足时才使用
- editor_note 必须包含两部分：
  1. 本周风险方向判断（一句话，要有方向性）
  2. 客户沟通时最该留意的变量（如利率、美元、油价、流动性、估值）
- 禁止出现 RM、IC、客户经理、口径无需调整 等内部表达
- 禁止输出“需持续关注”“市场仍在消化”等无行动价值表达
- editor_note 的方向判断必须与 items 的整体方向一致
- 若主题是黄金逻辑重估，editor_note 优先点明黄金本周价格方向或表现特征，并明确客户沟通时应聚焦的核心变量
- 严禁输出 JSON 以外的任何内容
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        return safeParseJson(payload.choices?.[0]?.message?.content ?? '') as FocusWeeklyProgressModelOutput | null;
    } catch {
        return null;
    }
}

async function generateTransmissionChain(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<ClientFocusTransmissionItem[] | null> {
    const cached = focusChainCache.get(topic.slug);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
    }

    if (topic.slug === 'middle-east-tensions') {
        const fixedItems = buildMiddleEastTransmissionChain(newsItems);

        focusChainCache.set(topic.slug, {
            expiresAt: Date.now() + FOCUS_CHAIN_CACHE_TTL_MS,
            value: fixedItems
        });

        return fixedItems;
    }

    if (topic.slug === 'private-credit-stress') {
        const fixedItems = buildPrivateCreditTransmissionChain(newsItems);

        focusChainCache.set(topic.slug, {
            expiresAt: Date.now() + FOCUS_CHAIN_CACHE_TTL_MS,
            value: fixedItems
        });

        return fixedItems;
    }

    if (topic.slug === 'gold-repricing') {
        const fixedItems = buildGoldTransmissionChain(newsItems);

        focusChainCache.set(topic.slug, {
            expiresAt: Date.now() + FOCUS_CHAIN_CACHE_TTL_MS,
            value: fixedItems
        });

        return fixedItems;
    }

    if (topic.slug === 'usd-strength') {
        const fixedItems = buildUsdReboundTransmissionChain(newsItems);

        focusChainCache.set(topic.slug, {
            expiresAt: Date.now() + FOCUS_CHAIN_CACHE_TTL_MS,
            value: fixedItems
        });

        return fixedItems;
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const newsSection = newsItems.length > 0
        ? newsItems
              .slice(0, 8)
              .map((item, index) => `${index + 1}. ${item.title} | ${item.source} | ${item.published_at}`)
              .join('\n')
        : '暂无最新新闻。';

    const systemPrompt =
        '你是一位香港私人银行前台助手，负责更新客户焦点的市场传导链条。请输出简洁、结构化、专业的中文 JSON。不要输出 markdown。不要写投资建议，不要替代机构 house view。';
    const userPrompt = `
请基于本周最新市场信息，更新以下主题的传导链条。

主题：${topic.title}

最新新闻：
${newsSection}

定价状态判断标准（必须严格遵守）：
- 已定价：市场价格已充分反映该事件影响，资产价格变动已相对稳定
- 部分定价：市场仍在博弈，预期还在变化
- 未充分定价：市场基本忽视，尚未明显反映到资产价格

请输出 JSON，字段如下：
{
  "updated_at": "YYYY-MM-DD",
  "topic": "${topic.title}",
  "first_order": {
    "title": "10字以内标题",
    "pricing_status": "已定价 / 部分定价 / 未充分定价",
    "description": "50字以内。说明市场正在交易什么以及本周定价逻辑变化。",
    "latest_evidence": "30字以内。本周支撑定价判断的具体数据，如无则填 null"
  },
  "second_order": {
    "title": "10字以内标题",
    "pricing_status": "已定价 / 部分定价 / 未充分定价",
    "description": "50字以内。说明正在扩散的二阶影响。",
    "latest_evidence": "30字以内。本周支撑定价判断的具体数据，如无则填 null"
  }
}

要求：
- 只输出一阶和二阶，不要三阶
- 语言简洁、机构化，不要口语化
- description 必须完整，不要半句
- 一阶 description 必须说明：市场当前正在交易的具体资产或指标（如油价、利率期货、能源股）
- 二阶 description 必须说明：从一阶如何传导到二阶，传导路径要显性化
- 禁止一阶和二阶 description 描述同一类资产或同一条传导链
- 每条 description 必须包含至少一个可验证的市场信号（如具体指数、利率水平、价格变动幅度）
- first_order 与 second_order 必须形成清晰的传导关系
- 若新闻不足，基于已有新闻与市场常识做保守总结
`.trim();

    try {
        const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt }
                ]
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const parsed = safeParseJson(payload.choices?.[0]?.message?.content ?? '') as FocusTransmissionModelOutput | null;
        if (!parsed?.first_order || !parsed?.second_order) {
            return null;
        }

        const items = [
            {
                order: '一阶传导',
                title: parsed.first_order.title?.trim() || '',
                pricing: sanitizePricingStatus(parsed.first_order.pricing_status),
                summary: parsed.first_order.description?.trim() || '',
                latest_evidence:
                    typeof parsed.first_order.latest_evidence === 'string' && parsed.first_order.latest_evidence.trim()
                        ? parsed.first_order.latest_evidence.trim()
                        : null
            },
            {
                order: '二阶传导',
                title: parsed.second_order.title?.trim() || '',
                pricing: sanitizePricingStatus(parsed.second_order.pricing_status),
                summary: parsed.second_order.description?.trim() || '',
                latest_evidence:
                    typeof parsed.second_order.latest_evidence === 'string' && parsed.second_order.latest_evidence.trim()
                        ? parsed.second_order.latest_evidence.trim()
                        : null
            }
        ].filter((item) => item.title && item.summary) as ClientFocusTransmissionItem[];

        if (items.length !== 2) {
            return null;
        }

        focusChainCache.set(topic.slug, {
            expiresAt: Date.now() + FOCUS_CHAIN_CACHE_TTL_MS,
            value: items
        });

        return items;
    } catch {
        return null;
    }
}

async function buildClientFocusDetail(topic: FocusTopicConfig): Promise<ClientFocusDetailResponse> {
    const cached = focusCache.get(topic.slug);
    const cachedHasQuestionCategories = Array.isArray(cached?.value.client_questions)
        && cached.value.client_questions.some((item) => typeof item?.category === 'string' && item.category.trim().length > 0);
    if (cached && cached.expiresAt > Date.now() && cachedHasQuestionCategories) {
        return cached.value;
    }

    const newsItems = await fetchFocusNewsItems(topic);
    console.log(`[focus-news] ${topic.slug}: fetched ${newsItems.length} items`);
    const modelOutput = await generateFocusContent(topic, newsItems);
    const skipWeeklyProgress =
        topic.slug === 'middle-east-tensions'
        || topic.slug === 'private-credit-stress'
        || topic.slug === 'hk-market-sentiment';
    const weeklyProgress = skipWeeklyProgress ? null : await generateWeeklyProgress(topic, newsItems);
    const transmissionChain = await generateTransmissionChain(topic, newsItems);
    const [marketSnapshot, marketChart, hibor, sectorRotation, focusPriceSnapshot, focusPriceHistory, focusSecondaryPriceSnapshot, focusSecondaryPriceHistory] = topic.slug === 'hk-market-sentiment'
        ? await Promise.all([
            fetchHongKongMarketSnapshot(),
            fetchSouthboundFlowChart(),
            fetchHiborRates(),
            fetchSectorRotation(),
            Promise.resolve(null),
            Promise.resolve(null),
            Promise.resolve(null),
            Promise.resolve(null)
        ])
        : await Promise.all([
            Promise.resolve(null),
            Promise.resolve(null),
            Promise.resolve(null),
            Promise.resolve(null),
            fetchFocusPriceSnapshot(topic.slug),
            fetchFocusPriceHistory(topic.slug),
            fetchFocusSecondaryPriceSnapshot(topic.slug),
            fetchFocusSecondaryPriceHistory(topic.slug)
        ]);
    const whatChanged =
        topic.slug === 'middle-east-tensions'
            ? await generateMiddleEastWhatChanged(newsItems)
            : topic.slug === 'private-credit-stress'
                ? await generatePrivateCreditWhatChanged(newsItems)
                : [];
    const dynamicClientQuestions = await generateDynamicClientQuestions(topic, newsItems);
    const dailyVerdict = await generateDailyVerdict(topic, newsItems);
    const questionCategoryPool = FOCUS_QUESTION_CATEGORIES[topic.slug] ?? [];
    const clientQuestions =
        dynamicClientQuestions
        ?? topic.clientQuestions.map((item, index) => ({
            question: item.question,
            answer: item.answer,
            category: item.category ?? questionCategoryPool[index % Math.max(questionCategoryPool.length, 1)] ?? '全部'
        }));

    const detail: ClientFocusDetailResponse = {
        slug: topic.slug,
        title: topic.title,
        status:
            topic.slug === 'middle-east-tensions'
                ? '持续发酵'
                : sanitizeFocusStatus(modelOutput?.status?.trim(), topic.fallbackStatus ?? '持续发酵'),
        updated_at: formatRelativeTime(newsItems[0]?.published_at),
        summary:
            (topic.slug === 'middle-east-tensions' ? buildMiddleEastSummaryOverride(newsItems) : null) ||
            sanitizeFocusSummary(weeklyProgress?.editor_note?.trim(), topic) ||
            sanitizeFocusSummary(modelOutput?.summary?.trim(), topic) ||
            buildFocusSummaryFallback(topic),
        accent: topic.accent,
        latest_updates:
            topic.slug === 'middle-east-tensions'
                ? sanitizeLatestUpdates(topic, modelOutput?.latest_updates, newsItems)
                : topic.slug === 'private-credit-stress'
                    ? []
                    : topic.slug === 'usd-strength'
                        ? []
                        : sanitizeWeeklyProgress(weeklyProgress?.items, newsItems),
        what_changed: whatChanged.length > 0 ? whatChanged : undefined,
        client_questions: clientQuestions,
        transmission_chain: transmissionChain ?? buildFallbackTransmission(topic),
        related_assets: topic.relatedAssets,
        market_snapshot: marketSnapshot,
        market_chart: marketChart,
        hibor: hibor ?? undefined,
        sector_rotation: sectorRotation ?? undefined,
        focus_price_snapshot: focusPriceSnapshot ?? undefined,
        focus_price_history: focusPriceHistory ?? undefined,
        focus_secondary_price_snapshot: focusSecondaryPriceSnapshot ?? undefined,
        focus_secondary_price_history: focusSecondaryPriceHistory ?? undefined,
        gold_drivers: topic.slug === 'gold-repricing' ? buildGoldDrivers(newsItems) : undefined,
        daily_verdict: dailyVerdict ?? null,
        disclaimer: DEFAULT_DISCLAIMER
    };

    focusCache.set(topic.slug, {
        expiresAt: Date.now() + (
            topic.slug === 'hk-market-sentiment'
                ? FOCUS_LIVE_MARKET_CACHE_TTL_MS
                : topic.slug === 'private-credit-stress'
                    ? FOCUS_LONG_CACHE_TTL_MS
                    : FOCUS_CACHE_TTL_MS
        ),
        value: detail
    });

    return detail;
}

export async function getClientFocusList(): Promise<ClientFocusListItem[]> {
    const details = await Promise.all(FOCUS_TOPICS.map((topic) => buildClientFocusDetail(topic)));
    return details.map((item) => ({
        slug: item.slug,
        title: item.title,
        status: item.status,
        updated_at: item.updated_at,
        summary: item.summary,
        accent: item.accent,
        preview_questions: buildClientFocusPreviewQuestions(
            getFocusTopic(item.slug) ?? {
                slug: item.slug,
                title: item.title,
                accent: item.accent,
                query: '',
                clientQuestions: item.client_questions,
                relatedAssets: [],
                fallbackSummary: item.summary
            },
            item.summary,
            item.client_questions
        ),
        client_questions: item.client_questions.length > 0
            ? item.client_questions.map((entry) => ({ question: entry.question }))
            : getPreviewQuestions(getFocusTopic(item.slug) ?? {
                slug: item.slug,
                title: item.title,
                accent: item.accent,
                query: '',
                clientQuestions: item.client_questions,
                relatedAssets: [],
                fallbackSummary: item.summary
            })
    }));
}

export async function getClientFocusMarketState(): Promise<ClientFocusMarketStateResponse> {
    const snapshot = await fetchClientFocusMarketStateSnapshot();
    return snapshot ?? {
        summary: '跨资产信号仍有分化，建议结合黄金、美元、美股与港股的相对表现理解今天的客户焦点。',
        indices: []
    };
}

export async function getClientFocusDetail(slug: string): Promise<ClientFocusDetailResponse | null> {
    const topic = getFocusTopic(slug);
    if (!topic) {
        return null;
    }

    return buildClientFocusDetail(topic);
}

export async function getMiddleEastPolymarket(): Promise<ClientFocusPolymarketResponse> {
    const cached = polymarketCache.get('middle-east-tensions');
    if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
    }

    const settled = await Promise.allSettled(
        MIDDLE_EAST_POLYMARKET_MARKETS.map((market) => fetchPolymarketMarket(market))
    );

    const markets = settled.flatMap((result) => (result.status === 'fulfilled' ? [result.value] : []));
    const payload = { markets };

    polymarketCache.set('middle-east-tensions', {
        expiresAt: Date.now() + POLYMARKET_CACHE_TTL_MS,
        value: payload
    });

    return payload;
}
