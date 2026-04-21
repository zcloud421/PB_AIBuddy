import type {
    ClientFocusConversationOpener,
    ClientFocusDailyVerdict,
    ClientFocusDriverItem,
    ClientFocusDetailResponse,
    ClientFocusHibor,
    ClientFocusListItem,
    ClientFocusMarketClientFocus,
    ClientFocusMarketStateResponse,
    ClientFocusMarketChart,
    ClientFocusMarketSnapshot,
    ClientFocusMiddleEastSignals,
    ClientFocusPolymarketMarket,
    ClientFocusPolymarketResponse,
    ClientFocusPriceHistoryPoint,
    ClientFocusPriceSnapshot,
    ClientFocusQuestion,
    ClientFocusSectorRotation,
    ClientFocusTransmissionItem,
    ClientFocusUpdate,
    DailyMarketNarrative,
    NewsItem,
    WhatChangedGroup
} from '../types/api';
import { fetchNewsItemsByQuery, fetchNewsItemsFromNewsData } from '../data/news-fetcher';
import { MassiveDataFetcher } from '../data/massive-fetcher';
import { MassiveClient } from '../data/massive-client';
import { getLatestClientFocusDailyVerdict, getLatestThemeBasketResult, getUpcomingEarningsNextNDays } from '../db/queries/ideas';
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
    'hk-market-sentiment': ['市场节奏', '科技板块', '南向资金'],
    'usd-strength': ['美元驱动', '汇率传导', '相关资产'],
    'private-credit-stress': ['信贷与债券', '股票影响', '房地产风险'],
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

const ALLOWED_FOCUS_STATUS = ['关注升温', '持续发酵', '压力上升', '风险溢价抬升', '供应扰动交易中', '风险溢价回吐中', '避险扩散确认中'] as const;
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
        query: 'US dollar USDCNH DXY ceasefire oil yields Hong Kong stocks latest',
        newsQueries: [
            'US dollar DXY USDCNH latest',
            'US dollar weakness CNH rebound latest',
            'ceasefire oil dollar safe haven latest',
            'Fed yields dollar Hong Kong stocks latest'
        ],
        clientQuestions: [
            {
                question: '停火后美元为何重新走弱？',
                answer: '美元回落通常意味着避险溢价开始消退，而不是联储立场突然转向。若油价同步回落、10年期美债收益率不再上冲，资金会先撤出防守型美元仓位。客户应观察DXY和USDCNH能否同时延续回落。'
            },
            {
                question: '美元回落后，USDCNH重新走低说明什么？',
                answer: 'USDCNH回落通常反映美元避险需求降温与人民币阶段性修复同步发生。若中间价继续偏稳、港股与中国资产止跌，说明汇率压力更多来自美元端而非人民币基本面恶化。'
            },
            {
                question: '美元方向变化对港股意味着什么？',
                answer: '美元不再单边走强时，港股面临的外资和流动性压力会先边际缓和。若联系汇率下港元流动性不再持续收紧，估值压制也会减轻。客户更该关注南向资金能否把这一步修复接住。'
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
const DAILY_NARRATIVE_CACHE_SCHEMA_VERSION = 3;
const dailyNarrativeCache = {
    expiresAt: 0,
    schemaVersion: DAILY_NARRATIVE_CACHE_SCHEMA_VERSION,
    value: null as DailyMarketNarrative | null
};
let dailyNarrativeRefreshPromise: Promise<DailyMarketNarrative | null> | null = null;
let previousRankedSlugs: string[] = [];
let narrativeHistory: Array<{ date: string; primary_slug: string }> = [];
const DAILY_NARRATIVE_CACHE_TTL_MS = 12 * 60 * 60 * 1000;

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

function normalizeMiddleEastFxQuestion(item: {
    question: string;
    answer: string;
    category?: string;
    logic?: string;
    observation?: string;
}) {
    const combined = `${item.question} ${item.answer} ${item.logic ?? ''} ${item.observation ?? ''}`.toLowerCase();
    if (
        item.category !== '汇率'
        && !combined.includes('cnh')
        && !combined.includes('usdcnh')
        && !combined.includes('人民币')
    ) {
        return item;
    }

    if (
        !combined.includes('cnh')
        && !combined.includes('usdcnh')
        && !combined.includes('人民币')
    ) {
        return item;
    }

    return {
        question: '地缘风险缓和后，美元的避险溢价会如何回吐？',
        answer: '美元若从避险交易回归，通常先看油价与10年期美债收益率是否同步回落，再看DXY是否失去支撑。若谈判推进但运输与供应端仍反复，美元回吐会偏慢；若中东供应担忧缓和，美元与黄金的避险溢价更可能一起压缩。',
        category: '汇率',
        logic: '先区分避险溢价还是利差驱动，再看DXY与美债是否同步回落。',
        observation: '观察DXY、10Y美债与布伦特油价是否同时回落。'
    };
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

function buildMiddleEastPrimaryEvent(newsItems: NewsItem[]): string | null {
    const recentTitles = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 10)
        .map((item) => item.title?.trim())
        .filter((title): title is string => Boolean(title));

    if (recentTitles.length === 0) {
        return null;
    }

    const joined = recentTitles.join(' ').toLowerCase();

    const hasBlockadeExecution = recentTitles.some((title) => /封锁|关闭霍尔木兹|生效|实施|execute|effective|blockade|seal/i.test(title));
    if (hasBlockadeExecution) {
        return '霍尔木兹海峡封锁进入执行阶段';
    }

    const hasTalkBreakdown = recentTitles.some((title) => /谈判破裂|会谈未达成|未达成协议|breakdown|talks fail|talks failed/i.test(title));
    if (hasTalkBreakdown) {
        return '美伊谈判破裂，市场重估冲突持续时间';
    }

    const hasCeasefireSignal = /停火|ceasefire|truce|deal|协议达成|和平协议/.test(joined);
    const hasEscalationSignal = /空袭|导弹|封锁|blockade|strike|missile|escalation/.test(joined);
    if (hasCeasefireSignal && !hasEscalationSignal) {
        return '停火信号落地，地缘风险溢价开始回吐';
    }

    const hasShippingDisruption = recentTitles.some((title) => /油轮|船只|航运|运费|保险|shipping|tanker|freight|insurance/i.test(title));
    if (hasShippingDisruption) {
        return '霍尔木兹通航受阻，原油运输风险重新定价';
    }

    const hasOilMove = recentTitles.some((title) => /油价|原油|wti|brent/i.test(title));
    if (hasOilMove) {
        return '油价波动扩大，能源风险溢价重新进入定价';
    }

    const hasCrossAssetMove = recentTitles.some((title) => /黄金|美债|收益率|美元|gold|treasury|yield|dollar/i.test(title));
    if (hasCrossAssetMove) {
        return '避险资产与利率路径开始同步反应';
    }

    return null;
}

function buildMiddleEastCommunicationFocus(newsItems: NewsItem[]): string | null {
    const recentTitles = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 10)
        .map((item) => item.title?.trim())
        .filter((title): title is string => Boolean(title));

    if (recentTitles.length === 0) {
        return null;
    }

    const joined = recentTitles.join(' ').toLowerCase();

    if (recentTitles.some((title) => /封锁|关闭霍尔木兹|生效|实施|execute|effective|blockade|seal/i.test(title))) {
        return '市场开始交易霍尔木兹相关供应与航运风险，是否扩散到更广泛资产重定价仍取决于执行强度与持续时间。';
    }

    if (recentTitles.some((title) => /谈判破裂|会谈未达成|未达成协议|breakdown|talks fail|talks failed/i.test(title))) {
        return '市场重新交易冲突持续时间与油价风险溢价，是否进一步扩散到利率与风险资产仍需观察。';
    }

    const hasCeasefireSignal = /停火|ceasefire|truce|deal|协议达成|和平协议/.test(joined);
    const hasEscalationSignal = /空袭|导弹|封锁|blockade|strike|missile|escalation/.test(joined);
    if (hasCeasefireSignal && !hasEscalationSignal) {
        return '市场正从避险回到持续性判断，关键在于停火能否稳定以及油价与避险溢价是否继续回吐。';
    }

    if (recentTitles.some((title) => /油轮|船只|航运|运费|保险|shipping|tanker|freight|insurance/i.test(title))) {
        return '运输与保险成本已经开始被重新定价，后续要观察这是否演变为更实质的供应扰动。';
    }

    if (recentTitles.some((title) => /油价|原油|wti|brent/i.test(title))) {
        return '市场正在把原油波动传导到通胀与利率路径预期，持续性仍需更多跨资产验证。';
    }

    if (recentTitles.some((title) => /黄金|美债|美元|收益率|gold|treasury|yield|dollar/i.test(title))) {
        return '避险资产与利率路径开始同步反应，关键在于这是否升级为更广泛的资产重定价。';
    }

    return null;
}

function buildMiddleEastStatus(newsItems: NewsItem[]): string {
    const recentTitles = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 10)
        .map((item) => item.title?.trim())
        .filter((title): title is string => Boolean(title));

    const joined = recentTitles.join(' ').toLowerCase();

    const hasCeasefireSignal = /停火|ceasefire|truce|deal|协议达成|和平协议/.test(joined);
    const hasEscalationSignal = /空袭|导弹|封锁|blockade|strike|missile|escalation/.test(joined);
    const hasNegotiationSignal = /谈判|会谈|外交|斡旋|原则性协议|延长停火|间接谈判|阿曼|卡塔尔|巴基斯坦/.test(joined);
    const hasShippingSignal = recentTitles.some((title) => /油轮|船只|航运|运费|保险|shipping|tanker|freight|insurance/i.test(title));
    const hasHardBlockadeSignal = recentTitles.some((title) => /封锁|关闭霍尔木兹|生效|实施|execute|effective|blockade|seal/i.test(title));

    if (hasCeasefireSignal && !hasEscalationSignal) {
        return '风险溢价回吐中';
    }

    if (hasNegotiationSignal && hasShippingSignal && !hasHardBlockadeSignal) {
        return '风险溢价回吐中';
    }

    if (hasHardBlockadeSignal) {
        return '供应扰动交易中';
    }

    if (hasShippingSignal && !hasNegotiationSignal) {
        return '供应扰动交易中';
    }

    if (recentTitles.some((title) => /黄金|美债|美元|收益率|gold|treasury|yield|dollar/i.test(title))) {
        return '避险扩散确认中';
    }

    if (hasEscalationSignal || recentTitles.some((title) => /谈判破裂|会谈未达成|未达成协议|breakdown|talks fail|talks failed/i.test(title))) {
        return '风险溢价抬升';
    }

    return '持续发酵';
}

function extractMiddleEastSignals(newsItems: NewsItem[]): ClientFocusMiddleEastSignals {
    const recentTitles = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 12)
        .map((item) => item.title?.trim() ?? '');
    const joined = recentTitles.join(' ').toLowerCase();

    return {
        has_ceasefire: /停火|ceasefire|truce|协议达成|停止敌对/.test(joined),
        has_escalation: /空袭|导弹|地面行动|封锁|strike|missile|ground operation|escalation/.test(joined),
        has_negotiation: /谈判|会谈|外交|斡旋|延长停火|间接谈判|阿曼|卡塔尔|巴基斯坦|indirect talks/.test(joined),
        has_hormuz_blockade: recentTitles.some((t) => /封锁霍尔木兹|关闭霍尔木兹|hormuz.*block|blockade.*strait/i.test(t.toLowerCase())),
        has_shipping_disruption: recentTitles.some((t) => /油轮|船只|航运|运费|tanker|freight|shipping.*disruption/i.test(t.toLowerCase())),
        has_deal_close: /原则性协议|框架协议|接近达成|deal.*close|agreement.*near|framework deal/.test(joined),
        has_breakdown: /谈判破裂|谈判失败|talks.*fail|breakdown|talks.*collapse|未达成协议/.test(joined),
    };
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

logic 写作要求：
- 15-20字，帮 RM 建立解释框架
- 格式：先区分 A 还是 B，再判断 C
- 例："先区分避险需求还是实际利率驱动，再看美元是否同步确认。"

observation 写作要求：
- 15-20字，今天这个情景下该看的具体变量
- 必须结合当前新闻情景，不能写成通用模板
- 例："布伦特与WTI的价差走向，以及霍尔木兹保险成本是否继续抬升。"

返回 JSON 数组格式：
[
  {
    "question": "...",
    "answer": "...",
    "category": "黄金",
    "logic": "...",
    "observation": "..."
  }
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
  * 汇率/美元：优先从美元指数、避险美元需求和美元回吐路径出发，解释汇率传导机制，不要从CNH或人民币角度发问
  * 利率/央行路径（美联储降息预期）：问降息预期的触发条件，引导查CME FedWatch
  * 大宗商品（油价、黄金）：问供应中断逻辑和避险溢价机制
  * 股票：优先问美股（标普/纳斯达克）和港股大盘的传导机制，其次问科技股的影响路径，能源股可作为补充，避免问航运股
  * 债券/固收：问美债、信用利差的路径
  * 结构性长期变化（如有，例如petroyuan、结算体系）

问题语气要求（基于情景，体现具体观察点，不包含价格数字）：
✓ "停火协议达成后，霍尔木兹通道风险溢价如何传导到油价？"
✓ "停火预期升温后，美元的避险溢价会如何回吐？"
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
以下是过去7天与美元、人民币、港股、油价和利率路径相关的新闻。请先判断当前美元情景，再基于这些最新信息，
为以下固定问题与市场框架延伸出 6 到 8 个客户常问问答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

情景判断规则：
- 若新闻以“停火/ceasefire/deal/谈判/油价回落/避险消退”为主，且美元与USDCNH同步回落 → 情景：美元走弱
- 若新闻以“空袭/escalation/油价上行/避险需求/降息推迟”为主，且美元与USDCNH同步走高 → 情景：美元走强
- 若美元、利率与风险资产信号互相冲突 → 情景：区间震荡

写作要求：
- 每条答案 90-110 字
- 必须采用“先判断 → 再补事实 → 再讲传导 → 最后给风险边界”的结构
- 第一两句必须能单独成立，像直接回复客户
- 必须至少包含 1 个具体变量或事实，例如 DXY、USDCNH、联储表态、人民币中间价、港元流动性、10Y美债收益率
- 问题必须与判断出的当前情景一致，不能继续沿用过时的“美元反弹”或“强美元承压”假设
- 第一条问题优先抓当前最新变化，例如“美元为何重新走弱”或“USDCNH为何快速回落”
- 重点解释避险需求、利率路径、油价通胀渠道，以及美元与港股流动性的关系
- 覆盖三个维度：美元驱动、汇率传导、相关资产；每个维度至少 1 题
- 尽量用“当前更像…”“本周新增变化是…”“整体来看…”这类客户沟通句式
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 若问题是“美元为何走弱/走强”，重点回答避险需求、油价和利率路径谁在主导
- 若问题是“人民币或CNH如何反应”，重点回答美元端变化与中间价/政策韧性如何共同作用
- 若问题是“美元方向变化对港股或中国资产意味着什么”，重点回答外资、流动性与估值传导

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
        const parsed = safeParseJsonArray(content) as Array<{
            question?: string;
            answer?: string;
            category?: string;
            logic?: string;
            observation?: string;
        }> | null;
        console.log(`[focus-client-questions] ${topic.slug}: parsed ${parsed ? parsed.length : 'null'} items, categories=${parsed ? [...new Set(parsed.map((i: any) => i?.category).filter(Boolean))].join(',') : 'n/a'}`);

        if (!parsed) {
            console.warn(`[focus-client-questions] ${topic.slug}: json_parse_failed`);
            return null;
        }

        const categoryPool = FOCUS_QUESTION_CATEGORIES[topic.slug] ?? [];
        const allowedCategories = new Set(categoryPool);
        const sanitized: Array<{
            question: string;
            answer: string;
            category?: string;
            logic?: string;
            observation?: string;
        }> = parsed
            .map((item, index) => {
                const question = item.question?.trim();
                const answer = item.answer?.trim();
                const rawCategory = item.category?.trim();
                const logic = item.logic?.trim();
                const observation = item.observation?.trim();
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
                    category,
                    logic: logic || undefined,
                    observation: observation || undefined
                };
            })
            .filter((item): item is NonNullable<typeof item> => item !== null);

        const normalized = topic.slug === 'middle-east-tensions'
            ? sanitized.map((item) => normalizeMiddleEastFxQuestion(item))
            : sanitized;

        const categoryCount = new Map<string, number>();
        const capped = normalized.filter((item) => {
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
                category: inferQuestionCategory(topic.slug, item.question, item.answer, fallbackCategory, false),
                logic: item.logic?.trim() || undefined,
                observation: item.observation?.trim() || undefined
            };
        });
    } catch {
        console.warn(`[focus-client-questions] ${topic.slug}: exception`);
        return null;
    }
}

function describeMiddleEastSignals(signals: ClientFocusMiddleEastSignals): string {
    const activeSignals = [
        signals.has_ceasefire ? '出现停火或缓和信号' : null,
        signals.has_escalation ? '仍有升级或军事打击信号' : null,
        signals.has_negotiation ? '外交谈判仍在推进' : null,
        signals.has_hormuz_blockade ? '霍尔木兹封锁风险被反复提及' : null,
        signals.has_shipping_disruption ? '航运与运输扰动开始被市场定价' : null,
        signals.has_deal_close ? '谈判接近达成的框架信号升温' : null,
        signals.has_breakdown ? '谈判破裂或失败信号已经出现' : null,
    ].filter((item): item is string => Boolean(item));

    return activeSignals.length > 0 ? activeSignals.join('；') : '当前信号仍偏混合，没有形成单一主线。';
}

async function generateMiddleEastMarketClientFocus(
    newsItems: NewsItem[],
    signals: ClientFocusMiddleEastSignals
): Promise<ClientFocusMarketClientFocus | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const recentNews = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 10);

    if (recentNews.length === 0) {
        return null;
    }

    const newsSection = recentNews
        .map((item, index) => `${index + 1}. ${item.title}`)
        .join('\n');

    const userPrompt = `
最新新闻标题列表：
${newsSection}

当前信号摘要：
${describeMiddleEastSignals(signals)}

生成 2 个条目，JSON 格式：
[
  { "label": "市场当前定价", "content": "..." },
  { "label": "客户普遍在问", "content": "..." }
]

写作要求：
- 市场当前定价：描述市场在押注什么，引用具体资产（油价/黄金/美债），不给方向判断
- 客户普遍在问：描述客户当下最关心的 1 个核心问题，不含“建议”“应当”等词
- 每条 content 25-40 字，客观陈述，主语为“市场”或“投资者”
- 必须基于当前新闻，不能写通用模板
- 禁止“风险升温”“情绪波动”“不确定性”等空泛表述
- 只输出 JSON
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
                    { role: 'system', content: '你是香港私人银行市场沟通助手。' },
                    { role: 'user', content: userPrompt }
                ],
                max_tokens: 220,
                temperature: 0.3
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const parsed = safeParseJson(payload.choices?.[0]?.message?.content ?? '') as
            | Array<{ label?: string; content?: string }>
            | null;

        const items = Array.isArray(parsed)
            ? parsed
                  .map((item) => ({
                      label: typeof item?.label === 'string' ? item.label.trim() : '',
                      content: typeof item?.content === 'string' ? item.content.trim() : ''
                  }))
                  .filter((item) => item.label.length > 0 && item.content.length > 0)
                  .slice(0, 2)
            : [];

        return items.length > 0 ? { items } : null;
    } catch {
        return null;
    }
}

async function generateMiddleEastConversationOpeners(
    newsItems: NewsItem[],
    signals: ClientFocusMiddleEastSignals
): Promise<ClientFocusConversationOpener[] | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const recentNews = newsItems
        .slice()
        .sort((a, b) => new Date(b.published_at).getTime() - new Date(a.published_at).getTime())
        .slice(0, 5);

    if (recentNews.length === 0) {
        return null;
    }

    const scenario: ClientFocusConversationOpener['scenario'] =
        signals.has_breakdown
            ? 'breakdown_scenario'
            : signals.has_deal_close || (signals.has_ceasefire && signals.has_negotiation)
                ? 'ceasefire_window'
                : signals.has_ceasefire && !signals.has_escalation
                    ? 'deal_scenario'
                    : signals.has_escalation || signals.has_hormuz_blockade
                        ? 'breakdown_scenario'
                        : 'general';

    const scenarioDescription: Record<ClientFocusConversationOpener['scenario'], string> = {
        ceasefire_window: '停火窗口期，谈判接近达成',
        deal_scenario: '缓和阶段，市场开始评估交易修复',
        breakdown_scenario: '谈判破裂或升级风险上升',
        general: '局势仍在拉锯，市场尚未形成单一主线'
    };

    const newsSection = recentNews
        .map((item, index) => `${index + 1}. ${item.title}`)
        .join('\n');

    const userPrompt = `
当前情景：${scenarioDescription[scenario]}
最新新闻摘要：
${newsSection}

生成 2 个对话切入问题，JSON 格式：
[
  { "scenario": "${scenario}", "question": "..." },
  { "scenario": "${scenario}", "question": "..." }
]

问题写作硬性约束：
1. 主语必须是客户的组合或持仓，不能是“市场”或“油价”
2. 问的是客户的认知和意图，不给任何方向性判断
3. 每个问题能自然引出产品或资产配置讨论，但不直接点名产品
4. 问题长度：20-35字
5. 禁止：“您觉得油价会...” “您认为黄金会...”等市场走势类问题
6. 只输出 JSON
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
                    { role: 'system', content: '你是香港私人银行资深RM培训师，帮助RM用一句问题开启客户对话。' },
                    { role: 'user', content: userPrompt }
                ],
                max_tokens: 220,
                temperature: 0.4
            })
        });

        if (!response.ok) {
            return null;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const parsed = safeParseJson(payload.choices?.[0]?.message?.content ?? '') as
            | Array<{ scenario?: string; question?: string }>
            | null;

        const openers = Array.isArray(parsed)
            ? parsed
                  .map((item) => ({
                      scenario,
                      question: typeof item?.question === 'string' ? item.question.trim() : ''
                  }))
                  .filter((item) => item.question.length > 0)
                  .slice(0, 2)
            : [];

        return openers.length > 0 ? openers : null;
    } catch {
        return null;
    }
}

type NarrativeTopicPromptContext = {
    slug: string;
    title: string;
    status: string;
    summary: string;
    latest_updates: Array<{ time: string; date: string | undefined; title: string; impact: string }>;
    transmission_chain: Array<{ order: string; title: string; pricing: string; summary: string }>;
};

function formatNarrativeMarketMove(
    item: ClientFocusMarketStateResponse['indices'][number] | undefined
): string | null {
    if (!item) {
        return null;
    }

    const todayChange = item.change_pct;
    if (typeof todayChange !== 'number' || Number.isNaN(todayChange)) {
        return null;
    }

    const timeLabel = ['SPX', 'NDX', 'TNX', 'DXY'].includes(item.code) ? '昨日收盘' : '今日';

    if (item.code === 'TNX') {
        return `${item.name}${timeLabel}${todayChange >= 0 ? '+' : ''}${todayChange.toFixed(1)}bps`;
    }

    return `${item.name}${timeLabel}${todayChange >= 0 ? '+' : ''}${todayChange.toFixed(2)}%`;
}

function computeCalendarYtdChange(
    history: ClientFocusPriceHistoryPoint[] | null | undefined
): number | null {
    if (!history || history.length < 2) {
        return null;
    }

    const currentYear = new Date().getFullYear();
    const yearStartPoint = history.find((point) => {
        if (!point.date || point.close === null) {
            return false;
        }
        return point.date >= `${currentYear}-01-01`;
    });
    const latestPoint = history[history.length - 1];

    if (!yearStartPoint || yearStartPoint.close === null || latestPoint?.close === null || yearStartPoint.close === 0) {
        return null;
    }

    return ((latestPoint.close - yearStartPoint.close) / yearStartPoint.close) * 100;
}

function computeTrailingChange(
    history: ClientFocusPriceHistoryPoint[] | null | undefined,
    tradingDays: number
): number | null {
    if (!history || history.length < tradingDays + 1) {
        return null;
    }

    const window = history.slice(-(tradingDays + 1));
    const startPoint = window[0];
    const latestPoint = window[window.length - 1];

    if (!startPoint || startPoint.close === null || latestPoint?.close === null || startPoint.close === 0) {
        return null;
    }

    return ((latestPoint.close - startPoint.close) / startPoint.close) * 100;
}

function computeCalendarYtdBpsChange(
    history: ClientFocusPriceHistoryPoint[] | null | undefined
): number | null {
    if (!history || history.length < 2) {
        return null;
    }

    const currentYear = new Date().getFullYear();
    const yearStartPoint = history.find((point) => {
        if (!point.date || point.close === null) {
            return false;
        }
        return point.date >= `${currentYear}-01-01`;
    });
    const latestPoint = history[history.length - 1];

    if (!yearStartPoint || yearStartPoint.close === null || latestPoint?.close === null) {
        return null;
    }

    return (latestPoint.close - yearStartPoint.close) * 100;
}

function computeTrailingBpsChange(
    history: ClientFocusPriceHistoryPoint[] | null | undefined,
    tradingDays: number
): number | null {
    if (!history || history.length < tradingDays + 1) {
        return null;
    }

    const window = history.slice(-(tradingDays + 1));
    const startPoint = window[0];
    const latestPoint = window[window.length - 1];

    if (!startPoint || startPoint.close === null || latestPoint?.close === null) {
        return null;
    }

    return (latestPoint.close - startPoint.close) * 100;
}

function formatMonitoringContext(
    item: ClientFocusMarketStateResponse['indices'][number] | undefined
): string | null {
    if (!item) {
        return null;
    }

    const parts: string[] = [];
    const todayLabel = ['SPX', 'NDX', 'TNX', 'DXY'].includes(item.code) ? '昨日收盘' : '今日';

    if (typeof item.change_pct === 'number' && !Number.isNaN(item.change_pct)) {
        if (item.code === 'TNX') {
            parts.push(`${todayLabel}${item.change_pct >= 0 ? '+' : ''}${item.change_pct.toFixed(1)}bps`);
        } else {
            parts.push(`${todayLabel}${item.change_pct >= 0 ? '+' : ''}${item.change_pct.toFixed(2)}%`);
        }
    }

    if (typeof item.change_5d_pct === 'number' && !Number.isNaN(item.change_5d_pct)) {
        if (item.code === 'TNX') {
            parts.push(`5日${item.change_5d_pct >= 0 ? '+' : ''}${item.change_5d_pct.toFixed(1)}bps`);
        } else {
            parts.push(`5日${item.change_5d_pct >= 0 ? '+' : ''}${item.change_5d_pct.toFixed(2)}%`);
        }
    }

    if (typeof item.change_ytd_pct === 'number' && !Number.isNaN(item.change_ytd_pct)) {
        if (item.code === 'TNX') {
            parts.push(`YTD${item.change_ytd_pct >= 0 ? '+' : ''}${item.change_ytd_pct.toFixed(1)}bps`);
        } else {
            parts.push(`YTD${item.change_ytd_pct >= 0 ? '+' : ''}${item.change_ytd_pct.toFixed(2)}%`);
        }
    }

    return parts.length > 0 ? `${item.name}${parts.join('，')}` : null;
}

function joinMonitoringContexts(
    items: Array<ClientFocusMarketStateResponse['indices'][number] | undefined>
): string {
    return items
        .map((item) => formatMonitoringContext(item))
        .filter((item): item is string => Boolean(item))
        .join('；');
}

function buildFallbackDailyNarrative(
    cachedTopics: NarrativeTopicPromptContext[],
    marketSnapshot: ClientFocusMarketStateResponse | null
): DailyMarketNarrative | null {
    const primaryTopic = cachedTopics.find((topic) => topic.status === '持续发酵') ?? cachedTopics[0];
    if (!primaryTopic) {
        return null;
    }

    const indices = marketSnapshot?.indices ?? [];
    const byCode = new Map(indices.map((item) => [item.code, item]));
    const rankedSlugs = cachedTopics.map((topic) => topic.slug);
    const narrative = (marketSnapshot?.summary ?? primaryTopic.summary ?? '').trim();
    const fallbackNarrative = narrative.length > 0 ? narrative : `${primaryTopic.title} 仍是今天最值得先和客户解释的市场主线。`;

    const assetBuckets: DailyMarketNarrative['asset_buckets'] = [];

    const spx = byCode.get('SPX');
    const ndx = byCode.get('NDX');
    const hsi = byCode.get('HSI');
    const hstech = byCode.get('HSTECH');
    const gold = byCode.get('GOLD');
    const tnx = byCode.get('TNX');
    const oil = byCode.get('OIL') ?? byCode.get('BRENT');

    const usEquitySignal = joinMonitoringContexts([spx, ndx]);
    if (usEquitySignal) {
        const spxPct = spx?.change_pct ?? null;
        const spx5d = spx?.change_5d_pct ?? null;
        let usAttribution = '美股回吐更像周末地缘扰动与财报前仓位回摆，尚不足以定义主线逆转。';
        let usImplication = '继续看本周财报能否验证反弹质量，并留意联储路径预期会否被事件与听证再度改写。';
        if (typeof spx5d === 'number' && spx5d >= 4) {
            usAttribution = '美股5日累涨后仍在高位，当前更像流动性与空头回补推动下等待财报验证的阶段。';
            usImplication = '重点看财报季能否证明这波上涨由盈利支撑，而不只是高位情绪延续。';
        } else if (typeof spxPct === 'number' && spxPct <= -1.5) {
            usAttribution = '昨夜美股明显回落，更像地缘与宏观扰动压制风险偏好，而非盈利主线突然逆转。';
            usImplication = '继续看财报与政策路径能否稳住风险偏好，判断回调是噪音还是反弹质量转弱。';
        } else if (typeof spxPct === 'number' && spxPct > -1 && spxPct < 0 && typeof spx5d === 'number' && spx5d >= 2.5) {
            usAttribution = '昨夜小幅回吐更像创新高后的温和整固，周末headline放大了短线情绪但未改写主线。';
            usImplication = '继续看财报窗口是否能验证AI与权重股盈利韧性，而不是把这波回吐当成主线转弱。';
        } else if (typeof spxPct === 'number' && spxPct >= 1) {
            usAttribution = '美股反弹更多反映地缘缓和后的risk-on延续，但成色仍取决于接下来的财报验证。';
            usImplication = '继续看财报季与利率路径是否支持这轮上涨从情绪修复扩散到盈利主线。';
        }
        assetBuckets.push({
            bucket: '美股',
            thesis_check: usAttribution,
            today_signal: usEquitySignal,
            portfolio_implication: usImplication
        });
    }

    const hkSignal = joinMonitoringContexts([hsi, hstech]);
    if (hkSignal) {
        const hsi5d = hsi?.change_5d_pct ?? null;
        let hkAttribution = '港股反弹更多反映地缘缓和、油价回落与中国增长/政策底对风险偏好的支撑。';
        let hkImplication = '继续看这波修复能否从流动性与情绪扩散到更广泛的盈利与政策主线。';
        if (typeof hsi5d === 'number' && hsi5d >= 3) {
            hkAttribution = '港股连续修复说明风险偏好回暖，但当前仍更多由流动性改善与主题热度驱动。';
            hkImplication = '继续看这波上行能否被盈利、政策与更广泛行业扩散验证，而不只停留在情绪提振。';
        } else if (typeof hsi5d === 'number' && hsi5d <= -3) {
            hkAttribution = '港股仍处在修复早段，单日反弹更像情绪回补，尚不足以单独确认趋势反转。';
            hkImplication = '继续看中国增长与政策信号能否稳住修复节奏，而不是把单日回升当成趋势确认。';
        } else if (typeof (hsi?.change_pct) === 'number' && Math.abs(hsi.change_pct) < 1) {
            hkAttribution = '港股今天在波动中收涨，说明市场更愿意交易地缘缓和与政策底，而不是重新进入避险模式。';
            hkImplication = '继续留意风险偏好修复会否扩散到IPO、科技与更广泛中国资产，而不只是一日情绪修复。';
        }
        assetBuckets.push({
            bucket: '港股',
            thesis_check: hkAttribution,
            today_signal: hkSignal,
            portfolio_implication: hkImplication
        });
    }

    const goldSignal = formatMonitoringContext(gold);
    const equityRiskOn =
        (typeof spx?.change_pct === 'number' && spx.change_pct >= 1)
        || (typeof ndx?.change_pct === 'number' && ndx.change_pct >= 1);
    const strongTreasuryMove = typeof tnx?.change_pct === 'number' && Math.abs(tnx.change_pct) >= 5;
    const shouldShowGold =
        Boolean(goldSignal)
        && (
            primaryTopic.slug === 'gold-repricing'
            || (typeof gold?.change_pct === 'number' && Math.abs(gold.change_pct) >= 1)
            || (equityRiskOn && strongTreasuryMove)
        );
    if (goldSignal && shouldShowGold) {
        const goldPct = gold?.change_pct ?? null;
        let goldAttribution = '黄金当前更像短期避险溢价回吐，不等于年内黄金主线已被逆转。';
        let goldImplication = '继续看谈判结果、油价与实际利率是否共振，判断这轮回调是短期risk-on还是主线转弱。';
        if (typeof goldPct === 'number' && goldPct <= -1) {
            goldAttribution = '黄金回落更像地缘风险溢价被挤出，短期避险需求弱化快于长期配置逻辑变化。';
            goldImplication = '继续看油价与实际利率是否继续压制黄金，确认这是短期回吐还是更持久的逻辑重估。';
        } else if (Math.abs(goldPct ?? 0) < 1 && equityRiskOn && strongTreasuryMove) {
            goldAttribution = '黄金自身波动不大，但与债股联动显示避险需求正在被风险偏好修复边际压制。';
            goldImplication = '继续看美债与风险资产的共振是否延续，再判断黄金是短期整理还是避险角色继续弱化。';
        }
        assetBuckets.push({
            bucket: '黄金',
            thesis_check: goldAttribution,
            today_signal: goldSignal,
            portfolio_implication: goldImplication
        });
    }

    const treasuryBucket = buildTreasuryBucket(marketSnapshot, primaryTopic.slug);
    if (treasuryBucket) {
        assetBuckets.push(treasuryBucket);
    }

    // 汇率只在有足够意义的方向信号时才纳入，避免弱波动下生成空泛内容
    const fxBucket = buildFxBucket(marketSnapshot);
    if (fxBucket && assetBuckets.length < 5) {
        assetBuckets.push(fxBucket);
    }

    // 大宗商品不作为独立bucket——原油是传导因子，PB客户不直接持有WTI

    if (assetBuckets.length === 0) {
        return null;
    }

    const prioritizedBuckets = ensurePriorityAssetBuckets(assetBuckets.slice(0, 5), primaryTopic.slug, marketSnapshot);

    return {
        primary_slug: primaryTopic.slug,
        regime_label: '今日',
        narrative: fallbackNarrative,
        ranked_slugs: rankedSlugs,
        rank_changes: {},
        momentum_days: 1,
        asset_buckets: prioritizedBuckets,
        default_expanded_bucket: prioritizedBuckets.some((item) => item.bucket === '美股') ? '美股' : prioritizedBuckets[0].bucket,
        generated_at: new Date().toISOString(),
    };
}

function collectNarrativeTopics(): NarrativeTopicPromptContext[] {
    return FOCUS_TOPICS
        .map((topic) => {
            const cached = focusCache.get(topic.slug);
            return cached?.value
                ? {
                      slug: topic.slug,
                      title: cached.value.title,
                      status: cached.value.status ?? '',
                      summary: (cached.value.summary ?? '').trim().slice(0, 60),
                      latest_updates: Array.isArray(cached.value.latest_updates)
                          ? cached.value.latest_updates
                              .slice(0, 3)
                              .map((item) => ({
                                  time: typeof item?.time === 'string' ? item.time.trim() : '',
                                  date: typeof item?.date === 'string' ? item.date.trim() : undefined,
                                  title: typeof item?.title === 'string' ? item.title.trim() : '',
                                  impact: typeof item?.impact === 'string' ? item.impact.trim() : ''
                              }))
                              .filter((item) => item.time.length > 0 && item.title.length > 0 && item.impact.length > 0)
                          : [],
                      transmission_chain: Array.isArray(cached.value.transmission_chain)
                          ? cached.value.transmission_chain
                              .slice(0, 2)
                              .map((item) => ({
                                  order: typeof item?.order === 'string' ? item.order.trim() : '',
                                  title: typeof item?.title === 'string' ? item.title.trim() : '',
                                  pricing: typeof item?.pricing === 'string' ? item.pricing.trim() : '',
                                  summary: typeof item?.summary === 'string' ? item.summary.trim() : ''
                              }))
                              .filter(
                                  (item) =>
                                      item.order.length > 0
                                      && item.title.length > 0
                                      && item.pricing.length > 0
                                      && item.summary.length > 0
                              )
                          : [],
                  }
                : null;
        })
        .filter((item): item is NarrativeTopicPromptContext => Boolean(item));
}

function computeMomentumDays(
    slug: string,
    history: Array<{ date: string; primary_slug: string }>
): number {
    const sorted = [...history].sort((a, b) => b.date.localeCompare(a.date));
    let count = 0;

    for (const record of sorted) {
        if (record.primary_slug === slug) {
            count += 1;
        } else {
            break;
        }
    }

    return Math.max(1, count);
}

function buildDailyNarrativeMarketSignalsSection(
    marketSnapshot: ClientFocusMarketStateResponse | null
): string {
    const indices = marketSnapshot?.indices ?? [];
    if (indices.length === 0) {
        return '';
    }

    const sortedSignals = indices
        .sort((left, right) => Math.abs(right.change_pct ?? 0) - Math.abs(left.change_pct ?? 0));

    if (sortedSignals.length === 0) {
        return '';
    }

    const lines = sortedSignals.map((item) => {
        const todayChange = item.change_pct;
        const fiveDayChange = item.change_5d_pct;
        const ytdChange = item.change_ytd_pct;
        const isFxOrRate = ['USDCNH', 'USDJPY', 'USDCHF', 'DXY'].includes(item.code);
        const latestStr =
            item.latest !== null && item.latest !== undefined
                ? ` (${item.latest.toFixed(isFxOrRate ? 4 : item.code === 'TNX' ? 2 : 2)})`
            : '';
        const parts: string[] = [`${item.name}${latestStr}`];

        if (item.code === 'TNX') {
            if (todayChange !== null && todayChange !== undefined && !Number.isNaN(todayChange)) {
                parts.push(`今日 ${todayChange >= 0 ? '+' : ''}${todayChange.toFixed(1)}bps`);
            }
            if (fiveDayChange !== null && fiveDayChange !== undefined && !Number.isNaN(fiveDayChange)) {
                parts.push(`5日 ${fiveDayChange >= 0 ? '+' : ''}${fiveDayChange.toFixed(1)}bps`);
            }
            if (ytdChange !== null && ytdChange !== undefined && !Number.isNaN(ytdChange)) {
                parts.push(`YTD ${ytdChange >= 0 ? '+' : ''}${ytdChange.toFixed(1)}bps`);
            }
        } else {
            if (todayChange !== null && todayChange !== undefined && !Number.isNaN(todayChange)) {
                parts.push(`今日 ${todayChange >= 0 ? '+' : ''}${todayChange.toFixed(2)}%`);
            }
            if (fiveDayChange !== null && fiveDayChange !== undefined && !Number.isNaN(fiveDayChange)) {
                parts.push(`5日 ${fiveDayChange >= 0 ? '+' : ''}${fiveDayChange.toFixed(2)}%`);
            }
            if (ytdChange !== null && ytdChange !== undefined && !Number.isNaN(ytdChange)) {
                parts.push(`YTD ${ytdChange >= 0 ? '+' : ''}${ytdChange.toFixed(2)}%`);
            }
        }

        return parts.join('  ');
    });

    return `今日跨资产变动（今日=当日变动；5日=近5交易日累计，用于判断是否处于极端位置；YTD=年初至今累计，用于判断是否已走出中期趋势；仅供叙事归因，不代表方向建议）：\n${lines.join('\n')}`;
}

async function buildEarningsCalendarSection(): Promise<string> {
    try {
        const rows = await getUpcomingEarningsNextNDays(2);
        if (rows.length === 0) return '';

        const today: string[] = [];
        const tomorrow: string[] = [];

        for (const row of rows) {
            if (row.days_until === 0) today.push(row.symbol);
            else if (row.days_until === 1) tomorrow.push(row.symbol);
        }

        const lines: string[] = [];
        if (today.length > 0) lines.push(`今日财报：${today.join('、')}`);
        if (tomorrow.length > 0) lines.push(`明日财报：${tomorrow.join('、')}`);
        if (lines.length === 0) return '';

        return `⚠️ 关键财报（优先级最高，必须在 narrative 和美股 bucket 中体现）：\n${lines.join('\n')}`;
    } catch {
        return '';
    }
}

const DAILY_NARRATIVE_BUCKETS = ['美股', '港股', '黄金', '美债', '汇率'] as const;

function isValidDailyNarrativeBucket(
    value: unknown
): value is DailyMarketNarrative['default_expanded_bucket'] {
    return typeof value === 'string' && DAILY_NARRATIVE_BUCKETS.includes(value as typeof DAILY_NARRATIVE_BUCKETS[number]);
}

function normalizeAssetBuckets(
    assetBuckets: unknown
): DailyMarketNarrative['asset_buckets'] {
    if (!Array.isArray(assetBuckets)) {
        return [];
    }

    return assetBuckets
        .filter((item): item is Record<string, unknown> => typeof item === 'object' && item !== null)
        .map((item) => ({
            bucket: item.bucket,
            thesis_check: item.thesis_check,
            today_signal: item.today_signal,
            portfolio_implication: item.portfolio_implication
        }))
        .filter(
            (
                item
            ): item is DailyMarketNarrative['asset_buckets'][number] =>
                isValidDailyNarrativeBucket(item.bucket)
                && typeof item.thesis_check === 'string'
                && typeof item.today_signal === 'string'
                && typeof item.portfolio_implication === 'string'
                && item.thesis_check.trim().length > 0
                && item.today_signal.trim().length > 0
                && item.portfolio_implication.trim().length > 0
        )
        .slice(0, 5)
        .map((item) => ({
            bucket: item.bucket,
            thesis_check: item.thesis_check.trim(),
            today_signal: item.today_signal.trim(),
            portfolio_implication: item.portfolio_implication.trim()
        }));
}

function buildTreasuryBucket(
    marketSnapshot: ClientFocusMarketStateResponse | null,
    primarySlug: string
): DailyMarketNarrative['asset_buckets'][number] | null {
    const tnx = marketSnapshot?.indices.find((item) => item.code === 'TNX');
    if (!tnx) {
        return null;
    }

    const oil = marketSnapshot?.indices.find((item) => item.code === 'OIL') ?? marketSnapshot?.indices.find((item) => item.code === 'BRENT');
    const spx = marketSnapshot?.indices.find((item) => item.code === 'SPX');
    const ndx = marketSnapshot?.indices.find((item) => item.code === 'NDX');
    const dxy = marketSnapshot?.indices.find((item) => item.code === 'DXY');
    const todayChange = tnx?.change_pct;
    const fiveDayChange = tnx?.change_5d_pct;
    const hasSignal =
        (typeof todayChange === 'number' && !Number.isNaN(todayChange) && Math.abs(todayChange) >= 3)
        || (typeof fiveDayChange === 'number' && !Number.isNaN(fiveDayChange) && Math.abs(fiveDayChange) >= 5)
        || ['middle-east-tensions', 'gold-repricing', 'usd-strength', 'private-credit-stress'].includes(primarySlug);

    const todaySignal = formatMonitoringContext(tnx) ?? '美债收益率路径出现变化，利率预期重新定价。';

    let thesisCheck = '美债当前更多反映利率与避险再定价，关键是区分通胀缓和、联储路径还是长端供给主导。';
    let portfolioImplication = '继续看收益率变化背后是通胀回落、避险买盘还是term premium回升，判断久期压力是缓解还是重来。';

    const oilDown = typeof oil?.change_pct === 'number' && oil.change_pct <= -3;
    const oilUp = typeof oil?.change_pct === 'number' && oil.change_pct >= 3;
    const equityRiskOff =
        (typeof spx?.change_pct === 'number' && spx.change_pct <= -0.8)
        || (typeof ndx?.change_pct === 'number' && ndx.change_pct <= -1);
    const equityRiskOn =
        (typeof spx?.change_pct === 'number' && spx.change_pct >= 0.8)
        || (typeof ndx?.change_pct === 'number' && ndx.change_pct >= 1);
    const tnxDown = typeof todayChange === 'number' && todayChange <= -4;
    const tnxUp = typeof todayChange === 'number' && todayChange >= 4;
    const dxyUp = typeof dxy?.change_pct === 'number' && dxy.change_pct >= 0.25;
    const dxyDown = typeof dxy?.change_pct === 'number' && dxy.change_pct <= -0.25;
    const longEndStillElevated = typeof tnx?.change_ytd_pct === 'number' && tnx.change_ytd_pct >= 25;
    const longEndSoftenedYtd = typeof tnx?.change_ytd_pct === 'number' && tnx.change_ytd_pct <= -15;
    const strongFiveDayRelief = typeof fiveDayChange === 'number' && fiveDayChange <= -8;
    const strongFiveDayBackup = typeof fiveDayChange === 'number' && fiveDayChange >= 8;

    if (!hasSignal) {
        thesisCheck = '美债波动仍温和，当前更像核心利率桶的常态跟踪，而不是新一轮久期压力突然开启。';
        portfolioImplication = '继续看本周PMI、油价与联储路径预期会否把收益率重新推回上行通道。';
    } else if (oilDown && (tnxDown || strongFiveDayRelief)) {
        thesisCheck = '收益率回落更像油价下行带来通胀溢价缓解，久期压力阶段性释放。';
        portfolioImplication = '继续看油价与通胀预期会否再次反弹；若没有，IG债券与长期国债ETF的久期压力仍可维持缓和。';
    } else if (oilUp && (tnxUp || strongFiveDayBackup)) {
        thesisCheck = '收益率走高更像油价反弹重新推升通胀担忧，久期再度承压。';
        portfolioImplication = '继续看油价是否把通胀预期重新抬高，若持续发酵，长久期与AT1利差都会更容易受压。';
    } else if ((primarySlug === 'middle-east-tensions' || equityRiskOff) && tnxDown) {
        thesisCheck = '收益率下行更像地缘扰动下的避险买盘回流，而不是久期主线本身被重新定价。';
        portfolioImplication = '继续看谈判进展是否逆转这波避险需求；若地缘缓和，当前压低的收益率可能会再度回摆。';
    } else if ((dxyDown || primarySlug === 'private-credit-stress') && tnxDown) {
        thesisCheck = '收益率回落更像联储路径被重新前移，市场重新押注更早的宽松窗口。';
        portfolioImplication = '继续看本周数据和联储表态能否延续这条路径；若成立，久期与高质量固收会继续受益。';
    } else if ((tnxUp || strongFiveDayBackup) && (dxyUp || longEndStillElevated)) {
        thesisCheck = '长端收益率走高更像财政供给与term premium重新主导，而不只是降息预期被推迟。';
        portfolioImplication = '继续看长债供给、拍卖与财政压力是否重新成为主因；若是，久期与AT1都会面临更高折价压力。';
    } else if ((tnxDown || strongFiveDayRelief) && equityRiskOn && !oilDown) {
        thesisCheck = '债股同步回稳说明利率压力在缓解，但这更像情绪修复，不等于长端风险已经消失。';
        portfolioImplication = '继续看这轮risk-on是否能持续压低收益率；若只是短期情绪修复，久期仍会重新回到供给与联储框架。';
    } else if (longEndSoftenedYtd && !tnxUp) {
        thesisCheck = '年内收益率从高位回落，说明久期压力已阶段性缓解，但未必等于利率下行主线已坐实。';
        portfolioImplication = '继续看收益率回落背后是通胀缓和还是联储路径变化，判断这一缓解是阶段性还是可持续。';
    }

    if (primarySlug === 'usd-strength') {
        thesisCheck = '美元与美债同向变化，说明利率与汇率避险逻辑再次绑定。';
        portfolioImplication = '继续看美元与收益率是否继续同向走强，判断当前防守资产表现是否仍由同一条宏观主线驱动。';
    } else if (primarySlug === 'gold-repricing') {
        thesisCheck = '黄金重估背景下，美债更像实际利率与避险偏好的对照资产。';
        portfolioImplication = '继续看黄金与美债是否重新同向，判断当前市场究竟在交易避险回吐还是实际利率再定价。';
    } else if (primarySlug === 'private-credit-stress') {
        thesisCheck = '私募信用压力扩散时，美债更像流动性缓冲与信用利差对照资产。';
        portfolioImplication = '继续看信用压力是否外溢到AT1与更广泛固收，判断美债防守价值会否继续抬升。';
    }

    return {
        bucket: '美债',
        thesis_check: thesisCheck,
        today_signal: todaySignal,
        portfolio_implication: portfolioImplication
    };
}

function buildFxBucket(
    marketSnapshot: ClientFocusMarketStateResponse | null
): DailyMarketNarrative['asset_buckets'][number] | null {
    const indices = marketSnapshot?.indices ?? [];
    const usdjpy = indices.find((item) => item.code === 'USDJPY');
    const usdchf = indices.find((item) => item.code === 'USDCHF');
    const usdcnh = indices.find((item) => item.code === 'USDCNH');
    const dxy = indices.find((item) => item.code === 'DXY');

    const jpySignificant =
        (typeof usdjpy?.change_pct === 'number' && Math.abs(usdjpy.change_pct) >= 0.5)
        || (typeof usdjpy?.change_5d_pct === 'number' && Math.abs(usdjpy.change_5d_pct) >= 1.5);
    const chfSignificant =
        (typeof usdchf?.change_pct === 'number' && Math.abs(usdchf.change_pct) >= 0.5)
        || (typeof usdchf?.change_5d_pct === 'number' && Math.abs(usdchf.change_5d_pct) >= 1.5);
    const cnhSignificant =
        (typeof usdcnh?.change_pct === 'number' && Math.abs(usdcnh.change_pct) >= 0.3)
        || (typeof usdcnh?.change_5d_pct === 'number' && Math.abs(usdcnh.change_5d_pct) >= 0.8);
    const dxyTrending =
        (typeof dxy?.change_5d_pct === 'number' && Math.abs(dxy.change_5d_pct) >= 1.5);

    if (!jpySignificant && !chfSignificant && !cnhSignificant && !dxyTrending) {
        return null;
    }

    const signalItems: Array<ClientFocusMarketStateResponse['indices'][number] | undefined> = [];
    if (jpySignificant && usdjpy) signalItems.push(usdjpy);
    if (chfSignificant && usdchf) signalItems.push(usdchf);
    if (cnhSignificant && usdcnh) signalItems.push(usdcnh);
    if (signalItems.length === 0 && dxyTrending && dxy) signalItems.push(dxy);

    const todaySignal = joinMonitoringContexts(signalItems) || '美元与主要融资货币方向出现变化。';

    // Determine primary FX narrative
    let thesisCheck = '汇率方向变化更多反映融资货币、人民币与美元主线的重新定价。';
    let portfolioImplication = '继续看这波汇率变化会否扩散到carry trade、港股或CNH资产表现。';

    const jpyAppreciating = typeof usdjpy?.change_pct === 'number' && usdjpy.change_pct < -0.5;
    const chfAppreciating = typeof usdchf?.change_pct === 'number' && usdchf.change_pct < -0.5;
    const cnhStrengthening = typeof usdcnh?.change_pct === 'number' && usdcnh.change_pct < -0.3;
    const cnhWeakening = typeof usdcnh?.change_pct === 'number' && usdcnh.change_pct > 0.3;

    if (jpyAppreciating || chfAppreciating) {
        const currencies = [jpyAppreciating && '日元', chfAppreciating && '瑞郎'].filter(Boolean).join('、');
        thesisCheck = `${currencies}升值说明carry unwind压力上升，融资货币重新回到市场焦点。`;
        portfolioImplication = `继续看${currencies}是否延续升值并压缩套利空间，判断套息仓位压力会否继续扩大。`;
    } else if (cnhStrengthening) {
        thesisCheck = '人民币走强说明中资资产的汇率环境边际改善，也减轻了港股的外部压力。';
        portfolioImplication = '继续看USDCNH是否延续回落，判断这波人民币修复能否继续支撑港股与CNH资产表现。';
    } else if (cnhWeakening) {
        thesisCheck = '人民币走弱会重新抬高港股与中资资产的汇兑压力，说明美元主线仍未明显松动。';
        portfolioImplication = '继续看USDCNH是否进一步上行，判断人民币贬值会否压制港股与中资资产的修复节奏。';
    } else if (dxyTrending && dxy && typeof dxy.change_5d_pct === 'number') {
        if (dxy.change_5d_pct > 0) {
            thesisCheck = '美元5日持续走强，说明EM与港股仍面临系统性估值与资金压力。';
            portfolioImplication = '继续看美元强势是否扩散到更广泛EM资产，判断港股和非美资产修复会否受限。';
        } else {
            thesisCheck = '美元5日走弱，说明非美资产面临的系统性压力开始边际缓和。';
            portfolioImplication = '继续看美元回落能否延续，判断这是否足以支持港股与其他非美资产风险偏好回升。';
        }
    }

    return { bucket: '汇率', thesis_check: thesisCheck, today_signal: todaySignal, portfolio_implication: portfolioImplication };
}

function ensurePriorityAssetBuckets(
    assetBuckets: DailyMarketNarrative['asset_buckets'],
    primarySlug: string,
    marketSnapshot: ClientFocusMarketStateResponse | null
): DailyMarketNarrative['asset_buckets'] {
    const nextBuckets = [...assetBuckets];

    // Deterministically inject 美债 as a PB core bucket
    const treasuryBucket = buildTreasuryBucket(marketSnapshot, primarySlug);
    if (treasuryBucket && !nextBuckets.some((item) => item.bucket === '美债')) {
        if (nextBuckets.length < 5) {
            nextBuckets.push(treasuryBucket);
        } else {
            // 美债是核心桶，可替换汇率腾出位置
            const fxIndex = nextBuckets.findIndex((item) => item.bucket === '汇率');
            if (fxIndex !== -1) {
                nextBuckets.splice(fxIndex, 1, treasuryBucket);
            }
        }
    }

    // Deterministically inject 汇率 if FX has significant signal
    const fxBucket = buildFxBucket(marketSnapshot);
    if (fxBucket && !nextBuckets.some((item) => item.bucket === '汇率')) {
        if (nextBuckets.length < 5) {
            nextBuckets.push(fxBucket);
        }
        // 汇率不是核心桶，满5个时不强制插入
    }

    const orderedBuckets = ['美股', '港股', '美债', '黄金', '汇率'] as const;

    return orderedBuckets
        .filter((bucket) => nextBuckets.some((item) => item.bucket === bucket))
        .map((bucket) => nextBuckets.find((item) => item.bucket === bucket)!)
        .slice(0, 5);
}

async function generateDailyMarketNarrative(): Promise<DailyMarketNarrative | null> {
    let cachedTopics = collectNarrativeTopics();

    if (cachedTopics.length < 2) {
        const uncachedSlugs = FOCUS_TOPICS
            .filter((topic) => !focusCache.get(topic.slug)?.value)
            .map((topic) => topic.slug);
        await Promise.allSettled(uncachedSlugs.map((slug) => getClientFocusDetail(slug)));
        cachedTopics = collectNarrativeTopics();
        if (cachedTopics.length < 2) {
            return null;
        }
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return buildFallbackDailyNarrative(cachedTopics, null);
    }

    const marketSnapshot = await fetchClientFocusMarketStateSnapshot().catch(() => null);
    const fallbackNarrative = buildFallbackDailyNarrative(cachedTopics, marketSnapshot);

    const topicSection = cachedTopics
        .map((item) => {
            const latestUpdatesSection =
                item.latest_updates.length > 0
                    ? item.latest_updates
                        .map((update) => `[${update.time}${update.date ? `/${update.date}` : ''}] ${update.title} → 市场影响：[${update.impact}]`)
                        .join('\n')
                    : '无';

            const transmissionChainSection =
                item.transmission_chain.length > 0
                    ? `\n\n传导链（分析框架）：\n${item.transmission_chain
                        .map((chain) => `[${chain.order}]：${chain.title}（${chain.pricing}）— ${chain.summary}`)
                        .join('\n')}`
                    : '';

            return `[${item.title}] [${item.status || '无'}]
核心摘要：${item.summary || '无'}

最新动态（时序，最新在前）：
${latestUpdatesSection}${transmissionChainSection}

---`;
        })
        .join('\n\n');
    const marketSignalsSection = buildDailyNarrativeMarketSignalsSection(marketSnapshot);
    const earningsSection = await buildEarningsCalendarSection();
    const narrativeHistorySection =
        narrativeHistory.map((record) => `${record.date}: ${record.primary_slug}`).join('\n') || '暂无历史';

    const userPrompt = `
你是香港私人银行资深策略师，职责是帮助 RM 每天识别真正影响客户 SAA 组合的结构性变量，而不是追逐 headline。
${earningsSection ? `\n${earningsSection}\n` : ''}
今日客户焦点话题摘要：

${topicSection}

${marketSignalsSection}

叙事连续主导天数参考（用于判断结构性 vs 噪音）：
${narrativeHistorySection}

任务：
1. 基于上述跨资产信号，判断今天哪个叙事框架解释力最强（primary_slug）
   - 如果当前 primary_slug 已连续主导多天，需要有新的强信号才能切换，否则保持原叙事
   - 单日价格跳动不足以触发叙事切换，除非同时有多个资产类别同向确认
2. 用 1 句话描述今天市场在定价什么（narrative，≤40字）
   - 如果顶部财报日历显示近期有财报，narrative必须体现"市场进入财报验证窗口"这一结构性判断，不要点名具体公司
   - 财报季窗口的结构性重要性高于单日价格跳动
3. 生成 3-5 个资产桶审视卡片（asset_buckets）
   - bucket 只能从：美股、港股、黄金、美债、汇率 中选择；大宗商品不生成独立bucket（原油只作为传导因子，体现在相关bucket的portfolio_implication里）
   - 必须至少生成 3 个桶；PB 客户的核心配置桶优先级应为：美股、港股、美债，其次才是黄金
   - 每个 bucket 必须包含：
     - today_signal：市场情况，≤40字，必须包含今日真实数字；若5日或YTD能帮助判断是否已处于阶段性高位/低位，应一并写出
     - thesis_check：归因，≤35字，解释今天为什么这样走；虽然字段名叫 thesis_check，但这里不要写问句，不要直接写客户持仓复核
     - portfolio_implication：今日需留意，≤40字，告诉RM接下来1-3天该盯什么催化、风险点或验证窗口；如确有必要，可轻带一句持仓含义，但不能喧宾夺主
- 先把今天市场讲明白，再告诉RM接下来该盯什么；不要一上来就写持仓复核模板
- 香港白天语境下，美股/美债/美元指数默认表述为“昨日收盘”或“隔夜”，不要写成“今日上涨/今日下跌”
- 若单日波动很小（例如黄金<1%、汇率信号未达显著阈值），不要硬写成“今天需要review”，可省略该 bucket，或明确说明“当前不足以单独触发复核”
- 今日/5日/YTD 的组合更适合 PB 监测语境：今日负责触发，5日负责判断是否过热，YTD负责判断中期趋势
4. 选择今日信号最强的资产类别作为 default_expanded_bucket
5. 将所有主题按今日客户关注度排序（ranked_slugs）

输出 JSON（只输出 JSON，不加任何额外文字）：
{
  "regime_label": "2-4字的宏观环境标签，例如：地缘重燃、滞胀担忧、流动性收紧",
  "primary_slug": "最能解释当前跨资产走势的focus topic slug",
  "narrative": "一句话解释今日跨资产走势的统一宏观逻辑（≤40字）",
  "ranked_slugs": ["slug1", "slug2"],
  "rank_changes": {"slug": "up|down|stable"},
  "momentum_days": 2,
  "default_expanded_bucket": "今日信号最强的资产类别",
  "asset_buckets": [
    {
      "bucket": "美股|港股|黄金|美债|汇率",
      "today_signal": "市场情况，必须包含今日真实数字；如相关可同时包含5日或YTD，≤40字",
      "thesis_check": "归因：解释今天为何这样走，≤35字，不写问句",
      "portfolio_implication": "今日需留意：未来1-3天最关键的催化或风险点，≤40字"
    }
  ]
}
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
                    { role: 'system', content: `你是一位私人银行策略师，为RM（关系经理）准备客户面谈前的资产审视工具。

你的输出将渲染成一个按大类资产折叠的卡片，RM根据客户持仓选择展开哪个资产类别。

核心哲学：
- 这是给RM早晨扫一眼的市场解释工具，不是交易信号，也不是先做持仓复核
- 每个资产类别要先把今天市场讲明白，再提示接下来最该盯什么
- today_signal必须锚定今日真实数据点（使用提供的市场数据中的具体数字）
- 不生成通用分析；每条都要对应一个具体的持仓复核场景

输出格式（严格JSON，不加任何额外文字）：
{
  "regime_label": "2-4字的宏观环境标签，例如：地缘重燃、滞胀担忧、流动性收紧",
  "primary_slug": "最能解释当前跨资产走势的focus topic slug",
  "narrative": "一句话解释今日跨资产走势的统一宏观逻辑（≤40字）",
  "ranked_slugs": ["slug1", "slug2", "按客户关注度排序"],
  "rank_changes": {"slug": "up|down|stable"},
  "momentum_days": 2,
  "default_expanded_bucket": "今日信号最强的资产类别",
  "asset_buckets": [
    {
      "bucket": "美股|港股|黄金|美债|汇率",
      "today_signal": "市场情况，必须包含今日真实数字的1句话，≤30字",
      "thesis_check": "归因，≤35字，不写问句",
      "portfolio_implication": "今日需留意，≤40字"
    }
  ]
}

3. 生成 3-5 个资产桶复核卡片（asset_buckets）
   - bucket 只能从：美股、港股、黄金、美债、汇率 中选择；大宗商品不生成独立bucket
   - 必须至少包含：美股、港股、美债（PB客户核心持仓，除非无任何市场数据否则必须生成）
   - 黄金：在避险/实际利率/黄金重估逻辑下优先生成；弱信号时可省略

   每个 bucket 的三个字段生成规则：

   【today_signal】
   - 必须包含今日真实数字，≤40字
   - 来自上方市场数据，不可编造
   - 如果该资产5日涨幅超过3%，必须提及（这是判断是否处于极端位置的依据）
   - 如果YTD方向已经很明确，优先加上YTD，帮助RM判断这是不是阶段性回摆而非主线逆转
   - 这是“市场情况”，先交代今天发生了什么

   【thesis_check】
   - 虽然字段名叫 thesis_check，但这里承载的是“归因”
   - 用一句话解释今天为什么这样走，≤35字，不写问句
   - 优先引用地缘、政策、财报、流动性、利率、油价等真正驱动，而不是泛泛风险提示

   【portfolio_implication】
   ⚠️ 这里承载的是“今日需留意”
   - 用一句话告诉RM接下来1-3天最该盯什么：财报、谈判、PMI、油价、联储路径、支撑位/风险点
   - 如确有必要，可轻带一句持仓含义，但不要把整句写成生硬的“若客户持仓…”模板
   - 禁止直接投资建议

   按资产类型的IC判断逻辑：

   美股：
   - 如果顶部财报日历显示近期有财报：
     → portfolio_implication必须明确：市场进入财报验证窗口，需核对持仓在当前反弹后是否已承担过多未经盈利支撑的预期；禁止点名具体公司
   - 如果SPX/NDX 5日涨幅≥3% 且 有近期财报：
     → 高位+财报窗口 = 核对客户持仓是否在5日累涨后仍以盈利韧性为假设前提，财报结果出来前该假设尚未被验证
   - 如果SPX/NDX 5日涨幅≥3% 但 无近期财报：
     → 判断反弹质量：是否为空头回补/流动性驱动（而非盈利支撑）；核对客户持仓是否在5日累涨后已承担过多预期，而非盈利韧性得到验证
   - 如果市场数据包含SPX绝对价位，优先引用（如"标普现报5,570点"），而非只说涨幅%
   - 香港白天默认写"昨收标普/纳指…"，不写"今日上涨/今日下跌"
   - 禁止："复核行业分布是否过度暴露于某板块"这类无方向判断

   港股：
   - 优先写清楚今天港股涨/跌的直接原因：谈判预期、油价、GDP/LPR、政策支持、IPO/科技主题热度
   - 南向资金只能作为流动性与风险偏好的解释变量，不能写成客户原始买入 thesis
   - 可以提科技次新股和IPO热度，但要写成市场风格和流动性扩散，而不是简单区分“有没有南向资金流入”
   - 如果只是温和反弹，应写成“风险偏好修复但尚待更广泛盈利与政策验证”，不要写成强趋势判断

   黄金：
   - 必须判断：当前避险逻辑是否仍然成立（地缘缓和 = 避险逻辑弱化）
   - 如果黄金今日下跌而风险资产上涨：直接点出"避险溢价正在被挤出"
   - 如果输入的topic摘要中有"黄金逻辑重估"相关内容：引用其核心传导逻辑
   - 如果黄金单日波动小于1%，且没有与美债/风险资产形成明显背离，不要强行写成“今天需要review”
   - 要区分“短期避险溢价回吐”和“长期黄金主线逆转”，不能把单日油价变化写成过强结论
   - 禁止："结合美元走势评估相对吸引力"这类模糊判断

   美债：
   - 必须说清楚 TNX 变动对不同久期产品的含义（收益率下行 = 债券价格上涨 = 久期敞口受益）
   - 点出受益的具体资产类型：IG债券、债券基金、长期国债ETF
   - 框架：是否符合机构 house view 的久期配置方向
   - 如果 primary_slug 是 middle-east-tensions 或 private-credit-stress：
     → portfolio_implication必须额外提及 AT1 债券（HSBC/BACR/BNP等）的信用利差敞口——地缘风险溢价上升或信贷压力扩散时，AT1是HK PB客户最直接受影响的固收持仓
   - 香港白天默认写"昨收10Y收益率…"，不是"今日收益率…"
   - 禁止："确认久期配置是否服务原先的判断"这类空话

   汇率（触发条件：USDJPY或USDCHF单日变动≥0.5%，或USDCNH单日变动≥0.3%，或DXY 5日变动≥1.5%）：
   - 香港PB高净值客户常做FX carry trade（借低息货币JPY/CHF做多高息资产），汇率大幅变动是carry unwind风险信号
   - USDJPY/USDCHF走低 = 日元/瑞郎升值 = carry trade亏损压力上升，需提示客户复核套息仓位
   - USDCNH走低 = 人民币升值 = 利好港股及CNH资产；走高 = 人民币贬值 = 对中资资产增加汇兑压力
   - DXY趋势方向对EM/港股有系统性影响
   - 若提到carry，必须尽量同时引用USDJPY与USDCHF；若提到人民币方向，必须把USDCNH对港股/中资资产的含义写清楚
   - 若当日FX波动很小，不要伪造紧迫感
   - portfolio_implication必须明确指出：是carry unwind风险、是人民币方向变化的配置含义，还是美元趋势对EM的影响

   大宗商品（不生成独立bucket）：
   - 原油是传导因子，不是PB客户持仓对象；禁止生成大宗商品bucket
   - 油价信号应体现在其他bucket的portfolio_implication里：例如油价大涨→通胀预期上升→美债收益率受压→体现在美债bucket；油价暴跌+地缘缓和→风险溢价回落→体现在美股或港股bucket

4. 选择今天最值得RM先点开的资产作为 default_expanded_bucket，优先给“市场情况最清楚、归因最完整、接下来催化最具体”的那一项

规则：
- 输出优先级必须是：市场总结 → 归因 → 今日需留意
- portfolio_implication不是投资建议，而是RM今天最该继续盯的催化、验证窗口或风险点
- 你是一位有10年经验的PB IC，这是给RM在客户见面前5分钟看的，必须有具体IC角度
- today_signal数字必须来自上方提供的市场数据，不可编造
- 生成 today_signal 时，优先使用“昨日收盘/今日 + 5日 + YTD”的监测框架，而不是只写单日波动
- 如果上方topic摘要中有传导链内容，portfolio_implication应优先引用其中的具体判断，而不是另起炉灶
- 禁止输出任何教科书式风险管理语言` },
                    { role: 'user', content: userPrompt }
                ],
                max_tokens: 800,
                temperature: 0.3
            })
        });

        if (!response.ok) {
            return fallbackNarrative;
        }

        const payload = (await response.json()) as {
            choices?: Array<{ message?: { content?: string } }>;
        };
        const parsed = safeParseJson(payload.choices?.[0]?.message?.content ?? '') as Partial<DailyMarketNarrative> | null;
        if (
            !parsed
            || typeof parsed.regime_label !== 'string'
            || typeof parsed.primary_slug !== 'string'
            || typeof parsed.narrative !== 'string'
            || !Array.isArray(parsed.ranked_slugs)
            || !Array.isArray((parsed as any).asset_buckets)
        ) {
            throw new Error('Invalid daily narrative schema');
        }

        const validSlugs = new Set(FOCUS_TOPICS.map((topic) => topic.slug));
        const ranked_slugs = parsed.ranked_slugs.filter((slug): slug is string => typeof slug === 'string' && validSlugs.has(slug));
        if (!validSlugs.has(parsed.primary_slug)) {
            throw new Error('Invalid primary slug');
        }
        const asset_buckets = ensurePriorityAssetBuckets(
            normalizeAssetBuckets((parsed as any).asset_buckets),
            parsed.primary_slug,
            marketSnapshot
        );
        if (asset_buckets.length < 1) {
            throw new Error('Missing asset buckets');
        }
        const default_expanded_bucket = isValidDailyNarrativeBucket(parsed.default_expanded_bucket)
            ? parsed.default_expanded_bucket
            : asset_buckets[0].bucket;

        const rank_changes: Record<string, 'up' | 'down' | 'stable'> = {};
        if (previousRankedSlugs.length > 0) {
            for (const slug of ranked_slugs) {
                const prev = previousRankedSlugs.indexOf(slug);
                const curr = ranked_slugs.indexOf(slug);
                if (prev === -1) {
                    rank_changes[slug] = 'stable';
                } else if (curr < prev) {
                    rank_changes[slug] = 'up';
                } else if (curr > prev) {
                    rank_changes[slug] = 'down';
                } else {
                    rank_changes[slug] = 'stable';
                }
            }
        }

        return {
            primary_slug: parsed.primary_slug,
            regime_label: parsed.regime_label.trim(),
            narrative: parsed.narrative.trim(),
            ranked_slugs,
            rank_changes,
            momentum_days: 1,
            asset_buckets,
            default_expanded_bucket,
            generated_at: new Date().toISOString(),
        };
    } catch (error) {
        return fallbackNarrative;
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

async function generateMiddleEastPitchFocusSummary(
    newsItems: NewsItem[],
    marketState: ClientFocusMarketStateResponse | null
): Promise<string | null> {
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

    const marketLines = (marketState?.indices ?? [])
        .filter((item) => ['SPX', 'NDX', 'GOLD', 'OIL', 'BRENT', 'TNX', 'DXY', 'HSI', 'HSTECH'].includes(item.code))
        .map((item) => `${item.name}(${item.code}) ${item.change_pct !== null && Number.isFinite(item.change_pct) ? `${item.change_pct >= 0 ? '+' : ''}${item.change_pct.toFixed(2)}%` : '--'}`)
        .join('；');

    const marketSummary = marketState?.summary?.trim() || '';

    const prompt = `
你是香港私人银行 IC，正在为 RM 准备“中东冲突”详情页里的今日沟通重点主句。

请结合两类输入：
1. 最新局势新闻（停火是否延长、间接谈判、海峡运输执行、制裁与军事动向）
2. 当日跨资产收盘表现（美股、原油、黄金、美债收益率、美元、港股）

目标：
- 只写 1 句话，35-55 字
- 必须点出“今天客户最该沟通的主线”
- 优先回答：市场现在是在交易停火/谈判，还是在交易运输/供应风险，风险资产是否已开始修复或重定价
- 语气专业、克制、适合私人银行 RM/IC
- 不要写成新闻摘要
- 不要写成投资建议
- 不要用“我们认为”“建议关注”等研究口吻

今日相关新闻：
${newsList}

今日市场状态摘要：
${marketSummary || '无'}

重点资产收盘：
${marketLines || '无'}

返回纯 JSON：
{"pitch_focus_summary":"..."}
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
                temperature: 0.2,
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

        const parsed = JSON.parse(raw) as { pitch_focus_summary?: string };
        const summary = typeof parsed.pitch_focus_summary === 'string' ? parsed.pitch_focus_summary.trim() : '';
        return summary || null;
    } catch {
        return null;
    }
}

async function buildFocusDailyVerdictSnapshot(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<ClientFocusDailyVerdict | null> {
    const dailyVerdict = await generateDailyVerdict(topic, newsItems);
    if (!dailyVerdict) {
        return null;
    }

    if (topic.slug !== 'middle-east-tensions') {
        return dailyVerdict;
    }

    const marketState = await fetchClientFocusMarketStateSnapshot().catch(() => null);
    const pitchFocusSummary = await generateMiddleEastPitchFocusSummary(newsItems, marketState);
    const primaryEvent = buildMiddleEastPrimaryEvent(newsItems);
    const communicationFocus = buildMiddleEastCommunicationFocus(newsItems);

    return {
        ...dailyVerdict,
        primary_event: primaryEvent ?? dailyVerdict.primary_event,
        key_change: communicationFocus ?? dailyVerdict.key_change,
        pitch_focus_summary: pitchFocusSummary ?? dailyVerdict.pitch_focus_summary
    };
}

export async function generateClientFocusDailyVerdictSnapshot(slug: string): Promise<ClientFocusDailyVerdict | null> {
    const topic = getFocusTopic(slug);
    if (!topic) {
        return null;
    }

    const newsItems = await fetchFocusNewsItems(topic);
    if (newsItems.length === 0) {
        return null;
    }

    return buildFocusDailyVerdictSnapshot(topic, newsItems);
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
            change_5d_pct: await fetchHongKongIndex5dChange(item.code, item.change_pct),
            change_ytd_pct: await fetchHongKongIndexYtdChange(item.code)
        }))
    );

    return enriched;
}

async function fetchUsMarketStateIndices() {
    const [spxMassive, ndxMassive, spxYahoo, ndxYahoo] = await Promise.all([
        fetchMassiveIndexSnapshot('I:SPX', { code: 'SPX', name: '标普500' }),
        fetchMassiveIndexSnapshot('I:NDX', { code: 'NDX', name: '纳斯达克' }),
        fetchYahooChartSeries('^GSPC', { code: 'SPX', name: '标普500' }),
        fetchYahooChartSeries('^IXIC', { code: 'NDX', name: '纳斯达克' }),
    ]);

    const rows = [
        spxMassive
            ? {
                code: spxMassive.code,
                name: spxMassive.name,
                latest: spxMassive.latest,
                change_pct: spxMassive.change_pct,
                change_5d_pct: computeTrailingChange(spxYahoo.history, 5),
                change_ytd_pct: computeCalendarYtdChange(spxYahoo.history)
            }
            : null,
        ndxMassive
            ? {
                code: ndxMassive.code,
                name: ndxMassive.name,
                latest: ndxMassive.latest,
                change_pct: ndxMassive.change_pct,
                change_5d_pct: computeTrailingChange(ndxYahoo.history, 5),
                change_ytd_pct: computeCalendarYtdChange(ndxYahoo.history)
            }
            : null
    ];

    return rows.filter((item): item is NonNullable<typeof item> => item !== null);
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
        usdCnhHistory,
        usdJpySeries,
        usdChfSeries,
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
        fetchForexHistory('USDCNH'),
        fetchYahooChartSeries('JPY=X', { code: 'USDJPY', name: '美元兑日元' }),
        fetchYahooChartSeries('CHF=X', { code: 'USDCHF', name: '美元兑瑞郎' }),
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
                change_pct: goldSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(goldSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(goldSnapshot.history)
            }
            : null,
        silverSnapshot.snapshot
            ? {
                code: silverSnapshot.snapshot.code,
                name: silverSnapshot.snapshot.name,
                latest: silverSnapshot.snapshot.latest,
                change_pct: silverSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(silverSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(silverSnapshot.history)
            }
            : null,
        dxySnapshot.snapshot
            ? {
                code: dxySnapshot.snapshot.code,
                name: dxySnapshot.snapshot.name,
                latest: dxySnapshot.snapshot.latest,
                change_pct: dxySnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(dxySnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(dxySnapshot.history)
            }
            : null,
        wtiSnapshot.snapshot
            ? {
                code: wtiSnapshot.snapshot.code,
                name: wtiSnapshot.snapshot.name,
                latest: wtiSnapshot.snapshot.latest,
                change_pct: wtiSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(wtiSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(wtiSnapshot.history)
            }
            : null,
        brentSnapshot.snapshot
            ? {
                code: brentSnapshot.snapshot.code,
                name: brentSnapshot.snapshot.name,
                latest: brentSnapshot.snapshot.latest,
                change_pct: brentSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(brentSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(brentSnapshot.history)
            }
            : null,
        naturalGasSnapshot.snapshot
            ? {
                code: naturalGasSnapshot.snapshot.code,
                name: naturalGasSnapshot.snapshot.name,
                latest: naturalGasSnapshot.snapshot.latest,
                change_pct: naturalGasSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingChange(naturalGasSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdChange(naturalGasSnapshot.history)
            }
            : null,
        tnxSnapshot.snapshot
            ? {
                code: tnxSnapshot.snapshot.code,
                name: tnxSnapshot.snapshot.name,
                latest: tnxSnapshot.snapshot.latest,
                change_pct: Number.isFinite(tnxBpsChange) ? tnxBpsChange : tnxSnapshot.snapshot.change_pct,
                change_5d_pct: computeTrailingBpsChange(tnxSnapshot.history, 5),
                change_ytd_pct: computeCalendarYtdBpsChange(tnxSnapshot.history)
            }
            : null,
        usdCnhSnapshot
            ? {
                code: usdCnhSnapshot.code,
                name: usdCnhSnapshot.name,
                latest: usdCnhSnapshot.latest,
                change_pct: usdCnhSnapshot.change_pct,
                change_5d_pct: computeTrailingChange(usdCnhHistory, 5),
                change_ytd_pct: computeCalendarYtdChange(usdCnhHistory)
            }
            : null,
        usdJpySnapshot
            ? {
                code: usdJpySnapshot.code,
                name: usdJpySnapshot.name,
                latest: usdJpySnapshot.latest,
                change_pct: usdJpySnapshot.change_pct,
                change_5d_pct: computeTrailingChange(usdJpySeries.history, 5),
                change_ytd_pct: computeCalendarYtdChange(usdJpySeries.history)
            }
            : null,
        usdChfSnapshot
            ? {
                code: usdChfSnapshot.code,
                name: usdChfSnapshot.name,
                latest: usdChfSnapshot.latest,
                change_pct: usdChfSnapshot.change_pct,
                change_5d_pct: computeTrailingChange(usdChfSeries.history, 5),
                change_ytd_pct: computeCalendarYtdChange(usdChfSeries.history)
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

async function fetchHongKongIndexYtdChange(code: string): Promise<number | null> {
    try {
        const history = await fetchHongKongIndexHistory(code, 260);
        return computeCalendarYtdChange(history);
    } catch {
        return null;
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
    const cachedHasRequiredPriceHistory =
        topic.slug !== 'usd-strength'
        || (
            Array.isArray(cached?.value.focus_price_history)
            && cached.value.focus_price_history.length > 1
        );
    if (cached && cached.expiresAt > Date.now() && cachedHasQuestionCategories && cachedHasRequiredPriceHistory) {
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
    const persistedDailyVerdict = await getLatestClientFocusDailyVerdict(topic.slug).catch(() => null);
    const dailyVerdict = persistedDailyVerdict?.verdict_json as ClientFocusDailyVerdict | null
        ?? await buildFocusDailyVerdictSnapshot(topic, newsItems);
    const themeResult = await getLatestThemeBasketResult(topic.slug).catch(() => null);
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
                ? buildMiddleEastStatus(newsItems)
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
        theme_winners_losers: (themeResult?.result_json as ClientFocusDetailResponse['theme_winners_losers']) ?? null,
        daily_verdict: dailyVerdict ?? null,
        disclaimer: DEFAULT_DISCLAIMER
    };

    focusCache.set(topic.slug, {
        expiresAt: Date.now() + (
            topic.slug === 'hk-market-sentiment'
                ? FOCUS_LIVE_MARKET_CACHE_TTL_MS
                : topic.slug === 'middle-east-tensions'
                    ? FOCUS_LONG_CACHE_TTL_MS
                : topic.slug === 'usd-strength'
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

export function getClientFocusStatusesSnapshot(): Array<{ slug: string; status: string; title: string }> {
    return FOCUS_TOPICS.flatMap((topic) => {
        const cached = focusCache.get(topic.slug);
        const status = cached?.value.status;

        if (
            !cached ||
            cached.expiresAt <= Date.now() ||
            typeof status !== 'string' ||
            status.trim().length === 0
        ) {
            return [];
        }

        return [{
            slug: cached.value.slug,
            status,
            title: cached.value.title
        }];
    });
}

export async function getClientFocusMarketState(): Promise<ClientFocusMarketStateResponse> {
    const snapshot = await fetchClientFocusMarketStateSnapshot();
    return snapshot ?? {
        summary: '跨资产信号仍有分化，建议结合黄金、美元、美股与港股的相对表现理解今天的客户焦点。',
        indices: []
    };
}

function isRenderableDailyNarrative(value: DailyMarketNarrative | null | undefined): value is DailyMarketNarrative {
    return Boolean(
        value
        && Array.isArray((value as any).asset_buckets)
        && (value as any).asset_buckets.length > 0
        && typeof value.momentum_days === 'number'
    );
}

async function refreshDailyMarketNarrative(
    fallback: DailyMarketNarrative | null
): Promise<DailyMarketNarrative | null> {
    if (dailyNarrativeRefreshPromise) {
        return dailyNarrativeRefreshPromise;
    }

    dailyNarrativeRefreshPromise = (async () => {
        if (dailyNarrativeCache.value?.ranked_slugs?.length) {
            previousRankedSlugs = [...dailyNarrativeCache.value.ranked_slugs];
        }

        try {
            const result = await generateDailyMarketNarrative();
            if (!result) {
                return fallback;
            }

            const today = new Date().toISOString().slice(0, 10);
            narrativeHistory = [
                ...narrativeHistory.filter((record) => record.date !== today),
                { date: today, primary_slug: result.primary_slug }
            ].slice(-7);

            const nextValue: DailyMarketNarrative = {
                ...result,
                momentum_days: computeMomentumDays(result.primary_slug, narrativeHistory),
                generated_at: new Date().toISOString()
            };

            dailyNarrativeCache.value = nextValue;
            dailyNarrativeCache.schemaVersion = DAILY_NARRATIVE_CACHE_SCHEMA_VERSION;
            dailyNarrativeCache.expiresAt = Date.now() + DAILY_NARRATIVE_CACHE_TTL_MS;
            return nextValue;
        } catch {
            return fallback;
        } finally {
            dailyNarrativeRefreshPromise = null;
        }
    })();

    return dailyNarrativeRefreshPromise;
}

export async function getDailyMarketNarrative(): Promise<DailyMarketNarrative | null> {
    if (dailyNarrativeCache.schemaVersion !== DAILY_NARRATIVE_CACHE_SCHEMA_VERSION) {
        dailyNarrativeCache.value = null;
        dailyNarrativeCache.expiresAt = 0;
        dailyNarrativeCache.schemaVersion = DAILY_NARRATIVE_CACHE_SCHEMA_VERSION;
    }

    const cached = isRenderableDailyNarrative(dailyNarrativeCache.value) ? dailyNarrativeCache.value : null;

    if (dailyNarrativeCache.value && !cached) {
        dailyNarrativeCache.value = null;
        dailyNarrativeCache.expiresAt = 0;
    }

    if (cached && dailyNarrativeCache.expiresAt > Date.now()) {
        return cached;
    }

    if (cached) {
        void refreshDailyMarketNarrative(cached);
        return cached;
    }

    return refreshDailyMarketNarrative(null);
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
