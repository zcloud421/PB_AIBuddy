import type {
    ClientFocusDetailResponse,
    ClientFocusListItem,
    ClientFocusPolymarketMarket,
    ClientFocusPolymarketResponse,
    ClientFocusQuestion,
    ClientFocusTransmissionItem,
    ClientFocusUpdate,
    NewsItem,
    WhatChangedGroup
} from '../types/api';
import { fetchNewsItemsByQuery, fetchNewsItemsFromNewsData } from '../data/news-fetcher';
import axios from 'axios';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';
const DEFAULT_DISCLAIMER = '本页内容仅供市场讨论准备，客户沟通请结合所属机构的 house view 与合规要求。';
const FOCUS_CACHE_TTL_MS = 60 * 60 * 1000;
const FOCUS_CHAIN_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const POLYMARKET_CACHE_TTL_MS = 5 * 60 * 1000;
const POLYMARKET_HISTORY_WINDOW_DAYS = 30;
const POLYMARKET_HISTORY_CHUNK_DAYS = 14;
const WHAT_CHANGED_WINDOW_HOURS = 72;
const PRIVATE_CREDIT_WHAT_CHANGED_WINDOW_DAYS = 7;

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

const ALLOWED_FOCUS_STATUS = ['持续发酵', '风险抬升', '局势缓和', '逻辑重估', '压力回升', '趋势明朗', '阶段性结束'] as const;
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
        fallbackSummary: '地缘风险正在通过原油、黄金与利率预期重塑市场定价，客户对风险传导的提问明显升温。',
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
        fallbackSummary: '信用事件开始从个别机构蔓延到风险偏好层面，客户更关注流动性与估值压缩风险。'
    },
    {
        slug: 'gold-repricing',
        title: '黄金逻辑重估',
        accent: '#C9A45C',
        query: 'gold price real yields dollar central bank buying gold miners latest',
        clientQuestions: [
            {
                question: '为什么中东冲突升级黄金反而下跌？',
                answer: '2024 到 2025 年黄金大牛市由三个因素共同驱动：各国央行持续大规模购金以减少美元依赖、市场对联储降息的预期，以及美元走弱。这三个支撑因素在 2026 年初开始同步发生变化，是当前金价承压的根本原因。'
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
        fallbackSummary: '支撑黄金上涨的核心驱动正在变化，市场开始重新评估黄金、ETF与金矿股之间的关系。'
    },
    {
        slug: 'usd-strength',
        title: '美元反弹',
        accent: '#C9A45C',
        query: 'US dollar strength USDCNH Hong Kong stocks China assets rates latest',
        clientQuestions: [
            {
                question: '为什么美元最近出现了反弹？',
                answer: 'DXY 从年初低点 96 反弹到 3 月高点 101 上方，随后回落至 99 附近。驱动因素有两个：一是中东冲突触发避险需求，美元成为本轮最稳定的避险目的地；二是油价飙升推高通胀预期，联储降息时间被推迟，美元利率优势得以延续。离岸人民币方面，人民银行把中间价设在近三年最强水平，CNH 相比其他亚洲货币更具政策韧性，但若霍尔木兹局势缓和，避险资金存在快速逆转风险。'
            },
            {
                question: '中国央行对人民币汇率的最新表态是什么？',
                answer: '央行行长潘功胜在 3 月人大记者会上明确表示，中国没有必要也无意通过汇率贬值获取贸易竞争优势。与此同时，央行将远期售汇外汇风险准备金率从 20% 下调至 0，市场解读为避免人民币过快升值的政策信号。央行立场是允许双向浮动，但会在必要时使用宏观审慎工具干预过度波动。'
            },
            {
                question: '强美元对港股意味着什么？',
                answer: '强美元通常压制非美资产风险偏好，对港股有双重影响：一是外资流出压力加大，二是联系汇率机制下 HKMA 被迫收紧本地流动性，进一步压制港股估值。历史上美元强势周期往往对应港股和中国资产承压，但若强美元更多来自短期避险而非利差驱动，持续性会相对有限。'
            }
        ],
        relatedAssets: ['USDCNH', '港股', '中国资产', '美元资产'],
        fallbackSummary: '美元反弹正在重新影响 USDCNH、中国资产与非美风险偏好，汇率压力重新进入客户视野。'
    }
];

const focusCache = new Map<string, { expiresAt: number; value: ClientFocusDetailResponse }>();
const focusChainCache = new Map<string, { expiresAt: number; value: ClientFocusTransmissionItem[] }>();
const polymarketCache = new Map<string, { expiresAt: number; value: ClientFocusPolymarketResponse }>();

const MIDDLE_EAST_POLYMARKET_MARKETS = [
    {
        pageUrl: 'https://polymarket.com/event/us-x-iran-ceasefire-by',
        label: '美伊4月底前直接谈判',
        outcomes: [
            { outcomeLabel: 'April 30', displayLabel: '4月底' },
            { outcomeLabel: 'May 31', displayLabel: '5月底' },
            { outcomeLabel: 'June 30', displayLabel: '6月底' }
        ]
    },
    {
        pageUrl: 'https://polymarket.com/event/us-forces-enter-iran-by',
        label: '美军何时进入伊朗',
        outcomes: [
            { outcomeLabel: 'April 30', displayLabel: '4月底' },
            { outcomeLabel: 'December 31', displayLabel: '年底' }
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

interface PolymarketPageMarket {
    id: string;
    question: string;
    groupItemTitle: string;
    outcomePrices: string[];
    clobTokenIds: string[];
}

function decodeHtmlEntities(value: string): string {
    return value
        .replace(/&nbsp;/g, ' ')
        .replace(/&#x27;/g, "'")
        .replace(/&quot;/g, '"')
        .replace(/&amp;/g, '&');
}

function extractPolymarketPageMarkets(html: string): PolymarketPageMarket[] {
    const pattern = /\{"id":"(?<id>\d+)".*?"question":"(?<question>.*?)".*?"outcomes":\[(?<outcomes>.*?)\].*?"outcomePrices":\[(?<prices>.*?)\].*?"groupItemTitle":"(?<title>.*?)".*?"clobTokenIds":\[(?<tokens>.*?)\]/gs;
    const markets: PolymarketPageMarket[] = [];

    for (const match of html.matchAll(pattern)) {
        const groups = match.groups;
        if (!groups) {
            continue;
        }

        const outcomePrices = Array.from(groups.prices.matchAll(/"([^"]+)"/g)).map((item) => item[1]);
        const clobTokenIds = Array.from(groups.tokens.matchAll(/"([^"]+)"/g)).map((item) => item[1]);

        markets.push({
            id: groups.id,
            question: decodeHtmlEntities(groups.question),
            groupItemTitle: decodeHtmlEntities(groups.title),
            outcomePrices,
            clobTokenIds
        });
    }

    return markets;
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
    pageMarkets: PolymarketPageMarket[],
    outcome: { outcomeLabel: string; displayLabel: string }
) {
    const targetMarket = pageMarkets.find((pageMarket) => pageMarket.groupItemTitle === outcome.outcomeLabel);
    if (!targetMarket) {
        throw new Error(`Target market not found for ${outcome.outcomeLabel}`);
    }

    const yesProbability = Number(targetMarket.outcomePrices[0]);
    const yesTokenId = targetMarket.clobTokenIds[0];
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
        condition_id: targetMarket.id,
        display_label: outcome.displayLabel,
        probability: Number.isFinite(latestProbability) ? latestProbability : Math.round(yesProbability * 1000) / 10,
        history: normalizedHistory
    };
}

async function fetchPolymarketMarket(
    market: (typeof MIDDLE_EAST_POLYMARKET_MARKETS)[number]
): Promise<ClientFocusPolymarketMarket> {
    const response = await axios.get<string>(
        market.pageUrl,
        {
            headers: {
                'User-Agent': 'Josan/1.0',
                Accept: 'text/html,application/xhtml+xml'
            },
            timeout: 15000
        }
    );

    const pageMarkets = extractPolymarketPageMarkets(response.data);
    const outcomes = await Promise.all(
        market.outcomes.map((outcome) => fetchPolymarketOutcome(pageMarkets, outcome))
    );

    return {
        condition_id: market.pageUrl,
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
        /本周无符合筛选标准的关键事件/
    ];

    if (blockedPatterns.some((pattern) => pattern.test(trimmed))) {
        return null;
    }

    if (topic.slug === 'gold-repricing' && /未出现明确变化/.test(trimmed)) {
        return null;
    }

    return trimmed;
}

function buildFocusSummaryFallback(topic: FocusTopicConfig): string {
    if (topic.slug === 'gold-repricing') {
        return '本周黄金表现弱于地缘避险直觉，客户沟通宜聚焦美元、实际利率与降息预期是否重新主导定价。';
    }

    return topic.fallbackSummary;
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
        'brookfield', 'sixth street', 'owl rock'
    ];
    const analysisKeywords = [
        'analysis', 'outlook', 'what it means', 'impact on',
        'why private', 'long-term', 'explained', 'guide to'
    ];
    const blockedSources = ['seeking alpha', 'benzinga', 'yahoo', 'substack', 'medium', 'blog'];

    const hasAction = actionKeywords.some((kw) => normalized.includes(kw));
    const hasInstitution = institutionKeywords.some((kw) => normalized.includes(kw));
    const isAnalysis = analysisKeywords.some((kw) => normalized.includes(kw));
    const isBlocked = blockedSources.some((kw) => source.includes(kw));

    return (hasAction || hasInstitution) && !isAnalysis && !isBlocked;
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
以下是过去7天的私募信贷相关新闻标题。请将它们整理成以下3个固定分组（每组必须出现，无内容则 items 为空数组）：

1. 赎回 & 流动性（icon: 💧）：赎回限制、gate、提款、流动性收紧、融资额度变化
2. 信用事件（icon: 🧨）：违约、减记、评级动作、重组、损失确认
3. 风险外溢（icon: 🏦）：银行、保险、养老金、BDC、资管平台敞口与二阶影响

每组最多3条，每条：
- time: 从新闻时间取 MM-DD；无法判断则留空字符串
- headline: 不超过35字，必须包含【机构/主体】+【具体动作或事实】

禁止：
- “市场承压”“风险升温”“持续发酵”等空泛表述
- 只有判断，没有具体机构或动作
- 纯背景分析和观点文章

输出格式（JSON数组）：
[
  { "group_label": "赎回 & 流动性", "group_icon": "💧", "items": [...] },
  { "group_label": "信用事件", "group_icon": "🧨", "items": [...] },
  { "group_label": "风险外溢", "group_icon": "🏦", "items": [...] }
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
            { group_label: '赎回 & 流动性', group_icon: '💧' },
            { group_label: '信用事件', group_icon: '🧨' },
            { group_label: '风险外溢', group_icon: '🏦' }
        ] as const;

        return fixedGroups.map((expectedGroup) => {
            const sourceGroup = parsed.find((group) => group.group_label?.trim() === expectedGroup.group_label);
            const parsedItems = Array.isArray(sourceGroup?.items)
                ? sourceGroup.items
                      .filter((item) => typeof item?.headline === 'string')
                      .map((item) => ({
                          time: item.time?.trim() ?? '',
                          headline: item.headline?.trim() ?? ''
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
        });
    } catch {
        return buildFallbackPrivateCreditWhatChangedGroups(candidates);
    }
}

async function generateDynamicClientQuestions(
    topic: FocusTopicConfig,
    newsItems: NewsItem[]
): Promise<Array<{ question: string; answer: string }> | null> {
    if (topic.slug !== 'middle-east-tensions' && topic.slug !== 'private-credit-stress') {
        return null;
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return null;
    }

    const contextItems = newsItems
        .slice()
        .filter((item) =>
            topic.slug === 'private-credit-stress'
                ? isWithinDays(item.published_at, 7)
                : true
        )
        .sort((left, right) => {
            const leftTs = left.published_at ? new Date(left.published_at).getTime() : 0;
            const rightTs = right.published_at ? new Date(right.published_at).getTime() : 0;
            return rightTs - leftTs;
        })
        .slice(0, 10);

    if (contextItems.length === 0) {
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

    const systemPrompt = topic.slug === 'middle-east-tensions'
        ? `
你是香港私人银行的市场沟通助手，面向 RM/IC 生成“客户常问”的标准回答。

回答目标不是复述新闻，而是帮助 RM 向客户解释：
1. 当前市场机制是什么
2. 最新新闻改变了什么
3. 对大类资产意味着什么

语气要求：
- 专业、克制、像私行客户沟通口径
- 先讲机制，再讲最新变化，再讲资产含义
- 不要写成快讯，不要像事件播报
- 不要使用“局势升级”“引发不确定性”等空泛表述
- 不要直接给投资建议
`.trim()
        : `
你是香港私人银行的市场沟通助手，面向 RM/IC 生成“客户常问”的标准回答。

回答目标不是复述私募信贷新闻，而是帮助 RM 向客户解释：
1. 风险主要出在哪里
2. 最近一周新增了什么事实
3. 这些变化对信用、流动性和相关资产意味着什么

语气要求：
- 专业、克制、像私行客户沟通口径
- 先讲机制，再讲本周新增变化，再讲资产含义
- 不要写成快讯，不要像事件播报
- 不要使用“市场承压”“风险升温”等空泛表述
- 不要直接给投资建议
`.trim();
    const userPrompt = topic.slug === 'middle-east-tensions'
        ? `
以下是过去48小时的中东冲突相关新闻。请基于这些最新信息，
为以下3个固定问题重写回答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 70-110 字
- 必须采用“市场机制 → 最新变化 → 资产含义”的结构
- 必须至少包含 1 个具体最新事实、数字或市场变量
- 重点解释对油价、黄金、美元、利率路径或风险偏好的影响
- 回答要有 3-7 天可用性，不要写成只对应某一条新闻的快讯
- 如果最新新闻没有改变原有市场框架，应明确说明“市场逻辑未变，只是新增了什么变量”
- 在保留基准回答逻辑框架的基础上，用最新新闻更新答案

禁止：
- 不要以“过去48小时内...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“局势升级”“引发不确定性”等空泛表述
- 不要给具体投资建议

输出格式（JSON数组）：
[{"question":"原问题文字","answer":"重写后的答案"}]

只输出JSON，不要解释。
`.trim()
        : `
以下是过去7天的私募信贷相关新闻。请基于这些最新信息，
为以下固定问题重写回答，供香港私人银行 RM/IC 与客户沟通时使用。

${questionsList}

每个问题的当前基准回答如下：
${baselineAnswers}

新闻列表：
${newsList}

写作要求：
- 每条答案 70-110 字
- 必须采用“风险机制 → 本周新增变化 → 资产含义”的结构
- 必须至少包含 1 个具体机构、金额、比例、评级动作或违约/减记事实
- 重点解释对流动性、违约风险、BDC/另类资管股或银行融资行为的影响
- 回答要有 1-2 周可用性，不要写成单条新闻复述
- 在保留基准回答逻辑框架的基础上，用本周新事实更新答案

禁止：
- 不要以“过去一周...”开头
- 不要整段围绕某个 headline 复述
- 不要使用“风险升温”“市场承压”等空泛表述
- 不要给具体投资建议

输出格式（JSON数组）：
[{"question":"原问题文字","answer":"重写后的答案"}]

只输出JSON，不要解释。
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
        const content = payload.choices?.[0]?.message?.content?.trim() ?? '';
        const parsed = safeParseJson(content) as Array<{ question?: string; answer?: string }> | null;

        if (!Array.isArray(parsed)) {
            return null;
        }

        return topic.clientQuestions.map((item) => {
            const matched = parsed.find((entry) => entry.question?.trim() === item.question);
            const answer = matched?.answer?.trim();
            return {
                question: item.question,
                answer: answer ? answer : item.answer
            };
        });
    } catch {
        return null;
    }
}

function buildFallbackPrivateCreditWhatChangedGroups(newsItems: NewsItem[]): WhatChangedGroup[] {
    const fixedGroups = [
        { group_label: '赎回 & 流动性', group_icon: '💧' },
        { group_label: '信用事件', group_icon: '🧨' },
        { group_label: '风险外溢', group_icon: '🏦' }
    ] as const;

    return fixedGroups.map((group) => ({
        group_label: group.group_label,
        group_icon: group.group_icon,
        items: buildFallbackPrivateCreditWhatChangedItems(newsItems, group.group_label)
    }));
}

function buildFallbackPrivateCreditWhatChangedItems(
    newsItems: NewsItem[],
    groupLabel: '赎回 & 流动性' | '信用事件' | '风险外溢'
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
): '赎回 & 流动性' | '信用事件' | '风险外溢' | null {
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
        return '赎回 & 流动性';
    }

    const creditKeywords = [
        'default',
        'restructuring',
        'restructure',
        'write-down',
        'writedown',
        'haircut',
        'downgrade',
        'rating',
        'loss',
        'losses',
        'missed payment'
    ];
    if (creditKeywords.some((keyword) => normalized.includes(keyword))) {
        return '信用事件';
    }

    const spilloverKeywords = [
        'bank',
        'banks',
        'insurer',
        'insurance',
        'pension',
        'bdc',
        'apollo',
        'ares',
        'blackstone',
        'blue owl',
        'jpmorgan',
        'morgan stanley',
        'goldman',
        'blackrock'
    ];
    if (spilloverKeywords.some((keyword) => normalized.includes(keyword))) {
        return '风险外溢';
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

    if (actor) {
        const actorPrefix = `【${actor}】`;
        if (cleanedTitle.startsWith(actorPrefix)) {
            return cleanedTitle.slice(0, 35);
        }
        return `${actorPrefix}${cleanedTitle}`.slice(0, 35);
    }

    return cleanedTitle.slice(0, 35);
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
    const userPrompt = `
主题：${topic.title}

最新新闻：
${newsSection}

请输出 JSON，字段如下：
{
  "status": "只能从以下选一个：持续发酵 / 风险抬升 / 局势缓和 / 逻辑重估 / 压力回升。不可自创。",
  "summary": "24字以内。描述当前最关键的市场变化及其对资产价格的直接影响。禁止出现RM、IC、客户经理等内部角色词汇。",
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
- summary 严禁输出情绪描述、泛化判断、无数据支撑的方向性表达
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
    if (cached && cached.expiresAt > Date.now()) {
        return cached.value;
    }

    const newsItems = await fetchFocusNewsItems(topic);
    const modelOutput = await generateFocusContent(topic, newsItems);
    const skipWeeklyProgress = topic.slug === 'middle-east-tensions' || topic.slug === 'private-credit-stress';
    const weeklyProgress = skipWeeklyProgress ? null : await generateWeeklyProgress(topic, newsItems);
    const transmissionChain = await generateTransmissionChain(topic, newsItems);
    const whatChanged =
        topic.slug === 'middle-east-tensions'
            ? await generateMiddleEastWhatChanged(newsItems)
            : topic.slug === 'private-credit-stress'
                ? await generatePrivateCreditWhatChanged(newsItems)
                : [];
    const dynamicClientQuestions = await generateDynamicClientQuestions(topic, newsItems);

    const detail: ClientFocusDetailResponse = {
        slug: topic.slug,
        title: topic.title,
        status:
            topic.slug === 'middle-east-tensions'
                ? '持续发酵'
                : sanitizeFocusStatus(modelOutput?.status?.trim(), topic.fallbackStatus ?? '持续发酵'),
        updated_at: formatRelativeTime(newsItems[0]?.published_at),
        summary:
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
        client_questions: dynamicClientQuestions ?? topic.clientQuestions,
        transmission_chain: transmissionChain ?? buildFallbackTransmission(topic),
        related_assets: topic.relatedAssets,
        disclaimer: DEFAULT_DISCLAIMER
    };

    focusCache.set(topic.slug, {
        expiresAt: Date.now() + FOCUS_CACHE_TTL_MS,
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
        client_questions: getPreviewQuestions(getFocusTopic(item.slug) ?? {
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
