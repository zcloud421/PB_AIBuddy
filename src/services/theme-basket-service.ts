import { MassiveDataFetcher } from '../data/massive-fetcher';
import { THEME_BASKETS, type ThemeBasket } from '../constants/theme-baskets';
import { upsertThemeBasketResult } from '../db/queries/ideas';
import type { ThemeBasketItem, ThemeWinnersLosersResult } from '../types/api';
import { getDailyMarketNarrative } from './client-focus-service';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';
const PRICE_FETCH_DELAY_MS = 2000;

interface BasketRawPerf {
    war_perf: number | null;
    ceasefire_perf: number | null;
    ytd_perf: number | null;
}

interface BasketPerfWithMeta extends ThemeBasket, BasketRawPerf {}

interface DeepSeekThemeBasketResponse {
    scenario_label?: string;
    updated_at?: string;
    interpretation?: {
        summary?: string;
        laggards?: string;
        client?: string;
    };
    winners?: Array<{
        id?: string;
        label?: string;
        labelEn?: string;
        war_perf?: number;
        ceasefire_perf?: number;
        ytd_perf?: number;
        driver?: string;
    }>;
    losers?: Array<{
        id?: string;
        label?: string;
        labelEn?: string;
        war_perf?: number;
        ceasefire_perf?: number;
        ytd_perf?: number;
        driver?: string;
    }>;
}

const fetcher = new MassiveDataFetcher();

async function buildDailyNarrativeContextSection(slug: string): Promise<string> {
    try {
        const narrative = await getDailyMarketNarrative();
        if (!narrative) {
            return '';
        }

        const assetLines = narrative.asset_buckets
            .map((bucket) => {
                const parts: string[] = [bucket.bucket];
                if (bucket.thesis_check) {
                    parts.push(`市场总结与归因：${bucket.thesis_check}`);
                }
                if (bucket.portfolio_implication) {
                    parts.push(`今日需留意：${bucket.portfolio_implication}`);
                }
                return `- ${parts.join('；')}`;
            })
            .join('\n');

        return `
今日叙事主线（用于和板块表现解读保持一致，但不要直接照抄）：
- primary_slug: ${narrative.primary_slug}
- regime_label: ${narrative.regime_label}
- narrative: ${narrative.narrative}
- 当前正在生成的情景主题: ${slug}
${assetLines ? `- 分资产拆解：\n${assetLines}` : ''}
`.trim();
    } catch {
        return '';
    }
}

export async function computeBasketPerf(basket: ThemeBasket): Promise<BasketRawPerf | null> {
    const perSymbolResults: BasketRawPerf[] = [];

    for (const [index, symbol] of basket.symbols.entries()) {
        try {
            const history = await fetcher.fetchPriceHistory(symbol, 365);
            const perf = computePerfFromHistory(history);
            if (perf) {
                perSymbolResults.push(perf);
            }
        } catch {
            // Skip failed symbols so one bad ticker does not invalidate the basket.
        }

        if (index < basket.symbols.length - 1) {
            await delay(PRICE_FETCH_DELAY_MS);
        }
    }

    if (perSymbolResults.length < 2) {
        return null;
    }

    return {
        war_perf: average(perSymbolResults.map((item) => item.war_perf)),
        ceasefire_perf: average(perSymbolResults.map((item) => item.ceasefire_perf)),
        ytd_perf: average(perSymbolResults.map((item) => item.ytd_perf))
    };
}

export async function runThemeBasketDaily(slug: string): Promise<void> {
    const relevantBaskets = THEME_BASKETS.filter((basket) => basket.relevantFor.includes(slug));
    if (relevantBaskets.length === 0) {
        return;
    }

    const rawResults: BasketPerfWithMeta[] = [];
    for (const basket of relevantBaskets) {
        const perf = await computeBasketPerf(basket);
        if (!perf) {
            continue;
        }

        rawResults.push({
            ...basket,
            ...perf
        });
    }

    if (rawResults.length === 0) {
        throw new Error(`No valid theme basket results for ${slug}`);
    }

    const result = await generateThemeWinnersLosers(slug, rawResults);
    if (!result) {
        throw new Error(`Failed to generate theme basket analysis for ${slug}`);
    }

    await upsertThemeBasketResult(slug, getTodayIsoDate(), result);
}

async function generateThemeWinnersLosers(
    slug: string,
    rawPerf: BasketPerfWithMeta[]
): Promise<ThemeWinnersLosersResult | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        throw new Error('Missing DEEPSEEK_API_KEY');
    }

    const basketPerformanceData = rawPerf.map((item) => ({
        id: item.id,
        label: item.label,
        labelEn: item.labelEn,
        war_perf: item.war_perf,
        ceasefire_perf: item.ceasefire_perf,
        ytd_perf: item.ytd_perf
    }));
    const scenarioLabel = getScenarioLabel(slug);
    const updatedAt = new Date().toISOString();
    const dailyNarrativeContext = await buildDailyNarrativeContextSection(slug);
    const userPrompt = `
你是一名私人银行市场分析助手。
以下是各主题板块在${scenarioLabel}情景下的实际表现数据，由系统实时计算：

${JSON.stringify(basketPerformanceData, null, 2)}

${dailyNarrativeContext ? `\n${dailyNarrativeContext}\n` : ''}

字段说明：
- war_perf：冲突升级阶段的板块平均涨跌幅（%）
- ceasefire_perf：停火/缓和阶段的板块平均涨跌幅（%）
- ytd_perf：年初至今板块平均涨跌幅（%）

你的任务：
1. 根据 ytd_perf 正负，将所有板块分入 winners 或 losers
   - ytd_perf > 0 → winners
   - ytd_perf <= 0 → losers
   - ytd_perf 相同时，war_perf 更高的归 winners
2. winners 按 ytd_perf 降序排列
3. losers 按 ytd_perf 升序排列
4. 每个板块生成一个 driver 字段，15字以内，说明其表现的核心机制
5. 再生成 interpretation 对象，基于当天真正的 winners / losers 结果写 3 句动态解读

driver 写作规则：
- 必须包含具体的传导机制，禁止泛化表达
- 禁止输出：'市场情绪好转'、'风险偏好下降'、'投资者关注'等无意义表达
- winners driver 示例：'霍尔木兹封锁预期推升原油供给溢价'、'停火后估值修复驱动半导体反弹'
- losers driver 示例：'高利率环境压制高估值软件折现率'、'AI替代预期压缩传统IT服务需求'

interpretation 写作规则：
- interpretation.summary：先总结当前 winners 最集中的主线，必须基于当天真实 winners，不可套模板，不可引用未进入 winners 的板块
- interpretation.laggards：总结当前 losers / laggards 最集中的压力来源；如果 losers 数量很少，可明确写“年内明确跑输板块有限，当前主要落后在……”
- interpretation.client：给 RM 一句客户沟通提示，说明这组结构更像在交易什么，以及接下来最该看什么
- 三句都必须动态跟随当前结果变化，不能默认写 AI 算力链或企业软件，除非它们今天真的在对应分组里
- 禁止复述固定模板，禁止输出与 winners / losers 结果不一致的行业
- 如果上方提供了“今日叙事主线”，interpretation 必须和这条主线保持一致：即沿用同一市场背景和主要驱动，再下钻到 sector winners / losers 的原因
- 但不要直接照抄今日叙事原句；你的任务是解释“在这条主线下，为什么这些板块赢、为什么这些板块输、接下来 sector 层面该看什么”

输出以下 JSON，不得输出任何 JSON 以外的内容：

{
  "scenario_label": "${scenarioLabel}",
  "updated_at": "${updatedAt}",
  "interpretation": {
    "summary": "一句动态总结 winners 主线",
    "laggards": "一句动态总结 losers/laggards",
    "client": "一句 RM 客户沟通提示"
  },
  "winners": [
    {
      "id": "必须来自传入数据的板块id，不可自创",
      "label": "板块中文名",
      "labelEn": "板块英文名",
      "war_perf": 数字（保留一位小数）,
      "ceasefire_perf": 数字（保留一位小数）,
      "ytd_perf": 数字（保留一位小数）,
      "driver": "15字以内"
    }
  ],
  "losers": [...]
}

严禁行为：
- 不可自创板块id或板块名，所有id和名称必须来自传入数据
- 不可编造或修改涨跌幅数字，必须原样输出传入数据
- driver不可超过15字
- interpretation 三句必须和当天 winners / losers 结果一致
- 不可输出JSON以外任何内容
`.trim();

    const response = await fetch(`${process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL}/chat/completions`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
            model: 'deepseek-chat',
            temperature: 0.2,
            max_tokens: 800,
            messages: [
                {
                    role: 'user',
                    content: userPrompt
                }
            ]
        })
    });

    if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(`DeepSeek theme basket call failed (${response.status}): ${text}`);
    }

    const payload = (await response.json()) as {
        choices?: Array<{
            message?: {
                content?: string;
            };
        }>;
    };
    const content = payload.choices?.[0]?.message?.content ?? '';
    const parsed = safeParseJson(content) as DeepSeekThemeBasketResponse | null;
    if (!parsed) {
        throw new Error(`Invalid DeepSeek theme basket JSON for ${slug}`);
    }

    const rawMap = new Map(rawPerf.map((item) => [item.id, item] as const));
    const winners = rebuildBasketItems(parsed.winners ?? [], rawMap);
    const losers = rebuildBasketItems(parsed.losers ?? [], rawMap);

    return {
        scenario_label: typeof parsed.scenario_label === 'string' && parsed.scenario_label.trim()
            ? parsed.scenario_label.trim()
            : scenarioLabel,
        updated_at: typeof parsed.updated_at === 'string' && parsed.updated_at.trim()
            ? parsed.updated_at.trim()
            : updatedAt,
        interpretation: sanitizeInterpretation(parsed.interpretation),
        winners,
        losers
    };
}

function sanitizeInterpretation(
    interpretation: DeepSeekThemeBasketResponse['interpretation'],
) {
    if (!interpretation) {
        return null;
    }

    const summary = typeof interpretation.summary === 'string' ? interpretation.summary.trim() : '';
    const laggards = typeof interpretation.laggards === 'string' ? interpretation.laggards.trim() : '';
    const client = typeof interpretation.client === 'string' ? interpretation.client.trim() : '';

    if (!summary || !laggards || !client) {
        return null;
    }

    return {
        summary,
        laggards,
        client,
    };
}

function rebuildBasketItems(
    items: DeepSeekThemeBasketResponse['winners'],
    rawMap: Map<string, BasketPerfWithMeta>
): ThemeBasketItem[] {
    const seen = new Set<string>();

    return (items ?? [])
        .map((item) => {
            const id = typeof item.id === 'string' ? item.id.trim() : '';
            if (!id || seen.has(id)) {
                return null;
            }

            const raw = rawMap.get(id);
            if (!raw || raw.war_perf === null || raw.ceasefire_perf === null || raw.ytd_perf === null) {
                return null;
            }

            seen.add(id);
            return {
                id,
                label: raw.label,
                labelEn: raw.labelEn,
                war_perf: roundToOneDecimal(raw.war_perf),
                ceasefire_perf: roundToOneDecimal(raw.ceasefire_perf),
                ytd_perf: roundToOneDecimal(raw.ytd_perf),
                driver: sanitizeDriver(item.driver)
            };
        })
        .filter((item): item is ThemeBasketItem => item !== null);
}

function computePerfFromHistory(
    history: Array<{ date: string; close: number }>
): BasketRawPerf | null {
    const validHistory = history
        .filter((bar) => typeof bar.date === 'string' && Number.isFinite(bar.close) && bar.close > 0)
        .sort((a, b) => a.date.localeCompare(b.date));

    if (validHistory.length < 40) {
        return null;
    }

    const latestIndex = getLatestCompletedBarIndex(validHistory);
    if (latestIndex < 5) {
        return null;
    }

    const latestBar = validHistory[latestIndex];
    const warStart = validHistory[latestIndex - 34];
    const warEnd = validHistory[latestIndex - 5];
    const ceasefireStart = validHistory[latestIndex - 5];
    const ceasefireEnd = latestBar;
    const ytdStart = validHistory.find((bar) => bar.date >= `${latestBar.date.slice(0, 4)}-01-01` && bar.date <= latestBar.date);

    if (!warStart || !warEnd || !ceasefireStart || !ceasefireEnd || !ytdStart) {
        return null;
    }

    return {
        war_perf: percentChange(warStart.close, warEnd.close),
        ceasefire_perf: percentChange(ceasefireStart.close, ceasefireEnd.close),
        ytd_perf: percentChange(ytdStart.close, latestBar.close)
    };
}

function getLatestCompletedBarIndex(history: Array<{ date: string }>): number {
    const today = getTodayIsoDate();
    for (let index = history.length - 1; index >= 0; index -= 1) {
        if (history[index].date < today) {
            return index;
        }
    }

    return history.length - 1;
}

function percentChange(start: number, end: number): number | null {
    if (!Number.isFinite(start) || !Number.isFinite(end) || start <= 0) {
        return null;
    }

    return ((end - start) / start) * 100;
}

function average(values: Array<number | null>): number | null {
    const valid = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
    if (valid.length === 0) {
        return null;
    }

    return valid.reduce((sum, value) => sum + value, 0) / valid.length;
}

function roundToOneDecimal(value: number): number {
    return Math.round(value * 10) / 10;
}

function sanitizeDriver(value: unknown): string {
    if (typeof value !== 'string') {
        return '传导机制待补充';
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return '传导机制待补充';
    }

    return trimmed.slice(0, 15);
}

function safeParseJson(content: string): Record<string, unknown> | null {
    const trimmed = content.trim().replace(/^```json\s*/i, '').replace(/^```/i, '').replace(/```$/i, '').trim();
    try {
        return JSON.parse(trimmed) as Record<string, unknown>;
    } catch {
        return null;
    }
}

function getScenarioLabel(slug: string): string {
    if (slug === 'gold-repricing') {
        return '黄金逻辑重估';
    }

    return '中东冲突';
}

function getTodayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
