import type { Flag } from '../types/api';

export interface NarrativeOutput {
    why_now: string;
    risk_note: string;
    sentiment_score: number;
}

interface NarrativeInput {
    symbol: string;
    theme: string;
    grade: string;
    recommended_strike: number;
    estimated_coupon_range: string;
    current_price: number | null;
    pct_from_52w_high: number | null;
    ma20: number | null;
    ma50: number | null;
    ma200: number | null;
    iv_level: string;
    flags: Flag[];
    tenor_days: number;
    news_headlines: string[];
    has_recent_earnings?: boolean;
    days_to_earnings?: number | null;
    days_since_earnings?: number | null;
}

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

export async function generateNarrative(input: NarrativeInput): Promise<NarrativeOutput> {
    if (input.grade === 'AVOID') {
        return buildAvoidNarrative(input);
    }

    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return buildFallbackNarrative(input);
    }

    try {
        const baseUrl = process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL;
        const maDescription = buildMaDescription(input.current_price, input.ma50, input.ma200);
        const moneynessText =
            input.current_price && input.current_price > 0
                ? `约${Math.round((input.recommended_strike / input.current_price) * 100)}%`
                : '比例数据不可用';
        const currentPriceText = input.current_price && input.current_price > 0 ? `$${input.current_price}` : '数据不可用';
        const pctFromHighText =
            input.pct_from_52w_high !== null && input.pct_from_52w_high !== undefined
                ? `${input.pct_from_52w_high}%`
                : '数据不可用';
        const newsSection = input.news_headlines.length > 0
            ? `最新相关新闻：\n${input.news_headlines.join('\n')}`
            : '';
        const recentEarningsContext =
            input.days_since_earnings !== null &&
            input.days_since_earnings !== undefined &&
            input.days_since_earnings >= 0 &&
            input.days_since_earnings <= 2
                ? `
重要：该公司刚于近期发布财报，
请在why_now第一句优先提及财报结果（超预期/符合预期/不及预期）及市场反应。`
                : '';
        const systemPrompt = buildSystemPrompt(input.grade, input.has_recent_earnings ?? false);
        const userPrompt = buildUserPrompt(input, newsSection, recentEarningsContext);

        const response = await fetch(`${baseUrl}/chat/completions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`
            },
            body: JSON.stringify({
                model: 'deepseek-chat',
                messages: [
                    {
                        role: 'system',
                        content: systemPrompt
                    },
                    {
                        role: 'user',
                        content: userPrompt
                    }
                ]
            })
        });

        if (!response.ok) {
            return buildFallbackNarrative(input);
        }

        const payload = await response.json() as {
            choices?: Array<{ message?: { content?: string } }>;
        };

        const content = payload.choices?.[0]?.message?.content ?? '';
        const parsed = safeParseJson(content);
        if (!parsed) {
            return buildFallbackNarrative(input);
        }

        return {
            why_now: typeof parsed.why_now === 'string' ? parsed.why_now : buildFallbackNarrative(input).why_now,
            risk_note: typeof parsed.risk_note === 'string' ? parsed.risk_note : buildFallbackNarrative(input).risk_note,
            sentiment_score: parseSentimentScore(parsed.sentiment_score, buildFallbackNarrative(input).sentiment_score)
        };
    } catch {
        return buildFallbackNarrative(input);
    }
}

function buildFallbackNarrative(input: NarrativeInput): NarrativeOutput {
    const hasBlock = input.flags.some((flag) => flag.severity === 'BLOCK');
    const hasWarn = input.flags.some((flag) => flag.severity === 'WARN');
    const earningsFirst =
        input.days_to_earnings !== null &&
        input.days_to_earnings !== undefined &&
        input.days_to_earnings >= 0 &&
        input.days_to_earnings <= 14;
    const sentimentScore = hasBlock ? 0.2 : earningsFirst ? 0.45 : input.news_headlines.length > 0 ? 0.78 : hasWarn ? 0.45 : 0.6;

    return {
        why_now: `当前IV处于${input.iv_level}水平，${Math.round(input.tenor_days / 30)}个月执行价$${input.recommended_strike}提供${input.estimated_coupon_range}参考票息区间，建议向交易台询价确认。`,
        risk_note: earningsFirst
            ? '财报临近，需优先关注业绩与指引是否满足市场预期。'
            : input.flags.some((flag) => flag.type === 'BEARISH_STRUCTURE')
              ? '注意：价格位于长期均线下方，建议向客户充分说明接股场景。'
              : '',
        sentiment_score: sentimentScore
    };
}

function buildSystemPrompt(grade: string, hasRecentEarnings: boolean): string {
    const base =
        '你是一位香港私人银行的FCN产品顾问助手，服务对象是高净值和超高净值客户。帮助理财经理生成简洁专业的中文话术，供RM内部参考。请结合该公司的具体产品线和近期催化剂写话术，不要使用泛泛的行业描述，要有具体产品或业务名称。';

    const gradeSpecific =
        grade === 'GO'
            ? '你是私人银行FCN产品专家，用积极推荐口吻写话术，重点说明为什么现在是好的入场时机，以及执行价提供的安全边际。语气自信，可以直接copy给客户使用。'
            : grade === 'CAUTION'
              ? "你是私人银行FCN产品专家，用审慎中性口吻写话术，标题逻辑是'留意风险'。重点说明：1. 标的当前技术面或基本面的具体不确定性是什么 2. 票息虽有吸引力，但存在哪些具体风险需要注意 3. 如客户坚持询价，建议缩短期限或降低执行价。不要使用推荐式语言，不说'建议询价'或'风险收益比具有吸引力'。"
              : '你是私人银行FCN产品专家，用明确拒绝口吻写不推荐原因，重点说明为什么现在不适合做这个标的的FCN，列出具体的风险因素，不要给任何正面评价。';

    const earnings =
        hasRecentEarnings
            ? '该公司刚于近期发布财报，新闻列表包含财报结果，why_now 第一句必须根据新闻内容定性描述财报结果是否超预期，并简短说明关键亮点如营收增速、利润变化，不需要列出具体EPS数字，用自然语言描述。'
            : '';

    return `${base}${gradeSpecific}${earnings}`;
}

function buildUserPrompt(
    input: NarrativeInput,
    newsSection: string,
    recentEarningsContext: string
): string {
    const moneynessText =
        input.current_price && input.current_price > 0
            ? `约${Math.round((input.recommended_strike / input.current_price) * 100)}%`
            : '比例数据不可用';
    const currentPriceText = input.current_price && input.current_price > 0 ? `$${input.current_price}` : '数据不可用';
    const pctFromHighText =
        input.pct_from_52w_high !== null && input.pct_from_52w_high !== undefined
            ? `${input.pct_from_52w_high}%`
            : '数据不可用';
    const maDescription = buildMaDescription(input.current_price, input.ma50, input.ma200);

    return `请为以下FCN询价机会生成推荐内容：

标的：${input.symbol}（${input.theme}）
评级：${input.grade}
推荐询价：3个月，执行价$${input.recommended_strike}（${moneynessText}）
参考票息区间：${input.estimated_coupon_range}
当前价：${currentPriceText}
距52周高点：${pctFromHighText}
均线结构：${maDescription}
IV水平：${input.iv_level}
距离财报：${input.days_to_earnings !== null && input.days_to_earnings !== undefined ? `${input.days_to_earnings}天` : '数据不可用'}
${newsSection}
${recentEarningsContext}

以下是两个高质量样本供参考：

样本1（避险/黄金主题）：
黄金中长期结构性牛市逻辑仍然成立——
全球央行持续增持黄金以分散美元储备、
地缘局势支撑避险需求、
金矿股盈利能力随金价创历史新高持续改善。
在此背景下，FCN提供固定票息，
且设有较大的下行缓冲空间，风险收益比具有吸引力。

样本2（科技/AI主题）：
英伟达刚交出了营收、利润、指引均超预期的成绩单，
近期股价回调更多是机构获利回吐和对资本支出的短期担忧。
当前执行价设在约80%水平，
意味着即便股价在未来三个月内再跌20%，
仍能获得可观的年化票息，风险收益比具有吸引力。

请根据评级采用对应口径输出：
1. GO：强调为什么当下值得切入，以及执行价提供的安全边际
2. CAUTION：强调不确定性、风险点，以及如客户坚持询价应更保守处理
3. AVOID：直接说明为什么当前不适合做FCN，不要写成推荐
4. why_now控制在60-80字
5. 风险提示按以下优先级排列：
   5.1 如果14天内有财报，必须第一句提及财报风险
   5.2 高IV或波动率风险
   5.3 行业或宏观风险
6. 同时根据近期新闻和风险判断一个sentiment_score：
   6.1 强正面催化（财报超预期/行业利好/机构看好）→ 0.75-1.0
   6.2 中性，无明显催化也无明显风险 → 0.40-0.74
   6.3 明显负面风险或不确定性 → 0-0.39

请严格按JSON格式输出，不要有其他内容：
{
  "why_now": "60-80字，结合市场数据和新闻背景",
  "risk_note": "风险提示，1-2句，中文",
  "sentiment_score": 0.85
}`;
}

function buildAvoidNarrative(input: NarrativeInput): NarrativeOutput {
    const reasons = summarizeAvoidReasons(input.flags, input.pct_from_52w_high);
    const primaryReason = reasons[0] ?? '当前风险收益结构不符合FCN询价标准。';
    const secondaryReason = reasons[1] ?? '建议暂不作为向客户推荐的FCN询价标的。';

    return {
        why_now: `${primaryReason}${secondaryReason}`,
        risk_note: reasons.slice(2).join('；'),
        sentiment_score: 0.2
    };
}

function summarizeAvoidReasons(flags: Flag[], pctFrom52wHigh: number | null): string[] {
    const reasons: string[] = [];

    if (flags.some((flag) => flag.type === 'EARNINGS_PROXIMITY')) {
        reasons.push('财报窗口过近，短期事件风险过高。');
    }

    if (flags.some((flag) => flag.type === 'BEARISH_STRUCTURE')) {
        reasons.push('股价位于长期均线下方，技术结构仍偏弱。');
    }

    if (pctFrom52wHigh !== null && pctFrom52wHigh < -40) {
        reasons.push('股价较52周高点回撤过深，趋势修复尚未确认。');
    }

    if (flags.some((flag) => flag.type === 'LOW_COUPON')) {
        reasons.push('当前票息补偿不足以覆盖接股风险。');
    }

    if (flags.some((flag) => flag.type === 'LOW_LIQUIDITY')) {
        reasons.push('期权流动性不足，询价与成交质量存在不确定性。');
    }

    if (flags.some((flag) => flag.type === 'HIGH_VOL_LOW_STRIKE')) {
        reasons.push('标的波动率高且下行波动大，适合性要求更高。');
    }

    if (reasons.length === 0) {
        reasons.push('当前风险收益结构不符合FCN询价标准。');
    }

    return reasons;
}

function buildMaDescription(price: number | null, ma50: number | null, ma200: number | null): string {
    if (price === null || ma50 === null || ma200 === null) {
        return '均线数据不可用';
    }
    if (price > ma50 && ma50 > ma200) {
        return '价格在各均线上方，趋势健康';
    }
    if (price > ma50 && price < ma200) {
        return '价格在MA50上方但低于长期均线';
    }
    return '价格在MA50下方，短期趋势偏弱';
}

function safeParseJson(content: string): Record<string, unknown> | null {
    const trimmed = content.trim().replace(/^```json\s*/i, '').replace(/^```/i, '').replace(/```$/i, '').trim();
    try {
        return JSON.parse(trimmed) as Record<string, unknown>;
    } catch {
        return null;
    }
}

function parseSentimentScore(value: unknown, fallback: number): number {
    const parsed = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(parsed)) {
        return fallback;
    }

    return Math.min(Math.max(parsed, 0), 1);
}
