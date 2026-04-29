import type { Flag } from '../types/api';

export interface NarrativeOutput {
    why_now: string;
    risk_note: string;
    sentiment_score: number;
    key_events: string[];
}

interface NarrativeNewsItem {
    title: string;
    source?: string;
    published_at?: string;
}

interface NarrativeInput {
    symbol: string;
    company_name?: string | null;
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
    news_items?: NarrativeNewsItem[];
    has_recent_earnings?: boolean;
    earnings_weight?: number;
    days_to_earnings?: number | null;
    days_since_earnings?: number | null;
    active_attribution_rules?: Array<{
        id: string;
        reason_zh: string;
        driver_type: string;
        family: string;
    }>;
}

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

interface ConferenceWindow {
    start: string;
    end: string;
}

const KNOWN_CONFERENCE_WINDOWS: Partial<Record<string, ConferenceWindow>> = {
    GTC: { start: '2026-03-16', end: '2026-03-19' },
    CES: { start: '2026-01-06', end: '2026-01-09' },
    OFC: { start: '2026-03-15', end: '2026-03-19' }
};

export async function generateNarrative(input: NarrativeInput): Promise<NarrativeOutput> {

    const fallback = buildFallbackNarrative(input);
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) {
        return fallback;
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
        const isEarningsWait =
            input.grade === 'AVOID' &&
            input.days_to_earnings !== null &&
            input.days_to_earnings !== undefined &&
            input.days_to_earnings >= 0 &&
            input.days_to_earnings <= 3;
        const systemPrompt = buildSystemPrompt(
            input.grade,
            input.has_recent_earnings ?? false,
            input.days_since_earnings ?? null,
            input.earnings_weight ?? 0,
            isEarningsWait
        );
        const userPrompt = buildUserPrompt(input, newsSection, recentEarningsContext, isEarningsWait);

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
            return fallback;
        }

        const payload = await response.json() as {
            choices?: Array<{ message?: { content?: string } }>;
        };

        const content = payload.choices?.[0]?.message?.content ?? '';
        const parsed = safeParseJson(content);
        if (!parsed) {
            return fallback;
        }

        const parsedKeyEvents = sanitizeKeyEvents(
            parseKeyEvents(parsed.key_events),
            input.news_items,
            input.symbol,
            input.company_name
        );

        return {
            why_now: typeof parsed.why_now === 'string' ? parsed.why_now : fallback.why_now,
            risk_note: typeof parsed.risk_note === 'string' ? parsed.risk_note : fallback.risk_note,
            sentiment_score: parseSentimentScore(parsed.sentiment_score, fallback.sentiment_score),
            key_events: parsedKeyEvents
        };
    } catch {
        return fallback;
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
        sentiment_score: sentimentScore,
        key_events: sanitizeKeyEvents(
            deriveKeyEvents(input, sentimentScore),
            input.news_items,
            input.symbol,
            input.company_name
        )
    };
}

function buildSystemPrompt(
    grade: string,
    hasRecentEarnings: boolean,
    daysSinceEarnings: number | null,
    earningsWeight: number,
    isEarningsWait: boolean = false
): string {
    const base =
        '你是一位香港私人银行的FCN产品顾问助手，服务对象是高净值和超高净值客户。帮助理财经理生成简洁专业的中文话术，供RM内部参考。请结合该公司的具体产品线和近期催化剂写话术，不要使用泛泛的行业描述，要有具体产品或业务名称。第一句必须先点明公司主营业务或核心产品，不得只写“该公司”或泛泛行业标签。禁止使用空泛表达，如“前景存疑”“市场质疑”“相关领域压力”“基本面不确定性较高”，除非后面紧跟具体经营原因（如高负债、客户集中、资本开支压力、backlog兑现风险、监管风险、价格战、需求放缓等）。禁止引用与该公司无直接可比关系的其他公司股价表现来支持论点。禁止使用竞争对手、同行或其他公司的负面新闻、财报或股价表现作为该标的pitch依据；话术只能基于该标的自身业务、财报、指引、监管和技术面信息。';

    const gradeSpecific =
        isEarningsWait
            ? '你是私人银行FCN产品专家。该标的因财报临近暂不推荐做FCN，财报窗口期信息已在独立模块展示。请用客观中立口吻介绍该公司的主营业务、核心产品或业务亮点，以及当前的技术面/基本面背景，帮助RM在等待期间建立对标的的基本认知。不要提财报时机、财报窗口期、建议等财报后询价等内容。不要给任何方向性判断或风险评价，结尾不需要说明何时可以重新评估。'
            : grade === 'GO'
            ? '你是私人银行FCN产品专家，用积极推荐口吻写话术，重点说明为什么现在是好的入场时机，以及执行价提供的安全边际。语气自信，可以直接copy给客户使用。'
            : grade === 'CAUTION'
              ? "你是私人银行FCN产品专家，用审慎中性口吻写话术，标题逻辑是'留意风险'。重点说明：1. 标的当前技术面或基本面的具体不确定性是什么 2. 票息虽有吸引力，但存在哪些具体风险需要注意 3. 如客户坚持询价，建议缩短期限或降低执行价。不要使用推荐式语言，不说'建议询价'或'风险收益比具有吸引力'。"
              : '你是私人银行FCN产品专家，用明确拒绝口吻写不推荐原因，重点说明为什么现在不适合做这个标的的FCN，列出具体的风险因素，不要给任何正面评价。';

    const earnings =
        hasRecentEarnings
            ? buildEarningsPrompt(grade, daysSinceEarnings, earningsWeight)
            : '若最近一次财报发布时间已超过14天，禁止使用“近期财报”“刚公布”“最新财报显示”“财报后股价”这类时效性表述，除非新闻明确显示该财报事件仍在持续发酵并成为当前市场主线。';

    return `${base}${gradeSpecific}${earnings}`;
}

function buildEarningsPrompt(
    grade: string,
    daysSinceEarnings: number | null,
    earningsWeight: number
): string {
    if (daysSinceEarnings !== null && daysSinceEarnings <= 3) {
        return grade === 'CAUTION' || grade === 'AVOID'
            ? "该公司刚于近期发布财报。请按以下顺序写话术：1. 第一句必须说明财报结果：是否低于预期、EPS/营收的具体表现、股价当日反应；2. 第二句才可以提业务亮点（如有）；3. 最后一句说明对FCN的具体影响。禁止使用以下表达：'虽然...但风险收益比仍具吸引力'、'可关注'、'建议询价'，以及任何接近推荐的语句。"
            : '该公司刚于近期发布财报，why_now 第一句必须根据新闻内容说明财报结果和市场反应，再提核心业务亮点。';
    }

    if (earningsWeight >= 0.6) {
        return grade === 'CAUTION' || grade === 'AVOID'
            ? '该公司在最近4至7天内发布财报。话术需要明确提及财报结果及市场反应，但不必强制放在第一句；整体仍应以风险和适合性为主。禁止使用推荐式语句。'
            : '该公司在最近4至7天内发布财报。请自然提及财报结果与市场反应，再说明业务亮点和FCN背景。';
    }

    return '该公司在最近8至14天内发布财报。可将财报作为背景信息简短提及，主体仍以当前行业和业务催化为主。';
}

function buildUserPrompt(
    input: NarrativeInput,
    newsSection: string,
    recentEarningsContext: string,
    isEarningsWait: boolean
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
    const conflictTradeGuardrail =
        ['GDX', 'XOM', 'USO'].includes(input.symbol)
            ? `9. 当前标的属于“地缘冲突交易”主题，请明确考虑近期美伊冲突及中东局势对油价、通胀预期、利率预期和避险情绪的影响：
   9.1 GDX 不是实物黄金，而是黄金矿商篮子；如油价推升通胀和利率预期，矿商股可能较金价本身承受更大波动
   9.2 XOM 和 USO 需结合油价脉冲、供给扰动与后续回落风险来写
   9.3 禁止把这三个标的一律写成“单纯避险受益”，必须写清具体传导机制`
            : '';
    const activeAttributionSection =
        input.active_attribution_rules && input.active_attribution_rules.length > 0
            ? `\n当前仍在持续的归因风险（按相关性排序，最多取前2条）：\n${input.active_attribution_rules
                  .slice(0, 2)
                  .map((r, i) => `${i + 1}. [${r.driver_type}] ${r.reason_zh}`)
                  .join('\n')}`
            : '';

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
${activeAttributionSection}
${recentEarningsContext}

请根据评级采用对应口径输出：
1. GO：强调为什么当下值得切入，以及执行价提供的安全边际
2. CAUTION：强调不确定性、风险点，以及如客户坚持询价应更保守处理
3. AVOID：直接说明为什么当前不适合做FCN，不要写成推荐
4. why_now控制在60-80字
4.1 第一句必须先点明公司主营业务或核心产品，不得只写“公司/标的”
5. FCN产品术语规范（必须严格遵守）：
   5.0 敲入（Knock-in）= 股价跌破执行价，客户须按执行价买入股票，是FCN的下行风险；敲出（Knock-out）= 股价涨回初始价格上方，FCN提前结束，客户拿回本金和票息，是有利结果。高隐含波动率环境下，风险是敲入概率增加，而非敲出概率增加。risk_note中严禁出现"敲出概率增加"，如需提及波动率风险，必须写"敲入风险上升"或"触及执行价风险上升"。
5. 风险提示按以下优先级排列：
   5.1 如果14天内有财报，必须第一句提及财报风险
   5.15 如果存在"当前仍在持续的归因风险"，risk_note必须在财报风险之后优先提及最相关的一条：
       - driver_type 为 macro/policy/geopolitical 的优先级最高
       - 格式：[风险简称] 仍在持续，[说明对盈利预期或接股风险的具体影响]
       - 禁止直接复制 reason_zh 原文，必须改写为面向客户的简洁风险提示（1句）
       - 如果 active_attribution_rules 为空，跳过此规则，不要生成空泛风险提示
   5.2 高IV或波动率风险
   5.3 行业或宏观风险
6. 如果最近一次财报发布时间距今超过14天，不得使用“近期财报”“刚公布”“最新财报显示”等表述
7. 禁止使用“前景存疑”“市场质疑”“相关领域压力”“基本面不确定性较高”等空泛词，必须写成具体经营或行业风险
8. 禁止引用无直接可比关系的其他公司股价表现作为论据
8.1 禁止使用竞争对手、同行或其他公司的负面新闻、财报或指引作为当前标的的pitch依据
8.2 why_now和risk_note只能基于当前标的自身业务、财报、指引、监管和技术面数据
${conflictTradeGuardrail}
${input.has_recent_earnings && (input.grade === 'CAUTION' || input.grade === 'AVOID')
    ? input.days_since_earnings !== null &&
      input.days_since_earnings !== undefined &&
      input.days_since_earnings <= 3
        ? `10. 若近期刚发布财报，why_now必须先写财报结果与股价反应，再写业务亮点，最后才落到FCN影响
11. 禁止使用“虽然...但风险收益比仍具吸引力”、“可关注”、“建议询价”等接近推荐的语句`
        : input.earnings_weight !== null && input.earnings_weight !== undefined && input.earnings_weight >= 0.6
          ? `10. 若财报发生在最近4至7天内，why_now必须明确提及财报结果与股价反应，但不要求第一句
11. 禁止使用“虽然...但风险收益比仍具吸引力”、“可关注”、“建议询价”等接近推荐的语句`
          : ''
    : ''}
${input.flags.some((flag) => flag.type === 'HIGH_COUPON_OVERRIDE')
    ? `12. 当前标的命中HIGH_COUPON_OVERRIDE：第一句必须说明技术面偏弱和下行风险；第二句说明执行价(${moneynessText})已充分反映下行空间，票息区间(${input.estimated_coupon_range})为风险承受能力较强客户提供较高安全边际；第三句说明仅适合了解FCN结构、能承受进一步下行风险的客户
13. 不要使用“建议询价”或任何强推荐语句`
    : ''}
${isEarningsWait
    ? `\n16. 当前标的距财报不足3天，财报窗口期信息已在独立模块展示。why_now和risk_note禁止提及财报临近、财报窗口期、建议财报后询价等内容，正文应聚焦于标的基本面、技术面或行业背景。`
    : ''}
14. 同时根据近期新闻和风险判断一个sentiment_score：
   14.1 强正面催化（财报超预期/行业利好/机构看好）→ 0.75-1.0
   14.2 中性，无明显催化也无明显风险 → 0.40-0.74
   14.3 明显负面风险或不确定性 → 0-0.39
15. key_events 生成规则：
   15.1 key_events 只能从“最新相关新闻”中逐条原样复制英文原标题，不得翻译、压缩、总结、改写、补写或拼接
   15.2 最多2条；如果没有符合条件的新闻，返回 []
   15.3 只接受与当前标的直接相关的新闻；标题必须明确出现该公司名称、品牌名或股票代码，否则不得输出
   15.4 严禁使用竞争对手、同行、供应链或客户公司的新闻标题替代当前标的新闻
   15.5 严禁输出中文、严禁添加日期前缀、严禁添加来源、严禁输出半句
   15.6 key_events 的每个元素必须与输入新闻中的某一条英文原标题完全一致

请严格按JSON格式输出，不要有其他内容：
{
  "why_now": "60-80字，结合市场数据和新闻背景",
  "risk_note": "风险提示，1-2句，中文",
  "sentiment_score": 0.85,
  "key_events": ["Exact English headline 1", "Exact English headline 2"]
}`;
}

function buildAvoidNarrative(input: NarrativeInput): NarrativeOutput {
    const reasons = summarizeAvoidReasons(input.flags, input.pct_from_52w_high);
    const primaryReason = reasons[0] ?? '当前风险收益结构不符合FCN询价标准。';
    const secondaryReason = reasons[1] ?? '建议暂不作为向客户推荐的FCN询价标的。';

    return {
        why_now: `${primaryReason}${secondaryReason}`,
        risk_note: reasons.slice(2).join('；'),
        sentiment_score: 0.2,
        key_events: sanitizeKeyEvents(
            deriveKeyEvents(input, 0.2),
            input.news_items,
            input.symbol,
            input.company_name
        )
    };
}

function shouldBypassAvoidNarrativeModel(input: NarrativeInput): boolean {
    return !input.has_recent_earnings && !hasMaterialHeadlineEvent(input.news_headlines);
}

function hasMaterialHeadlineEvent(headlines: string[]): boolean {
    const text = headlines.join(' ').toLowerCase();
    if (!text) {
        return false;
    }

    const materialKeywords = [
        'indict',
        'charged',
        'lawsuit',
        'sued',
        'probe',
        'investigation',
        'export control',
        'sanction',
        'fraud',
        'short seller',
        'short report',
        'downgrade',
        'rating cut',
        'subpoena',
        'accounting',
        'guidance',
        'earnings',
        'partner',
        'contract',
        'regulator',
        '监管',
        '诉讼',
        '起诉',
        '检方',
        '检察官',
        '指控',
        '调查',
        '出口管制',
        '制裁',
        '做空',
        '下调评级',
        '合作',
        '签约',
        '财报',
        '业绩',
        '诈欺',
        '欺诈',
        '审计'
    ];

    return materialKeywords.some((keyword) => text.includes(keyword));
}

function deriveKeyEvents(input: NarrativeInput, sentimentScore: number): string[] {
    const events: string[] = [];

    const preEarningsEvent = derivePreEarningsEvent(input.days_to_earnings);
    if (preEarningsEvent) {
        events.push(preEarningsEvent);
    }

    const postEarningsEvent = derivePostEarningsEvent(input.days_since_earnings, input.news_headlines);
    if (postEarningsEvent) {
        events.push(postEarningsEvent);
    }

    events.push(...deriveKeyEventsFromHeadlines(input.news_headlines));

    const uniqueEvents = Array.from(new Set(events.map((item) => normalizeKeyEvent(item))));
    if (uniqueEvents.length > 0) {
        return uniqueEvents.slice(0, 2);
    }

    return sentimentScore < 0.35 || sentimentScore > 0.75 ? deriveExtremeSentimentEvent(input.news_headlines) : [];
}

function derivePreEarningsEvent(daysToEarnings: number | null | undefined): string | null {
    if (daysToEarnings === null || daysToEarnings === undefined || daysToEarnings < 0 || daysToEarnings > 7) {
        return null;
    }

    return '财报即将发布，注意事件风险';
}

function derivePostEarningsEvent(daysSinceEarnings: number | null | undefined, headlines: string[]): string | null {
    if (daysSinceEarnings === null || daysSinceEarnings === undefined || daysSinceEarnings < 0 || daysSinceEarnings > 5) {
        return null;
    }

    const earningsHeadline = headlines.find((headline) => {
        const normalized = headline.toLowerCase();
        return normalized.includes('earnings') || normalized.includes('eps') || normalized.includes('guidance') || headline.includes('财报') || headline.includes('业绩');
    });

    if (!earningsHeadline) {
        return '最新财报发布，关注业绩反应';
    }

    const beatText =
        earningsHeadline.includes('超预期') || earningsHeadline.toLowerCase().includes('beat')
            ? '超预期'
            : earningsHeadline.includes('不及预期') || earningsHeadline.toLowerCase().includes('miss')
              ? '不及预期'
              : '符合预期';

    if (earningsHeadline.includes('EPS')) {
        return normalizeKeyEvent(`财报EPS${beatText}`);
    }

    if (earningsHeadline.includes('营收') || earningsHeadline.toLowerCase().includes('revenue')) {
        return normalizeKeyEvent(`财报营收${beatText}`);
    }

    if (earningsHeadline.includes('指引') || earningsHeadline.toLowerCase().includes('guidance')) {
        return normalizeKeyEvent(`财报指引${beatText}`);
    }

    if (earningsHeadline.includes('净利润')) {
        return normalizeKeyEvent(`财报净利润${beatText}`);
    }

    return normalizeKeyEvent(`最新财报${beatText}`);
}

function deriveKeyEventsFromHeadlines(headlines: string[]): string[] {
    const events: string[] = [];

    for (const headline of headlines) {
        const normalized = headline.toLowerCase();

        const conferenceEvent = deriveConferenceEvent(headline, normalized);
        if (conferenceEvent) {
            events.push(conferenceEvent);
            continue;
        }

        const majorBankEvent = deriveMajorBankRatingEvent(headline, normalized);
        if (majorBankEvent) {
            events.push(majorBankEvent);
            continue;
        }

        if (
            normalized.includes('indict') ||
            normalized.includes('charged') ||
            normalized.includes('lawsuit') ||
            normalized.includes('export control') ||
            normalized.includes('sec') ||
            normalized.includes('doj') ||
            normalized.includes('antitrust') ||
            headline.includes('起诉') ||
            headline.includes('指控') ||
            headline.includes('出口管制') ||
            headline.includes('反垄断') ||
            headline.includes('监管') ||
            headline.includes('证监')
        ) {
            events.push('重大监管事件落地');
            continue;
        }

        if (
            normalized.includes('short seller') ||
            normalized.includes('short report') ||
            headline.includes('做空')
        ) {
            events.push('遭遇做空报告，短期承压');
            continue;
        }

        if (
            normalized.includes('acquisition') ||
            normalized.includes('merger') ||
            normalized.includes('acquire') ||
            normalized.includes('partner') ||
            normalized.includes('contract') ||
            normalized.includes('index') ||
            normalized.includes('s&p 500') ||
            normalized.includes('nasdaq-100') ||
            headline.includes('合作') ||
            headline.includes('签约') ||
            headline.includes('并购') ||
            headline.includes('纳入') ||
            headline.includes('剔除') ||
            headline.includes('标普500') ||
            headline.includes('纳指100')
        ) {
            events.push(deriveMajorCompanyEvent(headline, normalized));
            continue;
        }
    }

    return Array.from(new Set(events)).slice(0, 2).map((item) => normalizeKeyEvent(item));
}

function deriveConferenceEvent(headline: string, normalized: string): string | null {
    const conferenceName = getConferenceName(normalized);
    if (!conferenceName) {
        return null;
    }

    const datedEvent = deriveConferenceEventFromWindow(conferenceName, headline, normalized);
    if (datedEvent) {
        return datedEvent;
    }

    if (normalized.includes('即将') || normalized.includes('preview') || normalized.includes('ahead of') || normalized.includes('next week') || normalized.includes('upcoming')) {
        return `${conferenceName}即将召开，关注产品发布催化`;
    }

    if (normalized.includes('ended') || normalized.includes('wrap') || normalized.includes('结束') || normalized.includes('收官')) {
        const launchText = extractConferenceHighlight(headline, normalized);
        return normalizeKeyEvent(`${conferenceName}刚结束，${launchText}`);
    }

    if (normalized.includes('underway') || normalized.includes('day 1') || normalized.includes('live') || normalized.includes('进行中') || normalized.includes('召开')) {
        const launchText = extractConferenceHighlight(headline, normalized);
        return normalizeKeyEvent(`${conferenceName}正在进行，${launchText}`);
    }

    return null;
}

function deriveConferenceEventFromWindow(conferenceName: string, headline: string, normalized: string): string | null {
    const window = KNOWN_CONFERENCE_WINDOWS[conferenceName];
    if (!window) {
        return null;
    }

    const today = currentIsoDate();
    const preStartWindow = shiftIsoDate(window.start, -7);
    const postEndWindow = shiftIsoDate(window.end, 5);
    const launchText = extractConferenceHighlight(headline, normalized);

    if (today >= preStartWindow && today < window.start) {
        return `${conferenceName}即将召开，关注产品发布催化`;
    }

    if (today >= window.start && today <= window.end) {
        return normalizeKeyEvent(`${conferenceName}正在进行，${launchText}`);
    }

    if (today > window.end && today <= postEndWindow) {
        return normalizeKeyEvent(`${conferenceName}刚刚结束，${launchText}`);
    }

    return null;
}

function getConferenceName(normalized: string): string | null {
    if (normalized.includes('gtc')) {
        return 'GTC';
    }
    if (normalized.includes('ces')) {
        return 'CES';
    }
    if (normalized.includes('ofc')) {
        return 'OFC';
    }
    if (normalized.includes('computex')) {
        return 'Computex';
    }
    return null;
}

function extractConferenceHighlight(headline: string, normalized: string): string {
    if (normalized.includes('gpu') || headline.includes('GPU')) {
        return '聚焦GPU新品';
    }
    if (normalized.includes('optical') || headline.includes('光') || headline.includes('通信')) {
        return '聚焦光互连新品';
    }
    if (normalized.includes('server') || headline.includes('服务器')) {
        return '聚焦服务器新品';
    }
    if (normalized.includes('network') || headline.includes('网络')) {
        return '聚焦网络互连';
    }

    return '关注新品发布';
}

function deriveMajorBankRatingEvent(headline: string, normalized: string): string | null {
    const bank = getMajorBankName(normalized);
    if (!bank) {
        return null;
    }

    const direction =
        normalized.includes('upgrade') || headline.includes('上调')
            ? '上调'
            : normalized.includes('downgrade') || headline.includes('下调')
              ? '下调'
              : null;
    if (!direction) {
        return null;
    }

    const rating = getRatingName(normalized, headline);
    return normalizeKeyEvent(`${bank}${direction}评级至${rating}`);
}

function getMajorBankName(normalized: string): string | null {
    if (normalized.includes('goldman') || headlineIncludesAny(normalized, ['gs'])) {
        return '高盛';
    }
    if (normalized.includes('morgan stanley')) {
        return '摩根士丹利';
    }
    if (normalized.includes('jpmorgan') || normalized.includes('jp morgan')) {
        return '摩根大通';
    }
    if (normalized.includes('bank of america') || normalized.includes('bofa')) {
        return '美银';
    }
    if (normalized.includes('citigroup') || normalized.includes('citi')) {
        return '花旗';
    }
    return null;
}

function getRatingName(normalized: string, headline: string): string {
    if (normalized.includes('buy') || headline.includes('买入')) {
        return '买入';
    }
    if (normalized.includes('sell') || headline.includes('卖出')) {
        return '卖出';
    }
    if (normalized.includes('neutral') || headline.includes('中性')) {
        return '中性';
    }
    if (normalized.includes('overweight') || headline.includes('增持')) {
        return '增持';
    }
    if (normalized.includes('underweight') || headline.includes('减持')) {
        return '减持';
    }
    return '中性';
}

function deriveMajorCompanyEvent(headline: string, normalized: string): string {
    if (normalized.includes('acquisition') || normalized.includes('merger') || normalized.includes('acquire') || headline.includes('并购')) {
        return '披露重大并购事项';
    }
    if (normalized.includes('partner') || normalized.includes('contract') || headline.includes('合作') || headline.includes('签约')) {
        return '披露重大合作进展';
    }
    if (normalized.includes('s&p 500') || headline.includes('标普500')) {
        return '纳入标普500指数';
    }
    if (normalized.includes('nasdaq-100') || headline.includes('纳指100')) {
        return '纳入纳指100指数';
    }
    if (normalized.includes('index') || headline.includes('纳入') || headline.includes('剔除')) {
        return '指数调整事件落地';
    }

    return '重大公司事件落地';
}

function deriveExtremeSentimentEvent(headlines: string[]): string[] {
    return [];
}

function headlineIncludesAny(value: string, keywords: string[]): boolean {
    return keywords.some((keyword) => value.includes(keyword));
}

function summarizeAvoidReasons(flags: Flag[], pctFrom52wHigh: number | null): string[] {
    const reasons: string[] = [];

    if (flags.some((flag) => flag.type === 'EARNINGS_PROXIMITY')) {
        reasons.push('财报窗口过近，短期事件风险过高。');
    }

    if (flags.some((flag) => flag.type === 'POST_EARNINGS_SHOCK')) {
        reasons.push('财报刚落地且价格正在重估，需等待正股和期权链重新定价。');
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

function parseKeyEvents(value: unknown): string[] {
    if (!Array.isArray(value)) {
        return [];
    }

    return value
        .filter((item): item is string => typeof item === 'string')
        .map((item) => item.trim())
        .filter(Boolean)
        .slice(0, 2);
}

export function sanitizeNarrativeOutput(
    narrative: NarrativeOutput,
    newsItems: NarrativeNewsItem[] = [],
    symbol?: string,
    companyName?: string | null
): NarrativeOutput {
    return {
        ...narrative,
        key_events: sanitizeKeyEvents(narrative.key_events ?? [], newsItems, symbol, companyName)
    };
}

function sanitizeKeyEvents(
    events: string[],
    newsItems: NarrativeNewsItem[] = [],
    symbol?: string,
    companyName?: string | null
): string[] {
    const relevantNewsItems = newsItems.filter((item) =>
        isNarrativeNewsItemRelevant(item, symbol, companyName)
    );

    if (relevantNewsItems.length === 0) {
        return [];
    }

    return Array.from(
        new Map(
            events
                .map((event) => matchKeyEventHeadline(event, relevantNewsItems))
                .filter((event): event is string => Boolean(event))
                .map((event) => [canonicalizeKeyEvent(event), event])
        ).values()
    ).slice(0, 2);
}

function isNarrativeNewsItemRelevant(
    item: NarrativeNewsItem,
    symbol?: string,
    companyName?: string | null
): boolean {
    const title = normalizeEventText(item.title ?? '');
    if (!title) {
        return false;
    }

    const needles = buildCompanyMatchNeedles(symbol, companyName);
    if (needles.length === 0) {
        return false;
    }

    return needles.some((needle) => title.includes(needle));
}

function buildCompanyMatchNeedles(symbol?: string, companyName?: string | null): string[] {
    const normalizedSymbol = symbol?.trim().toLowerCase();
    const normalizedCompany = normalizeCompanyMatchText(companyName ?? '');
    const parts = normalizedCompany
        .split(/\s+/)
        .map((part) => part.trim())
        .filter((part) => part.length >= 3);
    const longestParts = parts.filter((part) => part.length >= 4);

    return Array.from(
        new Set(
            [
                normalizedSymbol,
                normalizedCompany,
                ...longestParts
            ].filter((value): value is string => Boolean(value))
        )
    );
}

function normalizeCompanyMatchText(value: string): string {
    return normalizeEventText(value)
        .replace(/\b(inc|incorporated|corp|corporation|co|company|holdings|holding|group|ltd|limited|plc|sa|nv|ag)\b/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function matchKeyEventHeadline(
    value: string,
    newsItems: NarrativeNewsItem[]
): string | null {
    const normalized = normalizeKeyEvent(value);
    if (!normalized) {
        return null;
    }

    const stripped = stripTrailingPunctuation(stripLeadingKeyEventDate(normalized));
    if (!stripped) {
        return null;
    }

    const normalizedCandidate = normalizeEventText(stripped);
    const exactMatch = newsItems.find((item) => normalizeEventText(item.title ?? '') === normalizedCandidate);
    if (exactMatch) {
        return exactMatch.title.trim();
    }

    const fuzzyMatch = newsItems
        .map((item, index) => ({
            item,
            index,
            score: sharedTokenCount(normalizedCandidate, normalizeEventText(item.title ?? ''))
        }))
        .filter((entry) => entry.score > 0)
        .sort((a, b) => b.score - a.score || a.index - b.index)[0];

    if (!fuzzyMatch) {
        return null;
    }

    const headlineTokens = tokenizeNormalizedText(normalizeEventText(fuzzyMatch.item.title ?? ''));
    const candidateTokens = tokenizeNormalizedText(normalizedCandidate);
    const minimumOverlap = Math.max(2, Math.ceil(headlineTokens.length * 0.6));
    const overlapCount = candidateTokens.filter((token) => headlineTokens.includes(token)).length;

    return overlapCount >= minimumOverlap ? fuzzyMatch.item.title.trim() : null;
}

function isLikelyIncompleteKeyEvent(value: string): boolean {
    const trimmed = stripTrailingPunctuation(normalizeKeyEvent(value));
    if (!trimmed) {
        return true;
    }

    if (/[，、；：,.]\s*$/u.test(value.trim())) {
        return true;
    }

    const danglingEndings = [
        '引发股价',
        '导致股价',
        '拖累股价',
        '提振股价',
        '令股价',
        '使股价',
        '股价',
        '引发',
        '导致',
        '拖累',
        '提振',
        '带来',
        '关注',
        '承压',
        '升温',
        '回落',
        '进入',
        '超级',
        '计划',
        '预计',
        '与',
        '和',
        '及',
        'and',
        'with',
        'for',
        'to'
    ];

    const completeTailMarkers = [
        '上涨',
        '下跌',
        '大跌',
        '暴跌',
        '走高',
        '走低',
        '回调',
        '承压加剧',
        '风险抬升',
        '预期升温',
        '预期回落',
        '波动加剧',
        '波动抬升',
        '指引上修',
        '指引下修',
        '收入上修',
        '收入下滑',
        'EPS超预期',
        'EPS不及预期'
    ];

    if (completeTailMarkers.some((marker) => trimmed.endsWith(marker))) {
        return false;
    }

    if (/\b(and|with|for|to|of|on|in|as|from|by|or)\s*$/i.test(trimmed)) {
        return true;
    }

    return danglingEndings.some((ending) => trimmed.endsWith(ending));
}

function normalizeKeyEvent(value: string): string {
    return value
        .replace(/\s+/g, ' ')
        .trim();
}

function finalizeKeyEventSentence(value: string, newsItems: NarrativeNewsItem[] = []): string | null {
    const normalized = normalizeKeyEvent(value);
    if (!normalized || isLikelyIncompleteKeyEvent(normalized) || isLowSignalKeyEvent(normalized)) {
        return null;
    }

    const summary = stripTrailingPunctuation(stripLeadingKeyEventDate(normalized));
    if (!summary) {
        return null;
    }

    const date = extractKeyEventDate(normalized) ?? matchNarrativeNewsDate(summary, newsItems);
    if (!date) {
        return null;
    }

    return `${date} ${summary}。`;
}

function isLowSignalKeyEvent(value: string): boolean {
    const normalized = stripTrailingPunctuation(normalizeKeyEvent(value)).toLowerCase();
    if (!normalized) {
        return true;
    }

    const bannedPhrases = [
        '股价受行业利好推动',
        '近期录得显著涨幅',
        '录得显著涨幅',
        '行业利好推动',
        '行业前景改善',
        '市场情绪升温',
        '事件催化升温',
        '事件冲击加剧',
        '短期事件催化',
        '短期事件冲击',
        '近期事件',
        '重大公司事件落地',
        '披露重大合作进展',
        '披露重大并购事项',
        '重大监管事件落地',
        '指数调整事件落地'
    ];

    if (bannedPhrases.some((phrase) => normalized.includes(phrase.toLowerCase()))) {
        return true;
    }

    const hasSpecificNumber = /\d/.test(normalized) || /%|亿美元|百万|万|日连涨|周涨|月涨/u.test(normalized);
    const hasSpecificEventKeyword = [
        '财报',
        'eps',
        '营收',
        '指引',
        'gtc',
        'ofc',
        'ces',
        '合作',
        '签约',
        '并购',
        '评级',
        '买入',
        '卖出',
        '监管',
        '调查',
        '做空',
        '纳入',
        '剔除',
        '合同',
        '订单',
        'bushehr'
    ].some((keyword) => normalized.includes(keyword));

    return !hasSpecificNumber && !hasSpecificEventKeyword;
}

function stripTrailingPunctuation(value: string): string {
    return value.replace(/[，、；：,.。！？!?]+$/u, '').trim();
}

function stripLeadingKeyEventDate(value: string): string {
    return value.replace(/^\d{4}-\d{2}-\d{2}\s+/u, '').trim();
}

function canonicalizeKeyEvent(value: string): string {
    return stripTrailingPunctuation(normalizeKeyEvent(value));
}

function buildHeadlineFallbackKeyEvent(newsItems: NarrativeNewsItem[]): string | null {
    for (const item of newsItems) {
        const fallback = summarizeHeadlineKeyEvent(item.title ?? '');
        if (!fallback) {
            continue;
        }

        const date = formatIsoDate(item.published_at);
        if (!date) {
            continue;
        }

        return `${date} ${fallback}。`;
    }

    return null;
}

function extractKeyEventDate(value: string): string | null {
    const match = value.match(/\b(\d{4}-\d{2}-\d{2})\b/u);
    return match?.[1] ?? null;
}

function formatIsoDate(value?: string): string | null {
    if (!value) {
        return null;
    }

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return null;
    }

    const year = parsed.getUTCFullYear();
    const month = String(parsed.getUTCMonth() + 1).padStart(2, '0');
    const day = String(parsed.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function matchNarrativeNewsDate(event: string, newsItems: NarrativeNewsItem[]): string | null {
    if (!newsItems.length) {
        return null;
    }

    const normalizedEvent = normalizeEventText(event);
    const scoredMatches = newsItems
        .map((item, index) => ({
            item,
            index,
            score: sharedTokenCount(normalizedEvent, normalizeEventText(item.title ?? ''))
        }))
        .filter((entry) => entry.score > 0)
        .sort((a, b) => b.score - a.score || a.index - b.index);

    return formatIsoDate(scoredMatches[0]?.item.published_at ?? newsItems[0]?.published_at);
}

function normalizeEventText(value: string) {
    return value
        .toLowerCase()
        .replace(/[^\p{L}\p{N}]+/gu, ' ')
        .trim();
}

function tokenizeNormalizedText(value: string): string[] {
    return value.split(/\s+/).filter((token) => token.length >= 2);
}

function sharedTokenCount(left: string, right: string) {
    if (!left || !right) {
        return 0;
    }

    const leftTokens = new Set(tokenizeNormalizedText(left));
    const rightTokens = tokenizeNormalizedText(right);
    let score = 0;

    for (const token of rightTokens) {
        if (leftTokens.has(token)) {
            score += 1;
        }
    }

    return score;
}

function summarizeHeadlineKeyEvent(title: string): string | null {
    const cleaned = stripTrailingPunctuation(normalizeKeyEvent(title));
    if (!cleaned) {
        return null;
    }

    const normalized = cleaned.toLowerCase();

    const stockMoveMatch =
        cleaned.match(/(\d+(\.\d+)?%)/) ??
        cleaned.match(/(\d+日连涨)/) ??
        cleaned.match(/(five-day winning streak|5-day winning streak)/i);
    if (
        stockMoveMatch &&
        (
            normalized.includes('shares') ||
            normalized.includes('stock') ||
            normalized.includes('rose') ||
            normalized.includes('jumped') ||
            normalized.includes('rallied') ||
            normalized.includes('surged') ||
            normalized.includes('fell') ||
            normalized.includes('dropped') ||
            cleaned.includes('股价')
        )
    ) {
        return cleaned;
    }

    const eventKeywords = [
        'earnings',
        'eps',
        'guidance',
        'partner',
        'partnership',
        'contract',
        'acquisition',
        'merger',
        'gtc',
        'ofc',
        'ces',
        'rating',
        'upgrade',
        'downgrade',
        'regulator',
        'probe',
        'investigation',
        'short report',
        '财报',
        '业绩',
        '指引',
        '合作',
        '签约',
        '并购',
        '评级',
        '上调',
        '下调',
        '监管',
        '调查',
        '做空'
    ];

    if (eventKeywords.some((keyword) => normalized.includes(keyword) || cleaned.includes(keyword))) {
        return cleaned;
    }

    return null;
}

function formatPublishedDate(value: string | undefined): string {
    if (!value) {
        return '';
    }

    const match = value.match(/^(\d{4}-\d{2}-\d{2})/);
    if (match) {
        return match[1];
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return '';
    }

    return date.toISOString().slice(0, 10);
}

function currentIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function shiftIsoDate(isoDate: string, days: number): string {
    const date = new Date(`${isoDate}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() + days);
    return date.toISOString().slice(0, 10);
}
