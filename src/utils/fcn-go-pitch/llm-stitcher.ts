import type { HoldingTag, LitTags, TimingTag } from './tag-detector';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

const TAG_DESCRIPTIONS: Record<HoldingTag | TimingTag, string> = {
    guide_raise: '近期财报指引强劲 / 超预期',
    super_cycle: '所在行业处于上行周期(AI capex / 半导体 / 油气等)',
    backlog: '订单可见度 / backlog 较高,未来收入有支撑',
    earnings_strong_beat: '最近一期 EPS 明显超预期(>=5%)',
    earnings_modest_beat: '最近一期 EPS 小幅超预期(2-5%)',
    guidance_reaffirmed_or_raised: '近期指引上调或维持',
    index_inclusion: '近期被纳入重要指数',
    infrastructure_capacity_cycle: '数据中心供电 / 光通信 / 网络基础设施扩容周期',
    quality_pullback: '强基本面公司近期回调,提供合理 entry',
    momentum_intact: '技术形态健康,均线多头排列,动量未破'
};

export interface PitchInputs {
    symbol: string;
    company_short_desc: string;
    display_description: string;
    current_price: number;
    recommended_strike: number;
    discount_pct: number;
    coupon_low: number;
    coupon_high: number;
    tenor_label: string;
    lit_tags: LitTags;
    recent_news_titles?: string[];
    change_5d_pct?: number | null;
    pct_from_52w_high?: number | null;
    days_since_earnings?: number | null;
    earnings_surprise?: {
        eps_surprise_pct: number;
        period: string;
    } | null;
    financials_latest_quarter?: string;
    revenue_yoy_pct?: number | null;
    gross_margin_pct?: number | null;
    gross_margin_yoy_pp?: number | null;
    top_segment?: {
        name: string;
        yoy_pct: number;
    };
}

export interface PitchNumericClaim {
    value: number;
    unit: string;
    context?: string;
}

export interface PitchLLMOutput {
    comm_reference: string;
    used_tags: string[];
    timing_signal: string;
    referenced_news_index?: number;
    numeric_claims: PitchNumericClaim[];
}

export function buildPitchPrompt(p: PitchInputs): string {
    const allTags = [...p.lit_tags.holding, ...p.lit_tags.timing];
    const litList = allTags.map((tag) => `- ${tag}: ${TAG_DESCRIPTIONS[tag]}`).join('\n');
    const holdingList = p.lit_tags.holding.join(', ') || '(无)';
    const timingList = p.lit_tags.timing.join(', ') || '(无)';
    const newsList = p.recent_news_titles && p.recent_news_titles.length > 0
        ? p.recent_news_titles.map((title, index) => `${index}. ${title}`).join('\n')
        : '(本期无新闻)';
    const financialFacts = buildFinancialFacts(p);

    return `你是私行 RM 的客户沟通参考写作助手。你只负责写标的 thesis,不要写 FCN 条款、买卖建议或交易时点。

公司:${p.company_short_desc}
客户展示公司定位:${p.display_description}

已点亮 tags(只能从这里选,不许引入其他理由):
${litList}

holding tags: ${holdingList}
timing tags: ${timingList}

近期新闻标题(可引用其中事件作为 why-now,但不许编造未列出的事件):
${newsList}

可用数字事实(严禁修改任何数字,严禁编造新数字):
- 当前价 $${p.current_price.toFixed(2)}
${typeof p.change_5d_pct === 'number' ? `- 近 5 日 ${p.change_5d_pct.toFixed(1)}%` : ''}
${typeof p.pct_from_52w_high === 'number' ? `- 距 52 周高点 ${Math.abs(p.pct_from_52w_high).toFixed(1)}%` : ''}
${p.earnings_surprise ? `- ${p.earnings_surprise.period} EPS 超预期 ${p.earnings_surprise.eps_surprise_pct.toFixed(1)}%(仅当没有更硬的收入/催化事实时才可低优先级引用)` : ''}
${financialFacts.length > 0 ? financialFacts.map((fact) => `- ${fact}`).join('\n') : '- 财务收入/分部/margin 数据暂缺;缺失时不要编造,段①短写。'}

写作要求:
1. 输出 comm_reference,约 3 句、100-130 个中文字,RM 可整段复制。
2. 三段骨架:① 定位 + 已兑现硬数据 → 解读;② 前瞻驱动 + 未来 3-6 个月窗口 → 稳健性判断;③ 具体催化/护城河 → 战略含义。
3. 每个事实必须配 so-what,句式类似「[数据/事件],显示/带来/强化 [含义]」。
4. 服务于「敢持有」:客户若最终持有该股票,应理解为什么它是可持有的核心资产,不是中性行情快照。
5. 具体可验证 > 泛泛形容。优先级:收入/分部增速(若输入有,段①必须引用至少一个真实收入/分部/margin数字) > 具体新闻催化 > margin > 价格位置 > EPS surprise。禁用空话:「基本面强劲」「技术面强势」「长期向好」「市场关注度提升」「事实锚」「可持有属性」「依赖基本面兑现」。
6. 不碰条款、不碰买卖时点:禁止 strike / coupon / tenor / 执行价 / 票息 / 期限 / 若跌破 / sell put / FCN / 敲入 / 接货 / 安全垫 / 摊薄。
7. 数字必须来自可用数字事实;禁止补充背景知识里的数字。未提供 earnings_surprise 时严禁使用「超预期 / beat / 上调 / 强劲」等财报宣传词。
8. 段③只能引用上方新闻标题中的事件,优先选择投资/并购/政策/产品/指引等实质催化;没有实质催化就短写护城河,不要编造新闻。
9. used_tags 必须只包含已点亮 tags,且至少 1 个 holding tag;timing_signal 必填,不能只是「近期/最近/当前/市场关注/情绪改善」。
10. 禁止相对时间词:「本周 / 上周」;用 period 或「财报后 N 天」。

正例:
{
  "comm_reference": "Intel 是全球领先的 PC 与服务器 CPU 供应商,最近一季收入恢复增长,显示盈利修复正在兑现。新管理层改革与先进制程量产带来估值修复机会,未来 3-6 个月基本面相对稳健。同时英伟达战略投资与美国本土半导体政策支持,进一步强化其美国 AI 半导体核心资产定位。",
  "used_tags": ["guide_raise", "quality_pullback"],
  "timing_signal": "未来 3-6 个月改革和制程量产窗口",
  "referenced_news_index": -1,
  "numeric_claims": [{"value": 3, "unit": "个月", "context": "未来 3-6 个月窗口"}]
}

反例(会被拒绝:空话、条款、没有 so-what):
{
  "comm_reference": "公司基本面强劲,技术面强势,适合在 3 个月期限内 sell put,若跌破也有安全垫。",
  "used_tags": ["backlog", "quality_pullback"],
  "timing_signal": "",
  "referenced_news_index": -1,
  "numeric_claims": []
}

输出 JSON:
{
  "comm_reference": "...",
  "used_tags": [...],
  "timing_signal": "...",
  "referenced_news_index": 0,
  "numeric_claims": [{"value": 12.8, "unit": "%", "context": "距52周高点"}]
}
`;
}

function buildFinancialFacts(p: PitchInputs): string[] {
    const quarter = p.financials_latest_quarter ? `${p.financials_latest_quarter} ` : '';
    const facts: string[] = [];
    if (typeof p.revenue_yoy_pct === 'number') {
        facts.push(`${quarter}收入同比 ${formatSignedPct(p.revenue_yoy_pct)}`);
    }
    if (p.top_segment && typeof p.top_segment.yoy_pct === 'number') {
        facts.push(`${quarter}${p.top_segment.name} 收入同比 ${formatSignedPct(p.top_segment.yoy_pct)}`);
    }
    if (typeof p.gross_margin_pct === 'number') {
        facts.push(`${quarter}毛利率 ${p.gross_margin_pct.toFixed(1)}%`);
    }
    if (typeof p.gross_margin_yoy_pp === 'number') {
        facts.push(`${quarter}毛利率同比 ${formatSignedNumber(p.gross_margin_yoy_pp)}pct`);
    }
    return facts;
}

function formatSignedPct(value: number): string {
    return `${formatSignedNumber(value)}%`;
}

function formatSignedNumber(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}`;
}

export async function callDeepSeekForPitch(prompt: string): Promise<PitchLLMOutput | null> {
    const apiKey = process.env.DEEPSEEK_API_KEY;
    if (!apiKey) return null;

    const baseUrl = process.env.DEEPSEEK_BASE_URL ?? DEFAULT_BASE_URL;
    const response = await fetch(`${baseUrl.replace(/\/$/, '')}/chat/completions`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`
        },
        body: JSON.stringify({
            model: process.env.DEEPSEEK_MODEL ?? 'deepseek-chat',
            temperature: 0.2,
            max_tokens: 500,
            messages: [
                {
                    role: 'system',
                    content: prompt
                }
            ]
        })
    });

    if (!response.ok) {
        throw new Error(`DeepSeek pitch request failed: ${response.status}`);
    }

    const data = (await response.json()) as { choices?: Array<{ message?: { content?: string } }> };
    const content = data.choices?.[0]?.message?.content ?? '';
    return parsePitchOutput(content);
}

export function parsePitchOutput(content: string): PitchLLMOutput | null {
    const parsed = parseJsonObject(content);
    if (!parsed) return null;
    const commReference = typeof parsed.comm_reference === 'string'
        ? parsed.comm_reference
        : typeof parsed.why_sentence === 'string'
          ? parsed.why_sentence
          : null;
    if (!commReference) return null;

    return {
        comm_reference: commReference,
        used_tags: Array.isArray(parsed.used_tags) ? parsed.used_tags.map(String) : [],
        timing_signal: typeof parsed.timing_signal === 'string' ? parsed.timing_signal : '',
        referenced_news_index: typeof parsed.referenced_news_index === 'number' ? parsed.referenced_news_index : undefined,
        numeric_claims: parseNumericClaims(parsed.numeric_claims)
    };
}

function parseJsonObject(content: string): Record<string, unknown> | null {
    try {
        const parsed = JSON.parse(content) as Record<string, unknown>;
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        const match = content.match(/\{[\s\S]*\}/);
        if (!match) return null;
        try {
            const parsed = JSON.parse(match[0]) as Record<string, unknown>;
            return parsed && typeof parsed === 'object' ? parsed : null;
        } catch {
            return null;
        }
    }
}

function parseNumericClaims(value: unknown): PitchNumericClaim[] {
    if (!Array.isArray(value)) return [];
    const claims: PitchNumericClaim[] = [];
    for (const item of value) {
            if (typeof item !== 'object' || item === null) continue;
            const record = item as Record<string, unknown>;
            const numericValue = Number(record.value);
            if (!Number.isFinite(numericValue)) continue;
            claims.push({
                value: numericValue,
                unit: typeof record.unit === 'string' ? record.unit : '',
                context: typeof record.context === 'string' ? record.context : undefined
            });
    }
    return claims;
}
