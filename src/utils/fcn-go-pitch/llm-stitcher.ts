import type { HoldingTag, LitTags, TimingTag } from './tag-detector';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

const TAG_DESCRIPTIONS: Record<HoldingTag | TimingTag, string> = {
    guide_raise: '近期财报指引强劲 / 超预期',
    super_cycle: '所在行业处于上行周期(AI capex / 半导体 / 油气等)',
    backlog: '订单可见度 / backlog 较高,未来收入有支撑',
    post_earnings_beat: '近期财报 beat / 超预期',
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
}

export interface PitchNumericClaim {
    value: number;
    unit: string;
    context?: string;
}

export interface PitchLLMOutput {
    why_sentence: string;
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

    return `你是私行 FCN 产品 RM 写作助手。你只负责写 why-sentence,不要写 FCN 条款。

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
- 执行价 $${p.recommended_strike}
- 较现价低 ${p.discount_pct}%
- 年化票息 ${p.coupon_low}%-${p.coupon_high}%
- 期限 ${p.tenor_label}
${typeof p.change_5d_pct === 'number' ? `- 近 5 日 ${p.change_5d_pct.toFixed(1)}%` : ''}
${typeof p.pct_from_52w_high === 'number' ? `- 距 52 周高点 ${Math.abs(p.pct_from_52w_high).toFixed(1)}%` : ''}

写作要求:
1. why_sentence 只写 35-90 字中文,解释为什么现在 sell put 这只股票
2. why_sentence 必须包含 holding reason + timing reason,但不要写 strike / coupon / tenor / 若跌破 等 FCN 条款
3. 不要重复公司业务定位,系统会在前一句展示"${p.display_description}";你只写 why-now + holding signal
4. why_sentence 必须包含至少 1 个具体信号:可用数字事实 / 上方新闻标题事件 / 已点亮 tag 的具体表述
5. used_tags 必须只包含已点亮 tags,且至少 1 个 holding tag
6. timing_signal 必填,不能只是"近期"/"最近"/"当前"/"市场关注"/"情绪改善"
7. 数字必须来自可用数字事实;禁止补充背景知识里的数字
8. 避免泛化表达:"订单可见度较高" / "支撑未来收入" / "技术形态健康" / "均线多头排列" / "动量未破" / "AI 需求支撑"
9. 严禁:"正是好时机" / "不过是" / "您本就看好" / "敲入" / "接货" / "安全垫" / "摊薄" / 风险描述

正例:
{
  "why_sentence": "订单可见度较高,同时股价距 52 周高点回调 12.8%,当前承接水平更有纪律。",
  "used_tags": ["backlog", "quality_pullback"],
  "timing_signal": "股价距 52 周高点回调 12.8%",
  "referenced_news_index": -1,
  "numeric_claims": [{"value": 12.8, "unit": "%", "context": "距 52 周高点"}]
}

反例(会被拒绝:把 timing tag 塞进 holding 或不写 timing_signal):
{
  "why_sentence": "订单可见度较高,公司基本面稳健。",
  "used_tags": ["backlog", "quality_pullback"],
  "timing_signal": "",
  "referenced_news_index": -1,
  "numeric_claims": []
}

输出 JSON:
{
  "why_sentence": "...",
  "used_tags": [...],
  "timing_signal": "...",
  "referenced_news_index": 0,
  "numeric_claims": [{"value": 12.8, "unit": "%", "context": "距52周高点"}]
}
`;
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
    if (!parsed || typeof parsed.why_sentence !== 'string') return null;

    return {
        why_sentence: parsed.why_sentence,
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
