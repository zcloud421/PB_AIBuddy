import type { HoldingTag, LitTags, TimingTag } from './tag-detector';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

const TAG_DESCRIPTIONS: Record<HoldingTag | TimingTag, string> = {
    guide_raise: '近期财报指引强劲 / 超预期',
    super_cycle: '所在行业处于上行周期(AI capex / 半导体 / 油气等)',
    backlog: '订单可见度 / backlog 较高,未来收入有支撑',
    quality_pullback: '强基本面公司近期回调,提供合理 entry',
    momentum_intact: '技术形态健康,均线多头排列,动量未破'
};

export interface PitchInputs {
    symbol: string;
    company_short_desc: string;
    current_price: number;
    recommended_strike: number;
    discount_pct: number;
    coupon_low: number;
    coupon_high: number;
    tenor_label: string;
    lit_tags: LitTags;
}

export interface PitchLLMOutput {
    paragraph: string;
    used_holding_tags: string[];
    used_timing_tags: string[];
    numeric_claims: string[];
}

export function buildPitchPrompt(p: PitchInputs): string {
    const allTags = [...p.lit_tags.holding, ...p.lit_tags.timing];
    const litList = allTags.map((tag) => `- ${tag}: ${TAG_DESCRIPTIONS[tag]}`).join('\n');

    return `你是私行 FCN 产品 RM 写作助手。请用中文 prose 写一段 100-130 字的 pitch text,RM 会直接 copy 给 HNW 客户。

公司:${p.company_short_desc}

已点亮的可用理由(只能从这里选,不许引入其他理由):
${litList}

数字事实(严禁修改任何数字,严禁编造新数字):
- 当前价 $${p.current_price.toFixed(2)}
- 执行价 $${p.recommended_strike}(较现价低 ${p.discount_pct}%)
- 年化票息 ${p.coupon_low}%-${p.coupon_high}%
- 期限 ${p.tenor_label}

写作要求:
1. 一段连贯 prose,不分行不分段,不用 bullet
2. 100-130 字
3. 第二人称"您",中性语气
4. 数字嵌入句子里(如"以 $95、较现价低 15% 的水平承接...")
5. 严禁:"正是好时机" / "不过是" / "您本就看好" / "敲入" / "接货" / "安全垫" / "摊薄" 等用语
6. 严禁在 pitch 中提风险(非保本 / 信用风险 / 流动性 等,由 product term sheet 单独承担)
7. 必须用"若股价未跌破 $X,您收取票息并赎回本金;若跌破,则以 $X 持有..."条件句式收尾
8. 严禁引入未在"已点亮理由"中的任何理由 / 任何具体新闻事件
9. 严禁编造任何数字

输出 JSON:
{
  "paragraph": "...",
  "used_holding_tags": [...],
  "used_timing_tags": [...],
  "numeric_claims": ["$95", "较现价低 15%", "12%-16%", "3 个月"]
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
            max_tokens: 800,
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

function parsePitchOutput(content: string): PitchLLMOutput | null {
    try {
        const parsed = JSON.parse(content) as Partial<PitchLLMOutput>;
        if (typeof parsed.paragraph !== 'string') return null;
        return {
            paragraph: parsed.paragraph,
            used_holding_tags: Array.isArray(parsed.used_holding_tags) ? parsed.used_holding_tags.map(String) : [],
            used_timing_tags: Array.isArray(parsed.used_timing_tags) ? parsed.used_timing_tags.map(String) : [],
            numeric_claims: Array.isArray(parsed.numeric_claims) ? parsed.numeric_claims.map(String) : []
        };
    } catch {
        const match = content.match(/\{[\s\S]*\}/);
        if (!match) return null;
        try {
            const parsed = JSON.parse(match[0]) as Partial<PitchLLMOutput>;
            if (typeof parsed.paragraph !== 'string') return null;
            return {
                paragraph: parsed.paragraph,
                used_holding_tags: Array.isArray(parsed.used_holding_tags) ? parsed.used_holding_tags.map(String) : [],
                used_timing_tags: Array.isArray(parsed.used_timing_tags) ? parsed.used_timing_tags.map(String) : [],
                numeric_claims: Array.isArray(parsed.numeric_claims) ? parsed.numeric_claims.map(String) : []
            };
        } catch {
            return null;
        }
    }
}
