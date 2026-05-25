import type { ConcernPitchInputs } from './template';
import type { ConcernTag } from './tag-detector';

const DEFAULT_BASE_URL = 'https://api.deepseek.com';

const TAG_DESCRIPTIONS: Record<ConcernTag, string> = {
    earnings_window_imminent: '财报窗口临近,短期事件风险偏高',
    iv_too_low: 'IV 偏低,票息补偿不够',
    composite_score_borderline: '综合评分处于边界',
    guide_cut: '财报指引下调',
    earnings_miss_recent: '近期财报不及预期',
    breakdown_below_ma: '价格跌破关键均线',
    regulatory_overhang: '监管 / 法律事件未明朗',
    post_earnings_gap_down: '财报后股价下行',
    distribution_pattern: '短期动量转弱',
    failed_rebound: '反弹失败',
    single_name_news_overhang: '个股负面新闻密集',
    high_vol_event_risk: '高 IV + 事件窗口',
    liquidity_or_gap_risk: '流动性 / 跳空风险偏高',
    relative_underperformance_5d_20d: '短期和年内相对表现偏弱'
};

export interface ConcernLLMOutput {
    concern_sentence: string;
    used_tags: string[];
    primary_concern_signal: string;
    numeric_claims: Array<{ value: number; unit: string; context?: string }>;
}

export function buildConcernPrompt(p: ConcernPitchInputs): string {
    const tags = [...p.caution_tags, ...p.avoid_tags];
    const tagList = tags.map((tag) => `- ${tag}: ${TAG_DESCRIPTIONS[tag]}`).join('\n');
    return `你是私行 FCN 产品 RM 写作助手。你只负责写 CAUTION 评级的 concern_sentence,不要写 trigger 句,不要写 FCN 条款。

公司:${p.company_short_desc}
客户展示公司定位:${p.display_description}

已点亮 concern tags(只能从这里选):
${tagList}

写作要求:
1. concern_sentence 25-90 字,中性观察语气
2. 只说明当前 FCN 结构不适合卖 put 的条件,不是判断公司基本面差
3. 不要重复公司业务定位,系统会在前一句展示"${p.display_description}"
4. 必须包含至少 1 个具体 concern 信号:已点亮 tag 的具体表述 / 可用输入数字 / 事件窗口
5. 不预测价格下跌,不用恐慌词,不要绝对化措辞
6. 不要写"若/待/再评估"触发句,那部分由系统拼接
7. 数字只能来自输入事实,不要补充新数字
8. 避免泛化表达:"当前卖 put 条件不够友好" / "短期事件风险未落地" / "短期动量转弱"

正例:
{
  "concern_sentence": "财报窗口临近,短期事件风险仍未落地,当前卖 put 的条件不够友好。",
  "used_tags": ["earnings_window_imminent"],
  "primary_concern_signal": "财报窗口临近",
  "numeric_claims": []
}

反例(会被拒绝):
{
  "concern_sentence": "这只股票可能暴跌,千万别碰。",
  "used_tags": ["breakdown_below_ma"],
  "primary_concern_signal": "危险",
  "numeric_claims": []
}

输出 JSON:
{
  "concern_sentence": "...",
  "used_tags": [...],
  "primary_concern_signal": "...",
  "numeric_claims": []
}`;
}

export async function callDeepSeekForConcern(prompt: string): Promise<ConcernLLMOutput | null> {
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
            messages: [{ role: 'system', content: prompt }]
        })
    });
    if (!response.ok) throw new Error(`DeepSeek concern request failed: ${response.status}`);
    const data = (await response.json()) as { choices?: Array<{ message?: { content?: string } }> };
    return parseConcernOutput(data.choices?.[0]?.message?.content ?? '');
}

export function parseConcernOutput(content: string): ConcernLLMOutput | null {
    const parsed = parseJsonObject(content);
    if (!parsed || typeof parsed.concern_sentence !== 'string') return null;
    return {
        concern_sentence: parsed.concern_sentence,
        used_tags: Array.isArray(parsed.used_tags) ? parsed.used_tags.map(String) : [],
        primary_concern_signal: typeof parsed.primary_concern_signal === 'string' ? parsed.primary_concern_signal : '',
        numeric_claims: Array.isArray(parsed.numeric_claims)
            ? parsed.numeric_claims
                  .map((item): ConcernLLMOutput['numeric_claims'][number] | null => {
                      if (typeof item !== 'object' || item === null) return null;
                      const record = item as Record<string, unknown>;
                      const value = Number(record.value);
                      if (!Number.isFinite(value)) return null;
                      const claim: ConcernLLMOutput['numeric_claims'][number] = {
                          value,
                          unit: typeof record.unit === 'string' ? record.unit : ''
                      };
                      if (typeof record.context === 'string') claim.context = record.context;
                      return claim;
                  })
                  .filter((item): item is { value: number; unit: string; context?: string } => item !== null)
            : []
    };
}

function parseJsonObject(content: string): Record<string, unknown> | null {
    try {
        return JSON.parse(content) as Record<string, unknown>;
    } catch {
        const match = content.match(/\{[\s\S]*\}/);
        if (!match) return null;
        try {
            return JSON.parse(match[0]) as Record<string, unknown>;
        } catch {
            return null;
        }
    }
}
