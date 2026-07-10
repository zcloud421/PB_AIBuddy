/**
 * Static English-to-Chinese mapping for backend-generated note templates.
 *
 * Why static dictionary instead of LLM translation:
 *   - Backend notes are template strings with numeric variables — deterministic
 *   - LLM would produce inconsistent Chinese day-to-day (looks unprofessional)
 *   - Financial terms (OAS / 200DMA / Δ4w) need exact translation, not LLM guess
 *   - Zero runtime dependency, ~60 entries cover all indicators + side monitors
 *
 * Maintenance: when backend adds new indicator notes, append entries here.
 */

// Order matters: longer/more-specific phrases first to prevent partial overlap.
export const INDICATOR_GLOSSARY: Array<[RegExp | string, string]> = [
  // — Full phrases —
  ['Liquidity proxy — does not solo-trigger portfolio Critical', '流动性代理 · 不能单独触发整体告警'],
  ['Display-only context: tight spreads can persist and are not timing triggers', '仅作背景参考:利差偏紧可持续存在,不作择时触发'],
  ['Kept outside severity aggregator to avoid late-cycle alert fatigue', '不进严重度聚合器,避免晚周期告警疲劳'],
  ['Insufficient data', '数据不足'],
  ['SOX 200DMA Deviation', 'SOX 200日均线偏离度'],
  ['execution variance', '执行偏差'],
  ['hyperscaler', '云算力巨头'],
  ['Complacency', '信用利差极度收窄'],
  ['cycle low', '周期低点'],
  ['confirmation', '确认'],
  ['tightening', '利差收窄'],
  ['acceleration', '加速度'],
  ['Warning', '警示'],
  ['Critical', '极高'],
  ['Neutral', '中性'],
  ['Healthy', '平稳'],

  // — Tokens —
  [/^Δ4w\s+/, '4周变化 '],
  [/Δ4w\s+/, '4周变化 '],
  [/Δ8w\s+/, '8周变化 '],
  [/Δ4w-Δ8w/, '4周-8周加速度'],
  [/60d drawdown/, '60日回撤'],
  [/60d return/, '60日涨跌'],
  [/200DMA/, '200日均线'],
  [/(\d+)\/(\d+)\s+below 200DMA/, '仅 $1/$2 跌破 200 日均线'],
  [/(\d+)\s+below 200DMA/, '$1 只跌破 200 日均线'],
  [/above 200DMA/, '在 200 日均线上方'],
  [/vs MA200/, ' vs 200 日均线'],
  [/(\d+)\/(\d+) active/, '样本覆盖:$1/$2'],
  [/Top-10/, '前 10'],
  [/HHI/, 'HHI 集中度'],
  [/absolute /, '绝对水位 '],
  [/Latest 10Y/, '最新 10Y'],
  [/4w prior/, '4 周前'],
  [/Peak/, '高点'],
  [/last/, '当前'],
  [/momentum tripwire/, '短期动量预警'],
  [/regime health/, '市场体制健康度'],
  [/divergence/, '背离'],
  [/leadership/, '龙头'],
  [/(\d+) days/, '$1 天'],
  [/elevated/, '偏高'],
  [/extreme/, '极高'],
  [/healthy/, '平稳'],
  [/intact/, '完好'],
  [/softening/, '走弱'],
  [/breaking/, '断裂'],

  // Side monitor sub-signal names
  [/^KBE vs MA200$/, '银行股 KBE vs 200 日均线'],
  [/^HY OAS Δ4w-Δ8w$/, 'HY 利差 4 周 vs 8 周加速度'],
  [/^DGS3MO - DGS2$/, '3 个月 vs 2 年期利差'],

  // Misc fixed phrases
  [/investors not pricing tail risk/, '投资者未对尾部风险定价'],
  [/matches 2021 top, near 2000 record 24x/, '与 2021 顶部匹配,接近 2000 年 24x 纪录'],
  [/only dot-com peak comparable/, '历史仅 dot-com 顶部可比'],
  [/Mag7 vs SPY 60d excess return/, 'Mag7 vs SPY 60 日超额涨幅'],
  [/notional/, '名义敞口'],
  [/put exposure/, '看跌期权敞口'],
  [/tech valuations back near pre-AI-boom levels/, '科技股估值回到 AI 浪潮前水位'],
  [/divergence with sentiment/, '与情绪面背离'],
];

export function translateNote(note: string): string {
  let result = note;
  for (const [pattern, replacement] of INDICATOR_GLOSSARY) {
    if (typeof pattern === 'string') {
      result = result.split(pattern).join(replacement);
    } else {
      result = result.replace(pattern, replacement);
    }
  }
  return result;
}
