# Comm-reference Stage B — 全自动财报数据补全(FMP)

## 目标
Stage A 已修好 EPS bug + 三段框架,但缺真财务数据,文案是空壳。Stage B **全自动**接入财报数据,让生成器的段①有硬数据(收入/分部/margin),段③有真催化(新闻),达到"可直接 copy 给 HNW 客户"的质量。**无任何手工维护文件。**

理想质量(用户手写 INTC):"…2026Q1 收入同比 +7%、数据中心与 AI 收入同比 +22%,显示盈利改善正在兑现…NVDA 投 50亿、美政府持股 10%…"

## 1. 先审计 FMP 能力(动手前必做)
用现有 `FMP_API_KEY` 试这几个 endpoint,确认当前 plan 可用性,把结果写进交付:
- `/stable/income-statement?symbol=X&period=quarter` — 收入、毛利、营业利润(YoY 算最新季 vs 去年同季)。**基本款,大概率可用。**
- `/stable/revenue-product-segmentation?symbol=X` — 分部收入(算 top 分部 + YoY)。**可能要高 tier,先试;不可用就跳过分部,只用总收入。**
- `/stable/key-metrics?symbol=X&period=quarter` — margin / 比率(备选)。
不可用的 endpoint **不要硬上**,降级用可得字段,并在交付里标注。

## 2. 新建 financials fetcher + 缓存
- `src/data/financials-fetcher.ts`:`fetchSymbolFinancials(symbol)` → `{ revenue_yoy_pct, latest_quarter, gross_margin_pct, gross_margin_yoy_pp, top_segment?: { name, yoy_pct } }`。
- **必须缓存**(财报季度才变):新建 `symbol_financials_cache` 表(symbol, payload JSONB, fetched_at)或复用现有缓存模式;TTL 建议 7 天。走 `ensureSchemaGuards` + `ensureRowLevelSecurity`。
- **限流**:daily screener 跑 53 标的 + search。FMP 免费 250/天,可能不够 → 靠缓存(7天 TTL 下日均 miss 很少);若 plan 限制紧,加并发节流 + 失败 fail-soft(返回 null,生成器降级)。在交付里说明 plan/限流处理。

## 3. 接进 pitch input
- 扩 `PitchInputs`(`llm-stitcher.ts`):`revenue_yoy_pct?`、`top_segment?: {name, yoy_pct}`、`gross_margin_yoy_pp?`。
- `input-adapter.ts`:调 `fetchSymbolFinancials` 填充(fail-soft)。
- 数字严禁编造;只透传 fetcher 返回的真实值。

## 4. 更新 LLM prompt(段① 用真数据)
- 段①(定位+硬数据→解读)**优先用** `revenue_yoy_pct` / `top_segment` / `gross_margin` —— 例:"收入同比 +7%、[top分部] +22%,显示盈利改善正在兑现"。
- 这些字段缺失时才降级到 price-position/tag,且**降级时段①要短、不堆空话**。
- 优先级最终定:**收入/分部增速 > 具体催化 > margin > 价格位置 > (sanitized)EPS surprise**。

## 5. 强化催化(段③,用现有新闻)
- 从 `recent_news_titles` 选**最实质**的 1-2 条(投资/并购/政策/产品/指引),不是取第一条。可用关键词权重或轻量打分;严禁编造未列出的事件。

## 6. 瘦身兜底(template.ts)
- 删除空话:"显示当前定价有可验证的事实锚"、"依赖基本面兑现而非短期情绪"、"进一步强化其…可持有属性"。
- 有真数据 → 兜底也用真数字的三段;数据全缺 → **短而实**(公司定位 + 唯一可得真信号 + 新闻催化),宁可两句也不用空话撑三段。

## 约束 / 交付
- 不碰 FCN 评分/门/macro;只动 pitch 生成 + 新 fetcher/缓存表。
- `tsc --noEmit` 通过;更新 `fcn-go-pitch/*.test.ts` + 新 fetcher 单测(纯函数:YoY 计算、缓存 TTL)。
- comm-reference 绝不含条款/买卖建议。
- 交付:① FMP 能力审计结果 ② diff + 单测 ③ **拿真 DEEPSEEK key 跑 INTC/MSFT/LITE/AVGO 的 LLM 真实输出对比**(确认段①出现真实收入/分部数字、不再是"事实锚"空话)。停下我把关。
