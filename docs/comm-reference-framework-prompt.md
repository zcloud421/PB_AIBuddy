# 客户沟通参考(comm-reference)生成器重构 — 3 段框架 + EPS bug 修复

## 背景
GO pitch 的 why-sentence 由 `src/utils/fcn-go-pitch/llm-stitcher.ts` 的 `buildPitchPrompt` 生成。
现状两大问题:
1. **EPS surprise% 垃圾数据**:prompt 强制引用 "EPS 超预期 X%"(line 84/90/91)。当公司盈利接近盈亏平衡(分母趋零),微小 beat 算出 1971.4% 这类天文数字,给私行客户看会瞬间失信。
2. **数据太薄 + 文案是快照不是 thesis**:`PitchInputs` 只有价格/票息/距高点/EPS-surprise,LLM 写一句话快照,无收入/分部增速、无催化剂、无前瞻逻辑。达不到理想的"可直接 copy 给 HNW 客户"的质量。

理想样本(用户手写 INTC):
> Intel 是全球领先的 PC 与服务器 CPU 供应商,2026Q1 收入同比 +7%、数据中心与 AI 收入同比 +22%,显示盈利改善正在兑现。新管理层改革与先进制程量产带来估值修复机会,未来 3-6 个月基本面相对稳健。同时英伟达战略投资 50 亿美元、美政府持股约 10%,进一步强化其美国 AI 半导体核心资产定位。

## 提炼出的框架(必须实现)

**3 段骨架**(每段 = 事实 → so-what):
- ① **定位 + 已兑现硬数据 → 解读**:[行业地位] + [最近一季 1-2 个硬指标:收入/分部/margin] + [这数据证明了什么]
- ② **前瞻驱动 + 时间窗口**:[向前的驱动力/逻辑] + [对齐 FCN 期限的窗口,如"未来 3-6 个月"] + [稳健性判断]
- ③ **差异化催化 → 战略含义**:[近期具体催化/护城河] + [战略定位:为什么是核心资产]

**6 条铁律**(写进 prompt):
1. 每个事实必配 so-what,句式 "[数据/事件],显示/带来/强化 [含义]"。
2. 三段时间弧:过去已兑现 → 当下兑现中 → 未来锚定。
3. 服务于"敢持有"(FCN 客户 KI 时被 assign 该股 → 建立 conviction,不是中性快照)。
4. 具体可验证 > 泛泛形容:用收入/分部增速/具体催化的硬数;**禁用**"基本面强劲/技术面强势"空话 + **禁用 EPS surprise%**。
5. **不碰条款、不碰买卖时点**(strike/coupon/tenor/若跌破 全不写 —— 卡片已列;comm-reference 是纯标的 thesis)。
6. 长度 ~3 句 100-130 字,RM 可整段 copy。

泛化验证(steady compounder):MSFT、NVDA、cyclical 都能套同一骨架,③ 槽位取"催化"或"护城河"中最强者。

## Stage A — prompt 重写 + EPS bug 修复(必做)

1. **EPS surprise 根除**(`input-adapter.ts` 构造 `earnings_surprise` 处 + prompt):
   - 当 |estimate| 趋零(near-zero base,如 |EPS_est| < 0.10)或 |surprise%| 超过合理上限(如 > 100%)→ **不传 `earnings_surprise`**(置 null),surprise% 在此情形无意义。
   - prompt 删除 "EPS surprise > 一切、必须引用" 的强制(line 90-91);EPS surprise 降为可选,且仅在分母不趋零、幅度合理时可引用。**优先级改为:收入/分部增速 > 具体催化事件 > 价格位置 > EPS surprise。**
2. **`buildPitchPrompt` 重写**成 3 段框架 + 6 铁律 + fact→so-what 句式,正例换成 INTC/MSFT 那种 3 段结构。
3. **输出字段**:`why_sentence`(单句)→ 扩成 `comm_reference`(3 段 100-130 字)。更新 `PitchLLMOutput` + `parsePitchOutput` + 下游消费方(`index.ts` / `template.ts` fallback / `validator.ts` / `stitcher`)。保留 `used_tags` / `numeric_claims` 用于校验。
4. **validator.ts**:校验规则适配新格式 —— 仍禁条款词 / 禁泛化空话 / 禁 EPS-surprise(当被根除时)/ 数字必须来自输入;新增:鼓励(不强制)三段结构存在。
5. 模板兜底(`template.ts`):LLM 失败时的确定性兜底也按 3 段骨架降级(用现有可得字段)。

## Stage B — 数据补全(达到理想质量的前提,先审计再接)

理想样本用到的数据当前 `PitchInputs` 没有。先审计代码里已可得的源,再扩 `PitchInputs`:
- **最近一季收入 / 分部增速 / margin**:查 `FMP_API_KEY` / `FINNHUB_API_KEY` 是否已有 income-statement / segment 拉取;`fundamental_modifier`(capex 数据)也可复用。补 `recent_revenue_growth_yoy` / `segment_growth` 字段。
- **近期催化剂**:现有 `recent_news_titles` 已传但用得弱。强化为"从新闻里提取 1-2 个可验证的具体催化"(投资/政策/产品/管理层),作为 ③ 槽位输入;严禁编造未列出的事件。
- **战略定位**:可由 `display_description` + 行业 + 催化派生,LLM 负责。
- 若某数据源不可得,Stage A 的框架仍成立(用 news + description 降级),但要在交付里**明确标注哪些槽位因缺数据而弱**。

## 约束
- 不碰 FCN 评分/门/macro;只改 pitch 文案生成。
- `tsc --noEmit` 通过;更新 `fcn-go-pitch/*.test.ts`(stitcher/validator/template/input-adapter)。
- comm-reference **绝不含条款、绝不含买卖建议**(合规)。
- 数字严禁编造/修改,只能来自 input 的事实字段。

## 交付
Stage A:diff + 单测 + 3-5 个真实标的的前后对比(尤其 INTC,确认 1971% 消失、出现 3 段 thesis)。先做 A、停,给我看对比再做 B。
