# Markets 判读页 v2 — 机制判读 + distance-to-trigger（Codex 实施 spec）

## 目标
把 Markets 从"美化的指标 dashboard"变成"机制判读页"。核心改动是**信息语法**:
- HERO 讲 verdict（结论 + 因果一句话）
- **Nearest Watch** 讲"离翻转还差什么"（取代原 ticker-tape 留意行）
- **机制判据行**讲：状态 + 关键证据 + **下一触发**（distance-to-trigger），不是堆指标
- 展开才看 raw 指标详情

已对齐的设计决策（不要偏离）：
- **3 个刹车机制**（信用 / 利率 / 基本面）平级 + **1 行"背景"**（拥挤/估值/宽度/情绪，放大器、非刹车、视觉下沉）。**不要 5 等行。**
- **price/vol（VIX / QQQ 回撤）不单独成行** —— 它是 NOISE 的进入闸门，属于 verdict 判定逻辑，体现在 one_line / Nearest Watch / 信用的 VIX 交叉证据里。
- 终端用户是私行 RM/IC；只做客观判读，**不生成客户话术、不给买卖建议**；保守默认 STABLE/NOISE。

## 后端（先做，`src/services/macro-regime/regime-verdict.ts`）

### 1. 刹车状态改 4 档（统一词汇）
现在是 quiet/forming/confirmed。改成 **quiet / watch / forming / confirmed**：
- `quiet` 安静：无异动
- `watch` 观察：早期异动但**未达 forming 阈值**（不驱动 headline）。例：利率 +25~40bp 且 QQQ 平静 → 今天的利率应是 **watch（观察）**，不是 forming。这同时消除"利率异动 vs 顶层形成中差在哪"的困惑。
- `forming` 形成：达 forming 阈值 → 驱动 BREAK_FORMING
- `confirmed` 确认：→ 驱动 CONFIRMED_BREAK

（已有 `realRateBrake.status` 与 `verdictDrivingRatesStatus` 的分裂正好支持：驱动 headline→forming；仅异动未驱动→watch。）

拥挤/背景**不用刹车词**，单独用 `normal / elevated / extreme`（正常/偏高/极端）。

### 2. RegimeVerdict 输出新结构（additive，保留旧字段以防其他消费方）
```ts
regime_verdict: {
  state, mechanism, one_line, confidence,
  nearest_watch: string | null,   // 取代 watch 字符串展示；离翻转最近的 1-2 个信号 + 阈值；全 quiet → null
  mechanisms: {
    credit:      MechanismView,
    rates:       MechanismView,
    fundamental: MechanismView,
  },
  context: {                       // 背景放大器，非刹车
    status: 'normal' | 'elevated' | 'extreme',
    evidence: Array<{ label: string; value: string }>,  // 集中度 / SOX偏离 / 宽度 / F&G
  },
  // 旧字段保留：watch, brakes（标记 deprecated 注释）
}

type MechanismView = {
  status: 'quiet' | 'watch' | 'forming' | 'confirmed';
  evidence: Array<{ label: string; value: string }>;   // 市场化标签+数值，后端权威生成
  next_trigger: string | null;                          // "下一触发"，quiet 且无临近 → null
};
```

### 3. 各机制 evidence + next_trigger（后端权威生成，市场化措辞）
- **信用**：evidence = HY利差 `${bp}bp` · CCC领先 `${bp}bp` · VIX交叉 `是/否` · 股信背离 `出现/无`。next_trigger（watch/quiet 时）= "CCC 持续领先 HY 且第二信号 corroborate（VIX 交叉 / 背离）→ 形成中"。
- **利率**：evidence = 实际利率8周 `${bp}bp` · 10Y `${x}%` · QQQ距高 `-${x}%`（**DFII10 必须进这里**，它是利率刹车核心证据，现在只在留意行是 IA gap）。next_trigger = "升至 +40bp，或 +25bp 且 QQQ 回撤 ≥3% → 形成中"。
- **基本面**：evidence = capex指引 `${raised}/${n}上调` · 营收背离 `${state}` · SOX偏离 `+${x}%`。next_trigger = "capex 指引转 cut 或营收-capex 背离扩大 → 形成中"。
- 数值缺失用 `—`，不要崩。

### 4. nearest_watch（HERO 用）
取状态最高(watch/forming/confirmed 优先)的那个机制，生成一句"离翻转还差什么"：
> 实际利率 8周 +27bp，尚未压低 QQQ;若升至 +40bp 或 QQQ 回撤 ≥3%,利率机制进入形成中。
所有机制 quiet → `nearest_watch = null`（前端省略该行，不要为填满 HERO 而堆 raw values）。

### 5. context（背景）
status: 集中度/SOX/宽度/F&G 任一极端→extreme，偏高→elevated，否则 normal（沿用现有 crowdingElevated 逻辑扩展）。F&G 从 `late_cycle_context` 取回。evidence 同上 label/value。

### 6. 测试
扩展 `regime-verdict.test.ts`：4 档状态映射、nearest_watch（有/无临近）、各机制 next_trigger 文案、context 分级。`tsc --noEmit` 通过。不动 9 指标主干、FCN 门、aggregate severity。**先做后端，停下给我看结构化输出的真实 dry-run（用 prod key）再做前端。**

## 前端（后端 review 通过后再做，`mobile/`）

信息流：**HERO（判定 + 因果 one_line + Nearest Watch）→ 3 机制判据行（异动排前）→ 背景行（下沉）→ 展开详情**。

- 类型：mobile `macro-regime.ts` 加 `RegimeVerdict.mechanisms / context / nearest_watch`（mirror 后端）。
- HERO：圆点+状态词带色；one_line 白；Nearest Watch 有则显示（"留意/下一触发"标签暗，内容亮），无则省略。
- 机制行（仅 credit/rates/fundamental，**按状态降序排，异动在前**）：机制名 · 状态色字（安静/观察/形成/确认）· evidence（标签暗+数值亮 mono）· `下一触发 …`（暗、次级、一行）· chevron 展开 → 复用现有 RiskRow 丰富详情。
- 背景行：**视觉明显下沉**（分隔线下、字号更小、不带刹车色），label "背景" + 状态(正常/偏高/极端) + evidence。
- 状态词彻底两套分离:刹车=安静/观察/形成/确认;背景=正常/偏高/极端。**不要让"异动/恶化"再出现在机制行。**
- 现有前端 `buildMechanisms` 的 evidence 拼装**移除**，改为直接渲染后端 `mechanisms`（后端权威）。drill-down 的 raw 指标仍从 `snapshot.indicators` 映射。

## 不要做
- 不加第 5 行 / 不把 VIX 单独成"市场压力"行 / VIX 归信用交叉或背景。
- 不生成客户话术。
- 不动 9 指标 aggregate、FCN 门、mobile 其它 tab。

## 交付
后端 Stage：结构化 verdict 输出 + 真实 dry-run（展示 mechanisms/next_trigger/nearest_watch/context）+ 单测,停。前端 Stage：渲染重构 + 截图/描述,停。
