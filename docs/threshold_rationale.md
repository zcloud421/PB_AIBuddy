# Threshold Rationale — Anchor Episodes Reference

为防止未来阈值 drift / 维护者不知道某个 threshold 为什么是某个值,本文档记录
每个 indicator 的 severity 阈值及其历史 anchor episode。

**核心原则:** 所有阈值都是 seasoned-eye anchor,**不是从 backtest 优化出来的数字**。
任何修改阈值的 PR 必须 update 这个 doc + 给出新的 anchor 理由。

---

## 1. HY_OAS (High-Yield Credit Spread)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < 350bp | Healthy | 2017-2019 长期低位区间 |
| 350-450bp | Neutral | 2015 能源 mini-cycle / 正常周期波动 |
| 450-600bp | Warning | 2018Q4 / 2015 中国汇改 / 2011 欧债 |
| > 600bp | Critical | 2020Q1 COVID / 2008 GFC 中段 |

**Δ4w > +75bp:** 强制升级 1 档(acceleration 信号)。Anchor: 2008/9 雷曼前 4 周 / 2020/3 流动性枯竭。

**Note:** < 300bp 是 complacency tight,但**不进 severity aggregator**(避免 alert fatigue,2017-2019 常年 <350bp 但未出事)。

### HY_OAS 子带 + Widening Confirmation

#### Tight zone (<300bp)

历史 anchor:
- 2007-06 cycle low **241bp**(GFC 7 个月后爆发,Bear Stearns 基金 1 个月后崩盘)
- 2021-10 cycle low **290bp**(2022 全年熊市)
- 1998 LTCM 危机前同样 sub-300

3/3 历史 sub-300 tight 期均以重定价收场。

**当前(2026-05)**:HY OAS 约 280bp,已进入信用-基本面背离区:HY default rate 抬升,但 spread 仍在 cycle low 附近。

**系统行为**:
- row 显示 Healthy(spread 绝对水位不触发 stress)
- 展开态显示 Complacency badge + 历史 anchor
- 不进 aggregation,不影响 overall

#### Widening Confirmation

Druckenmiller 框架:credit spread widening **持续 >=2 周才视为 risk-off 信号**,单日尖刺不动 severity。

- 升档(Healthy → Neutral / Warning / Critical):需要 **10 个交易日** confirmation
- 降档:立即生效(收窄要立即体现 risk-off 退潮)
- pending 期间 row 显示 "升档待确认 · X/10 天",但 severity 仍为旧值
- 仅对 HY_OAS 启用(其他指标不变)

#### hy_acceleration_signal 修复(2026-05-21)

原逻辑 `acceleration = delta4w - delta8w` 在两者都为负(都在 tightening)时仍可能输出正值,触发 false "watch"。修复:仅当 `delta4w > 0`(确实在 widening)时打分。

#### hy_acceleration_signal noise floor (2026-05-21 进一步优化)

继 P1 修复 tightening 减速误报后,实测发现 cycle low 区(286bp)+ Δ4w +1bp 仍触发 "watch" — 严格按规则是 widening,但 1bp 移动在 cycle low 区是数值噪音。

**历史 reference**:真实 4-week credit widening stress 案例:
- 2022 bear market: 平均 +33bp/4w
- 2018 Q4: +85bp/4w
- 2015-16 oil crash: +75bp/4w
- 2020 COVID: +750bp/4w(极端)

任何真实 stress 4-周扩张都 >=25-30bp。

**双层 noise floor**:
- **Tight zone (<300bp)**:noise_floor = **25bp**(cycle low 区噪音空间大)
- **正常区 (>=300bp)**:noise_floor = **15bp**(已离开 complacency,15bp 是有意义 widening)

打分逻辑(必须三个条件同时满足):
1. `delta4w > 0`(排除 tightening)
2. `delta4w >= noise_floor`(排除数值噪音)
3. `acceleration >= 25`(打分起点)

副作用验证:
| HY OAS | Δ4w | acceleration | 行为 |
|---|---|---|---|
| 286 | +1 | +34 | score 0 ✅(原 bug case) |
| 295 | +30 | +35 | score 1 ✅(tight zone 真实 widening) |
| 400 | +20 | +30 | score 1 ✅(正常区真实 widening) |
| 400 | +10 | +30 | score 0 ✅(正常区未达门槛) |

---

## 2. YIELD_CURVE (10Y-2Y Spread)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| > 50bp | Healthy | 正常扩张期 term structure |
| 0-50bp | Neutral | Flatten 区,但未倒挂 |
| -50 to 0bp | Warning | 2006-2007 浅倒挂 / 2019 短暂倒挂 |
| < -50bp (持续 >=60 天) | Critical | 1999 / 2006-2007 深度持续倒挂 |

**Note:** 2022-2024 持续倒挂 >=-100bp 但未触发即时衰退,**单一信号不可靠**。
持续 60 天才升 Critical 是为防止单日抖动。

---

## 3. VIX (+ SPY RV20 quality check)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < 18 | Healthy | 长期均值约 19,低位区 |
| 18-25 | Neutral | 正常波动 |
| 25-35 (持续 >=3 天) | Warning | 2018Q4 / 2022 加息冲击 |
| > 35 (持续 >=3 天) | Critical | 2008/9 / 2020/3 / 2018/2 vol-shock |

**Persistence requirement:** Warning+ 必须持续 3 个交易日,防止单日 spike 误报。
**RV20 quality check:** 如果 VIX Healthy 但 SPY RV20 > VIX + 3,vol 被压制,降级 Healthy → Neutral。

---

## 4. DGS10_ABS_LEVEL (10Y Treasury Yield Absolute)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < 4.0% | Healthy | 长期低位区(2008-2021 区间) |
| 4.0-4.5% | Neutral | 估值压力开始累积 |
| 4.5-5.0% | Warning | P/E compression zone,历史多次验证 |
| > 5.0% | Critical | 2023-10 风险资产破位点 |

---

## 5. DGS10_4W_SHOCK (10Y 4-Week Change)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < 25bp | Healthy | 正常波动 |
| 25-50bp | Neutral | 加息预期变化典型节奏 |
| 50-75bp | Warning | 2022 加息周期典型 4 周节奏 |
| > 75bp | Critical | 2013 taper tantrum / 2022/9 加速段 |

---

## 6. CONCENTRATION (Top-10 SPY Weight)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < 25% | Healthy | 长期均值区(1990-2015) |
| 25-32% | Neutral | 2017-2020 mega-cap rise 期 |
| 32-38% | Warning | 2024 mid-year / 当前 mag7 主导期 |
| > 38% | Critical (soft-capped to Warning) | 2024Q4 / 2025-2026 当前 |

**Soft-cap rule:** 即使 >=38%,聚合上 capped 到 Warning(结构性指标不应 solo trigger Critical)。

---

## 7. AI_BREADTH (16 mega-cap above 50DMA, tier-weighted)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| > 70% | Healthy | leadership 健康 |
| 50-70% | Neutral | partial weakness |
| 30-50% | Warning | leadership 转弱(2022 mega-cap 失血期典型) |
| < 30% | Critical | leadership death |

**Tier-1 guardrail:** 如果 Tier-1 中 >=4 只在 50DMA 下方,强制升 Critical(2022/1 mega-cap 顶部前期 typical)。

**Note:** 用 50DMA 是 by design,作为 short-term leadership tripwire,与 BROAD_BREADTH (200DMA) 测不同维度。

---

## 8. BROAD_BREADTH (NDX-100 above 200DMA, equal-weight)

| Threshold | Severity | Anchor Episode |
|---|---|---|
| > 70% | Healthy | regime healthy |
| 50-70% | Neutral | mid-cycle 正常波动 |
| 30-50% | Warning | 2022 / 2018Q4 broad weakness |
| < 30% | Critical | 2008 / 2020/3 broad collapse |

**Note:** 用 200DMA = StockCharts / Schwab IC 标准 long-term trend 阈值。

---

## 9. SOX_200DMA_DEVIATION

**意图:** 衡量 AI 算力板块位置伸展度,补齐 AI pillar 在"位置极端 / 泡沫尾段"维度的缺口。

| Threshold | Severity | Anchor Episode |
|---|---|---|
| < +15% | Healthy | 正常牛市 stretch |
| +15% to +30% | Neutral | 偏热,但远离泡沫尾段 |
| +30% to +50% | Warning | 接近 1700 年以来主要资产泡沫峰值均值 +35% |
| > +50% | Critical (soft-capped to Warning) | Dotcom NASDAQ 2000 (+55%) / Mississippi 1720 (+73%) 量级 |

**Aggregation behavior:** SOX row-level Critical 被 soft-cap 到 Warning,仅在与 AI_BREADTH / VIX / DGS10_ABS_LEVEL 其中一个 Warning+ 共振时,才允许在聚合层表现为 Critical。

**Rationale:** 估值/位置类信号 timing 性差,可持续数月(参考 NASDAQ 1999 stretch 超过 12 个月)。单独触发 Critical 会重蹈 CAPE 老路。它是 entry-risk context,不是独立 de-risk trigger。

---

## Aggregation Hardening(2026-05-21)

### Persistence 分级原则

| 指标类型 | Persistence 要求 | 理由 |
|---|---|---|
| volatility / breadth | 2-3 trading days | 高频信号,过滤太严会迟钝 |
| rates shock / SOX stretch / valuation | **5 trading days** | 慢变量,过滤尖刺 |
| credit widening | 10 trading days | Druckenmiller 框架,sustained widening 才算 risk-off |
| manual context / quarterly | 不直接触发 Critical | 季度评审本就 lag,只做 context |

### SOX Resonance 升级规则(2026-05-21 修订)

SOX_200DMA_DEVIATION Critical AND (
AI_BREADTH >= Warning OR
VIX >= Warning OR
DGS10_4W_SHOCK >= Warning
)
AND SOX persistence >= 5 trading days
→ overall Critical(仍可能被 guardrail 降级)。

修订点:使用 fresh rate shock(`DGS10_4W_SHOCK`),不再使用 `DGS10_ABS_LEVEL` backdrop。

### Soft-Derived Critical Guardrail

当 overall Critical **完全来自 soft-capped 指标 escalation**(SOX / CONCENTRATION / BTC),且市场未定价 risk-off,降级 Warning:

- base_overall != Critical(hard signal 不在场)
- 所有 escalation reasons 都是 soft-derived
- HY_OAS < 350bp
- HY acceleration normal(score 0)
- VIX < 25

→ overall cap 到 Warning
→ note: "市场未定价 risk-off — 估值伸展风险维持 Warning,等待波动率 / 信用 / 宽度确认"

**关键:** hard signal Critical(VIX/HY/breadth/fundamental 自身 Critical)**穿透** guardrail,不受影响。

### Soft-Cap 指标列表(timing reliability 维度)

- BTC_DRAWDOWN
- CONCENTRATION
- SOX_200DMA_DEVIATION
- 未来候选(若加入):CAPE / Forward P/E / F&G Extreme Greed

这些指标共性:**估值/位置类,timing 性差,可持续数月**。永远不能 solo Critical。

### 不加降级 confirmation

退潮立即生效。如果系统在风险消退后仍持续喊高危,信任流失成本高于反向。

---

## 10-12. Side Monitors

### AI Cloud Stress (CRWV + NBIS)

- score 0-3 per ticker,合计 0-6
- score >=3 = Watch, >=5 = Warning, =6 = Critical
- **Anchor:** neocloud / GPU rental financing fragility — 2024-2026 AI infra 周期专属信号
- **Trust caveat:** 仅 2 只样本,单股事件易扭曲(P2 改进:单股触发降权)

### Credit / Funding Stress (KBE + HY accel + 3M-2Y)

- 3 个子信号各 score 0-3
- overall normal / watch / warning / crisis
- **Anchor:** 2008/9 银行股 + HY 双重压力 / 2023/3 SVB 银行业 mini-crisis

### Fundamental Modifier (AI Capex JSON)

- 季度人工 review,intact / softening / breaking
- Context-only: 不再机械上调整体 severity；用于提示 RM/IC 将 AI Capex 敞口拿回 house view / IC 框架复核
- **Anchor:** 2022/11 ChatGPT 推出 / 2025/1 DeepSeek shock 季度复核结果

---

## 13. CNN Fear & Greed (Layer B context, 不进 severity aggregator)

| Score | Band | Tone (PB asymmetric) |
|---|---|---|
| 0-25 | 极度恐惧 | balanced 灰(已跌,非 add risk) |
| 25-45 | 恐惧 | balanced 灰 |
| 45-55 | 中性 | balanced 灰 |
| 55-75 | 贪婪 | heated 橙 |
| 75-100 | 极度贪婪 | stress 红 |

**Asymmetric mapping rationale:** PB FCN 业务关心 "新建仓 风险 vs 机会"。Greed 端是
顶部加仓 risk,Fear 端市场已跌是 potential buy 机会,不应映射为 risk 红。

---

## Threshold Versioning Policy

- 任何 threshold 修改 → 必须 update 本 doc + commit message 包含 anchor episode
- 至少每季度复核一次(check market regime drift)
- 单个 threshold 调整 <= 10%(防止 fit-to-recent)
- 全面调整必须经过 cross-review(类似当前流程)
