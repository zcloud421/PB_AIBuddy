# FCN Pair Suitability 评分逻辑

## 使用场景

RM/IC pre-pitch / pre-RFQ sanity check。用户已有 pair idea,在询价或 pitch 客户前快速判断相关性是否值得继续推进。不是客户对话脚本。

## 风险优先级(worst-of 结构)

1. **下跌同步率**: 压力情景下两只能否同向运动(proxy 而非 KI 概率本身)
2. **2022 熊市相关性**: 最近完整熊市的压力测试
3. **日常相关性**(90/180/252): 配对合理性 sanity check
4. **波动率差**: caveat,高 vol leg 更易成为 worst performer

## 阈值

| 指标 | LOW gate | HIGH gate | 锚点 |
|---|---|---|---|
| downside_sync | < 0.55 | >= 0.70 | 0.55 是 worst-of 业内 baseline;0.70+ 高度同步 |
| bear_2022 | < 0.40 | >= 0.60 | 0.40 压力下最弱可接受;0.60+ 联动稳健 |
| max(corr90/180/252) | < 0.30 | >= 0.50 AND corr180 >= 0.40 | 0.30 配对不合理;0.50 同 sector baseline;corr180 二次过滤窗口偶然性 |

## 评级规则

- **LOW**: 任一 OR hard fail
- **HIGH**: 三类 AND 全达标
- **MEDIUM**: 其他(预期大多数 pair 落此)

## Vol gap flag

`vol_ratio > 1.4` → note 提示 worst-of 风险倾向高 vol leg。仅进 note,不进 gate。

## 文案口吻

所有 note 是 RM/IC 内部决策视角,不是客户对话脚本。
