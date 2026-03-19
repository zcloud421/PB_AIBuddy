import type { Flag } from '../types/api';

const THEME_NARRATIVES: Record<string, string> = {
    'AI Infrastructure': '受益于算力扩张、云资本开支与企业AI落地，主题具备中期景气支撑，适合做核心成长型FCN询价方向。',
    Semiconductors: '半导体仍是AI投资主线核心环节，景气度、订单能见度和资金关注度较高，适合作为高关注度科技组合参考。',
    Mag7: 'Mag7兼具流动性、机构覆盖与客户认知度，适合作为私行客户零解释成本的核心询价标的池。',
    'High Volatility': '高波动主题通常带来更高票息区间参考，但需更严格控制执行价与客户适当性，不宜机械追求高收益。',
    'Consumer Tech': '消费科技兼具平台属性与品牌认知，若基本面稳定，适合作为易理解、易沟通的成长型FCN主题。',
    Payments: '支付龙头现金流稳定、商业模式成熟，通常适合作为防御性较强、客户理解成本较低的优质询价方向。',
    'China Tech': '中概科技兼具政策与情绪弹性，票息吸引力通常较高，但更适合能接受外部变量波动的客户。',
    Crypto: '加密主题具备高波动和强叙事属性，适合作为进取型客户的票息增强方向，但必须严控下档风险。',
    Gold: '黄金主题具备避险与对冲属性，在宏观不确定环境下可作为组合防御型FCN询价补充。',
    Defensive: '防御主题更强调持仓承接能力与下行韧性，适合风险偏好较低客户作为稳健型询价备选。',
    Nuclear: '核电主题受益于能源转型与电力需求再定价，具备明确产业叙事，但更适合一句话可解释清楚的二级推荐。',
    Energy: '能源主题与宏观、商品周期相关度高，若趋势明确可提供较好票息参考，但需留意外部事件波动。',
    Healthcare: '医疗防御主题通常具备业绩稳定与配置属性，适合作为组合中偏稳健的FCN候选。',
    Commodities: '商品主题更偏宏观驱动，在风险事件或通胀环境下具备配置意义，可作为差异化主题补充。',
    Materials: '材料板块受资源价格与周期驱动，若供需结构改善，可提供有吸引力的防御或周期型询价机会。'
};

export function buildThemeNarrative(themes: string[], flags: Flag[] = []): string {
    const primaryTheme = themes[0] ?? 'General';
    const base = THEME_NARRATIVES[primaryTheme] ?? '该主题具备一定客户认知基础与结构性机会，适合作为FCN询价清单中的观察方向。';
    const riskSuffix = flags.some((flag) => flag.type === 'BEARISH_STRUCTURE')
        ? ' 但当前价格结构偏弱，询价时需特别关注接股后的下档承受能力。'
        : '';
    return `${base}${riskSuffix}`;
}
