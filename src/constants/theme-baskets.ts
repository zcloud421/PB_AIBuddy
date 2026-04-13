export interface ThemeBasket {
    id: string;
    label: string;
    labelEn: string;
    symbols: string[];
    relevantFor: string[];
}

export const THEME_BASKETS: ThemeBasket[] = [
    {
        id: 'ai-infra',
        label: 'AI算力链',
        labelEn: 'AI Infrastructure',
        symbols: ['NVDA', 'AVGO', 'MRVL', 'ANET', 'VRT'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'data-center',
        label: '数据中心',
        labelEn: 'Data Center & Related',
        symbols: ['EQIX', 'DLR', 'SMCI', 'DELL'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'optical-networking',
        label: '光通信',
        labelEn: 'Optical Networking',
        symbols: ['LITE', 'COHR', 'CIEN'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'memory-storage',
        label: '内存与存储',
        labelEn: 'Memory & Storage',
        symbols: ['MU', 'SNDK', 'WDC', 'STX'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'energy-oil',
        label: '油气能源',
        labelEn: 'Energy & E&Ps',
        symbols: ['XOM', 'CVX', 'COP', 'OXY', 'SLB'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'defense',
        label: '防务',
        labelEn: 'Defense',
        symbols: ['LMT', 'RTX', 'NOC', 'GD', 'LHX'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'gold-miners',
        label: '黄金矿业',
        labelEn: 'Gold Miners',
        symbols: ['NEM', 'GOLD', 'AEM', 'WPM'],
        relevantFor: ['middle-east-tensions', 'gold-repricing']
    },
    {
        id: 'enterprise-software',
        label: '企业软件',
        labelEn: 'Enterprise Software',
        symbols: ['CRM', 'SNOW', 'WDAY', 'ADBE', 'NOW'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'it-services',
        label: 'IT服务',
        labelEn: 'IT Services',
        symbols: ['ACN', 'IBM'],
        relevantFor: ['middle-east-tensions']
    },
    {
        id: 'china-tech',
        label: '中概科技',
        labelEn: 'China Tech',
        symbols: ['BABA', 'PDD', 'JD', 'BIDU', 'NTES'],
        relevantFor: ['hk-market-sentiment']
    }
];
