import { getMassiveTickerOverview } from './massive-client';
import { mapIndustryToZh } from './industry-zh-mapping';

export interface CompanyDescription {
    symbol: string;
    short_description: string;
    sector: string | null;
    industry: string | null;
}

const TTL_MS = 7 * 24 * 60 * 60 * 1000;
const cache = new Map<string, { data: CompanyDescription | null; expires: number }>();

export const CURATED_DESCRIPTIONS: Record<string, string> = {
    NVDA: 'NVIDIA 是 AI 算力 GPU 全球龙头供应商',
    META: 'Meta 是全球社交媒体与广告平台龙头',
    GOOGL: 'Alphabet 是搜索广告与 AI 云平台龙头',
    GOOG: 'Alphabet 是搜索广告与 AI 云平台龙头',
    MSFT: 'Microsoft 是云计算与企业软件龙头',
    AAPL: 'Apple 是全球消费电子与服务生态龙头',
    AMZN: 'Amazon 是电商与 AWS 云计算龙头',
    AVGO: 'Broadcom 是网络芯片与定制 ASIC 供应商',
    AMD: 'AMD 是 AI 数据中心 GPU 与 CPU 供应商',
    INTC: 'Intel 是 PC 与服务器 CPU 供应商',
    MU: 'Micron 是 DRAM / NAND 与 HBM 供应商',
    TSM: '台积电是先进制程晶圆代工核心龙头',
    ASML: 'ASML 是 EUV 光刻机全球核心供应商',
    TXN: 'Texas Instruments 是模拟芯片龙头',
    ADI: 'Analog Devices 是模拟与信号链芯片龙头',
    LRCX: 'Lam Research 是晶圆制造设备供应商',
    KLAC: 'KLA 是半导体检测量测设备龙头',
    AMAT: 'Applied Materials 是半导体设备龙头',
    MRVL: 'Marvell 是数据中心网络芯片供应商',
    QCOM: 'Qualcomm 是移动通信芯片全球龙头',
    NXPI: 'NXP 是汽车与工业芯片供应商',
    ON: 'ON Semiconductor 是汽车功率芯片供应商',
    MCHP: 'Microchip 是 MCU 与模拟芯片供应商',
    ANET: 'Arista 是数据中心高速网络交换机供应商',
    CIEN: 'Ciena 是光网络设备与通信软件供应商',
    LITE: 'Lumentum 是数据中心光通信器件供应商',
    VRT: 'Vertiv 是数据中心电源与散热基础设施供应商',
    ETN: 'Eaton 是电气设备与数据中心电力供应商',
    PWR: 'Quanta Services 是电网工程建设服务商',
    CRWD: 'CrowdStrike 是云原生终端安全龙头',
    NET: 'Cloudflare 是边缘网络与安全平台供应商',
    DDOG: 'Datadog 是云监控与可观测性平台龙头',
    MDB: 'MongoDB 是云数据库软件供应商',
    GTLB: 'GitLab 是 DevOps 软件平台供应商',
    ZS: 'Zscaler 是云安全访问平台龙头',
    OKTA: 'Okta 是企业身份认证软件供应商',
    PANW: 'Palo Alto Networks 是网络安全平台龙头',
    PLTR: 'Palantir 是 AI 数据分析软件平台供应商',
    ORCL: 'Oracle 是数据库与云基础设施供应商',
    ADBE: 'Adobe 是创意软件与数字体验龙头',
    CRM: 'Salesforce 是企业 CRM 与云软件龙头',
    SNOW: 'Snowflake 是云数据平台核心供应商',
    SHOP: 'Shopify 是电商 SaaS 平台龙头',
    JPM: '摩根大通是美国综合金融与投行龙头',
    BAC: '美国银行是美国零售与商业银行龙头',
    GS: '高盛是全球投行与资管龙头',
    MS: '摩根士丹利是全球财富管理与投行龙头',
    BLK: 'BlackRock 是全球资产管理龙头',
    V: 'Visa 是全球银行卡支付网络龙头',
    MA: 'Mastercard 是全球银行卡支付网络龙头',
    PYPL: 'PayPal 是全球数字支付平台龙头',
    SQ: 'Block 是商户收单与数字钱包平台',
    COIN: 'Coinbase 是美国加密资产交易平台',
    HOOD: 'Robinhood 是零售券商与交易平台',
    FUTU: '富途是中港美股互联网券商平台',
    UNH: 'UnitedHealth 是美国管理式医疗龙头',
    LLY: 'Eli Lilly 是 GLP-1 与创新药龙头',
    NVO: 'Novo Nordisk 是 GLP-1 糖尿病药龙头',
    MRK: 'Merck 是全球创新药与疫苗龙头',
    PFE: 'Pfizer 是全球制药与疫苗供应商',
    TMO: 'Thermo Fisher 是生命科学工具龙头',
    DHR: 'Danaher 是生命科学与诊断平台',
    ABBV: 'AbbVie 是免疫与肿瘤药物供应商',
    JNJ: 'Johnson & Johnson 是医疗健康综合龙头',
    CAT: 'Caterpillar 是工程机械全球龙头',
    DE: 'Deere 是农业机械全球龙头',
    GE: 'GE Aerospace 是航空发动机供应商',
    GEV: 'GE Vernova 是电网与能源设备供应商',
    HON: 'Honeywell 是工业自动化与航空设备龙头',
    RTX: 'RTX 是航空航天与防务系统供应商',
    LMT: 'Lockheed Martin 是美国防务系统龙头',
    BA: 'Boeing 是全球商用飞机制造商',
    TSLA: 'Tesla 是电动车与储能系统龙头',
    COST: 'Costco 是会员制仓储零售龙头',
    WMT: 'Walmart 是全球折扣零售龙头',
    HD: 'Home Depot 是美国家装零售龙头',
    NKE: 'Nike 是全球运动鞋服品牌龙头',
    LULU: 'Lululemon 是高端运动服饰品牌龙头',
    SBUX: 'Starbucks 是全球咖啡连锁龙头',
    MCD: 'McDonald’s 是全球快餐连锁龙头',
    DIS: 'Disney 是全球内容与主题乐园龙头',
    NFLX: 'Netflix 是全球流媒体内容平台龙头',
    TGT: 'Target 是美国大型零售连锁标的',
    BABA: 'Alibaba 是中国电商与云计算平台',
    PDD: '拼多多是中国折扣电商与跨境平台',
    JD: '京东是中国自营电商与物流平台',
    BIDU: '百度是中国搜索与 AI 云平台',
    NIO: '蔚来是中国高端智能电动车品牌',
    XPEV: '小鹏汽车是中国智能电动车品牌',
    LI: '理想汽车是中国增程式电动车龙头',
    BILI: '哔哩哔哩是中国年轻用户视频社区',
    NTES: '网易是中国游戏与在线内容平台',
    XOM: 'ExxonMobil 是全球综合油气龙头',
    CVX: 'Chevron 是全球综合油气龙头之一',
    OXY: 'Occidental 是美国油气勘探生产商',
    SLB: 'SLB 是全球油服设备与技术龙头',
    VST: 'Vistra 是美国电力与发电资产运营商',
    CEG: 'Constellation 是美国核电运营龙头',
    GDX: 'GDX 是全球黄金矿企 ETF',
    GLD: 'GLD 是黄金现货 ETF',
    NEM: 'Newmont 是全球黄金矿业龙头',
    MSTR: 'MicroStrategy 是比特币资产持有平台',
    CRCL: 'Circle 是稳定币支付基础设施平台',
    DELL: 'Dell 是服务器与企业硬件供应商'
};

export async function getCompanyDescription(symbol: string): Promise<CompanyDescription | null> {
    const normalized = symbol.toUpperCase();
    const cached = cache.get(normalized);
    if (cached && cached.expires > Date.now()) return cached.data;

    const curated = CURATED_DESCRIPTIONS[normalized];
    if (curated) {
        const result = {
            symbol: normalized,
            short_description: formatDisplayDescription(curated, normalized),
            sector: null,
            industry: null
        };
        cache.set(normalized, { data: result, expires: Date.now() + TTL_MS });
        return result;
    }

    const overview = await getMassiveTickerOverview(normalized);
    if (!overview) {
        cache.set(normalized, { data: null, expires: Date.now() + TTL_MS });
        return null;
    }

    const industryZh = mapIndustryToZh(overview.sic_description);
    const shortDescription = buildMassiveDescription(normalized, overview.name, industryZh);
    const result: CompanyDescription = {
        symbol: normalized,
        short_description: shortDescription,
        sector: null,
        industry: overview.sic_description
    };
    cache.set(normalized, { data: result, expires: Date.now() + TTL_MS });
    return result;
}

export async function getDisplayDescription(symbol: string, companyName?: string | null): Promise<string> {
    const normalized = symbol.toUpperCase();
    const curated = CURATED_DESCRIPTIONS[normalized];
    if (curated) return formatDisplayDescription(curated, normalized);

    const overview = await getMassiveTickerOverview(normalized);
    const industryZh = mapIndustryToZh(overview?.sic_description);
    if (overview && industryZh) {
        return buildMassiveDescription(normalized, overview.name ?? companyName ?? normalized, industryZh);
    }

    return buildFallbackDescription(normalized);
}

function buildMassiveDescription(symbol: string, name: string | null, industryZh: string | null): string {
    if (!industryZh) return buildFallbackDescription(symbol);
    const displayName = compactCompanyName(name ?? symbol, symbol);
    return formatDisplayDescription(`${displayName} 是${industryZh}公司`, symbol);
}

function buildFallbackDescription(symbol: string): string {
    return `${symbol} 是美股核心标的`;
}

function formatDisplayDescription(text: string, symbol: string): string {
    const cleaned = text
        .replace(/[,.，。]+$/g, '')
        .replace(/\s+/g, ' ')
        .replace(/\s*\/\s*/g, ' / ')
        .replace(/’/g, "'")
        .trim();
    if (cleaned.length >= 18 && cleaned.length <= 28) return cleaned;
    if (cleaned.length > 28) return truncateDisplayDescription(cleaned);

    if (cleaned.includes('是')) {
        if (cleaned.endsWith('标的')) return cleaned;
        if (cleaned.includes('龙头')) return truncateDisplayDescription(`${cleaned}标的`);
        return truncateDisplayDescription(`${cleaned}${/[A-Za-z0-9]$/.test(cleaned) ? ' ' : ''}核心标的`);
    }
    if (cleaned.includes('龙头')) return truncateDisplayDescription(`${cleaned}标的`);
    if (cleaned.includes('供应商')) return truncateDisplayDescription(`${cleaned}核心标的`);
    if (cleaned.includes('平台')) return truncateDisplayDescription(`${cleaned}核心标的`);
    if (cleaned.includes('公司')) return truncateDisplayDescription(`${cleaned}核心标的`);
    return truncateDisplayDescription(`${cleaned || symbol} 是美股核心标的`);
}

function truncateDisplayDescription(text: string): string {
    if (text.length <= 28) return text;

    const punctuationWindow = text.slice(20, 28);
    const punctuationMatch = [...punctuationWindow.matchAll(/[，。、；;,\s]/g)].pop();
    if (punctuationMatch && punctuationMatch.index !== undefined) {
        const cutAt = 20 + punctuationMatch.index;
        const byPunctuation = text.slice(0, cutAt).replace(/[，。、；;,\s]+$/g, '');
        if (byPunctuation.length >= 18) return byPunctuation;
    }

    const protectedTerms = ['供应商', '标的', '龙头', '平台', '厂商', '公司'];
    let sliced = text.slice(0, 28);
    for (const term of protectedTerms) {
        if (sliced.endsWith(term[0]) && text.slice(0, 29).endsWith(term)) {
            sliced = sliced.slice(0, -1);
            break;
        }
    }
    return `${sliced.replace(/[，。、；;,\s]+$/g, '')}…`;
}

function compactCompanyName(name: string, symbol: string): string {
    const withoutSuffix = name
        .replace(/\b(incorporated|inc|corp|corporation|holdings?|holding|limited|ltd|plc|company|co|class a|common stock)\b\.?/gi, '')
        .replace(/\s*[,\-.，。]+\s*$/g, '')
        .replace(/\s+/g, ' ')
        .trim()
        .replace(/\s*[,\-.，。]+\s*$/g, '');
    const candidate = withoutSuffix || symbol;
    if (candidate.length <= 18) return candidate;
    const firstTwoWords = candidate.split(/\s+/).slice(0, 2).join(' ');
    return firstTwoWords.length <= 18 ? firstTwoWords : symbol;
}
