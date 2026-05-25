interface CompanyDescription {
    symbol: string;
    short_description: string;
    sector: string | null;
    industry: string | null;
}

const TTL_MS = 7 * 24 * 60 * 60 * 1000;
const RETRY_TTL_MS = 60_000;

const cache = new Map<string, { data: CompanyDescription | null; expires: number }>();

const CURATED_DESCRIPTIONS: Record<string, string> = {
    NVDA: 'NVIDIA 是 AI 算力 GPU 供应商',
    AMD: 'AMD 是 AI 数据中心 GPU 与 CPU 主要供应商',
    INTC: 'Intel 是 PC / 服务器 CPU 供应商',
    MU: 'Micron 是 DRAM / NAND 全球三大龙头,HBM 关键供应商',
    TSM: '台积电是先进制程晶圆代工核心龙头',
    AVGO: 'Broadcom 是网络芯片与定制 ASIC 供应商',
    LITE: 'Lumentum 是数据中心光通信器件核心供应商',
    VRT: 'Vertiv 是数据中心电源与散热基础设施核心供应商',
    SNDK: 'SanDisk 是 NAND 闪存全球主要供应商',
    CRWV: 'CoreWeave 是 GPU 算力云专业服务商',
    NBIS: 'Nebius 是 GPU 算力云服务商',
    AAPL: 'Apple 是全球消费电子龙头,服务收入占比持续提升',
    MSFT: 'Microsoft 是云计算 + 企业软件 + AI Copilot 龙头',
    GOOGL: 'Alphabet 是搜索广告 + 云 + AI 模型核心玩家',
    META: 'Meta 是社交广告龙头,AI Llama 主导开源生态',
    AMZN: 'Amazon 是电商 + AWS 云 + 物流综合龙头',
    ORCL: 'Oracle 是数据库与云基础设施供应商',
    TSLA: 'Tesla 是电动车 + 储能 + 自动驾驶综合厂商',
    ADBE: 'Adobe 是创意软件与数字体验龙头',
    CRM: 'Salesforce 是企业 CRM 与云软件龙头',
    SNOW: 'Snowflake 是云数据平台核心供应商',
    PYPL: 'PayPal 是全球数字支付平台龙头',
    SHOP: 'Shopify 是电商 SaaS 平台龙头',
    TGT: 'Target 是美国大型零售连锁标的',
    DELL: 'Dell 是服务器与企业硬件供应商',
    BABA: 'Alibaba 是中国电商与云计算平台',
    XOM: 'ExxonMobil 是全球综合油气龙头',
    CVX: 'Chevron 是全球综合油气龙头之一',
    JPM: '摩根大通是美国综合金融龙头',
    GS: '高盛是全球投行与资管龙头',
    BAC: '美国银行是综合零售银行龙头'
};

const TRANSLATIONESE_PATTERNS = [
    /is a company that/i,
    /provides?/i,
    /offers?/i,
    /engages? in/i,
    /operates? as/i,
    /together with its subsidiaries/i
];

export async function getCompanyDescription(symbol: string): Promise<CompanyDescription | null> {
    const normalized = symbol.toUpperCase();
    const cached = cache.get(normalized);
    if (cached && cached.expires > Date.now()) {
        return cached.data;
    }

    const apiKey = process.env.FMP_API_KEY;
    if (!apiKey) {
        const fallback = buildCuratedFallback(normalized);
        cache.set(normalized, { data: fallback, expires: Date.now() + TTL_MS });
        return fallback;
    }

    try {
        const res = await fetch(`https://financialmodelingprep.com/api/v3/profile/${normalized}?apikey=${apiKey}`, {
            signal: AbortSignal.timeout(5000)
        });
        if (!res.ok) {
            throw new Error(`status ${res.status}`);
        }

        const rows = (await res.json()) as Array<{
            description?: string;
            sector?: string;
            industry?: string;
        }>;
        const profile = rows[0];
        const result: CompanyDescription = {
            symbol: normalized,
            short_description:
                CURATED_DESCRIPTIONS[normalized] ??
                extractFirstSentence(profile?.description ?? '') ??
                `${normalized} 标的`,
            sector: profile?.sector ?? null,
            industry: profile?.industry ?? null
        };

        cache.set(normalized, { data: result, expires: Date.now() + TTL_MS });
        return result;
    } catch (error) {
        console.warn('[fmp-company-desc]', normalized, error);
        const fallback = buildCuratedFallback(normalized);
        cache.set(normalized, { data: fallback, expires: Date.now() + RETRY_TTL_MS });
        return fallback;
    }
}

export async function getDisplayDescription(symbol: string, companyName?: string | null): Promise<string> {
    const normalized = symbol.toUpperCase();
    const desc = await getCompanyDescription(normalized);
    const curated = CURATED_DESCRIPTIONS[normalized];
    if (curated) return normalizeDisplayDescription(curated, normalized, desc?.industry ?? null, companyName);

    const candidate = desc?.short_description && !looksTranslationese(desc.short_description)
        ? desc.short_description
        : null;
    if (candidate) return normalizeDisplayDescription(candidate, normalized, desc?.industry ?? null, companyName);

    const fallbackName = companyName || normalized;
    const industry = desc?.industry ? simplifyIndustry(desc.industry) : '美股';
    return normalizeDisplayDescription(`${fallbackName} 是${industry}标的`, normalized, desc?.industry ?? null, companyName);
}

function buildCuratedFallback(symbol: string): CompanyDescription | null {
    const description = CURATED_DESCRIPTIONS[symbol];
    return description
        ? {
              symbol,
              short_description: description,
              sector: null,
              industry: null
          }
        : null;
}

function extractFirstSentence(text: string): string | null {
    const trimmed = text.trim();
    if (!trimmed) return null;
    const match = trimmed.match(/^[^.!?。！？]+[.!?。！？]/);
    return (match ? match[0] : trimmed.slice(0, 100)).trim();
}

function normalizeDisplayDescription(text: string, symbol: string, industry: string | null, companyName?: string | null): string {
    const cleaned = cleanDescription(text);
    const compact = compressDescription(cleaned);
    const len = compact.length;
    if (len >= 18 && len <= 28) return compact;
    if (len > 28) return compact.slice(0, 28);

    const name = companyName || symbol;
    const industryLabel = industry ? simplifyIndustry(industry) : '美股';
    if (compact.includes('是') && !/标的$/.test(compact)) return compact;
    const padded = `${name} 是${industryLabel}核心标的`;
    return padded.length > 28 ? padded.slice(0, 28) : padded;
}

function cleanDescription(text: string): string {
    return text
        .replace(/[,.，。]+$/g, '')
        .replace(/\s+/g, ' ')
        .replace(/\s*\/\s*/g, ' / ')
        .replace(/,向/g, '，向')
        .replace(/,服务/g, '，服务')
        .trim();
}

function compressDescription(text: string): string {
    return text
        .replace('全球 ', '全球')
        .replace('全球主要', '主要')
        .replace('综合厂商', '综合龙头')
        .replace('基础设施核心供应商', '基础设施供应商')
        .replace('核心供应商', '供应商')
        .replace('主要供应商', '供应商')
        .replace('全球先进制程晶圆代工龙头', '先进制程晶圆代工龙头')
        .replace('服务收入占比持续提升', '服务收入持续提升')
        .replace('核心玩家', '核心平台');
}

function looksTranslationese(text: string): boolean {
    return TRANSLATIONESE_PATTERNS.some((pattern) => pattern.test(text));
}

function simplifyIndustry(industry: string): string {
    const lower = industry.toLowerCase();
    if (lower.includes('semiconductor')) return '半导体';
    if (lower.includes('software')) return '软件';
    if (lower.includes('oil') || lower.includes('gas')) return '油气';
    if (lower.includes('bank')) return '银行';
    if (lower.includes('communication')) return '通信设备';
    if (lower.includes('hardware')) return '硬件';
    if (lower.includes('internet') || lower.includes('content')) return '互联网';
    if (lower.includes('auto')) return '汽车';
    if (lower.includes('retail')) return '零售';
    return industry.replace(/[-&]/g, ' ').split(/\s+/).slice(0, 2).join('');
}
