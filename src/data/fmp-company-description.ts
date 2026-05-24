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
    NVDA: 'NVIDIA 是全球 AI 算力 GPU 核心供应商',
    AMD: 'AMD 是 AI 数据中心 GPU 与 CPU 主要供应商',
    INTC: 'Intel 是全球 PC / 服务器 CPU 核心供应商,向 AI 加速转型',
    MU: 'Micron 是 DRAM / NAND 全球三大龙头,HBM 关键供应商',
    TSM: '台积电是全球先进制程晶圆代工龙头',
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
    ORCL: 'Oracle 是企业数据库 + 云 + AI 基础设施供应商',
    TSLA: 'Tesla 是电动车 + 储能 + 自动驾驶综合厂商',
    XOM: 'ExxonMobil 是全球综合油气龙头',
    CVX: 'Chevron 是全球综合油气龙头之一',
    JPM: '摩根大通是美国综合金融龙头',
    GS: '高盛是全球投行与资管龙头',
    BAC: '美国银行是综合零售银行龙头'
};

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
