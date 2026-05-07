import axios from 'axios';

const SINA_MACRO_GOLD_URL =
    'https://quotes.sina.cn/mac/api/jsonp_v3.php/SINAREMOTECALLCALLBACK1601651495761/MacPage_Service.get_pagedata';
const FETCH_HEADERS = {
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    Referer: 'http://finance.sina.com.cn/mac/'
};
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

export interface ChinaGoldReserveDataPoint {
    period: string;
    gold_reserve_wan_oz: number;
    fx_reserve_100m_usd: number;
}

export interface ChinaGoldReserveTrend {
    latest_period: string;
    latest_gold_reserve_wan_oz: number;
    latest_gold_reserve_tonnes: number;
    mom_change_wan_oz: number | null;
    consecutive_months_increase: number;
    consecutive_months_decrease: number;
    history_size: number;
}

let memoryCache: { fetchedAt: number; trend: ChinaGoldReserveTrend } | null = null;

async function fetchRawSinaMacroGold(): Promise<ChinaGoldReserveDataPoint[]> {
    const response = await axios.get<string>(SINA_MACRO_GOLD_URL, {
        params: {
            cate: 'fininfo',
            event: '5',
            from: '0',
            num: '10000',
            condition: ''
        },
        headers: FETCH_HEADERS,
        timeout: 10000,
        responseType: 'text'
    });

    const raw = response.data;
    const dataStart = raw.lastIndexOf('data:');
    if (dataStart < 0) {
        throw new Error('Sina macro gold payload missing data array');
    }

    const dataPayload = raw.slice(dataStart + 'data:'.length);
    const match = dataPayload.match(/^(\[[\s\S]*\])\s*\}\)\);?\s*$/);
    if (!match) {
        throw new Error('Sina macro gold data array format unexpected');
    }

    const parsed = JSON.parse(match[1]) as Array<[string, string, string]>;

    return parsed
        .map(([period, gold, fx]) => ({
            period,
            gold_reserve_wan_oz: Number(gold),
            fx_reserve_100m_usd: Number(fx)
        }))
        .filter((point) => Number.isFinite(point.gold_reserve_wan_oz));
}

function computeTrend(points: ChinaGoldReserveDataPoint[]): ChinaGoldReserveTrend {
    const chronological = [...points].reverse();
    const latest = chronological[chronological.length - 1];
    const prior = chronological[chronological.length - 2];

    if (!latest) {
        throw new Error('Sina gold reserve payload is empty');
    }

    let consecutiveIncrease = 0;
    let consecutiveDecrease = 0;
    for (let index = chronological.length - 1; index > 0; index -= 1) {
        const diff = chronological[index].gold_reserve_wan_oz - chronological[index - 1].gold_reserve_wan_oz;
        if (diff > 0) {
            if (consecutiveDecrease > 0) break;
            consecutiveIncrease += 1;
        } else if (diff < 0) {
            if (consecutiveIncrease > 0) break;
            consecutiveDecrease += 1;
        } else {
            break;
        }
    }

    return {
        latest_period: latest.period,
        latest_gold_reserve_wan_oz: latest.gold_reserve_wan_oz,
        latest_gold_reserve_tonnes: Math.round(latest.gold_reserve_wan_oz * 0.31103477 * 10) / 10,
        mom_change_wan_oz: prior ? latest.gold_reserve_wan_oz - prior.gold_reserve_wan_oz : null,
        consecutive_months_increase: consecutiveIncrease,
        consecutive_months_decrease: consecutiveDecrease,
        history_size: points.length
    };
}

/**
 * Returns latest PBOC gold reserve trend with 24h cache. Failures return null so
 * narrative generation can degrade gracefully if Sina is unavailable.
 */
export async function getChinaGoldReserveTrend(): Promise<ChinaGoldReserveTrend | null> {
    if (memoryCache && Date.now() - memoryCache.fetchedAt < CACHE_TTL_MS) {
        return memoryCache.trend;
    }

    try {
        const points = await fetchRawSinaMacroGold();
        if (points.length < 2) {
            return null;
        }
        const trend = computeTrend(points);
        memoryCache = { fetchedAt: Date.now(), trend };
        return trend;
    } catch (error) {
        console.warn('[macro-china-fetcher] failed:', error instanceof Error ? error.message : error);
        return null;
    }
}
