import dotenv from 'dotenv';

import { MassiveClient } from '../data/massive-client';
import { MassiveDataFetcher } from '../data/massive-fetcher';
import { deleteTodayIdeaCandidate } from '../db/queries/ideas';
import { approveTenors } from '../scoring-engine';
import * as ideasService from '../services/ideas-service';

dotenv.config();

async function main(): Promise<void> {
    const symbol = 'AMZN';
    const fetcher = new MassiveDataFetcher();
    const client = new MassiveClient();
    const fromDate = isoDateOffsetDays(-365);
    const toDate = todayIsoDate();
    const minExpiry = isoDateOffsetDays(85);
    const maxExpiry = isoDateOffsetDays(181);

    console.log(`\n[smoke] raw fetch connectivity check (${symbol})`);
    const debugUrl = `https://api.massive.com/v3/snapshot/options/${symbol}?contract_type=put&limit=5&apiKey=${process.env.MASSIVE_API_KEY ?? ''}`;
    const debugResponse = await fetch(debugUrl);
    console.log('[debug] status:', debugResponse.status);
    console.log('[debug] body:', await debugResponse.text());

    console.log(`\n[smoke] reference contracts check (${symbol})`);
    const refUrl = `https://api.massive.com/v3/reference/options/contracts?underlying_ticker=${symbol}&contract_type=put&expiration_date.gte=${minExpiry}&expiration_date.lte=${maxExpiry}&limit=10&sort=expiration_date&order=asc&apiKey=${process.env.MASSIVE_API_KEY ?? ''}`;
    const refResponse = await fetch(refUrl);
    console.log('[debug] reference contracts status:', refResponse.status);
    console.log('[debug] reference contracts body:', await refResponse.text());

    console.log(`\n[smoke] snapshot with expiry filter check (${symbol})`);
    const snapUrl = `https://api.massive.com/v3/snapshot/options/${symbol}?contract_type=put&expiration_date.gte=${minExpiry}&limit=10&sort=expiration_date&order=asc&apiKey=${process.env.MASSIVE_API_KEY ?? ''}`;
    const snapResponse = await fetch(snapUrl);
    console.log('[debug] snapshot with expiry filter status:', snapResponse.status);
    const snapBody = await snapResponse.json() as { results?: Array<{ details?: { expiration_date?: string } }> };
    console.log('[debug] snapshot first result expiry:', snapBody?.results?.[0]?.details?.expiration_date ?? null);

    console.log(`\n[smoke] raw prev close check (${symbol})`);
    const prevUrl = `https://api.massive.com/v2/aggs/ticker/${symbol}/prev?adjusted=true&apiKey=${process.env.MASSIVE_API_KEY ?? ''}`;
    const prevResponse = await fetch(prevUrl);
    const prevBodyText = await prevResponse.text();
    let prevClose: unknown = null;
    try {
        const prevJson = JSON.parse(prevBodyText) as { results?: Array<Record<string, unknown>> };
        prevClose = prevJson.results?.[0]?.c ?? null;
    } catch {
        prevClose = null;
    }
    console.log('[debug] prev status:', prevResponse.status);
    console.log('[debug] prev results[0].c:', prevClose);
    console.log('[debug] prev body:', prevBodyText);

    console.log(`\n[smoke] raw aggs summary (${symbol})`);
    const rawAggs = await client.get<{ results?: Array<Record<string, unknown>> }>(
        `/v2/aggs/ticker/${symbol}/range/1/day/${fromDate}/${toDate}`,
        {
            adjusted: true,
            sort: 'asc',
            limit: 365
        }
    );
    const aggsResults = rawAggs.results ?? [];
    const firstAgg = aggsResults[0] ?? null;
    const lastAgg = aggsResults[aggsResults.length - 1] ?? null;
    console.log(
        JSON.stringify(
            {
                from: fromDate,
                to: toDate,
                results_length: aggsResults.length,
                first_result: firstAgg
                    ? {
                          t: (firstAgg as Record<string, unknown>).t ?? null,
                          c: (firstAgg as Record<string, unknown>).c ?? null
                      }
                    : null,
                last_result: lastAgg
                    ? {
                          t: (lastAgg as Record<string, unknown>).t ?? null,
                          c: (lastAgg as Record<string, unknown>).c ?? null
                      }
                    : null
            },
            null,
            2
        )
    );

    console.log(`\n[smoke] fetchSymbolData(${symbol})`);
    const symbolData = await fetcher.fetchSymbolData(symbol);
    console.log(
        JSON.stringify(
            {
                current_price: symbolData.current_price,
                ma20: symbolData.ma20,
                ma50: symbolData.ma50,
                ma200: symbolData.ma200,
                pct_from_52w_high: symbolData.pct_from_52w_high,
                earnings_date: symbolData.earnings_date
            },
            null,
            2
        )
    );

    console.log(`\n[smoke] fetchChainData(${symbol})`);
    const referenceContracts = await client.get<{ results?: Array<Record<string, unknown>> }>(
        `/v3/reference/options/contracts`,
        {
            underlying_ticker: symbol,
            contract_type: 'put',
            'expiration_date.gte': minExpiry,
            'expiration_date.lte': maxExpiry,
            limit: 250,
            sort: 'expiration_date',
            order: 'asc'
        }
    );
    const stage1Tickers = new Set(
        (referenceContracts.results ?? [])
            .map((row) => getString(row, 'ticker'))
            .filter((ticker): ticker is string => ticker !== null)
    );

    const snapshotFirstPage = await client.get<{ results?: Array<Record<string, unknown>> }>(
        `/v3/snapshot/options/${symbol}`,
        {
            contract_type: 'put',
            'expiration_date.gte': minExpiry,
            limit: 250,
            sort: 'expiration_date',
            order: 'asc'
        }
    );
    const stage2Rows = snapshotFirstPage.results ?? [];
    const afterTickerIntersection = stage2Rows.filter((row) => {
        const ticker = getNestedString(row, ['details', 'ticker']);
        return ticker !== null && stage1Tickers.has(ticker);
    });
    const afterOiFilter = afterTickerIntersection.filter((row) => {
        const openInterest = getNumber(row, 'open_interest');
        return openInterest !== null && openInterest >= 10;
    });
    const afterGreeksFilter = afterOiFilter.filter((row) => getObject(row, 'greeks') !== null);
    const afterDeltaFilter = afterGreeksFilter.filter((row) => {
        const delta = getNestedNumber(row, ['greeks', 'delta']);
        return delta !== null && delta >= -0.40 && delta <= -0.10;
    });

    const chainData = await fetcher.fetchChainData(symbol, symbolData.current_price);
    const first = chainData[0] ?? null;
    const tenorWindows = approveTenors(symbolData, chainData);
    const preferredWindow = tenorWindows[0] ?? null;
    const strikeCandidates =
        preferredWindow === null
            ? []
            : preferredWindow.strikes
                  .filter((strike) => strike.delta >= -0.40 && strike.delta <= -0.10)
                  .map((strike) => {
                      const coupon = calculateRefCouponPct(strike.mid_price, strike.strike, preferredWindow.tenor_days);
                      return {
                          strike: strike.strike,
                          delta: Number(strike.delta.toFixed(3)),
                          coupon_pct: coupon !== null ? Number(coupon.toFixed(1)) : null,
                          pass: coupon !== null && coupon >= 10.0
                      };
                  });

    console.log(
        JSON.stringify(
            {
                stage1_reference_tickers: stage1Tickers.size,
                stage2_snapshot_rows_before_filter: stage2Rows.length,
                after_ticker_intersection: afterTickerIntersection.length,
                after_oi_filter: afterOiFilter.length,
                after_greeks_filter: afterGreeksFilter.length,
                after_delta_filter: afterDeltaFilter.length,
                final_contracts: chainData.length,
                first_final_contract: first
                    ? {
                          strike: first.strike,
                          expiry_date: first.expiry_date,
                          iv: first.iv,
                          delta: first.delta,
                          mid_price: first.mid_price,
                          mid_price_source: first.mid_price_source ?? 'none',
                          volume: first.volume
                      }
                    : null
            },
            null,
            2
        )
    );
    console.log('[debug] stage1 reference tickers:', stage1Tickers.size);
    console.log('[debug] stage2 snapshot rows (before filter):', stage2Rows.length);
    console.log('[debug] after ticker intersection:', afterTickerIntersection.length);
    console.log('[debug] after oi filter:', afterOiFilter.length);
    console.log('[debug] after greeks filter:', afterGreeksFilter.length);
    console.log('[debug] after delta filter:', afterDeltaFilter.length);
    console.log('[debug] strike candidates:', strikeCandidates);
    console.log('[debug] final contracts:', chainData.length);
    console.log(
        '[debug] first final contract:',
        first
            ? {
                  strike: first.strike,
                  expiry: first.expiry_date,
                  delta: first.delta,
                  iv: first.iv,
                  mid_price: first.mid_price,
                  mid_price_source: first.mid_price_source ?? 'none',
                  volume: first.volume
              }
            : null
    );

    console.log(`\n[smoke] raw options snapshot first result (${symbol})`);
    const rawOptions = await client.get<{ results?: Array<Record<string, unknown>> }>(
        `/v3/snapshot/options/${symbol}`,
        {
            contract_type: 'put',
            'expiration_date.gte': minExpiry,
            limit: 250,
            sort: 'expiration_date',
            order: 'asc'
        }
    );
    console.log(JSON.stringify(rawOptions.results?.[0] ?? null, null, 2));

    console.log(`\n[smoke] scoreAndGrade result (${symbol})`);
    await deleteTodayIdeaCandidate(symbol);
    const result = await ideasService.getSymbolIdea(symbol);
    console.log('[smoke] scoreAndGrade result:', JSON.stringify(result, null, 2));
}

main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error('\n[smoke] error');
    console.error(message);
    process.exitCode = 1;
});

function daysUntilExpiry(expiryDate: string): number {
    const today = new Date();
    const expiry = new Date(expiryDate);
    return Math.ceil((expiry.getTime() - today.getTime()) / (24 * 60 * 60 * 1000));
}

function todayIsoDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function isoDateOffsetDays(offsetDays: number): string {
    const date = new Date();
    date.setUTCDate(date.getUTCDate() + offsetDays);
    return date.toISOString().slice(0, 10);
}

function getObject(row: Record<string, unknown>, key: string): Record<string, unknown> | null {
    const value = row[key];
    return value && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null;
}

function getString(row: Record<string, unknown>, key: string): string | null {
    const value = row[key];
    return typeof value === 'string' ? value : null;
}

function getNumber(row: Record<string, unknown>, key: string): number | null {
    const value = row[key];
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getNestedString(row: Record<string, unknown>, path: string[]): string | null {
    const value = getNestedValue(row, path);
    return typeof value === 'string' ? value : null;
}

function calculateRefCouponPct(midPrice: number | null, strike: number, tenorDays: number): number | null {
    if (midPrice === null || tenorDays <= 0) {
        return null;
    }

    return (midPrice / strike) * (365 / tenorDays) * 100;
}

function getNestedNumber(row: Record<string, unknown>, path: string[]): number | null {
    const value = getNestedValue(row, path);
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function getNestedValue(row: Record<string, unknown>, path: string[]): unknown {
    let current: unknown = row;

    for (const key of path) {
        if (!current || typeof current !== 'object' || !(key in current)) {
            return null;
        }
        current = (current as Record<string, unknown>)[key];
    }

    return current;
}
