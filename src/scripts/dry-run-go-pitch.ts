/**
 * Dry-run GO pitch hybrid (D+A) against real DeepSeek for 5 tickers.
 *
 * Usage:
 *   DEEPSEEK_API_KEY=xxx npx ts-node src/scripts/dry-run-go-pitch.ts
 *
 * Prints per-ticker: pass/fail, source_quality, final paragraph.
 * No DB writes. No deploy side-effects.
 */

import { generateGoPitch } from '../utils/fcn-go-pitch';
import type { NarrativeInput } from '../utils/narrative-generator';

interface TickerFixture {
    symbol: string;
    company_name: string;
    current_price: number;
    recommended_strike: number;
    coupon_range: string;
    tenor_days: number;
    change_5d_pct: number;
    pct_from_52w_high: number;
    days_since_earnings: number;
    ma50: number;
    ma200: number;
    composite_score: number;
    news_headlines: string[];
}

const FIXTURES: TickerFixture[] = [
    {
        symbol: 'LITE',
        company_name: 'Lumentum Holdings',
        current_price: 100,
        recommended_strike: 85,
        coupon_range: '12-16%',
        tenor_days: 90,
        change_5d_pct: -1.8,
        pct_from_52w_high: -12.8,
        days_since_earnings: 25,
        ma50: 98,
        ma200: 92,
        composite_score: 78,
        news_headlines: [
            'Lumentum Sees Strong Datacom Demand From AI Buildout',
            'Lumentum Q2 Revenue Beats Estimates on AI Optics'
        ]
    },
    {
        symbol: 'TSM',
        company_name: 'Taiwan Semiconductor',
        current_price: 200,
        recommended_strike: 170,
        coupon_range: '10-14%',
        tenor_days: 90,
        change_5d_pct: -2.3,
        pct_from_52w_high: -8.5,
        days_since_earnings: 18,
        ma50: 198,
        ma200: 180,
        composite_score: 82,
        news_headlines: [
            'TSMC Reports Strong N3 Demand From AI Customers',
            'TSMC Capex Guidance Reaffirmed for 2026'
        ]
    },
    {
        symbol: 'VRT',
        company_name: 'Vertiv Holdings',
        current_price: 120,
        recommended_strike: 100,
        coupon_range: '14-18%',
        tenor_days: 90,
        change_5d_pct: -3.2,
        pct_from_52w_high: -15.0,
        days_since_earnings: 22,
        ma50: 118,
        ma200: 105,
        composite_score: 76,
        news_headlines: [
            'Vertiv Backlog Hits Record on Datacenter Power Demand',
            'Vertiv Q3 Orders Up 30% Year Over Year'
        ]
    },
    {
        symbol: 'NVDA',
        company_name: 'NVIDIA Corporation',
        current_price: 180,
        recommended_strike: 155,
        coupon_range: '12-15%',
        tenor_days: 90,
        change_5d_pct: 3.4,
        pct_from_52w_high: -5.2,
        days_since_earnings: 15,
        ma50: 175,
        ma200: 160,
        composite_score: 85,
        news_headlines: [
            'NVIDIA Q3 Datacenter Revenue Beats Estimates',
            'NVIDIA Guides Q4 Above Consensus on Blackwell Ramp'
        ]
    },
    {
        symbol: 'AVGO',
        company_name: 'Broadcom',
        current_price: 190,
        recommended_strike: 160,
        coupon_range: '12-16%',
        tenor_days: 90,
        change_5d_pct: 3.2,
        pct_from_52w_high: -6.5,
        days_since_earnings: 12,
        ma50: 186,
        ma200: 170,
        composite_score: 80,
        news_headlines: [
            'Broadcom Custom ASIC Wins Drive Q4 Beat',
            'Broadcom Guides 2026 AI Revenue Above Street'
        ]
    }
];

function toNarrativeInput(f: TickerFixture): NarrativeInput {
    return {
        symbol: f.symbol,
        company_name: f.company_name,
        theme: 'AI',
        grade: 'GO',
        composite_score: f.composite_score,
        recommended_strike: f.recommended_strike,
        estimated_coupon_range: f.coupon_range,
        current_price: f.current_price,
        change_5d_pct: f.change_5d_pct,
        pct_from_52w_high: f.pct_from_52w_high,
        ma20: f.ma50,
        ma50: f.ma50,
        ma200: f.ma200,
        iv_level: 'high',
        flags: [],
        tenor_days: f.tenor_days,
        news_headlines: f.news_headlines,
        news_items: f.news_headlines.map((t) => ({ title: t, url: '', published_at: '' })),
        days_since_earnings: f.days_since_earnings,
        has_recent_earnings: true
    } as NarrativeInput;
}

async function main() {
    if (!process.env.DEEPSEEK_API_KEY) {
        console.error('DEEPSEEK_API_KEY missing.');
        process.exit(1);
    }

    let passed = 0;
    for (const fixture of FIXTURES) {
        const input = toNarrativeInput(fixture);
        process.stdout.write(`\n=== ${fixture.symbol} ===\n`);
        try {
            const out = await generateGoPitch(input);
            if (!out) {
                console.log('result: null (pre-flight rejected)');
                continue;
            }
            const isHybrid = out.source_quality === 'go_pitch_hybrid_validated';
            if (isHybrid) passed += 1;
            console.log(`source_quality: ${out.source_quality}`);
            console.log(`length: ${out.why_now.length}`);
            console.log(`paragraph: ${out.why_now}`);
        } catch (err) {
            console.error('error:', err);
        }
    }

    console.log(`\n=== SUMMARY ===`);
    console.log(`hybrid_validated pass: ${passed}/${FIXTURES.length}`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
