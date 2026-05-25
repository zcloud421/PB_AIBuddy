/**
 * Dry-run CAUTION / AVOID concern pitch engine.
 *
 * Usage:
 *   DEEPSEEK_API_KEY=xxx npx ts-node src/scripts/dry-run-concern-pitch.ts
 *
 * CAUTION fixtures use real DeepSeek when DEEPSEEK_API_KEY is present and
 * fallback to deterministic templates otherwise. AVOID is deterministic.
 * No DB writes. No deploy side-effects.
 */

import { generateConcernPitch } from '../utils/fcn-concern-pitch';
import type { NarrativeInput } from '../utils/narrative-generator';

type FixtureKind = 'CAUTION_EARNINGS' | 'CAUTION_TREND' | 'AVOID_EARNINGS' | 'AVOID_TECHNICAL';

interface ConcernFixture {
    kind: FixtureKind;
    symbol: string;
    company_name: string;
    grade: 'CAUTION' | 'AVOID';
    current_price: number;
    recommended_strike: number;
    coupon_range: string;
    tenor_days: number;
    change_5d_pct: number;
    change_ytd_pct: number;
    pct_from_52w_high: number;
    ma20: number;
    ma50: number;
    ma200: number;
    iv_level: string;
    composite_score: number;
    days_to_earnings?: number | null;
    days_since_earnings?: number | null;
    has_recent_earnings?: boolean;
    news_headlines?: string[];
    flags?: Array<{ type: string; severity: 'INFO' | 'WARN' | 'BLOCK'; message: string }>;
}

const FIXTURES: ConcernFixture[] = [
    {
        kind: 'CAUTION_EARNINGS',
        symbol: 'ADBE',
        company_name: 'Adobe',
        grade: 'CAUTION',
        current_price: 430,
        recommended_strike: 360,
        coupon_range: '9-12%',
        tenor_days: 90,
        change_5d_pct: -1.4,
        change_ytd_pct: -3.5,
        pct_from_52w_high: -18,
        ma20: 428,
        ma50: 440,
        ma200: 455,
        iv_level: 'high',
        composite_score: 0.46,
        days_to_earnings: 4,
        news_headlines: ['Adobe Earnings Due Next Week as Investors Watch AI Monetization']
    },
    {
        kind: 'CAUTION_EARNINGS',
        symbol: 'ORCL',
        company_name: 'Oracle',
        grade: 'CAUTION',
        current_price: 152,
        recommended_strike: 130,
        coupon_range: '10-13%',
        tenor_days: 90,
        change_5d_pct: 0.8,
        change_ytd_pct: 6,
        pct_from_52w_high: -7,
        ma20: 150,
        ma50: 148,
        ma200: 132,
        iv_level: 'high',
        composite_score: 0.47,
        days_to_earnings: 6,
        news_headlines: ['Oracle Earnings Preview Focuses on Cloud Backlog']
    },
    {
        kind: 'CAUTION_EARNINGS',
        symbol: 'CRM',
        company_name: 'Salesforce',
        grade: 'CAUTION',
        current_price: 270,
        recommended_strike: 225,
        coupon_range: '10-14%',
        tenor_days: 90,
        change_5d_pct: -0.9,
        change_ytd_pct: -1,
        pct_from_52w_high: -14,
        ma20: 272,
        ma50: 278,
        ma200: 265,
        iv_level: 'high',
        composite_score: 0.44,
        days_to_earnings: 7,
        news_headlines: ['Salesforce Earnings Ahead With Margin Outlook in Focus']
    },
    {
        kind: 'CAUTION_TREND',
        symbol: 'TSLA',
        company_name: 'Tesla',
        grade: 'CAUTION',
        current_price: 190,
        recommended_strike: 155,
        coupon_range: '15-20%',
        tenor_days: 90,
        change_5d_pct: -4.8,
        change_ytd_pct: -6,
        pct_from_52w_high: -22,
        ma20: 198,
        ma50: 205,
        ma200: 185,
        iv_level: 'high',
        composite_score: 0.45,
        days_since_earnings: 35,
        news_headlines: ['Tesla Shares Slip as Delivery Debate Continues']
    },
    {
        kind: 'CAUTION_TREND',
        symbol: 'SNOW',
        company_name: 'Snowflake',
        grade: 'CAUTION',
        current_price: 145,
        recommended_strike: 120,
        coupon_range: '13-17%',
        tenor_days: 90,
        change_5d_pct: -3.6,
        change_ytd_pct: -5,
        pct_from_52w_high: -17,
        ma20: 150,
        ma50: 155,
        ma200: 142,
        iv_level: 'high',
        composite_score: 0.46,
        days_since_earnings: 42,
        news_headlines: ['Snowflake Pullback Extends After Software Rotation']
    },
    {
        kind: 'CAUTION_TREND',
        symbol: 'PYPL',
        company_name: 'PayPal',
        grade: 'CAUTION',
        current_price: 68,
        recommended_strike: 55,
        coupon_range: '11-15%',
        tenor_days: 90,
        change_5d_pct: -3.4,
        change_ytd_pct: -8,
        pct_from_52w_high: -19,
        ma20: 70,
        ma50: 72,
        ma200: 75,
        iv_level: 'normal',
        composite_score: 0.43,
        days_since_earnings: 50,
        news_headlines: ['PayPal Underperforms as Fintech Sentiment Stays Soft']
    },
    {
        kind: 'AVOID_EARNINGS',
        symbol: 'TGT',
        company_name: 'Target',
        grade: 'AVOID',
        current_price: 95,
        recommended_strike: 78,
        coupon_range: '14-18%',
        tenor_days: 90,
        change_5d_pct: -7.4,
        change_ytd_pct: -16,
        pct_from_52w_high: -31,
        ma20: 104,
        ma50: 110,
        ma200: 118,
        iv_level: 'high',
        composite_score: 0.31,
        days_since_earnings: 3,
        has_recent_earnings: true,
        news_headlines: ['Target Cuts Guidance After Quarterly Earnings Miss Estimates']
    },
    {
        kind: 'AVOID_EARNINGS',
        symbol: 'DELL',
        company_name: 'Dell Technologies',
        grade: 'AVOID',
        current_price: 98,
        recommended_strike: 80,
        coupon_range: '13-17%',
        tenor_days: 90,
        change_5d_pct: -6.2,
        change_ytd_pct: -4,
        pct_from_52w_high: -24,
        ma20: 104,
        ma50: 108,
        ma200: 94,
        iv_level: 'high',
        composite_score: 0.34,
        days_since_earnings: 4,
        has_recent_earnings: true,
        news_headlines: ['Dell Shares Fall After Earnings Miss and Guidance Cut']
    },
    {
        kind: 'AVOID_TECHNICAL',
        symbol: 'BABA',
        company_name: 'Alibaba',
        grade: 'AVOID',
        current_price: 72,
        recommended_strike: 58,
        coupon_range: '14-19%',
        tenor_days: 90,
        change_5d_pct: -4.6,
        change_ytd_pct: -12,
        pct_from_52w_high: -28,
        ma20: 76,
        ma50: 81,
        ma200: 88,
        iv_level: 'high',
        composite_score: 0.33,
        news_headlines: ['Alibaba Faces Antitrust Probe as Shares Break Below Key Averages']
    },
    {
        kind: 'AVOID_TECHNICAL',
        symbol: 'INTC',
        company_name: 'Intel',
        grade: 'AVOID',
        current_price: 24,
        recommended_strike: 19,
        coupon_range: '16-22%',
        tenor_days: 90,
        change_5d_pct: -5.2,
        change_ytd_pct: -20,
        pct_from_52w_high: -36,
        ma20: 26,
        ma50: 29,
        ma200: 32,
        iv_level: 'high',
        composite_score: 0.29,
        news_headlines: ['Intel Lawsuit Adds to Pressure as Stock Breaks Down']
    },
    {
        kind: 'CAUTION_TREND',
        symbol: 'SHOP',
        company_name: 'Shopify',
        grade: 'CAUTION',
        current_price: 88,
        recommended_strike: 72,
        coupon_range: '12-16%',
        tenor_days: 90,
        change_5d_pct: -3.8,
        change_ytd_pct: -2,
        pct_from_52w_high: -13,
        ma20: 90,
        ma50: 91,
        ma200: 80,
        iv_level: 'low',
        composite_score: 0.45,
        news_headlines: ['Shopify Drifts Lower as Software Multiples Compress']
    },
    {
        kind: 'CAUTION_EARNINGS',
        symbol: 'AMD',
        company_name: 'AMD',
        grade: 'CAUTION',
        current_price: 145,
        recommended_strike: 120,
        coupon_range: '12-16%',
        tenor_days: 90,
        change_5d_pct: 1.2,
        change_ytd_pct: 4,
        pct_from_52w_high: -9,
        ma20: 144,
        ma50: 140,
        ma200: 130,
        iv_level: 'high',
        composite_score: 0.47,
        days_to_earnings: 5,
        news_headlines: ['AMD Earnings Preview Focuses on MI300 Demand']
    }
];

function toNarrativeInput(f: ConcernFixture): NarrativeInput {
    return {
        symbol: f.symbol,
        company_name: f.company_name,
        theme: 'FCN',
        grade: f.grade,
        composite_score: f.composite_score,
        recommended_strike: f.recommended_strike,
        estimated_coupon_range: f.coupon_range,
        current_price: f.current_price,
        change_5d_pct: f.change_5d_pct,
        change_ytd_pct: f.change_ytd_pct,
        pct_from_52w_high: f.pct_from_52w_high,
        ma20: f.ma20,
        ma50: f.ma50,
        ma200: f.ma200,
        iv_level: f.iv_level,
        flags: f.flags ?? [],
        tenor_days: f.tenor_days,
        news_headlines: f.news_headlines ?? [],
        news_items: (f.news_headlines ?? []).map((title) => ({ title, source: 'fixture', url: '', published_at: '' })),
        has_recent_earnings: f.has_recent_earnings ?? false,
        days_to_earnings: f.days_to_earnings ?? null,
        days_since_earnings: f.days_since_earnings ?? null
    } as NarrativeInput;
}

async function main(): Promise<void> {
    if (!process.env.DEEPSEEK_API_KEY) {
        console.log('[dry-run-concern] DEEPSEEK_API_KEY missing; CAUTION fixtures will use deterministic templates.');
    }

    const counts = new Map<string, number>();
    for (const fixture of FIXTURES) {
        const out = await generateConcernPitch(toNarrativeInput(fixture));
        console.log(`\n=== ${fixture.symbol} (${fixture.kind}) ===`);
        if (!out) {
            console.log('source_quality: null');
            console.log('paragraph: <not generated>');
            continue;
        }
        counts.set(out.source_quality ?? 'unknown', (counts.get(out.source_quality ?? 'unknown') ?? 0) + 1);
        console.log(`source_quality: ${out.source_quality}`);
        console.log(`length: ${out.why_now.length}`);
        console.log(`paragraph: ${out.why_now}`);
    }

    console.log('\n=== SUMMARY ===');
    for (const [sourceQuality, count] of [...counts.entries()].sort()) {
        console.log(`${sourceQuality}: ${count}`);
    }
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
