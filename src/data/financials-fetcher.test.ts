import assert from 'node:assert/strict';
import {
    buildFinancialsFromFmp,
    extractTopSegmentGrowth,
    isCacheFresh,
    type IncomeStatementRow
} from './financials-fetcher';

const incomeRows: IncomeStatementRow[] = [
    {
        date: '2026-03-31',
        calendarYear: 2026,
        period: 'Q1',
        revenue: 10700,
        grossProfit: 4280,
        operatingIncome: 1605
    },
    {
        date: '2025-12-31',
        calendarYear: 2025,
        period: 'Q4',
        revenue: 10000,
        grossProfit: 3500,
        operatingIncome: 1200
    },
    {
        date: '2025-03-31',
        calendarYear: 2025,
        period: 'Q1',
        revenue: 10000,
        grossProfit: 3700,
        operatingIncome: 1300
    }
];

const segmentationRows = [
    {
        date: '2026-03-31',
        calendarYear: 2026,
        period: 'Q1',
        Client: 6000,
        'Data Center and AI': 3000,
        Foundry: 1700
    },
    {
        date: '2025-03-31',
        calendarYear: 2025,
        period: 'Q1',
        Client: 5200,
        'Data Center and AI': 2400,
        Foundry: 1600
    }
];

const financials = buildFinancialsFromFmp('INTC', incomeRows, segmentationRows);
assert.ok(financials);
assert.equal(financials?.latest_quarter, 'Q1 2026');
assert.equal(financials?.revenue_yoy_pct, 7);
assert.equal(financials?.gross_margin_pct, 40);
assert.equal(financials?.gross_margin_yoy_pp, 3);
assert.equal(financials?.operating_margin_pct, 15);
assert.deepEqual(financials?.top_segment, { name: 'Client', yoy_pct: 15.4 });

const nestedSegment = extractTopSegmentGrowth([
    {
        date: '2026-06-30',
        calendarYear: 2026,
        period: 'Q2',
        revenueProductSegmentation: {
            Cloud: 120,
            Devices: 80
        }
    },
    {
        date: '2025-06-30',
        calendarYear: 2025,
        period: 'Q2',
        revenueProductSegmentation: {
            Cloud: 100,
            Devices: 100
        }
    }
]);
assert.deepEqual(nestedSegment, { name: 'Cloud', yoy_pct: 20 });

assert.equal(isCacheFresh(new Date('2026-06-20T00:00:00Z'), new Date('2026-06-26T00:00:00Z')), true);
assert.equal(isCacheFresh(new Date('2026-06-18T00:00:00Z'), new Date('2026-06-26T00:00:00Z')), false);

assert.equal(buildFinancialsFromFmp('BAD', [{ date: '2026-03-31', period: 'Q1', calendarYear: 2026 }], []), null);

console.log('financials-fetcher tests passed');
