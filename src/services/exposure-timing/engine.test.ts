import assert from 'node:assert/strict';

import type { DailyPriceBar } from '../../data/massive-fetcher';
import { buildExposureTimingSnapshot, computeAtr, computeExposureTiming } from './engine';

function bars(options: {
    count?: number;
    start?: number;
    dailyChange?: number;
    lastChanges?: number[];
    volume?: number;
} = {}): DailyPriceBar[] {
    const count = options.count ?? 240;
    const closes: number[] = [];
    let close = options.start ?? 100;
    for (let i = 0; i < count; i += 1) {
        close += options.dailyChange ?? 0.08;
        closes.push(close);
    }
    for (const change of options.lastChanges ?? []) {
        close += change;
        closes.push(close);
    }
    return closes.map((value, index) => ({
        date: new Date(Date.UTC(2025, 0, 1 + index)).toISOString().slice(0, 10),
        open: value - 0.2,
        high: value + 0.5,
        low: value - 0.5,
        close: value,
        volume: options.volume ?? 1_000_000
    }));
}

function supportRetestBars(lastVolume = 1_000_000, finalClose?: number): DailyPriceBar[] {
    const closes: number[] = [];
    for (let index = 0; index < 180; index += 1) {
        closes.push(80 + (index * 0.12) + (Math.sin(index * 0.45) * 0.5));
    }
    for (let index = 0; index < 54; index += 1) {
        closes.push(104 + (Math.sin(index * Math.PI / 10) * 3));
    }
    closes.push(101.0, 101.2, 101.4, 101.2, 101.3, finalClose ?? 101.6);
    return closes.map((close, index) => ({
        date: new Date(Date.UTC(2025, 0, 1 + index)).toISOString().slice(0, 10),
        open: close - 0.2,
        high: close + 0.6,
        low: close - 0.6,
        close,
        volume: index === closes.length - 1 ? lastVolume : 1_000_000
    }));
}

function lowerLowRetestBars(): DailyPriceBar[] {
    const result = supportRetestBars();
    const closes = [102, 101, 99, 100, 101, 101.4, 101.7, 102];
    for (let offset = 0; offset < closes.length; offset += 1) {
        const index = result.length - closes.length + offset;
        const close = closes[offset];
        result[index] = {
            ...result[index],
            open: close - 0.2,
            high: close + 0.6,
            low: close - 0.6,
            close
        };
    }
    return result;
}

const atrWithGap = computeAtr([
    { date: '2026-01-01', open: 100, high: 101, low: 99, close: 100, volume: 1 },
    { date: '2026-01-02', open: 105, high: 106, low: 104, close: 105, volume: 1 }
], 1);
assert.equal(atrWithGap, 6);

const stableTrend = computeExposureTiming('SPY', '美国大盘', supportRetestBars(), 'STABLE');
assert.equal(stableTrend.status, 'BUILD_WINDOW');
assert.equal(stableTrend.support?.kind, 'swing_low');
assert.ok((stableTrend.support?.prior_touch_count ?? 0) >= 2);
assert.match(stableTrend.next_trigger, /前低|MA/);

const weakVolumeRetest = computeExposureTiming('SPY', '美国大盘', supportRetestBars(400_000), 'STABLE');
assert.equal(weakVolumeRetest.status, 'WATCH_SUPPORT');
assert.match(weakVolumeRetest.summary, /量能偏弱/);

const highVolumeBreak = computeExposureTiming('SPY', '美国大盘', supportRetestBars(2_000_000, 98.5), 'STABLE');
assert.equal(highVolumeBreak.status, 'WAIT');
assert.match(highVolumeBreak.summary, /放量跌破|下跌/);

const lowerLowRetest = computeExposureTiming('SOXX', '半导体', lowerLowRetestBars(), 'STABLE');
assert.equal(lowerLowRetest.price_structure, 'LOWER_LOW');
assert.notEqual(lowerLowRetest.status, 'BUILD_WINDOW');

const extended = computeExposureTiming('QQQ', '美国成长', bars({ dailyChange: 0.04, lastChanges: [3, 3, 3, 3] }), 'STABLE');
assert.equal(extended.status, 'EXTENDED');

const fallingKnife = computeExposureTiming(
    'KWEB',
    '中国互联网',
    bars({ dailyChange: 0.02, lastChanges: [-3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3] }),
    'STABLE'
);
assert.equal(fallingKnife.status, 'WAIT');
assert.match(fallingKnife.summary, /下行|下跌/);
assert.notEqual(fallingKnife.support?.kind === 'moving_average' && (fallingKnife.support.slope_20d_pct ?? -1) < 0, true);

const macroBlocked = computeExposureTiming('GLD', '黄金', bars({ dailyChange: 0.05 }), 'CONFIRMED_BREAK');
assert.equal(macroBlocked.status, 'WAIT');
assert.match(macroBlocked.summary, /系统性调整/);

// 软闸门:FORMING 期间技术确认仍给 BUILD_WINDOW,但文案带置信度降档。
const formingMacro = computeExposureTiming('SOXX', '半导体', supportRetestBars(), 'BREAK_FORMING');
assert.equal(formingMacro.status, 'BUILD_WINDOW');
assert.match(formingMacro.summary, /置信度降档/);

const limited = computeExposureTiming('DRAM', '存储芯片', bars({ count: 40 }), 'STABLE');
assert.equal(limited.status, 'WAIT');
assert.match(limited.summary, /数据不足/);

const snapshot = buildExposureTimingSnapshot('2026-07-10', 'STABLE', {
    SPY: bars(),
    QQQ: bars(),
    SOXX: bars(),
    DRAM: bars(),
    XLV: bars(),
    XLF: bars(),
    XLE: bars(),
    MCHI: bars(),
    GLD: bars(),
    GDX: bars()
});
assert.deepEqual(snapshot.assets.map((asset) => asset.symbol), [
    'SPY',
    'QQQ',
    'SOXX',
    'DRAM',
    'XLV',
    'XLF',
    'XLE',
    'MCHI',
    'GLD',
    'GDX'
]);
assert.equal(snapshot.macro_state, 'STABLE');

console.log('exposure-timing engine tests passed');
