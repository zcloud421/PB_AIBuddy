import assert from 'node:assert/strict';

import type { DailyPriceBar } from '../../data/massive-fetcher';
import { buildExposureTimingSnapshot, computeExposureTiming } from './engine';

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

const stableTrend = computeExposureTiming('SPY', '美国大盘', bars({ dailyChange: 0.05 }), 'STABLE');
assert.equal(stableTrend.status, 'BUILD_WINDOW');
assert.match(stableTrend.next_trigger, /MA20|MA50/);

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

const macroBlocked = computeExposureTiming('GLD', '黄金', bars({ dailyChange: 0.05 }), 'CONFIRMED_BREAK');
assert.equal(macroBlocked.status, 'WAIT');
assert.match(macroBlocked.summary, /系统性调整/);

// 软闸门:FORMING 期间技术确认仍给 BUILD_WINDOW,但文案带置信度降档。
const formingMacro = computeExposureTiming('SOXX', '半导体', bars({ dailyChange: 0.05 }), 'BREAK_FORMING');
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
    KWEB: bars(),
    GLD: bars()
});
assert.deepEqual(snapshot.assets.map((asset) => asset.symbol), ['SPY', 'QQQ', 'SOXX', 'DRAM', 'KWEB', 'GLD']);
assert.equal(snapshot.macro_state, 'STABLE');

console.log('exposure-timing engine tests passed');
