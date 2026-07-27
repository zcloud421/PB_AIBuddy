import assert from 'node:assert/strict';

import type { DailyPriceBar } from '../../data/massive-fetcher';
import {
    buildExposureTimingSnapshot,
    computeAtr,
    computeExposureTiming,
    computeExposureTimingHealth,
    computeRelativeStrength5d,
    resolveExposureTimingStatus
} from './engine';
import type { ExposureTimingAsset, ExposureTimingSnapshot, ExposureTimingStatus } from './engine';

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

function withDateAndStatus(
    asset: ExposureTimingAsset,
    date: string,
    status: ExposureTimingStatus
): ExposureTimingAsset {
    return {
        ...asset,
        status,
        raw_status: status,
        status_label: status,
        data_as_of: date
    };
}

function singleAssetSnapshot(asOf: string, asset: ExposureTimingAsset): ExposureTimingSnapshot {
    return { as_of: asOf, macro_state: 'STABLE', assets: [asset] };
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

// P0: raw BUILD requires an actual short-term reversal, not merely proximity to support.
const noReversalBars = supportRetestBars();
const lastNoReversal = noReversalBars.length - 1;
noReversalBars[lastNoReversal] = {
    ...noReversalBars[lastNoReversal],
    open: 101.3,
    high: 101.7,
    low: 100.9,
    close: 101.3
};
const noReversal = computeExposureTiming('SPY', '美国大盘', noReversalBars, 'STABLE');
assert.notEqual(noReversal.status, 'BUILD_WINDOW');
assert.ok(noReversal.evidence.some((item) => item.label === '短线反转' && item.value === '尚待确认'));

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

// P0: a single green trading session remains WATCH; the second confirms BUILD.
const firstRawGreen = withDateAndStatus(stableTrend, '2026-07-10', 'BUILD_WINDOW');
const firstDisplay = resolveExposureTimingStatus(firstRawGreen, null);
assert.equal(firstDisplay.status, 'WATCH_SUPPORT');
assert.equal(firstDisplay.confirmation_days, 1);
assert.match(firstDisplay.summary, /单日反弹/);

const secondRawGreen = withDateAndStatus(stableTrend, '2026-07-13', 'BUILD_WINDOW');
const secondDisplay = resolveExposureTimingStatus(secondRawGreen, firstDisplay);
assert.equal(secondDisplay.status, 'BUILD_WINDOW');
assert.equal(secondDisplay.confirmation_days, 2);

// Re-running on the same market close cannot manufacture an extra confirmation day.
const sameSessionDisplay = resolveExposureTimingStatus(firstRawGreen, firstDisplay);
assert.equal(sameSessionDisplay.status, 'WATCH_SUPPORT');
assert.equal(sameSessionDisplay.confirmation_days, 1);

// One soft reversal is buffered; two consecutive weak sessions demote the display.
const firstRawWatch = withDateAndStatus(stableTrend, '2026-07-14', 'WATCH_SUPPORT');
const bufferedDisplay = resolveExposureTimingStatus(firstRawWatch, secondDisplay);
assert.equal(bufferedDisplay.status, 'BUILD_WINDOW');
assert.equal(bufferedDisplay.deterioration_days, 1);
const secondRawWatch = withDateAndStatus(stableTrend, '2026-07-15', 'WATCH_SUPPORT');
const demotedDisplay = resolveExposureTimingStatus(secondRawWatch, bufferedDisplay);
assert.equal(demotedDisplay.status, 'WATCH_SUPPORT');
assert.equal(demotedDisplay.deterioration_days, 2);

// Hard deterioration bypasses hysteresis immediately.
const rawWait = withDateAndStatus(stableTrend, '2026-07-14', 'WAIT');
assert.equal(resolveExposureTimingStatus(rawWait, secondDisplay).status, 'WAIT');

// A materially different support reference resets the two-session confirmation clock.
const shiftedSupport = {
    ...secondRawGreen,
    data_as_of: '2026-07-14',
    support: secondRawGreen.support && secondRawGreen.atr_20
        ? { ...secondRawGreen.support, level: secondRawGreen.support.level + secondRawGreen.atr_20 }
        : secondRawGreen.support
};
const resetDisplay = resolveExposureTimingStatus(shiftedSupport, secondDisplay);
assert.equal(resetDisplay.status, 'WATCH_SUPPORT');
assert.equal(resetDisplay.confirmation_days, 1);

// P2: 5-day relative strength is an explanatory fact, not a status gate.
const relativeAssetBars = bars({ dailyChange: 0, lastChanges: [1, 1, 1, 1, 1, 1] });
const relativeBenchmarkBars = bars({ dailyChange: 0, lastChanges: [0.2, 0.2, 0.2, 0.2, 0.2, 0.2] });
const relativeStrength = computeRelativeStrength5d(relativeAssetBars, relativeBenchmarkBars, 'SPY');
assert.ok(relativeStrength !== null && relativeStrength.change_5d_pct > 3);
const relativeTiming = computeExposureTiming('QQQ', '科技成长', supportRetestBars(), 'STABLE', relativeStrength);
assert.ok(relativeTiming.evidence.some((item) => item.label === '相对SPY 5日'));

// P1: confirmed green transitions are evaluated after five distinct trading sessions.
const healthBase = {
    ...secondDisplay,
    current_price: 101.6,
    atr_20: 1,
    support: secondDisplay.support
        ? { ...secondDisplay.support, level: 101 }
        : null
};
const healthSnapshots = [
    singleAssetSnapshot('2026-07-09', withDateAndStatus(healthBase, '2026-07-09', 'WATCH_SUPPORT')),
    singleAssetSnapshot('2026-07-10', withDateAndStatus(healthBase, '2026-07-10', 'BUILD_WINDOW')),
    singleAssetSnapshot('2026-07-13', withDateAndStatus(healthBase, '2026-07-13', 'BUILD_WINDOW')),
    singleAssetSnapshot('2026-07-14', withDateAndStatus(healthBase, '2026-07-14', 'WATCH_SUPPORT')),
    // Duplicate data date emulates a weekend/app rerun and must not count as a session.
    singleAssetSnapshot('2026-07-15', withDateAndStatus(healthBase, '2026-07-14', 'WATCH_SUPPORT')),
    singleAssetSnapshot('2026-07-16', {
        ...withDateAndStatus(healthBase, '2026-07-16', 'WAIT'),
        current_price: 100.5
    }),
    singleAssetSnapshot('2026-07-17', withDateAndStatus(healthBase, '2026-07-17', 'WAIT')),
    singleAssetSnapshot('2026-07-20', withDateAndStatus(healthBase, '2026-07-20', 'WATCH_SUPPORT'))
];
const health = computeExposureTimingHealth(healthSnapshots, 5);
assert.equal(health.matured_confirmations, 1);
assert.equal(health.reverted_to_watch, 1);
assert.equal(health.support_failures, 1);
assert.equal(health.reversion_rate_pct, 100);
assert.equal(health.support_failure_rate_pct, 100);

console.log('exposure-timing engine tests passed');
