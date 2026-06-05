import assert from 'assert';
import {
    classifyCreditRegimeState,
    evaluateCccLeadsHy,
    evaluateCreditEquityDivergence
} from './side-monitors';
import type { DailyPriceBar } from '../../data/massive-fetcher';
import type { SideSubSignal } from './types';

function series(start: number, step: number, length = 60): number[] {
    return Array.from({ length }, (_, index) => start + index * step);
}

function flatThenMove(start: number, move: number, length = 60): number[] {
    return Array.from({ length }, (_, index) => index < length - 20 ? start : start + ((index - (length - 20) + 1) / 20) * move);
}

function barsFromCloses(closes: number[]): DailyPriceBar[] {
    return closes.map((close, index) => ({
        date: `2026-01-${String((index % 28) + 1).padStart(2, '0')}`,
        close,
        open: close,
        high: close,
        low: close,
        volume: 1_000_000
    }));
}

function signal(score: 0 | 1 | 2 | 3): SideSubSignal {
    return {
        name: 'mock',
        value: null,
        score,
        status: score === 3 ? 'crisis' : score === 2 ? 'stress' : score === 1 ? 'watch' : 'normal',
        notes: []
    };
}

function run(): void {
    const quietCcc = flatThenMove(800, 10);
    const quietHy = flatThenMove(300, 8);
    const quiet = evaluateCccLeadsHy(quietCcc, quietHy);
    assert.strictEqual(quiet.score, 0, 'small CCC lead should remain noise');

    const cccLead = evaluateCccLeadsHy(flatThenMove(820, 125), flatThenMove(310, 25));
    assert.strictEqual(cccLead.score, 2, 'CCC widening materially faster than HY should score stress');
    assert.ok(cccLead.notes.some((note) => note.includes('CCC-HY lead')));

    const cccBreak = evaluateCccLeadsHy(flatThenMove(1_050, 180), flatThenMove(360, 40));
    assert.strictEqual(cccBreak.score, 3, 'large CCC lead plus high absolute level should score crisis-level signal');

    const qqqNearHigh = barsFromCloses(series(100, 1));
    const divergence = evaluateCreditEquityDivergence(qqqNearHigh, flatThenMove(300, 20), flatThenMove(800, 60));
    assert.strictEqual(divergence.score, 2, 'QQQ near highs while CCC widens should score divergence');

    const qqqNotNearHigh = barsFromCloses([...series(100, 1, 50), ...series(140, -2, 10)]);
    const noDivergence = evaluateCreditEquityDivergence(qqqNotNearHigh, flatThenMove(300, 20), flatThenMove(800, 100));
    assert.strictEqual(noDivergence.score, 0, 'credit widening without equity near highs should not score divergence');

    assert.strictEqual(
        classifyCreditRegimeState({
            cccLeadsHy: signal(3),
            creditEquityDivergence: signal(0),
            hyAcceleration: signal(0)
        }),
        'BREAK_FORMING',
        'single severe leading signal must not escalate to BREAK'
    );

    assert.strictEqual(
        classifyCreditRegimeState({
            cccLeadsHy: signal(3),
            creditEquityDivergence: signal(2),
            hyAcceleration: signal(0)
        }),
        'BREAK',
        'BREAK requires corroboration from at least two credit signals'
    );

    assert.strictEqual(
        classifyCreditRegimeState({
            cccLeadsHy: signal(1),
            creditEquityDivergence: signal(1),
            hyAcceleration: signal(0)
        }),
        'BREAK_FORMING',
        'two watch-level signals should form a break watch but not BREAK'
    );

    console.log('credit-break-signals tests passed');
}

if (require.main === module) {
    run();
}
