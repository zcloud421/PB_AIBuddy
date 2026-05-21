import assert from 'assert';
import { buildHyOasTightZone, computeHyOasRawSeverity } from './indicators';
import { countWeekdaysInclusive } from './persistence';
import { evaluateHyOasAcceleration, scoreHyOasAcceleration } from './side-monitors';

function run() {
    assert.strictEqual(
        scoreHyOasAcceleration(-4, -36),
        0,
        'tightening deceleration must not score as HY acceleration'
    );

    assert.strictEqual(
        scoreHyOasAcceleration(40, 10),
        1,
        'true widening acceleration should score watch'
    );

    const measuredNoise = evaluateHyOasAcceleration(1, -33, 286);
    assert.strictEqual(
        measuredNoise.score,
        0,
        'HY OAS 286bp + Δ4w +1bp + Δ8w -33bp must stay score 0 under tight-zone noise floor'
    );
    assert.ok(measuredNoise.notes.some((note) => note.includes('未达噪音门槛 25bp')));
    assert.ok(measuredNoise.notes.some((note) => note.includes('tight zone')));

    assert.strictEqual(
        evaluateHyOasAcceleration(30, -5, 295).score,
        1,
        'tight-zone true widening acceleration should score once Δ4w clears 25bp'
    );

    assert.strictEqual(
        evaluateHyOasAcceleration(20, -10, 400).score,
        1,
        'normal-zone widening acceleration should score once Δ4w clears 15bp'
    );

    const normalNoise = evaluateHyOasAcceleration(10, -20, 400);
    assert.strictEqual(
        normalNoise.score,
        0,
        'normal-zone +10bp widening should stay score 0 under 15bp noise floor'
    );
    assert.ok(normalNoise.notes.some((note) => note.includes('未达噪音门槛 15bp')));
    assert.ok(normalNoise.notes.some((note) => note.includes('正常区')));

    assert.strictEqual(computeHyOasRawSeverity(283), 'Healthy');
    assert.strictEqual(buildHyOasTightZone(283)?.active, true);
    assert.match(buildHyOasTightZone(283)?.historical_anchor ?? '', /2007-06/);

    assert.strictEqual(buildHyOasTightZone(320), undefined);
    assert.strictEqual(computeHyOasRawSeverity(360), 'Neutral');

    assert.strictEqual(
        countWeekdaysInclusive('2026-05-11', '2026-05-22'),
        10,
        '10 trading days should confirm a Monday-to-next-Friday pending upgrade window'
    );

    console.log('hy-oas-framework tests passed');
}

if (require.main === module) {
    run();
}
