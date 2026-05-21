import assert from 'assert';
import { buildHyOasTightZone, computeHyOasRawSeverity } from './indicators';
import { countWeekdaysInclusive } from './persistence';
import { scoreHyOasAcceleration } from './side-monitors';

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
