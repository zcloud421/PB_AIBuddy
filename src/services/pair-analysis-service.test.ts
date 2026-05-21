import assert from 'assert';

import {
    buildSuitabilityNote,
    determineSuitability
} from './pair-analysis-service';

function runPairSuitabilityTests(): void {
    assert.strictEqual(
        determineSuitability(0.51, 0.58, 0.55, 0.69, 0.74),
        'HIGH',
        'MU+TSM should be HIGH'
    );

    assert.strictEqual(
        determineSuitability(0.36, 0.43, 0.43, 0.75, 0.69),
        'MEDIUM',
        'MU+AVGO should be MEDIUM, not LOW'
    );

    const downsideLow = buildSuitabilityNote('LOW', {
        corr90: 0.7,
        corr180: 0.7,
        corr252: 0.7,
        corrBear2022: 0.7,
        downsideSync: 0.54,
        volGapFlag: false,
        volGapLeg: null,
        volRatio: 1.2
    });
    assert.match(downsideLow.reason, /下跌同步率/);
    assert.strictEqual(determineSuitability(0.7, 0.7, 0.7, 0.7, 0.54), 'LOW');

    const bearLow = buildSuitabilityNote('LOW', {
        corr90: 0.7,
        corr180: 0.7,
        corr252: 0.7,
        corrBear2022: 0.39,
        downsideSync: 0.7,
        volGapFlag: false,
        volGapLeg: null,
        volRatio: 1.2
    });
    assert.match(bearLow.reason, /压力情景|2022 熊市/);
    assert.strictEqual(determineSuitability(0.7, 0.7, 0.7, 0.39, 0.7), 'LOW');

    const dailyLow = buildSuitabilityNote('LOW', {
        corr90: 0.29,
        corr180: 0.28,
        corr252: 0.27,
        corrBear2022: 0.7,
        downsideSync: 0.7,
        volGapFlag: false,
        volGapLeg: null,
        volRatio: 1.2
    });
    assert.match(dailyLow.reason, /日常相关性/);
    assert.strictEqual(determineSuitability(0.29, 0.28, 0.27, 0.7, 0.7), 'LOW');

    assert.strictEqual(determineSuitability(0.50, 0.40, 0.45, 0.60, 0.70), 'HIGH');
    assert.strictEqual(determineSuitability(0.55, 0.39, 0.45, 0.60, 0.70), 'MEDIUM');

    const highVolGap = buildSuitabilityNote('HIGH', {
        corr90: 0.55,
        corr180: 0.50,
        corr252: 0.52,
        corrBear2022: 0.70,
        downsideSync: 0.75,
        volGapFlag: true,
        volGapLeg: 'MU',
        volRatio: 1.5
    });
    assert.match(highVolGap.weakness, /MU 年化波动率高出对手 50%/);
    assert.match(highVolGap.next_step, /vol\/skew/);

    const noVolGap = buildSuitabilityNote('HIGH', {
        corr90: 0.55,
        corr180: 0.50,
        corr252: 0.52,
        corrBear2022: 0.70,
        downsideSync: 0.75,
        volGapFlag: false,
        volGapLeg: null,
        volRatio: 1.2
    });
    assert.doesNotMatch(noVolGap.weakness, /波动率/);

    const joined = [noVolGap.reason, noVolGap.weakness, noVolGap.next_step].join('\n\n');
    assert.strictEqual(joined.split('\n\n').length, 3);
}

if (require.main === module) {
    runPairSuitabilityTests();
    console.log('pair-analysis-service tests passed');
}
