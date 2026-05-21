import assert from 'assert';
import { buildHyOasTightZone, computeHyOasRawSeverity } from './indicators';
import { countWeekdaysInclusive } from './persistence';
import { evaluateHyOasAcceleration, scoreHyOasAcceleration } from './side-monitors';

function run() {
    assert.strictEqual(
        scoreHyOasAcceleration(-4, -36),
        0,
        '利差收窄减速不应计入 HY 加速度评分'
    );

    assert.strictEqual(
        scoreHyOasAcceleration(40, 10),
        1,
        '真实扩张加速应进入观察评分'
    );

    const measuredNoise = evaluateHyOasAcceleration(1, -33, 286);
    assert.strictEqual(
        measuredNoise.score,
        0,
        'HY OAS 286bp + Δ4w +1bp + Δ8w -33bp 在利差极低区噪音门槛下应保持 score 0'
    );
    assert.ok(measuredNoise.notes.some((note) => note.includes('未达噪音门槛 25bp')));
    assert.ok(measuredNoise.notes.some((note) => note.includes('利差极低区')));

    assert.strictEqual(
        evaluateHyOasAcceleration(30, -5, 295).score,
        1,
        '利差极低区真实扩张加速在 Δ4w 超过 25bp 后应计分'
    );

    assert.strictEqual(
        evaluateHyOasAcceleration(20, -10, 400).score,
        1,
        '正常区扩张加速在 Δ4w 超过 15bp 后应计分'
    );

    const normalNoise = evaluateHyOasAcceleration(10, -20, 400);
    assert.strictEqual(
        normalNoise.score,
        0,
        '正常区 +10bp 扩张未达 15bp 噪音门槛时应保持 score 0'
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
        '10 个交易日应确认从周一到下周五的待确认窗口'
    );

    console.log('hy-oas-framework tests passed');
}

if (require.main === module) {
    run();
}
