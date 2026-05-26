import assert from 'node:assert';
import { getEngineMode, isGatedLive, isShadowMode } from './engine-mode';

const original = process.env.FCN_ENGINE_MODE;

process.env.FCN_ENGINE_MODE = '';
assert.equal(getEngineMode(), 'gated_shadow');

process.env.FCN_ENGINE_MODE = 'gated_shadow';
assert.equal(getEngineMode(), 'gated_shadow');
assert.equal(isShadowMode(), true);
assert.equal(isGatedLive(), false);

process.env.FCN_ENGINE_MODE = 'gated_live';
assert.equal(getEngineMode(), 'gated_live');
assert.equal(isShadowMode(), false);
assert.equal(isGatedLive(), true);

process.env.FCN_ENGINE_MODE = 'nonsense';
assert.equal(getEngineMode(), 'gated_shadow');

if (original === undefined) {
    delete process.env.FCN_ENGINE_MODE;
} else {
    process.env.FCN_ENGINE_MODE = original;
}

console.log('fcn-gates engine-mode tests passed');
