import assert from 'node:assert/strict';

import {
    getHealthTelegramEnableEnv,
    isHealthTelegramEnabled
} from './health-notification-policy';

assert.equal(isHealthTelegramEnabled('attribution', {}), false);
assert.equal(isHealthTelegramEnabled('gate_distribution', {}), false);
assert.equal(isHealthTelegramEnabled('narrative', {}), false);

assert.equal(isHealthTelegramEnabled('attribution', {
    ATTRIBUTION_HEALTH_TELEGRAM_ENABLED: ' true '
}), true);
assert.equal(isHealthTelegramEnabled('gate_distribution', {
    GATE_DISTRIBUTION_TELEGRAM_ENABLED: 'TRUE'
}), true);
assert.equal(isHealthTelegramEnabled('narrative', {
    NARRATIVE_HEALTH_TELEGRAM_ENABLED: 'false'
}), false);

assert.equal(
    getHealthTelegramEnableEnv('narrative'),
    'NARRATIVE_HEALTH_TELEGRAM_ENABLED'
);

console.log('health-notification-policy tests passed');
