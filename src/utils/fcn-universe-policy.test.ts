import assert from 'node:assert/strict';
import {
    CHINA_ADR_WEAK_TREND_PENALTY,
    canAddChinaAdrExposure,
    chinaAdrTacticalPenalty
} from './fcn-universe-policy';

const healthy = { flags: [] } as any;
const bearish = {
    flags: [{ type: 'BEARISH_STRUCTURE', severity: 'WARN', message: 'below MA200' }]
} as any;

assert.equal(chinaAdrTacticalPenalty(healthy, { adr_risk: true }), 0, 'healthy ADR is not penalized by nationality');
assert.equal(chinaAdrTacticalPenalty(bearish, { adr_risk: false }), 0, 'weak non-ADR is handled by normal scoring');
assert.equal(
    chinaAdrTacticalPenalty(bearish, { adr_risk: true }),
    CHINA_ADR_WEAK_TREND_PENALTY,
    'weak-trend ADR gets a tactical presentation penalty'
);
assert.equal(canAddChinaAdrExposure({ adr_risk: true }, 0), true);
assert.equal(canAddChinaAdrExposure({ adr_risk: true }, 1), false, 'showcase permits at most one China ADR');
assert.equal(canAddChinaAdrExposure({ adr_risk: false }, 1), true);

console.log('fcn-universe-policy tests passed');
