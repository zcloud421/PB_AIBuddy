import assert from 'node:assert/strict';
import { hasHomepageWaitContext } from './homepage-recommendation-eligibility';

function runTests(): void {
    assert.equal(
        hasHomepageWaitContext({
            wait_reason: null,
            flags: [{ type: 'HEALTHY_PULLBACK' }]
        }),
        false,
        'ordinary GO context should remain homepage eligible'
    );

    assert.equal(
        hasHomepageWaitContext({
            wait_reason: null,
            flags: [{ type: 'EARNINGS_PROXIMITY' }]
        }),
        true,
        'earnings proximity should exclude a ticker from homepage recommendations'
    );

    assert.equal(
        hasHomepageWaitContext({
            wait_reason: null,
            flags: [{ type: 'POST_EARNINGS_SHOCK' }]
        }),
        true,
        'post-earnings shock should exclude a ticker from homepage recommendations'
    );

    assert.equal(
        hasHomepageWaitContext({
            wait_reason: 'WAIT_SETUP_RESET',
            flags: []
        }),
        true,
        'an explicit wait reason should always exclude a ticker'
    );

    assert.equal(
        hasHomepageWaitContext({}),
        false,
        'missing optional context should fail open'
    );

    console.log('homepage-recommendation-eligibility tests passed');
}

runTests();

