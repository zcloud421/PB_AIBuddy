import assert from 'node:assert/strict';
import {
    detectGuidanceCutNews,
    detectMaterialNegativeNews,
    type NewsItem
} from './news-fetcher';

function item(title: string): NewsItem {
    return {
        title,
        source: 'fixture',
        url: 'https://example.com',
        published_at: '2026-07-27T00:00:00.000Z'
    };
}

assert.equal(
    detectGuidanceCutNews([item('Company lowers full-year revenue guidance after Q2')]),
    true
);
assert.equal(
    detectGuidanceCutNews([item('Alphabet raises 2026 capex guidance after strong cloud growth')]),
    false,
    'raising capex must not be classified as an operating guidance cut'
);
assert.equal(
    detectGuidanceCutNews([item('Alphabet stock drops after a monster Q2 report')]),
    false,
    'a post-earnings price decline is not a guidance cut'
);
assert.equal(
    detectGuidanceCutNews([item('Analyst lowers Alphabet price target after earnings')]),
    false,
    'an analyst target change is not company guidance'
);
assert.equal(
    detectMaterialNegativeNews([item('Alphabet stock drops after a monster Q2 report')]),
    false,
    'market reaction alone is not a material adverse company event'
);
assert.equal(
    detectMaterialNegativeNews([item('New AI entrant threatens Google Search dominance')]),
    false,
    'competitive commentary without a concrete adverse event must not trigger an overhang'
);
assert.equal(
    detectMaterialNegativeNews([item('DOJ opens antitrust investigation into Alphabet')]),
    true
);
assert.equal(
    detectMaterialNegativeNews([item('Draft bill would restrict stablecoin reward payments')]),
    true
);

console.log('news-fetcher classification tests passed');
